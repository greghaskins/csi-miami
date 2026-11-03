// Package systemd starts, observes and stops the transient services that
// outlive the plugin pod.
//
// The block server of a staged volume runs as a transient systemd service,
// started over systemd's private socket, so that its lifetime is the
// volume's and not the plugin pod's (design decision 41): restarting the
// plugin must not tear down a mounted volume. The private socket is a plain
// unix socket served by PID 1 itself; there is no bus daemon behind it, so
// the connection authenticates with EXTERNAL and sends no Hello.
//
// Every call is bounded by the context it is given: a wedged systemd costs
// the caller its deadline and nothing else.
package systemd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	sdbus "github.com/coreos/go-systemd/v22/dbus"
	godbus "github.com/godbus/dbus/v5"

	"github.com/greghaskins/csi-miami/internal/volume"
)

// DefaultSocket is where systemd serves its private API.
const DefaultSocket = "/run/systemd/private"

// ErrNoSystemd is returned by New when the private socket cannot be dialled:
// the host runs no systemd, or the socket is not mounted into this
// container.
var ErrNoSystemd = errors.New("systemd private socket unreachable")

// unitPrefix starts every unit name this package manages.
const unitPrefix = "miami-blockd-"

// unitNameShape is what Start, Stop and State accept: the prefix, a label of
// lower-case letters, digits and dashes, and the service suffix. It admits
// no path separator, no space and no dot but the suffix's.
var unitNameShape = regexp.MustCompile(`^` + unitPrefix + `[a-z0-9][a-z0-9-]*\.service$`)

// dialTimeout bounds the dial in New; the socket is local, so it is long.
const dialTimeout = 10 * time.Second

// D-Bus error names of the two conditions this package treats as outcomes.
const (
	errNoSuchUnit = "org.freedesktop.systemd1.NoSuchUnit"
	errUnitExists = "org.freedesktop.systemd1.UnitExists"
)

// Unit is a transient service to start. ExecStart is the argv, first element
// the absolute path of the binary. Environment is KEY=value entries; it must
// never carry a key, which would be visible to every process on the host.
type Unit struct {
	Name, Description string
	ExecStart         []string
	Environment       []string
}

// State is what systemd reports about a unit. Present is false for a unit
// systemd does not know, and the other fields are then zero.
type State struct {
	Active, Sub string
	MainPID     int
	Present     bool
}

// Manager runs transient services.
type Manager interface {
	// Start creates and starts the transient service u. A unit of that name
	// that is already active with an identical ExecStart is not an error;
	// any other existing unit is, and the error names it.
	Start(ctx context.Context, u Unit) error
	// Stop stops the unit and clears its failed state, so systemd forgets
	// it. A unit systemd does not know is not an error.
	Stop(ctx context.Context, name string) error
	// State reports the unit's ActiveState, SubState and MainPID.
	State(ctx context.Context, name string) (State, error)
}

// UnitName is the unit that runs the block server of a volume,
// miami-blockd-<volume>.service. It returns "" for anything but a pvc-<uuid>
// name, so a unit name can never carry a path or a space, and the Manager
// refuses "".
func UnitName(vol string) string {
	if !volume.IsPVName(vol) {
		return ""
	}
	return unitPrefix + vol + ".service"
}

// bus is the part of the go-systemd connection the manager uses, so that the
// manager's decisions can be tested without a systemd.
type bus interface {
	StartTransientUnitContext(ctx context.Context, name, mode string, props []sdbus.Property, ch chan<- string) (int, error)
	StopUnitContext(ctx context.Context, name, mode string, ch chan<- string) (int, error)
	ResetFailedUnitContext(ctx context.Context, name string) error
	GetUnitPropertiesContext(ctx context.Context, unit string) (map[string]any, error)
	GetServiceExecStart(ctx context.Context, unit string) ([][]string, error)
	Close()
}

// realBus adapts *sdbus.Conn.
type realBus struct{ *sdbus.Conn }

// GetServiceExecStart returns the argv of each ExecStart entry of a service.
func (b realBus) GetServiceExecStart(ctx context.Context, unit string) ([][]string, error) {
	p, err := b.GetUnitTypePropertyContext(ctx, unit, "Service", "ExecStart")
	if err != nil {
		return nil, err
	}
	// a(sasbttttuii): path, argv, ignore-failure, then timestamps, pid, code
	// and status.
	var entries []struct {
		Path        string
		Args        []string
		Ignore      bool
		StartTime   uint64
		StartMono   uint64
		ExitTime    uint64
		ExitMono    uint64
		PID         uint32
		Code, State int32
	}
	if err := p.Value.Store(&entries); err != nil {
		return nil, fmt.Errorf("decode ExecStart of %s: %w", unit, err)
	}
	argvs := make([][]string, len(entries))
	for i, e := range entries {
		argvs[i] = e.Args
	}
	return argvs, nil
}

type manager struct{ bus bus }

// New connects to systemd's private socket (DefaultSocket when socket is
// empty). It returns an error wrapping ErrNoSystemd when the dial fails.
func New(socket string) (Manager, error) {
	if socket == "" {
		socket = DefaultSocket
	}
	c, err := sdbus.NewConnection(func() (*godbus.Conn, error) { return dial(socket) })
	if err != nil {
		return nil, fmt.Errorf("%w at %s: %v", ErrNoSystemd, socket, err)
	}
	return &manager{bus: realBus{c}}, nil
}

// dial opens one authenticated connection to the private socket. The
// connection lives as long as the manager, so the context that bounds the
// dial is not the connection's; the dial of a local socket does not hang, and
// the handshake gets a deadline from the socket itself.
func dial(socket string) (*godbus.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	type result struct {
		c   *godbus.Conn
		err error
	}
	done := make(chan result, 1)
	go func() {
		c, err := godbus.Dial("unix:path=" + socket)
		if err != nil {
			done <- result{nil, err}
			return
		}
		// EXTERNAL with the numeric uid: no user lookup. No Hello: PID 1
		// serves this socket directly, with no bus daemon behind it.
		if err := c.Auth([]godbus.Auth{godbus.AuthExternal(strconv.Itoa(os.Getuid()))}); err != nil {
			_ = c.Close()
			done <- result{nil, err}
			return
		}
		done <- result{c, nil}
	}()
	select {
	case r := <-done:
		return r.c, r.err
	case <-ctx.Done():
		// The dial is abandoned, never joined; if it finishes later, the
		// goroutine below closes what it produced.
		go func() {
			if r := <-done; r.c != nil {
				_ = r.c.Close()
			}
		}()
		return nil, fmt.Errorf("dial %s: %w", socket, ctx.Err())
	}
}

// properties are the transient unit's properties. ExecStart is not "unclean
// is failure": a block server that is killed by the kernel's OOM handling is
// restarted by the reconciler, which reads State.
func properties(u Unit) []sdbus.Property {
	return []sdbus.Property{
		sdbus.PropType("simple"),
		sdbus.PropExecStart(u.ExecStart, false),
		{Name: "Environment", Value: godbus.MakeVariant(append([]string(nil), u.Environment...))},
		sdbus.PropDescription(u.Description),
		{Name: "CollectMode", Value: godbus.MakeVariant("inactive-or-failed")},
		{Name: "KillMode", Value: godbus.MakeVariant("control-group")},
		{Name: "StandardOutput", Value: godbus.MakeVariant("journal")},
		{Name: "StandardError", Value: godbus.MakeVariant("journal")},
	}
}

func checkName(name string) error {
	if !unitNameShape.MatchString(name) {
		return fmt.Errorf("systemd: refusing unit name %q", name)
	}
	return nil
}

func isDBusError(err error, name string) bool {
	var v godbus.Error
	if errors.As(err, &v) {
		return v.Name == name
	}
	var p *godbus.Error
	if errors.As(err, &p) && p != nil {
		return p.Name == name
	}
	return false
}

// waitJob waits for the result of a job systemd queued.
func waitJob(ctx context.Context, ch <-chan string, what, name string) error {
	select {
	case r := <-ch:
		if r != "done" {
			return fmt.Errorf("systemd: %s of %s: job %s", what, name, r)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("systemd: %s of %s: %w", what, name, ctx.Err())
	}
}

// Start implements Manager.
func (m *manager) Start(ctx context.Context, u Unit) error {
	if err := checkName(u.Name); err != nil {
		return err
	}
	if len(u.ExecStart) == 0 || !strings.HasPrefix(u.ExecStart[0], "/") {
		return fmt.Errorf("systemd: unit %s: ExecStart must start with an absolute path", u.Name)
	}
	ch := make(chan string, 1)
	_, err := m.bus.StartTransientUnitContext(ctx, u.Name, "replace", properties(u), ch)
	if err != nil {
		if isDBusError(err, errUnitExists) {
			return m.adopt(ctx, u)
		}
		return fmt.Errorf("systemd: start %s: %w", u.Name, err)
	}
	return waitJob(ctx, ch, "start", u.Name)
}

// adopt accepts a unit that already exists when it is the one Start would
// have made: active, with the same ExecStart. Anything else is for the
// reconciler to decide.
func (m *manager) adopt(ctx context.Context, u Unit) error {
	st, err := m.State(ctx, u.Name)
	if err != nil {
		return err
	}
	if !st.Present || st.Active != "active" {
		return fmt.Errorf("systemd: unit %s already exists and is %q", u.Name, st.Active)
	}
	argvs, err := m.bus.GetServiceExecStart(ctx, u.Name)
	if err != nil {
		return fmt.Errorf("systemd: unit %s already exists: %w", u.Name, err)
	}
	if len(argvs) != 1 || !reflect.DeepEqual(argvs[0], u.ExecStart) {
		return fmt.Errorf("systemd: unit %s already exists with a different ExecStart", u.Name)
	}
	return nil
}

// Stop implements Manager.
func (m *manager) Stop(ctx context.Context, name string) error {
	if err := checkName(name); err != nil {
		return err
	}
	ch := make(chan string, 1)
	if _, err := m.bus.StopUnitContext(ctx, name, "replace", ch); err != nil {
		if !isDBusError(err, errNoSuchUnit) {
			return fmt.Errorf("systemd: stop %s: %w", name, err)
		}
	} else if err := waitJob(ctx, ch, "stop", name); err != nil {
		return err
	}
	if err := m.bus.ResetFailedUnitContext(ctx, name); err != nil && !isDBusError(err, errNoSuchUnit) {
		return fmt.Errorf("systemd: reset failed state of %s: %w", name, err)
	}
	return nil
}

// State implements Manager.
func (m *manager) State(ctx context.Context, name string) (State, error) {
	if err := checkName(name); err != nil {
		return State{}, err
	}
	props, err := m.bus.GetUnitPropertiesContext(ctx, name)
	if err != nil {
		if isDBusError(err, errNoSuchUnit) {
			return State{}, nil
		}
		return State{}, fmt.Errorf("systemd: properties of %s: %w", name, err)
	}
	if load, _ := props["LoadState"].(string); load == "not-found" {
		return State{}, nil
	}
	active, ok := props["ActiveState"].(string)
	if !ok {
		return State{}, fmt.Errorf("systemd: properties of %s carry no ActiveState", name)
	}
	sub, _ := props["SubState"].(string)
	var pid int
	if v, ok := props["MainPID"].(uint32); ok {
		pid = int(v)
	}
	return State{Active: active, Sub: sub, MainPID: pid, Present: true}, nil
}
