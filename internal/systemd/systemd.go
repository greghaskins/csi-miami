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
	"io"
	"net"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
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
var dialTimeout = 10 * time.Second

// D-Bus error names of the two conditions this package treats as outcomes.
const (
	errNoSuchUnit = "org.freedesktop.systemd1.NoSuchUnit"
	errUnitExists = "org.freedesktop.systemd1.UnitExists"
)

// Unit is a transient service to start. ExecStart is the argv, first element
// the absolute path of the binary. Environment is KEY=value entries; it must
// never carry a key, which would be visible to every process on the host.
//
// Restart, when set, is systemd's Restart= policy ("on-failure", "always"),
// and RestartDelay the delay before a restart; zero leaves systemd's default.
// A unit with a restart policy that exits is not collected while it waits to
// restart: State reports it as "activating", sub-state "auto-restart".
type Unit struct {
	Name, Description string
	ExecStart         []string
	Environment       []string
	Restart           string
	RestartDelay      time.Duration

	// Sandbox, when set, confines the service (see Sandbox).
	Sandbox *Sandbox
}

// Sandbox is the confinement of a unit whose process works on files an
// untrusted writer can influence (the shared store) while running as root: a
// flaw in how it handles them is then bounded by what the unit can reach.
// systemd applies it with a private mount namespace:
//
//	NoNewPrivileges=yes   nothing it runs gains privileges;
//	ProtectSystem=strict  the whole file system is read-only except /dev,
//	                      /proc and /sys, and ReadWritePaths;
//	ProtectHome=yes       /home, /root and /run/user are empty;
//	CapabilityBoundingSet only Capabilities can ever be held.
type Sandbox struct {
	// ReadWritePaths are the existing paths the service may write below. There
	// is no PrivateTmp: a cache or store directory may be under /tmp (the smoke
	// tests put them there), which a private /tmp would hide, and the read-only
	// file system already stops writes anywhere else.
	ReadWritePaths []string
	// Capabilities are the capability numbers (unix.CAP_*) it may hold.
	Capabilities []int
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
	// it. A unit systemd does not know is not an error. Because units are
	// created with CollectMode=inactive-or-failed, a unit that crashed has
	// already been collected, so clearing its failed state is almost always
	// a no-op.
	Stop(ctx context.Context, name string) error
	// State reports the unit's ActiveState, SubState and MainPID. Because
	// units are created with CollectMode=inactive-or-failed, systemd forgets
	// a crashed unit at once: it reports Present=false, never "failed". A
	// caller must treat a unit that is absent while its volume is staged as
	// a crash. A unit started with a restart policy is the exception: while it
	// waits to restart it is Present with Active "activating" and Sub
	// "auto-restart", and only a unit that gave up (the start limit) is
	// collected.
	State(ctx context.Context, name string) (State, error)
}

// ErrExecStartDiffers is wrapped by the error Start returns when a unit of
// that name is active with a different ExecStart: the binary changed under a
// running unit. The unit is left as it is.
var ErrExecStartDiffers = errors.New("systemd: the unit is running a different ExecStart")

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
	GetServiceProperties(ctx context.Context, unit string) (map[string]any, error)
	GetServiceExecStart(ctx context.Context, unit string) ([][]string, error)
	Close()
}

// realBus adapts *sdbus.Conn.
type realBus struct{ *sdbus.Conn }

// GetServiceProperties returns the properties of the unit's Service
// interface; MainPID lives there, not on the Unit interface.
func (b realBus) GetServiceProperties(ctx context.Context, unit string) (map[string]any, error) {
	return b.GetUnitTypePropertiesContext(ctx, unit, "Service")
}

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

// manager implements Manager over a bus that re-dials (see redialBus).
type manager struct{ bus bus }

// New connects to systemd's private socket (DefaultSocket when socket is
// empty). It returns an error wrapping ErrNoSystemd when the dial fails.
//
// The connection is re-dialled once, transparently, when a call finds it
// closed (a daemon-reexec or a restart of PID 1 drops every client of the
// private socket). The returned Manager also implements io.Closer; a caller
// that owns it for a bounded time type-asserts and closes it.
func New(socket string) (Manager, error) {
	if socket == "" {
		socket = DefaultSocket
	}
	r := &redialBus{dial: func() (bus, error) {
		c, err := sdbus.NewConnection(func() (*godbus.Conn, error) { return dial(socket) })
		if err != nil {
			return nil, fmt.Errorf("%w at %s: %v", ErrNoSystemd, socket, err)
		}
		return realBus{c}, nil
	}}
	if _, err := r.current(); err != nil {
		return nil, err
	}
	return &manager{bus: r}, nil
}

// Close closes the connection. Later calls fail; they do not re-dial.
func (m *manager) Close() error {
	m.bus.Close()
	return nil
}

// redialBus is a bus over one connection at a time. A call that fails because
// the connection is closed or its socket is gone replaces the connection with
// a fresh one and is retried once; a second failure is returned.
type redialBus struct {
	dial   func() (bus, error)
	mu     sync.Mutex
	cur    bus
	closed bool
}

var errManagerClosed = errors.New("systemd: manager closed")

// current returns the live connection, dialling when there is none.
func (r *redialBus) current() (bus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errManagerClosed
	}
	if r.cur == nil {
		b, err := r.dial()
		if err != nil {
			return nil, err
		}
		r.cur = b
	}
	return r.cur, nil
}

// drop forgets and closes failed, unless another call already replaced it.
func (r *redialBus) drop(failed bus) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cur == failed {
		r.cur = nil
		failed.Close()
	}
}

// Close closes the connection and refuses further dials.
func (r *redialBus) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	if r.cur != nil {
		r.cur.Close()
		r.cur = nil
	}
}

// connLost reports whether err means the connection itself is gone, as
// opposed to systemd having answered with an error.
func connLost(err error) bool {
	return errors.Is(err, godbus.ErrClosed) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET)
}

// redo runs f on the live connection and, if the connection was lost, once
// more on a fresh one. A context that is done is not retried.
func redo[T any](ctx context.Context, r *redialBus, f func(bus) (T, error)) (T, error) {
	b, err := r.current()
	if err != nil {
		var zero T
		return zero, err
	}
	v, err := f(b)
	if err == nil || !connLost(err) || ctx.Err() != nil {
		return v, err
	}
	r.drop(b)
	if b, err = r.current(); err != nil {
		var zero T
		return zero, err
	}
	return f(b)
}

func (r *redialBus) StartTransientUnitContext(ctx context.Context, name, mode string, props []sdbus.Property, ch chan<- string) (int, error) {
	return redo(ctx, r, func(b bus) (int, error) { return b.StartTransientUnitContext(ctx, name, mode, props, ch) })
}

func (r *redialBus) StopUnitContext(ctx context.Context, name, mode string, ch chan<- string) (int, error) {
	return redo(ctx, r, func(b bus) (int, error) { return b.StopUnitContext(ctx, name, mode, ch) })
}

func (r *redialBus) ResetFailedUnitContext(ctx context.Context, name string) error {
	_, err := redo(ctx, r, func(b bus) (struct{}, error) { return struct{}{}, b.ResetFailedUnitContext(ctx, name) })
	return err
}

func (r *redialBus) GetUnitPropertiesContext(ctx context.Context, unit string) (map[string]any, error) {
	return redo(ctx, r, func(b bus) (map[string]any, error) { return b.GetUnitPropertiesContext(ctx, unit) })
}

func (r *redialBus) GetServiceProperties(ctx context.Context, unit string) (map[string]any, error) {
	return redo(ctx, r, func(b bus) (map[string]any, error) { return b.GetServiceProperties(ctx, unit) })
}

func (r *redialBus) GetServiceExecStart(ctx context.Context, unit string) ([][]string, error) {
	return redo(ctx, r, func(b bus) ([][]string, error) { return b.GetServiceExecStart(ctx, unit) })
}

// dial opens one authenticated connection to the private socket. The
// connection lives as long as the manager, so no context bounds it; the
// dial and the handshake share one deadline on the socket itself, which is
// cleared once the handshake is done, so a wedged PID 1 costs dialTimeout and
// leaks nothing.
func dial(socket string) (*godbus.Conn, error) {
	nc, err := net.DialTimeout("unix", socket, dialTimeout)
	if err != nil {
		return nil, err
	}
	if err := nc.SetDeadline(time.Now().Add(dialTimeout)); err != nil {
		_ = nc.Close()
		return nil, err
	}
	c, err := godbus.NewConn(nc)
	if err != nil {
		_ = nc.Close()
		return nil, err
	}
	// EXTERNAL with the numeric uid: no user lookup. No Hello: PID 1 serves
	// this socket directly, with no bus daemon behind it.
	if err := c.Auth([]godbus.Auth{godbus.AuthExternal(strconv.Itoa(os.Getuid()))}); err != nil {
		_ = c.Close()
		return nil, err
	}
	if err := nc.SetDeadline(time.Time{}); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

// properties are the transient unit's properties. The bool of PropExecStart
// is systemd's ignore-failure flag: false means a non-zero exit counts as a
// failure.
func properties(u Unit) []sdbus.Property {
	props := []sdbus.Property{
		sdbus.PropType("simple"),
		sdbus.PropExecStart(u.ExecStart, false),
		{Name: "Environment", Value: godbus.MakeVariant(append([]string(nil), u.Environment...))},
		sdbus.PropDescription(u.Description),
		{Name: "CollectMode", Value: godbus.MakeVariant("inactive-or-failed")},
		{Name: "KillMode", Value: godbus.MakeVariant("control-group")},
		{Name: "StandardOutput", Value: godbus.MakeVariant("journal")},
		{Name: "StandardError", Value: godbus.MakeVariant("journal")},
	}
	if u.Restart != "" {
		props = append(props, sdbus.Property{Name: "Restart", Value: godbus.MakeVariant(u.Restart)})
	}
	if u.Sandbox != nil {
		props = append(props, sandboxProperties(*u.Sandbox)...)
	}
	if u.RestartDelay > 0 {
		props = append(props, sdbus.Property{Name: "RestartUSec", Value: godbus.MakeVariant(uint64(u.RestartDelay / time.Microsecond))})
	}
	return props
}

// sandboxProperties are the D-Bus properties of a Sandbox. The capability
// bounding set is a bit mask (CapabilityBoundingSet is a uint64 on the bus).
func sandboxProperties(sb Sandbox) []sdbus.Property {
	var mask uint64
	for _, c := range sb.Capabilities {
		mask |= 1 << uint(c) //nolint:gosec // capability numbers are small constants
	}
	return []sdbus.Property{
		{Name: "NoNewPrivileges", Value: godbus.MakeVariant(true)},
		{Name: "ProtectSystem", Value: godbus.MakeVariant("strict")},
		{Name: "ProtectHome", Value: godbus.MakeVariant("yes")},
		{Name: "ReadWritePaths", Value: godbus.MakeVariant(append([]string(nil), sb.ReadWritePaths...))},
		{Name: "CapabilityBoundingSet", Value: godbus.MakeVariant(mask)},
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
		return fmt.Errorf("systemd: unit %s already exists with a different ExecStart: %w", u.Name, ErrExecStartDiffers)
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
	svc, err := m.bus.GetServiceProperties(ctx, name)
	if err != nil {
		if isDBusError(err, errNoSuchUnit) {
			return State{}, nil
		}
		return State{}, fmt.Errorf("systemd: service properties of %s: %w", name, err)
	}
	var pid int
	if v, ok := svc["MainPID"].(uint32); ok {
		pid = int(v)
	}
	return State{Active: active, Sub: sub, MainPID: pid, Present: true}, nil
}
