// Package prereq probes what the volume kind needs from the node it runs on:
// the kernel's nbd, the device-mapper control node, the Adiantum cipher,
// systemd's private socket and a writable cache directory. The probe is cheap,
// bounded and read-only in effect, so the reconciler can run it on every pass;
// its report is layer 1 of the volume item's converge (a missing feature
// refuses the volume with the prerequisite-missing condition) and the source
// of the miami_node_capability gauges.
package prereq

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/greghaskins/csi-miami/internal/deadline"
	"github.com/greghaskins/csi-miami/internal/nbd"
)

// The feature names: the label values of miami_node_capability and the entries
// of Report.Missing. They name no path and no address.
const (
	FeatureNBD           = "nbd"
	FeatureDeviceMapper  = "device-mapper"
	FeatureAdiantum      = "adiantum"
	FeatureSystemdSocket = "systemd-socket"
	FeatureCacheDir      = "cache-dir"
)

// Features are all the feature names, in the order a Report lists them.
var Features = []string{FeatureNBD, FeatureDeviceMapper, FeatureAdiantum, FeatureSystemdSocket, FeatureCacheDir}

// Defaults for the zero fields of Options.
const (
	DefaultSysfs         = "/sys"
	DefaultSystemdSocket = "/run/systemd/private"
	DefaultTimeout       = 2 * time.Second

	defaultDeviceMapperControl = "/dev/mapper/control"
	defaultProcCrypto          = "/proc/crypto"
)

// Options say where to look. Only CacheDir has no default.
type Options struct {
	// Sysfs is the sysfs mount. Empty means DefaultSysfs.
	Sysfs string
	// SystemdSocket is systemd's private socket. Empty means
	// DefaultSystemdSocket.
	SystemdSocket string
	// CacheDir is the directory the volume caches live under; it is made if
	// absent and must be writable. Required.
	CacheDir string

	// NBD is the kernel's nbd. Nil means nbd.Real(Sysfs). Tests replace it.
	NBD nbd.Devicer
	// DeviceMapperControl and ProcCrypto default to /dev/mapper/control and
	// /proc/crypto. Tests point them at fixtures.
	DeviceMapperControl string
	ProcCrypto          string
	// Timeout bounds each check. Zero means DefaultTimeout.
	Timeout time.Duration
}

// Feature is one checked feature. Err is nil when it is present; otherwise it
// says why not. Err can name a path: it is for the log, never for a metric.
type Feature struct {
	Name string
	Err  error
}

// Report is the result of one Probe: one entry per feature, in Features order.
type Report struct {
	Features []Feature
}

// Missing returns the names of the features that are absent, in order. Empty
// when everything is present.
func (r Report) Missing() []string {
	var out []string
	for _, f := range r.Features {
		if f.Err != nil {
			out = append(out, f.Name)
		}
	}
	return out
}

// Present reports whether the named feature was found.
func (r Report) Present(name string) bool {
	for _, f := range r.Features {
		if f.Name == name {
			return f.Err == nil
		}
	}
	return false
}

// Err returns nil when nothing is missing, otherwise one error naming every
// missing feature and why.
func (r Report) Err() error {
	var errs []error
	for _, f := range r.Features {
		if f.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", f.Name, f.Err))
		}
	}
	return errors.Join(errs...)
}

// inflight keeps a check on a hung file system from piling up goroutines: a
// check that has not returned is not started again (deadline.Run says so).
var inflight deadline.Set

// Probe checks every feature. It never blocks for longer than
// Options.Timeout per check, whatever the state of the sockets and file
// systems it looks at.
func Probe(ctx context.Context, o Options) Report {
	if o.Sysfs == "" {
		o.Sysfs = DefaultSysfs
	}
	if o.SystemdSocket == "" {
		o.SystemdSocket = DefaultSystemdSocket
	}
	if o.NBD == nil {
		o.NBD = nbd.Real(o.Sysfs)
	}
	if o.DeviceMapperControl == "" {
		o.DeviceMapperControl = defaultDeviceMapperControl
	}
	if o.ProcCrypto == "" {
		o.ProcCrypto = defaultProcCrypto
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	checks := []struct {
		name string
		fn   func(context.Context) error
	}{
		{FeatureNBD, func(context.Context) error { return checkNBD(o) }},
		{FeatureDeviceMapper, func(context.Context) error { return checkExists(o.DeviceMapperControl) }},
		{FeatureAdiantum, func(context.Context) error { return checkAdiantum(o.ProcCrypto) }},
		{FeatureSystemdSocket, func(c context.Context) error { return checkSocket(c, o.SystemdSocket, o.Timeout) }},
		{FeatureCacheDir, func(context.Context) error { return checkWritable(o.CacheDir) }},
	}
	rep := Report{Features: make([]Feature, 0, len(checks))}
	for _, c := range checks {
		_, err := deadline.Run(ctx, o.Timeout, "probing "+c.name, &inflight, c.name+"\x00"+o.CacheDir, func(cx context.Context) (struct{}, error) {
			return struct{}{}, c.fn(cx)
		})
		rep.Features = append(rep.Features, Feature{Name: c.name, Err: err})
	}
	return rep
}

// checkNBD wants the kernel's nbd family to resolve and its devices to be
// listable under sysfs.
func checkNBD(o Options) error {
	if err := o.NBD.Available(); err != nil {
		return err
	}
	if _, err := o.NBD.Devices(); err != nil {
		return err
	}
	return nil
}

func checkExists(path string) error {
	if _, err := os.Stat(path); err != nil {
		return err
	}
	return nil
}

// checkAdiantum looks for the cipher in /proc/crypto.
func checkAdiantum(path string) error {
	f, err := os.Open(path) //nolint:gosec // the kernel's crypto list, or a test fixture
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if strings.Contains(sc.Text(), "adiantum") {
			return nil
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return errors.New("the cipher adiantum is not listed in " + path)
}

// checkSocket dials the socket and hangs up; the D-Bus handshake is the
// systemd package's business and costs more than a pass should.
func checkSocket(ctx context.Context, socket string, timeout time.Duration) error {
	d := net.Dialer{Timeout: timeout}
	c, err := d.DialContext(ctx, "unix", socket)
	if err != nil {
		return err
	}
	return c.Close()
}

// checkWritable makes the directory if it is absent and writes and removes a
// file in it.
func checkWritable(dir string) error {
	if dir == "" {
		return errors.New("no cache directory is configured")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".probe-*")
	if err != nil {
		return err
	}
	name := f.Name()
	_, werr := f.Write([]byte{0})
	cerr := f.Close()
	rerr := os.Remove(filepath.Clean(name))
	return errors.Join(werr, cerr, rerr)
}
