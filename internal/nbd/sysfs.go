package nbd

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// Device is an nbd device as sysfs shows it. Pid is the serving process the
// kernel recorded on connect, 0 when the device is not configured; Backend is
// the identifier given on connect ("" if none); Size is in bytes.
type Device struct {
	Index   int
	Pid     int
	Backend string
	Size    int64
}

// Devices lists the nbd devices under sysfs (the sysfs mount, "/sys" on a
// host) from /block/nbd*/{pid,backend,size}, ordered by index. A device whose
// pid file is absent or empty is unconfigured: Pid 0.
func Devices(sysfs string) ([]Device, error) {
	dir := filepath.Join(sysfs, "block")
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("nbd: list devices: %w", err)
	}
	var out []Device
	for _, e := range ents {
		rest, ok := strings.CutPrefix(e.Name(), "nbd")
		if !ok {
			continue
		}
		idx, err := strconv.Atoi(rest)
		if err != nil || idx < 0 || strconv.Itoa(idx) != rest {
			continue
		}
		d := Device{Index: idx}
		base := filepath.Join(dir, e.Name())
		if s, ok, err := readAttr(base, "pid"); err != nil {
			return nil, err
		} else if ok && s != "" {
			if d.Pid, err = strconv.Atoi(s); err != nil {
				return nil, fmt.Errorf("nbd: nbd%d pid %q: %w", idx, s, err)
			}
		}
		if d.Backend, _, err = readAttr(base, "backend"); err != nil {
			return nil, err
		}
		if s, ok, err := readAttr(base, "size"); err != nil {
			return nil, err
		} else if ok && s != "" {
			sectors, err := strconv.ParseInt(s, 10, 64)
			if err != nil || sectors < 0 || sectors > 1<<54 {
				return nil, fmt.Errorf("nbd: nbd%d size %q is not a sector count", idx, s)
			}
			d.Size = sectors * 512
		}
		out = append(out, d)
	}
	slices.SortFunc(out, func(a, b Device) int { return a.Index - b.Index })
	return out, nil
}

// readAttr reads one sysfs attribute, trimmed; ok is false when it is absent.
func readAttr(dir, name string) (string, bool, error) {
	b, err := os.ReadFile(filepath.Join(dir, name)) //nolint:gosec // a sysfs path we built
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("nbd: read %s: %w", filepath.Join(filepath.Base(dir), name), err)
	}
	return strings.TrimSpace(string(b)), true, nil
}

// Devicer is the kernel's nbd as the reconciler uses it; tests use a fake.
type Devicer interface {
	Devices() ([]Device, error)
	Available() error
	Connect(context.Context, *os.File, ConnectOptions) (int, error)
	Reconfigure(context.Context, int, int64) error
	Disconnect(context.Context, int) error
}

// Real is the kernel, with devices observed under sysfs ("/sys").
func Real(sysfs string) Devicer { return kernel{sysfs: sysfs} }

type kernel struct{ sysfs string }

func (k kernel) Devices() ([]Device, error) { return Devices(k.sysfs) }
func (kernel) Available() error             { return Available() }
func (kernel) Connect(ctx context.Context, sock *os.File, o ConnectOptions) (int, error) {
	return Connect(ctx, sock, o)
}
func (k kernel) Reconfigure(ctx context.Context, index int, size int64) error {
	return reconfigure(ctx, k.sysfs, index, size)
}
func (kernel) Disconnect(ctx context.Context, index int) error { return Disconnect(ctx, index) }
