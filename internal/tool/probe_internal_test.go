package tool

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// The probe opens the device read-only, not exclusively, with O_DIRECT (through
// the open seam: a test has no block device, and tmpfs refuses O_DIRECT, so the
// seam records the flags and opens without it), reads the sector at the offset,
// and says whether it is zeros.
func TestProbeReadsOneSectorDirectly(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "dev")
	data := make([]byte, 4*ProbeSize)
	data[2*ProbeSize+10] = 1 // the third sector is not blank
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	var flags []int
	h := host{open: func(name string, flag int, perm os.FileMode) (*os.File, error) {
		flags = append(flags, flag)
		return os.OpenFile(name, flag&^syscall.O_DIRECT, perm) //nolint:gosec // a test path
	}}
	for off, blank := range map[int64]bool{0: true, ProbeSize: true, 2 * ProbeSize: false, 3 * ProbeSize: true} {
		d, err := h.readSector(path, off)
		if err != nil || d.Blank != blank || d.Size != int64(len(data)) {
			t.Errorf("offset %d: %+v, %v, want blank %v and size %d", off, d, err, blank, len(data))
		}
	}
	for _, f := range flags {
		if f&syscall.O_DIRECT == 0 || f&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_EXCL) != 0 {
			t.Errorf("opened with flags %#x: want read-only, not exclusive, O_DIRECT", f)
		}
	}
	if len(flags) != 4 {
		t.Errorf("%d opens, want 4", len(flags))
	}
	if _, err := h.readSector(path, 4*ProbeSize); err == nil {
		t.Error("a read past the end must be an error")
	}
}

func TestProbeRefusesAnUnalignedOffsetAndANonBlockDevice(t *testing.T) {
	t.Parallel()
	if _, err := (host{}).ProbeDevice("/dev/null", 512); err == nil {
		t.Error("an unaligned offset was accepted")
	}
	if _, err := (host{}).ProbeDevice("/dev/null", 0); err == nil {
		t.Error("a character device was accepted")
	}
}
