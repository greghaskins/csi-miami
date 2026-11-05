package ext4_test

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"slices"
	"testing"

	"github.com/greghaskins/csi-miami/internal/ext4"
	"github.com/greghaskins/csi-miami/internal/tool"
	"github.com/greghaskins/csi-miami/internal/tool/fake"
)

var ctx = context.Background()

func TestMkfsArgv(t *testing.T) {
	t.Parallel()
	f := &fake.Runner{}
	if err := ext4.Mkfs(ctx, f, "/dev/mapper/x"); err != nil {
		t.Fatal(err)
	}
	want := []string{"mkfs.ext4", "-q", "-F", "-E", "lazy_itable_init=0,lazy_journal_init=0", "/dev/mapper/x"}
	if got := f.Calls()[0].Argv; !slices.Equal(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
	bad := &fake.Runner{Replies: []fake.Reply{{Code: 1, Stderr: "no space"}}}
	if err := ext4.Mkfs(ctx, bad, "/dev/x"); err == nil {
		t.Fatal("non-zero mkfs must fail")
	}
}

func TestResizeArgv(t *testing.T) {
	t.Parallel()
	f := &fake.Runner{}
	if err := ext4.Resize(ctx, f, "/dev/mapper/x"); err != nil {
		t.Fatal(err)
	}
	if got := f.Calls()[0].Argv; !slices.Equal(got, []string{"resize2fs", "/dev/mapper/x"}) {
		t.Fatalf("argv = %v", got)
	}
}

func TestFsckCodes(t *testing.T) {
	t.Parallel()
	for code, ok := range map[int]bool{0: true, 1: true, 2: true, 3: true, 4: false, 8: false, 12: false} {
		f := &fake.Runner{Replies: []fake.Reply{{Code: code}}}
		err := ext4.Fsck(ctx, f, "/dev/x")
		if ok && err != nil {
			t.Errorf("code %d: %v", code, err)
		}
		if !ok && !errors.Is(err, ext4.ErrFsck) {
			t.Errorf("code %d: err = %v, want ErrFsck", code, err)
		}
		if !slices.Equal(f.Calls()[0].Argv, []string{"e2fsck", "-p", "/dev/x"}) {
			t.Errorf("argv = %v", f.Calls()[0].Argv)
		}
	}
	f := &fake.Runner{Replies: []fake.Reply{{Err: errors.New("start")}}}
	if err := ext4.Fsck(ctx, f, "/dev/x"); err == nil || errors.Is(err, ext4.ErrFsck) {
		t.Fatalf("runner error = %v", err)
	}
}

func TestFreezeBadFd(t *testing.T) {
	t.Parallel()
	if err := ext4.Freeze(-1); err == nil {
		t.Fatal("Freeze(-1) must fail")
	}
	if err := ext4.Thaw(-1); err == nil {
		t.Fatal("Thaw(-1) must fail")
	}
}

func TestHasFilesystem(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		reply   fake.Reply
		want    bool
		wantErr bool
	}{
		"ext4":      {reply: fake.Reply{Stdout: "ext4\n"}, want: true},
		"blank":     {reply: fake.Reply{Code: 2}},
		"no output": {reply: fake.Reply{}},
		"failure":   {reply: fake.Reply{Code: 4, Stderr: "boom"}, wantErr: true},
		"no start":  {reply: fake.Reply{Err: errors.New("start")}, wantErr: true},
	} {
		f := &fake.Runner{Replies: []fake.Reply{tc.reply}}
		got, err := ext4.HasFilesystem(ctx, f, "/dev/x", ext4.Area{Dev: "/dev/x"})
		if got != tc.want || (err != nil) != tc.wantErr {
			t.Errorf("%s: got %v, %v", name, got, err)
		}
		if want := []string{"blkid", "-p", "-o", "value", "-s", "TYPE", "/dev/x"}; !slices.Equal(f.Calls()[0].Argv, want) {
			t.Errorf("%s: argv = %v", name, f.Calls()[0].Argv)
		}
	}
}

// blkid exits 2 for a device that is not there as for a blank one, so the
// device is looked at first and a device that cannot be looked at is an error,
// with blkid never run.
func TestHasFilesystemNeedsTheDevice(t *testing.T) {
	t.Parallel()
	for name, probeErr := range map[string]error{
		"missing":    fs.ErrNotExist,
		"unreadable": fs.ErrPermission,
		"io error":   errors.New("input/output error"),
	} {
		f := &fake.Runner{
			Replies: []fake.Reply{{Code: 2}},
			Device:  func(string, int64) (tool.Device, error) { return tool.Device{}, probeErr },
		}
		got, err := ext4.HasFilesystem(ctx, f, "/dev/x", ext4.Area{Dev: "/dev/x"})
		if err == nil || got || !errors.Is(err, probeErr) {
			t.Errorf("%s: got %v, %v, want an error naming the device's problem", name, got, err)
		}
		if n := len(f.Calls()); n != 0 {
			t.Errorf("%s: %d tools ran, want none: %v", name, n, f.Calls())
		}
	}
}

// The real probe: a path with no node, and things that are not block devices,
// are errors and blkid does not run.
func TestHasFilesystemRealProbeRefusesWhatIsNotABlockDevice(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	regular := dir + "/file"
	if err := os.WriteFile(regular, make([]byte, 8192), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, dev := range map[string]string{"missing": dir + "/nonesuch", "a file": regular, "a directory": dir, "a character device": "/dev/zero"} {
		got, err := ext4.HasFilesystem(ctx, tool.Real(), dev, ext4.Area{Dev: dev})
		if err == nil || got {
			t.Errorf("%s: got %v, %v, want an error", name, got, err)
		}
	}
}

func TestHasFilesystemBlankNeedsZeros(t *testing.T) {
	t.Parallel()
	blank := &fake.Runner{Replies: []fake.Reply{{Code: 2}}, Device: func(string, int64) (tool.Device, error) { return tool.Device{Blank: true}, nil }}
	if got, err := ext4.HasFilesystem(ctx, blank, "/dev/x", ext4.Area{Dev: "/dev/x"}); err != nil || got {
		t.Errorf("a blank device: got %v, %v, want no file system", got, err)
	}
	data := &fake.Runner{Replies: []fake.Reply{{Code: 2}}, Device: func(string, int64) (tool.Device, error) { return tool.Device{}, nil }}
	if got, err := ext4.HasFilesystem(ctx, data, "/dev/x", ext4.Area{Dev: "/dev/x"}); err == nil || got {
		t.Errorf("a device with data and no signature: got %v, %v, want an error", got, err)
	}
	ext := &fake.Runner{Replies: []fake.Reply{{Stdout: "ext4\n"}}, Device: func(string, int64) (tool.Device, error) { return tool.Device{}, nil }}
	if got, err := ext4.HasFilesystem(ctx, ext, "/dev/x", ext4.Area{Dev: "/dev/x"}); err != nil || !got {
		t.Errorf("an ext4: got %v, %v, want a file system", got, err)
	}
}

// An encrypted volume's file system is on the mapping, which reads as
// ciphertext over a zero payload: the blank test is on the raw device at the
// payload's offset, not on the mapping.
func TestHasFilesystemJudgesBlankOnTheRawPayload(t *testing.T) {
	t.Parallel()
	const mapper, raw, off = "/dev/mapper/m", "/dev/nbd0", int64(16 << 20)
	area := ext4.Area{Dev: raw, Offset: off}
	device := func(payloadBlank bool) func(string, int64) (tool.Device, error) {
		return func(dev string, offset int64) (tool.Device, error) {
			switch {
			case dev == mapper:
				return tool.Device{Size: 1 << 30}, nil // ciphertext: never blank
			case dev == raw && offset == off:
				return tool.Device{Size: 1 << 30, Blank: payloadBlank}, nil
			}
			return tool.Device{}, errors.New("unexpected probe")
		}
	}
	fresh := &fake.Runner{Replies: []fake.Reply{{Code: 2}}, Device: device(true)}
	if got, err := ext4.HasFilesystem(ctx, fresh, mapper, area); err != nil || got {
		t.Errorf("a fresh format (zero payload, ciphertext mapping): got %v, %v, want no file system", got, err)
	}
	dirty := &fake.Runner{Replies: []fake.Reply{{Code: 2}}, Device: device(false)}
	if got, err := ext4.HasFilesystem(ctx, dirty, mapper, area); err == nil || got {
		t.Errorf("a payload with data and no signature: got %v, %v, want an error", got, err)
	}
	made := &fake.Runner{Replies: []fake.Reply{{Stdout: "ext4\n"}}, Device: device(false)}
	if got, err := ext4.HasFilesystem(ctx, made, mapper, area); err != nil || !got {
		t.Errorf("a made file system: got %v, %v, want one", got, err)
	}
	gone := &fake.Runner{Replies: []fake.Reply{{Code: 2}}, Device: func(dev string, _ int64) (tool.Device, error) {
		if dev == mapper {
			return tool.Device{}, fs.ErrNotExist
		}
		return tool.Device{Size: 1 << 30, Blank: true}, nil
	}}
	if got, err := ext4.HasFilesystem(ctx, gone, mapper, area); err == nil || got || len(gone.Calls()) != 0 {
		t.Errorf("a missing mapping: got %v, %v with %d tools run, want an error and none", got, err, len(gone.Calls()))
	}
}
