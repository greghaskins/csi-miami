package crypt_test

import (
	"context"
	"errors"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/crypt"
	"github.com/greghaskins/csi-miami/internal/tool"
	"github.com/greghaskins/csi-miami/internal/tool/fake"
)

var ctx = context.Background()

func TestFormatArgv(t *testing.T) {
	t.Parallel()
	f := &fake.Runner{}
	if err := crypt.Format(ctx, f, "/dev/nbd0", []byte("k"), crypt.Params{}); err != nil {
		t.Fatal(err)
	}
	want := []string{"cryptsetup", "luksFormat", "--type", "luks2",
		"--cipher", "xchacha12,aes-adiantum-plain64", "--key-size", "256", "--sector-size", "4096",
		"--pbkdf", "pbkdf2", "--pbkdf-force-iterations", "1000",
		"--batch-mode", "--key-file", "/dev/fd/3", "/dev/nbd0"}
	c := f.Calls()
	if len(c) != 1 || !slices.Equal(c[0].Argv, want) {
		t.Fatalf("argv = %v, want %v", c, want)
	}
	if c[0].Extra != 1 || string(c[0].Data[0]) != "k" {
		t.Fatalf("key wiring: extra %d data %q", c[0].Extra, c[0].Data)
	}
}

func TestFormatCustomParams(t *testing.T) {
	t.Parallel()
	f := &fake.Runner{}
	p := crypt.Params{Cipher: "aes-xts-plain64", KeySize: 512, SectorSize: 512}
	if err := crypt.Format(ctx, f, "/dev/x", []byte("k"), p); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(f.Calls()[0].Argv, " ")
	for _, w := range []string{"--cipher aes-xts-plain64", "--key-size 512", "--sector-size 512"} {
		if !strings.Contains(got, w) {
			t.Errorf("argv %q lacks %q", got, w)
		}
	}
}

func TestOpenPassesKeyOnFd3(t *testing.T) {
	t.Parallel()
	f := &fake.Runner{}
	if err := crypt.Open(ctx, f, "/dev/nbd0", "miami-v", []byte("s3cret")); err != nil {
		t.Fatal(err)
	}
	c := f.Calls()[0]
	want := []string{"cryptsetup", "open", "--type", "luks2", "--disable-keyring", "--key-file", "/dev/fd/3", "/dev/nbd0", "miami-v"}
	if !slices.Equal(c.Argv, want) {
		t.Fatalf("argv = %v, want %v", c.Argv, want)
	}
	if c.Extra != 1 || string(c.Data[0]) != "s3cret" {
		t.Fatalf("fd3 = %q (extra %d)", c.Data, c.Extra)
	}
	for _, a := range c.Argv {
		if strings.Contains(a, "s3cret") {
			t.Fatalf("key on argv: %v", c.Argv)
		}
	}
}

func TestRotateUsesFd3AndFd4(t *testing.T) {
	t.Parallel()
	f := &fake.Runner{}
	if err := crypt.Rotate(ctx, f, "/dev/nbd0", []byte("old"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	c := f.Calls()[0]
	want := []string{"cryptsetup", "luksChangeKey", "--key-file", "/dev/fd/3", "/dev/nbd0", "/dev/fd/4"}
	if !slices.Equal(c.Argv, want) {
		t.Fatalf("argv = %v, want %v", c.Argv, want)
	}
	if c.Extra != 2 || string(c.Data[0]) != "old" || string(c.Data[1]) != "new" {
		t.Fatalf("fds = %q", c.Data)
	}
}

func TestExitCodeTwoIsErrBadKey(t *testing.T) {
	t.Parallel()
	bad := func() *fake.Runner {
		return &fake.Runner{Replies: []fake.Reply{{Code: 2, Stderr: "No key available with this passphrase."}}}
	}
	if err := crypt.Open(ctx, bad(), "/dev/x", "n", []byte("k")); !errors.Is(err, crypt.ErrBadKey) {
		t.Fatalf("Open err = %v, want ErrBadKey", err)
	}
	if err := crypt.Rotate(ctx, bad(), "/dev/x", []byte("a"), []byte("b")); !errors.Is(err, crypt.ErrBadKey) {
		t.Fatalf("Rotate err = %v, want ErrBadKey", err)
	}
	other := &fake.Runner{Replies: []fake.Reply{{Code: 4, Stderr: "boom"}}}
	err := crypt.Open(ctx, other, "/dev/x", "n", []byte("k"))
	if err == nil || errors.Is(err, crypt.ErrBadKey) || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("exit 4 err = %v", err)
	}
	failed := &fake.Runner{Replies: []fake.Reply{{Err: errors.New("start failed")}}}
	if err := crypt.Close(ctx, failed, "n"); err == nil {
		t.Fatal("a runner error must surface")
	}
}

func TestCloseResizeArgv(t *testing.T) {
	t.Parallel()
	f := &fake.Runner{}
	if err := crypt.Close(ctx, f, "miami-v"); err != nil {
		t.Fatal(err)
	}
	if err := crypt.Resize(ctx, f, "miami-v"); err != nil {
		t.Fatal(err)
	}
	c := f.Calls()
	if !slices.Equal(c[0].Argv, []string{"cryptsetup", "close", "miami-v"}) ||
		!slices.Equal(c[1].Argv, []string{"cryptsetup", "resize", "miami-v"}) {
		t.Fatalf("calls = %v", c)
	}
	if c[0].Extra != 0 || c[1].Extra != 0 {
		t.Fatal("close and resize take no key")
	}
}

func TestIsLUKS(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		code int
		want bool
		err  bool
	}{{0, true, false}, {1, false, false}, {4, false, true}} {
		f := &fake.Runner{Replies: []fake.Reply{{Code: tt.code}}}
		got, err := crypt.IsLUKS(ctx, f, "/dev/x")
		if got != tt.want || (err != nil) != tt.err {
			t.Errorf("code %d: got %v, %v", tt.code, got, err)
		}
		if !slices.Equal(f.Calls()[0].Argv, []string{"cryptsetup", "isLuks", "/dev/x"}) {
			t.Errorf("argv = %v", f.Calls()[0].Argv)
		}
	}
}

func TestNames(t *testing.T) {
	t.Parallel()
	if crypt.Name("pvc-1") != "miami-pvc-1" || crypt.MapperPath("miami-pvc-1") != "/dev/mapper/miami-pvc-1" {
		t.Fatal("names")
	}
}

func TestErrorCarriesStdoutWhenStderrIsEmpty(t *testing.T) {
	t.Parallel()
	f := &fake.Runner{Replies: []fake.Reply{
		{Stdout: "lock failed\n", Code: 5},
		{Stdout: "ignored", Stderr: "from stderr\n", Code: 5},
		{Stdout: "open said this\n", Code: 5},
	}}
	_, err := crypt.IsLUKS(ctx, f, "/dev/x")
	if err == nil || !strings.Contains(err.Error(), "exit 5: lock failed") {
		t.Fatalf("isLuks error = %v, want the standard output in it", err)
	}
	_, err = crypt.IsLUKS(ctx, f, "/dev/x")
	if err == nil || !strings.Contains(err.Error(), "exit 5: from stderr") || strings.Contains(err.Error(), "ignored") {
		t.Fatalf("isLuks error = %v, want the standard error alone", err)
	}
	err = crypt.Open(ctx, f, "/dev/x", "name", []byte("key"))
	if err == nil || !strings.Contains(err.Error(), "exit 5: open said this") {
		t.Fatalf("open error = %v", err)
	}
}

// cryptsetup isLuks exits 1 for a header it could not read as for a device with
// none, and "none" is what leads to a luksFormat: a device that is not there is
// an error with cryptsetup never run, and a device with data and no header is
// an error too.
func TestIsLUKSNeedsTheDeviceAndZeros(t *testing.T) {
	t.Parallel()
	missing := &fake.Runner{Replies: []fake.Reply{{Code: 1}}, Device: func(string, int64) (tool.Device, error) { return tool.Device{}, fs.ErrNotExist }}
	if got, err := crypt.IsLUKS(ctx, missing, "/dev/x"); err == nil || got || !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a missing device: got %v, %v", got, err)
	}
	if n := len(missing.Calls()); n != 0 {
		t.Errorf("a missing device: %d tools ran", n)
	}
	data := &fake.Runner{Replies: []fake.Reply{{Code: 1}}, Device: func(string, int64) (tool.Device, error) { return tool.Device{}, nil }}
	if got, err := crypt.IsLUKS(ctx, data, "/dev/x"); err == nil || got {
		t.Errorf("data and no header: got %v, %v", got, err)
	}
	blank := &fake.Runner{Replies: []fake.Reply{{Code: 1}}, Device: func(string, int64) (tool.Device, error) { return tool.Device{Blank: true}, nil }}
	if got, err := crypt.IsLUKS(ctx, blank, "/dev/x"); err != nil || got {
		t.Errorf("a blank device: got %v, %v, want not LUKS", got, err)
	}
	luks := &fake.Runner{Replies: []fake.Reply{{Code: 0}}, Device: func(string, int64) (tool.Device, error) { return tool.Device{}, nil }}
	if got, err := crypt.IsLUKS(ctx, luks, "/dev/x"); err != nil || !got {
		t.Errorf("a LUKS device: got %v, %v", got, err)
	}
}

func TestPayloadOffset(t *testing.T) {
	t.Parallel()
	dump := func(off string) fake.Reply {
		return fake.Reply{Stdout: `{"keyslots":{},"segments":{"0":{"type":"crypt","offset":"` + off + `","size":"dynamic"}}}`}
	}
	f := &fake.Runner{Replies: []fake.Reply{dump("16777216")}}
	got, err := crypt.PayloadOffset(ctx, f, "/dev/nbd0")
	if err != nil || got != 16<<20 {
		t.Fatalf("got %d, %v, want 16 MiB", got, err)
	}
	if want := []string{"cryptsetup", "luksDump", "--dump-json-metadata", "/dev/nbd0"}; !slices.Equal(f.Calls()[0].Argv, want) {
		t.Errorf("argv = %v", f.Calls()[0].Argv)
	}
	for name, rep := range map[string]fake.Reply{
		"not a number":     dump("x"),
		"zero":             dump("0"),
		"not whole sector": dump("1000"),
		"not 4k aligned":   dump("1536"),
		"beyond device":    dump("2147483648"),
		"not json":         {Stdout: "LUKS header"},
		"no segment":       {Stdout: `{"segments":{}}`},
		"failure":          {Code: 4, Stderr: "no device"},
	} {
		f := &fake.Runner{Replies: []fake.Reply{rep}}
		if got, err := crypt.PayloadOffset(ctx, f, "/dev/nbd0"); err == nil {
			t.Errorf("%s: accepted, offset %d", name, got)
		}
	}
}

func TestCheckKeyBounds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		n  int
		ok bool
	}{{0, false}, {31, false}, {32, true}, {4096, true}, {4097, false}, {1 << 20, false}} {
		err := crypt.CheckKey(make([]byte, tc.n))
		if (err == nil) != tc.ok || (err != nil && !errors.Is(err, crypt.ErrKeyLength)) {
			t.Errorf("CheckKey(%d bytes) = %v, want ok=%v", tc.n, err, tc.ok)
		}
	}
}

// A key too large for a pipe is refused up front instead of blocking the
// writer for good.
func TestOversizeKeyIsRefusedNotBlocked(t *testing.T) {
	t.Parallel()
	f := &fake.Runner{}
	big := make([]byte, 1<<20)
	done := make(chan error, 1)
	go func() { done <- crypt.Open(ctx, f, "/dev/nbd0", "miami-v", big) }()
	select {
	case err := <-done:
		if !errors.Is(err, crypt.ErrKeyLength) {
			t.Fatalf("Open with a 1 MiB key = %v, want ErrKeyLength", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Open blocked writing an oversize key to a pipe")
	}
	if len(f.Calls()) != 0 {
		t.Fatal("cryptsetup ran with an oversize key")
	}
}
