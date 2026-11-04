package crypt_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/greghaskins/csi-miami/internal/crypt"
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
