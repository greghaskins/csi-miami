package ext4_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/greghaskins/csi-miami/internal/ext4"
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
