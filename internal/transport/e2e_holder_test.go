//go:build e2e

package transport_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/transport"
)

// The holder file between two NFS clients: two mounts of one export, each
// its own client cache. The second cannot acquire while the first holds; once
// the judgement says the holder is stale the second takes over at epoch 2 and
// the first's Renew reports the loss.
func TestE2ENFS3HolderTwoClients(t *testing.T) {
	server := os.Getenv(serverEnv)
	if server == "" {
		t.Fatalf("%s is not set: run this through hack/e2e.sh", serverEnv)
	}
	export := os.Getenv(exportEnv)
	if export == "" {
		export = "/export"
	}
	base, m := kernelEnv(t)
	ctx := context.Background()

	spec := transport.Spec{Kind: transport.KindNFS3, Server: server, Export: export}
	var dirs [2]string
	for i, n := range []string{"a", "b"} {
		root := filepath.Join(base, n)
		mkdir(t, root)
		tr, err := transport.New(spec, root, m, transport.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := tr.Mount(ctx); err != nil {
			t.Fatalf("mounting the export at %s: %v", n, err)
		}
		dirs[i] = filepath.Join(tr.Dir(), "holder-e2e")
	}
	mkdir(t, dirs[0])
	t.Cleanup(func() { _ = os.RemoveAll(dirs[0]) })
	const vol = "vol"

	first, err := holder.Acquire(ctx, dirs[0], vol, "kind-worker", holder.Judgement{})
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if first.Epoch() != 1 {
		t.Errorf("first epoch = %d, want 1", first.Epoch())
	}
	if err := first.Renew(ctx); err != nil {
		t.Fatalf("Renew: %v", err)
	}

	// The second client sees the first's file and is refused.
	info, err := holder.Read(ctx, dirs[1], vol)
	if err != nil || !info.Present || info.Node != "kind-worker" {
		t.Fatalf("Read through the second mount = %+v, %v", info, err)
	}
	t.Logf("age on the server's clock: %s", info.Age)
	if info.Age > 30*time.Second {
		t.Errorf("a holder renewed just now is %s old on the server's clock", info.Age)
	}
	live := holder.Judgement{Reason: "no emergency detach", Info: info}
	if _, err := holder.Acquire(ctx, dirs[1], vol, "kind-worker2", live); !errors.Is(err, holder.ErrHeld) {
		t.Fatalf("second Acquire = %v, want ErrHeld", err)
	}
	if ok, err := first.Mine(ctx); err != nil || !ok {
		t.Fatalf("first Mine = %v, %v", ok, err)
	}

	// An operator abandons the first holder (the emergency detach); the
	// judgement is the reconciler's, whose rules its own tests drive.
	stale := holder.Judgement{Takeover: true, Reason: "emergency detach", Info: info}
	second, err := holder.Acquire(ctx, dirs[1], vol, "kind-worker2", stale)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if second.Epoch() != 2 {
		t.Errorf("second epoch = %d, want 2", second.Epoch())
	}
	if err := first.Renew(ctx); !errors.Is(err, holder.ErrLost) {
		t.Errorf("first Renew after the takeover = %v, want ErrLost", err)
	}
	if ok, err := first.Mine(ctx); err != nil || ok {
		t.Errorf("first Mine after the takeover = %v, %v; want false", ok, err)
	}
	if err := first.Release(ctx); err != nil {
		t.Errorf("first Release = %v", err)
	}
	if ok, err := second.Mine(ctx); err != nil || !ok {
		t.Errorf("second Mine = %v, %v; want true", ok, err)
	}
	if err := second.Release(ctx); err != nil {
		t.Errorf("second Release = %v", err)
	}
	if info, err := holder.Read(ctx, dirs[0], vol); err != nil || info.Present {
		t.Errorf("after Release, Read = %+v, %v", info, err)
	}
	noSillyRename(t, dirs[0])

	// A takeover landing between Renew's checks and its touch: the touch is
	// answered for the inode the takeover replaced (ESTALE on a real server),
	// and the late renewer is told it lost, leaving the taker's file as it is.
	const vol2 = "vol2"
	a, err := holder.Acquire(ctx, dirs[0], vol2, "kind-worker", holder.Judgement{})
	if err != nil {
		t.Fatal(err)
	}
	var taker *holder.Holder
	restore := holder.SetHook(func(point string) {
		if point != "renew-checked" {
			return
		}
		info, err := holder.Read(ctx, dirs[1], vol2)
		if err != nil {
			t.Error(err)
			return
		}
		taker, err = holder.Acquire(ctx, dirs[1], vol2, "kind-worker2", holder.Judgement{Takeover: true, Reason: "e2e", Info: info})
		if err != nil {
			t.Error(err)
		}
	})
	err = a.Renew(ctx)
	restore()
	if !errors.Is(err, holder.ErrLost) {
		t.Fatalf("a Renew that raced a takeover = %v, want ErrLost", err)
	}
	if taker == nil {
		t.Fatal("the takeover did not happen")
	}
	if info, err := holder.Read(ctx, dirs[1], vol2); err != nil || info.Node != "kind-worker2" || info.Epoch != 2 {
		t.Errorf("the taker's file after the late renewal = %+v, %v", info, err)
	}
	if err := a.Release(ctx); err != nil && !errors.Is(err, holder.ErrLost) {
		t.Errorf("late Release = %v", err)
	}
	if err := taker.Release(ctx); err != nil {
		t.Errorf("taker Release = %v", err)
	}
	noSillyRename(t, dirs[0])
	noSillyRename(t, dirs[1])
}

// noSillyRename fails if an NFS client left a .nfsXXXX file in dir: what it
// does when a file is unlinked while the same client still has it open.
func noSillyRename(t *testing.T, dir string) {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), ".nfs") {
			t.Errorf("a silly-renamed file %q is left in %s", e.Name(), dir)
		}
	}
}
