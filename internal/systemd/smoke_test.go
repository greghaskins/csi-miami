//go:build smoke

package systemd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"
)

// The smoke tests drive the host's systemd over its private socket. They
// must run as root on a host that runs systemd, and fail, never skip, when
// they cannot.

func smokeManager(t *testing.T) (Manager, string) {
	t.Helper()
	if os.Getuid() != 0 {
		t.Fatal("smoke: must run as root")
	}
	m, err := New(DefaultSocket)
	if err != nil {
		t.Fatal(err)
	}
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	name := unitPrefix + "test-" + hex.EncodeToString(b[:]) + ".service"
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = m.Stop(ctx, name)
	})
	return m, name
}

func TestSmokeTransientUnit(t *testing.T) {
	m, name := smokeManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	u := Unit{Name: name, Description: "csi-miami smoke", ExecStart: []string{"/bin/sleep", "30"}, Environment: []string{"MIAMI_SMOKE=1"}}
	if err := m.Start(ctx, u); err != nil {
		t.Fatal(err)
	}
	st, err := m.State(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if !st.Present || st.Active != "active" || st.MainPID <= 0 {
		t.Fatalf("State = %+v, want active with a pid", st)
	}
	// Starting it again with the same ExecStart is not an error.
	if err := m.Start(ctx, u); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	u.ExecStart = []string{"/bin/sleep", "31"}
	if err := m.Start(ctx, u); err == nil {
		t.Fatal("Start with a different ExecStart over an active unit succeeded")
	}
	if err := m.Stop(ctx, name); err != nil {
		t.Fatal(err)
	}
	if st, err = m.State(ctx, name); err != nil || st.Present {
		t.Fatalf("after Stop: %+v, %v", st, err)
	}
	if err := m.Stop(ctx, name); err != nil {
		t.Fatalf("Stop of an absent unit: %v", err)
	}
}

func TestSmokeFailedUnitIsReset(t *testing.T) {
	m, name := smokeManager(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	if err := m.Start(ctx, Unit{Name: name, ExecStart: []string{"/bin/false"}}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		st, err := m.State(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if st.Present && st.Active == "failed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("unit never failed: %+v", st)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := m.Stop(ctx, name); err != nil {
		t.Fatal(err)
	}
	if st, err := m.State(ctx, name); err != nil || st.Present {
		t.Fatalf("after Stop a failed unit remains: %+v, %v", st, err)
	}
}
