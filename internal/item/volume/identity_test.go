package volume

import (
	"errors"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/systemd"
)

// A server that names another volume gets nothing: it is the wrong server for
// this socket, so no Boundary, no Publish and no Stop reaches it.
const otherVolume = "pvc-99999999-9999-4999-8999-999999999999"

func TestTeardownRefusesAServerOfAnotherVolume(t *testing.T) {
	e := dirtyEnv(t)
	e.ctl.volume = otherVolume
	err := e.it.Teardown(bg, e.in, item.Observed{})
	if !errors.Is(err, item.ErrServerDown) {
		t.Fatalf("Teardown = %v, want server-down", err)
	}
	e.rec.none(t, "ctl.Boundary", "ctl.Publish", "ctl.Stop")
	if !e.cacheExists() {
		t.Error("the cache was removed")
	}
}

func TestDrainRefusesAServerOfAnotherVolume(t *testing.T) {
	e := orphanEnv(t)
	e.attach()
	e.sd.SetState(e.v.unit, systemd.State{Active: "active", Sub: "running", MainPID: 42, Present: true})
	e.ctl.volume = otherVolume
	orphans, err := e.it.Orphans(bg)
	if err != nil || len(orphans) == 0 {
		t.Fatalf("Orphans = %v, %v", orphans, err)
	}
	err = e.it.Drain(bg, orphans[0])
	if !errors.Is(err, item.ErrServerDown) {
		t.Fatalf("Drain = %v, want server-down", err)
	}
	e.rec.none(t, "ctl.Boundary", "ctl.Publish", "ctl.Stop")
	if !e.cacheExists() {
		t.Error("the cache was removed")
	}
}

func TestStopServerChecksTheVolumeFirst(t *testing.T) {
	e := dirtyEnv(t)
	e.ctl.volume = otherVolume
	if err := e.it.stopServerAndUnit(bg, e.v); !errors.Is(err, item.ErrServerDown) {
		t.Fatalf("stopServerAndUnit = %v, want server-down", err)
	}
	e.rec.none(t, "ctl.Stop", "systemd.Stop")
}

func TestGenerationRefusesAServerOfAnotherVolume(t *testing.T) {
	e := dirtyEnv(t)
	e.ctl.volume = otherVolume
	err := e.it.generate(bg, e.v, e.it.state(e.in.VolumeID), "request")
	if !errors.Is(err, item.ErrServerDown) {
		t.Fatalf("generate = %v, want server-down", err)
	}
	e.rec.none(t, "ctl.Boundary", "ctl.Publish")
}

func TestConvergeAndStatusRefuseAServerOfAnotherVolume(t *testing.T) {
	e := dirtyEnv(t)
	e.ctl.volume = otherVolume
	if _, err := e.it.Status(bg, e.in); !errors.Is(err, item.ErrServerDown) {
		t.Errorf("Status = %v, want server-down", err)
	}
}

// A drain that has to start the server finds the wrong one answering: it is
// not served, and nothing is sent to it.
func TestDrainStartedServerOfAnotherVolumeGetsNothing(t *testing.T) {
	e := orphanEnv(t)
	e.ctl.volume = otherVolume
	e.ctl.up()
	e.it.cfg.StartTimeout = 100 * time.Millisecond
	orphans, _ := e.it.Orphans(bg)
	err := e.it.Drain(bg, orphans[0])
	if !errors.Is(err, errWrongServer) {
		t.Fatalf("Drain = %v, want the wrong-server error", err)
	}
	e.rec.none(t, "ctl.Boundary", "ctl.Publish", "ctl.Stop")
	if !e.cacheExists() {
		t.Error("the cache was removed")
	}
}

// A status that cannot be read leaves the server's identity unknown, so no
// Stop request goes to it; the unit stop still happens, and a device the
// server leaves behind is disconnected by the fallback, so nothing is stranded.
func TestStopServerSendsNoStopWhenTheIdentityIsUnknown(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.ctl.statusErr = errors.New("status unreadable")
	if err := e.it.unmountAndClose(bg, e.v, e.it.state(e.in.VolumeID)); err != nil {
		t.Fatal(err)
	}
	if err := e.it.stopServerAndUnit(bg, e.v); err != nil {
		t.Fatalf("stopServerAndUnit = %v", err)
	}
	e.rec.none(t, "ctl.Stop")
	e.rec.inOrder(t, "systemd.Stop", "nbd.Disconnect")
	if _, ok, _ := e.it.findDevice(e.v); ok {
		t.Error("the device is still there")
	}
}
