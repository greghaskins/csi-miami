package fake

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/greghaskins/csi-miami/internal/systemd"
)

const vol = "pvc-0b9d4c1e-6a4f-4f6e-9d57-2f1c1d8f3a10"

func TestFakeRecordsStartProperties(t *testing.T) {
	var m Manager
	u := systemd.Unit{
		Name:        systemd.UnitName(vol),
		Description: "block server",
		ExecStart:   []string{"/usr/bin/miami", "blockd", "--volume", vol},
		Environment: []string{"A=b"},
	}
	if err := m.Start(context.Background(), u); err != nil {
		t.Fatal(err)
	}
	calls := m.Starts()
	if len(calls) != 1 || !reflect.DeepEqual(calls[0].Unit, u) {
		t.Fatalf("calls = %+v", calls)
	}
	want := map[string]any{
		"Type":           "simple",
		"ExecStart":      u.ExecStart,
		"Environment":    u.Environment,
		"Description":    "block server",
		"CollectMode":    "inactive-or-failed",
		"KillMode":       "control-group",
		"StandardOutput": "journal",
		"StandardError":  "journal",
	}
	if !reflect.DeepEqual(calls[0].Properties, want) {
		t.Fatalf("properties = %v", calls[0].Properties)
	}
}

func TestFakeStartFailsOnRequest(t *testing.T) {
	var m Manager
	boom := errors.New("boom")
	m.FailStart(boom)
	if err := m.Start(context.Background(), systemd.Unit{Name: "x"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if len(m.Starts()) != 1 {
		t.Fatal("a failed Start is still recorded")
	}
	m.FailStart(nil)
	if err := m.Start(context.Background(), systemd.Unit{Name: "x"}); err != nil {
		t.Fatal(err)
	}
}

func TestFakeStateAndStop(t *testing.T) {
	var m Manager
	ctx := context.Background()
	name := systemd.UnitName(vol)
	if s, err := m.State(ctx, name); err != nil || s.Present {
		t.Fatalf("unset unit = %+v, %v", s, err)
	}
	want := systemd.State{Active: "active", Sub: "running", MainPID: 42, Present: true}
	m.SetState(name, want)
	if s, _ := m.State(ctx, name); s != want {
		t.Fatalf("state = %+v", s)
	}
	if err := m.Stop(ctx, name); err != nil {
		t.Fatal(err)
	}
	if s, _ := m.State(ctx, name); s.Present {
		t.Fatalf("stopped unit still present: %+v", s)
	}
	if !reflect.DeepEqual(m.Stops(), []string{name}) {
		t.Fatalf("stops = %v", m.Stops())
	}
}
