package systemd

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	sdbus "github.com/coreos/go-systemd/v22/dbus"
	godbus "github.com/godbus/dbus/v5"
)

const vol = "pvc-0b9d4c1e-6a4f-4f6e-9d57-2f1c1d8f3a10"

func TestUnitName(t *testing.T) {
	if got, want := UnitName(vol), "miami-blockd-"+vol+".service"; got != want {
		t.Fatalf("UnitName = %q, want %q", got, want)
	}
	for _, bad := range []string{"", "pvc-x", "../etc/x", "pvc-0b9d4c1e-6a4f-4f6e-9d57-2f1c1d8f3a10 x", vol + ".service", "PVC-0B9D4C1E-6A4F-4F6E-9D57-2F1C1D8F3A10"} {
		if got := UnitName(bad); got != "" {
			t.Errorf("UnitName(%q) = %q, want empty", bad, got)
		}
	}
}

func TestNewReturnsErrNoSystemdOnMissingSocket(t *testing.T) {
	path := t.TempDir() + "/private"
	_, err := New(path)
	if !errors.Is(err, ErrNoSystemd) {
		t.Fatalf("err = %v, want ErrNoSystemd", err)
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("err %q does not name the socket", err)
	}
}

func TestPropertiesAreExactlyTheDesignedSet(t *testing.T) {
	u := Unit{Name: UnitName(vol), Description: "d", ExecStart: []string{"/bin/true", "a"}, Environment: []string{"K=v"}}
	got := map[string]any{}
	for _, p := range properties(u) {
		got[p.Name] = p.Value.Value()
	}
	for name, want := range map[string]any{
		"Type": "simple", "Description": "d", "CollectMode": "inactive-or-failed",
		"KillMode": "control-group", "StandardOutput": "journal", "StandardError": "journal",
		"Environment": []string{"K=v"},
	} {
		if !reflect.DeepEqual(got[name], want) {
			t.Errorf("%s = %v, want %v", name, got[name], want)
		}
	}
	if _, ok := got["ExecStart"]; !ok || len(got) != 8 {
		t.Errorf("properties = %v", got)
	}
}

// stubBus is a bus whose answers a test sets.
type stubBus struct {
	startErr, stopErr, resetErr, propsErr error
	result                                string // job result sent on the channel; "" sends none
	props                                 map[string]any
	exec                                  [][]string
	execErr                               error
	calls                                 []string
}

func (b *stubBus) job(ch chan<- string) {
	if b.result != "" {
		ch <- b.result
	}
}

func (b *stubBus) StartTransientUnitContext(_ context.Context, name, mode string, _ []sdbus.Property, ch chan<- string) (int, error) {
	b.calls = append(b.calls, "start "+name+" "+mode)
	if b.startErr != nil {
		return 0, b.startErr
	}
	b.job(ch)
	return 1, nil
}

func (b *stubBus) StopUnitContext(_ context.Context, name, mode string, ch chan<- string) (int, error) {
	b.calls = append(b.calls, "stop "+name+" "+mode)
	if b.stopErr != nil {
		return 0, b.stopErr
	}
	b.job(ch)
	return 1, nil
}

func (b *stubBus) ResetFailedUnitContext(_ context.Context, name string) error {
	b.calls = append(b.calls, "reset "+name)
	return b.resetErr
}

func (b *stubBus) GetUnitPropertiesContext(context.Context, string) (map[string]any, error) {
	return b.props, b.propsErr
}

func (b *stubBus) GetServiceExecStart(context.Context, string) ([][]string, error) {
	return b.exec, b.execErr
}

func (b *stubBus) Close() {}

func dbusErr(name string) error { return godbus.Error{Name: name} }

var testUnit = Unit{Name: UnitName(vol), ExecStart: []string{"/bin/sleep", "30"}}

func TestStartWaitsForTheJob(t *testing.T) {
	b := &stubBus{result: "done"}
	if err := (&manager{b}).Start(context.Background(), testUnit); err != nil {
		t.Fatal(err)
	}
	if want := "start " + testUnit.Name + " replace"; b.calls[0] != want {
		t.Fatalf("calls = %v", b.calls)
	}
	b = &stubBus{result: "failed"}
	if err := (&manager{b}).Start(context.Background(), testUnit); err == nil || !strings.Contains(err.Error(), "failed") {
		t.Fatalf("err = %v", err)
	}
}

func TestStartIsBoundedByTheContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := (&manager{&stubBus{}}).Start(ctx, testUnit) // no job result ever arrives
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestStartRefusesBadUnits(t *testing.T) {
	m := &manager{&stubBus{result: "done"}}
	for _, u := range []Unit{
		{Name: "", ExecStart: []string{"/bin/true"}},
		{Name: "miami-blockd-../x.service", ExecStart: []string{"/bin/true"}},
		{Name: testUnit.Name},
		{Name: testUnit.Name, ExecStart: []string{"true"}},
	} {
		if err := m.Start(context.Background(), u); err == nil {
			t.Errorf("Start(%+v) succeeded", u)
		}
	}
}

func TestStartAdoptsAnIdenticalActiveUnit(t *testing.T) {
	active := map[string]any{"ActiveState": "active", "SubState": "running", "MainPID": uint32(7)}
	cases := []struct {
		name    string
		props   map[string]any
		exec    [][]string
		wantErr string
	}{
		{"identical", active, [][]string{testUnit.ExecStart}, ""},
		{"different exec", active, [][]string{{"/bin/sleep", "31"}}, "different ExecStart"},
		{"failed", map[string]any{"ActiveState": "failed", "SubState": "failed"}, [][]string{testUnit.ExecStart}, "failed"},
	}
	for _, c := range cases {
		b := &stubBus{startErr: dbusErr(errUnitExists), props: c.props, exec: c.exec}
		err := (&manager{b}).Start(context.Background(), testUnit)
		switch {
		case c.wantErr == "" && err != nil:
			t.Errorf("%s: %v", c.name, err)
		case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr) || !strings.Contains(err.Error(), testUnit.Name)):
			t.Errorf("%s: err = %v", c.name, err)
		}
	}
}

func TestStopResetsFailedAndToleratesAbsence(t *testing.T) {
	b := &stubBus{result: "done"}
	if err := (&manager{b}).Stop(context.Background(), testUnit.Name); err != nil {
		t.Fatal(err)
	}
	if want := []string{"stop " + testUnit.Name + " replace", "reset " + testUnit.Name}; !reflect.DeepEqual(b.calls, want) {
		t.Fatalf("calls = %v", b.calls)
	}
	b = &stubBus{stopErr: dbusErr(errNoSuchUnit), resetErr: dbusErr(errNoSuchUnit)}
	if err := (&manager{b}).Stop(context.Background(), testUnit.Name); err != nil {
		t.Fatalf("absent unit: %v", err)
	}
	b = &stubBus{stopErr: errors.New("io")}
	if err := (&manager{b}).Stop(context.Background(), testUnit.Name); err == nil {
		t.Fatal("a real error must surface")
	}
	b = &stubBus{result: "done", resetErr: errors.New("io")}
	if err := (&manager{b}).Stop(context.Background(), testUnit.Name); err == nil {
		t.Fatal("a reset failure must surface")
	}
}

func TestStateReadsThePropertiesAndMapsAbsence(t *testing.T) {
	b := &stubBus{props: map[string]any{"LoadState": "loaded", "ActiveState": "active", "SubState": "running", "MainPID": uint32(99)}}
	got, err := (&manager{b}).State(context.Background(), testUnit.Name)
	if want := (State{Active: "active", Sub: "running", MainPID: 99, Present: true}); err != nil || got != want {
		t.Fatalf("State = %+v, %v", got, err)
	}
	for _, b := range []*stubBus{
		{propsErr: dbusErr(errNoSuchUnit)},
		{props: map[string]any{"LoadState": "not-found", "ActiveState": "inactive"}},
	} {
		got, err := (&manager{b}).State(context.Background(), testUnit.Name)
		if err != nil || got.Present {
			t.Fatalf("absent unit = %+v, %v", got, err)
		}
	}
	if _, err := (&manager{&stubBus{propsErr: errors.New("io")}}).State(context.Background(), testUnit.Name); err == nil {
		t.Fatal("a real error must surface")
	}
}
