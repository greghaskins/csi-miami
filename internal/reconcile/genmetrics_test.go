package reconcile

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/blockd"
	"github.com/greghaskins/csi-miami/internal/item"
)

// genMetrics records what the generation timers report beside the usual
// metrics.
type genMetrics struct {
	*recMetrics
	mu       sync.Mutex
	vols     map[string]VolumeGeneration
	deleted  []string
	publish  []string
	durs     []time.Duration
	served   map[string]int
	servedAt int
}

func (m *genMetrics) SetVolumeGeneration(id string, g VolumeGeneration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.vols[id] = g
}

func (m *genMetrics) DeleteVolumeGeneration(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.vols, id)
	m.deleted = append(m.deleted, id)
}

func (m *genMetrics) ObservePublish(result string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publish = append(m.publish, result)
	m.durs = append(m.durs, d)
}

func (m *genMetrics) SetVolumesServed(counts map[string]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.served = map[string]int{}
	for k, v := range counts {
		m.served[k] = v
	}
	m.servedAt++
}

func (m *genMetrics) vol(id string) (VolumeGeneration, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.vols[id]
	return g, ok
}

func (m *genMetrics) results() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.publish...)
}

func (m *genMetrics) servedCount(state string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.served[state]
}

func newGenMetricsEnv(t *testing.T) (*genEnv, *genMetrics) {
	t.Helper()
	e := newGenEnv(t)
	gm := &genMetrics{recMetrics: e.metrics, vols: map[string]VolumeGeneration{}}
	e.r.metrics = gm
	return e, gm
}

func TestVolumeGenerationGaugesFollowTheStatus(t *testing.T) {
	t.Parallel()
	e, gm := newGenMetricsEnv(t)
	fetch := blockd.FetchStats{Count: 3, Errors: 1}
	e.g.set(func(g *genItem) {
		g.status.Dirty = 4096
		g.status.Cached = 10000
		g.status.LastPublish = e.clock.Now().Add(-90 * time.Second)
		g.status.Fetch = fetch
	})
	e.tick()
	got, ok := gm.vol(e.in.VolumeID)
	if !ok {
		t.Fatal("no generation gauges for the staged volume")
	}
	if got.Age != 90*time.Second || got.DirtyBytes != 4096 || got.CleanBytes != 10000-4096 || got.Fetch.Count != 3 || got.Fetch.Errors != 1 {
		t.Fatalf("gauges = %+v", got)
	}
}

// A server that has not published since it began is aged from when the timers
// first saw the volume, as the overdue verdict is.
func TestVolumeGenerationAgeWithoutAPublishStartsAtFirstSight(t *testing.T) {
	t.Parallel()
	e, gm := newGenMetricsEnv(t)
	e.g.set(func(g *genItem) { g.status.Dirty = 4096 })
	e.tick()
	e.clock.Advance(2 * time.Minute)
	e.tick()
	got, _ := gm.vol(e.in.VolumeID)
	if got.Age != 2*time.Minute {
		t.Fatalf("age = %v, want 2m0s", got.Age)
	}
}

func TestVolumeGenerationSeriesEndWithTheVolume(t *testing.T) {
	t.Parallel()
	e, gm := newGenMetricsEnv(t)
	e.g.set(func(g *genItem) { g.status.Dirty = 4096 })
	e.tick()
	if _, ok := gm.vol(e.in.VolumeID); !ok {
		t.Fatal("no gauges before the unstage")
	}
	if err := e.r.runUnstageForTest(e.in.VolumeID); err != nil {
		t.Logf("unstage: %v", err)
	}
	e.r.gens.wait()
	if _, ok := gm.vol(e.in.VolumeID); ok {
		t.Error("the gauges of an unstaged volume are still there")
	}
}

// A block server that does not answer has no figures to report: its gauges go
// rather than keep the last ones.
func TestVolumeGenerationSeriesGoWhileTheServerIsDown(t *testing.T) {
	t.Parallel()
	e, gm := newGenMetricsEnv(t)
	e.g.set(func(g *genItem) { g.status.Dirty = 4096 })
	e.tick()
	e.g.set(func(g *genItem) { g.statusEr = errors.New("no answer") })
	e.tick()
	if _, ok := gm.vol(e.in.VolumeID); ok {
		t.Error("the gauges of a volume whose server is down are still there")
	}
	e.tick() // the count of states is the pass's, after the read

	if n := gm.servedCount(ServedDown); n != 1 {
		t.Errorf("down = %d, want 1", n)
	}
}

func TestVolumesServedByState(t *testing.T) {
	t.Parallel()
	e, gm := newGenMetricsEnv(t)
	e.tick()
	e.tick() // the first pass starts the status read; the next reports it
	want := func(serving, outdated, down int) {
		t.Helper()
		for state, n := range map[string]int{ServedServing: serving, ServedOutdatedBinary: outdated, ServedDown: down} {
			if got := gm.servedCount(state); got != n {
				t.Errorf("%s = %d, want %d", state, got, n)
			}
		}
	}
	want(1, 0, 0)
	e.g.set(func(g *genItem) { g.outdated = true })
	e.tick()
	e.tick()
	want(0, 1, 0)
	e.g.set(func(g *genItem) { g.statusEr = errors.New("no answer") })
	e.tick()
	e.tick()
	want(0, 0, 1)
	if err := e.r.runUnstageForTest(e.in.VolumeID); err != nil {
		t.Logf("unstage: %v", err)
	}
	e.r.gens.wait()
	e.tick()
	want(0, 0, 0)
}

func TestPublishResultsAreCountedByOutcome(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
		want []string
	}{
		{"ok", nil, []string{PublishOK}},
		{"refused", fmt.Errorf("x: %w", item.ErrPublishRefused), []string{PublishRefused}},
		{"failed", errors.New("the store said no"), []string{PublishFailed}},
		// Another publish is running (and counted by its own finish).
		{"skipped", fmt.Errorf("x: %w", item.ErrGenerationSkipped), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, gm := newGenMetricsEnv(t)
			e.g.set(func(g *genItem) { g.status.Dirty = genCache; g.err = tc.err })
			e.tick()
			got := gm.results()
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("results = %v, want %v", got, tc.want)
			}
		})
	}
}
