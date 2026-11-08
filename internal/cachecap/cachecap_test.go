package cachecap

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	gib = int64(1) << 30
)

func TestBudgetFormula(t *testing.T) {
	t.Parallel()
	c := Config{Accounting: true, Share: 0.5, Reserve: 15 * gib}
	for _, tt := range []struct {
		name   string
		fs     int64
		cfg    Config
		budget int64
	}{
		{"220Gi", 220 * gib, c, 95 * gib},
		{"466Gi", 466 * gib, c, 218 * gib},
		{"small fs clamps to zero", 20 * gib, c, 0},
		{"rounds down to a whole MiB", 10*gib + 700<<10, Config{Accounting: true, Share: 0.5}, 5 * gib},
	} {
		if got := Budget(tt.fs, tt.cfg); got != tt.budget {
			t.Errorf("%s: Budget = %d, want %d", tt.name, got, tt.budget)
		}
	}
}

func TestBudgetAbsoluteWins(t *testing.T) {
	t.Parallel()
	c := Config{Accounting: true, Share: 0.5, Reserve: 15 * gib, Absolute: 100 * gib, HaveAbsolute: true}
	for _, fs := range []int64{20 * gib, 466 * gib} {
		if got := Budget(fs, c); got != 100*gib {
			t.Errorf("fs %d: Budget = %d, want 100Gi", fs, got)
		}
	}
	z := Config{Accounting: true, Share: 0.5, Absolute: 0, HaveAbsolute: true}
	s := NewSource(z, "/c", fakeStat(&fsFigures{size: 466 * gib, free: gib}))
	b, enabled, err := s.Current(context.Background())
	if err != nil || !enabled || b != 0 {
		t.Errorf("Current = %d, %v, %v; want 0, true, nil", b, enabled, err)
	}
}

func TestSourceAccountingOff(t *testing.T) {
	t.Parallel()
	s := NewSource(Config{Accounting: false, Share: 0.5}, "/c", fakeStat(&fsFigures{size: 100 * gib}))
	if _, enabled, err := s.Current(context.Background()); err != nil || enabled {
		t.Errorf("enabled = %v, err = %v; want false, nil", enabled, err)
	}
}

func TestSourceStats(t *testing.T) {
	t.Parallel()
	f := &fsFigures{size: 100 * gib, free: 40 * gib}
	s := NewSource(Config{Accounting: true, Share: 0.5}, "/c", fakeStat(f))
	if size, free := s.Stats(); size != 0 || free != 0 {
		t.Errorf("Stats before Current = %d, %d", size, free)
	}
	if _, _, err := s.Current(context.Background()); err != nil {
		t.Fatal(err)
	}
	if size, free := s.Stats(); size != 100*gib || free != 40*gib {
		t.Errorf("Stats = %d, %d", size, free)
	}
	f.size, f.free = 200*gib, 10*gib
	if _, _, err := s.Current(context.Background()); err != nil {
		t.Fatal(err)
	}
	if size, free := s.Stats(); size != 200*gib || free != 10*gib {
		t.Errorf("Stats after change = %d, %d", size, free)
	}
}

type fsFigures struct{ size, free int64 }

func fakeStat(f *fsFigures) FSStat {
	return func(string) (int64, int64, error) { return f.size, f.free, nil }
}

type fakeNC struct {
	have                bool
	bytes               int64
	getErr, set, remove error
	unfenced            bool
	probeCleanErr       error
	removeProbeErr      error
	removeProbes        int
	probeErr            error
	probes              int
	calls               []string
	nodes               []string
	setBytes            []int64
	setCh               chan struct{}
}

func (n *fakeNC) CacheCapacity(_ context.Context, node string) (int64, bool, error) {
	n.nodes = append(n.nodes, node)
	return n.bytes, n.have, n.getErr
}

func (n *fakeNC) SetCacheCapacity(_ context.Context, node string, b int64) error {
	n.nodes = append(n.nodes, node)
	n.calls = append(n.calls, "set")
	n.setBytes = append(n.setBytes, b)
	if n.setCh != nil {
		select {
		case n.setCh <- struct{}{}:
		default:
		}
	}
	return n.set
}

func (n *fakeNC) ProbeFence(context.Context, string) (FenceState, error) {
	n.probes++
	switch {
	case n.probeErr != nil:
		return FenceUnknown, n.probeErr
	case n.unfenced:
		return Unfenced, n.probeCleanErr
	}
	return Fenced, nil
}

func (n *fakeNC) RemoveFenceProbe(context.Context, string) error {
	n.removeProbes++
	return n.removeProbeErr
}

func (n *fakeNC) RemoveCacheCapacity(_ context.Context, node string) error {
	n.nodes = append(n.nodes, node)
	n.calls = append(n.calls, "remove")
	return n.remove
}

type fakeMetrics struct {
	n, nComm                 int
	failures                 []string
	states                   []string
	budget, comm, size, free int64
}

func (m *fakeMetrics) SetCacheCapacity(b, s, f int64) {
	m.n++
	m.budget, m.size, m.free = b, s, f
}

func (m *fakeMetrics) SetCacheFenceState(s string) { m.states = append(m.states, s) }

func (m *fakeMetrics) IncCachePatchFailure(reason string) { m.failures = append(m.failures, reason) }

func (m *fakeMetrics) SetCacheCommitted(c int64) {
	m.nComm++
	m.comm = c
}

func TestRunnerPass(t *testing.T) {
	t.Parallel()
	on := Config{Accounting: true, Share: 0.5, Reserve: 15 * gib}
	const node = "node-a"
	boom := errors.New("boom")
	for _, tt := range []struct {
		name      string
		cfg       Config
		nc        *fakeNC
		fs        int64
		wantCalls []string
		wantSet   int64
		wantErr   bool
		wantMet   bool
	}{
		{"absent key is set", on, &fakeNC{}, 220 * gib, []string{"set"}, 95 * gib, false, true},
		{"equal key is left alone", on, &fakeNC{have: true, bytes: 95 * gib}, 220 * gib, nil, 0, false, true},
		{"different key is set once", on, &fakeNC{have: true, bytes: 50 * gib}, 220 * gib, []string{"set"}, 95 * gib, false, true},
		{"zero is set, not removed", on, &fakeNC{have: true, bytes: 5 * gib}, 20 * gib, []string{"set"}, 0, false, true},
		{"accounting off removes a present key", Config{Share: 0.5}, &fakeNC{have: true, bytes: gib}, 220 * gib, []string{"remove"}, 0, false, false},
		{"accounting off with no key does nothing", Config{Share: 0.5}, &fakeNC{}, 220 * gib, nil, 0, false, false},
		{"read error is returned, metrics set", on, &fakeNC{getErr: boom}, 220 * gib, nil, 0, true, true},
		{"set error is returned, metrics set", on, &fakeNC{set: boom}, 220 * gib, []string{"set"}, 95 * gib, true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := &fsFigures{size: tt.fs, free: 7 * gib}
			m := &fakeMetrics{}
			committed := func(context.Context) (int64, int, error) { return 3 * gib, 2, nil }
			r := NewRunner(NewSource(tt.cfg, "/c", fakeStat(f)), tt.nc, node, committed, m, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Minute, time.Second, 0)
			err := r.Pass(context.Background())
			if (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if len(tt.nc.calls) != len(tt.wantCalls) {
				t.Fatalf("calls = %v, want %v", tt.nc.calls, tt.wantCalls)
			}
			for i, c := range tt.wantCalls {
				if tt.nc.calls[i] != c {
					t.Errorf("call %d = %s, want %s", i, tt.nc.calls[i], c)
				}
			}
			if len(tt.nc.setBytes) == 1 && tt.nc.setBytes[0] != tt.wantSet {
				t.Errorf("set %d, want %d", tt.nc.setBytes[0], tt.wantSet)
			}
			for _, n := range tt.nc.nodes {
				if n != node {
					t.Errorf("call for node %q", n)
				}
			}
			if tt.wantMet {
				if m.n != 1 || m.nComm != 1 || m.budget != Budget(tt.fs, tt.cfg) || m.comm != 3*gib || m.size != tt.fs || m.free != 7*gib {
					t.Errorf("metrics = %+v", *m)
				}
			} else if m.n != 0 {
				t.Errorf("metrics set with accounting off")
			}
		})
	}
}

func TestRunnerRunPassesImmediatelyAndStops(t *testing.T) {
	t.Parallel()
	nc := &fakeNC{setCh: make(chan struct{}, 1)}
	f := &fsFigures{size: 220 * gib}
	r := NewRunner(NewSource(Config{Accounting: true, Share: 0.5}, "/c", fakeStat(f)), nc, "n",
		func(context.Context) (int64, int, error) { return 0, 0, nil }, &fakeMetrics{}, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Hour, time.Second, 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	select {
	case <-nc.setCh:
	case <-time.After(5 * time.Second):
		t.Fatal("no immediate pass")
	}
	cancel()
	<-done
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func zeroCommitted(context.Context) (int64, int, error) { return 0, 0, nil }

// TestPassFSStatError: only the formula needs the file system's size.
func TestPassFSStatError(t *testing.T) {
	t.Parallel()
	boom := errors.New("statfs failed")
	failing := func(string) (int64, int64, error) { return 0, 0, boom }
	t.Run("formula", func(t *testing.T) {
		t.Parallel()
		nc, m := &fakeNC{}, &fakeMetrics{}
		r := NewRunner(NewSource(Config{Accounting: true, Share: 0.5}, "/c", failing), nc, "n", zeroCommitted, m, quietLog(), time.Minute, time.Second, 0)
		if err := r.Pass(context.Background()); !errors.Is(err, boom) {
			t.Errorf("err = %v, want the statfs error", err)
		}
		if len(nc.calls) != 0 || len(nc.nodes) != 0 {
			t.Errorf("the node was touched: %v", nc.nodes)
		}
		if m.n != 0 || m.nComm != 0 {
			t.Errorf("gauges set on a failed pass: %+v", *m)
		}
	})
	t.Run("absolute", func(t *testing.T) {
		t.Parallel()
		nc, m := &fakeNC{}, &fakeMetrics{}
		cfg := Config{Accounting: true, Absolute: 100 * gib, HaveAbsolute: true}
		r := NewRunner(NewSource(cfg, "/c", failing), nc, "n", zeroCommitted, m, quietLog(), time.Minute, time.Second, 0)
		if err := r.Pass(context.Background()); err != nil {
			t.Fatalf("err = %v", err)
		}
		if len(nc.setBytes) != 1 || nc.setBytes[0] != 100*gib {
			t.Errorf("set = %v, want [100Gi]", nc.setBytes)
		}
		if m.budget != 100*gib || m.size != 0 || m.free != 0 {
			t.Errorf("gauges = %+v, want the budget and zero figures", *m)
		}
	})
	t.Run("absolute keeps the last known figures", func(t *testing.T) {
		t.Parallel()
		f := &fsFigures{size: 200 * gib, free: 50 * gib}
		fail := false
		stat := func(d string) (int64, int64, error) {
			if fail {
				return 0, 0, boom
			}
			return fakeStat(f)(d)
		}
		m := &fakeMetrics{}
		cfg := Config{Accounting: true, Absolute: 10 * gib, HaveAbsolute: true}
		r := NewRunner(NewSource(cfg, "/c", stat), &fakeNC{}, "n", zeroCommitted, m, quietLog(), time.Minute, time.Second, 0)
		if err := r.Pass(context.Background()); err != nil {
			t.Fatal(err)
		}
		fail = true
		if err := r.Pass(context.Background()); err != nil {
			t.Fatal(err)
		}
		if m.size != 200*gib || m.free != 50*gib {
			t.Errorf("gauges = %+v, want the last known figures", *m)
		}
	})
}

// TestPassCommittedError: an unreadable commitment never reads as zero.
func TestPassCommittedError(t *testing.T) {
	t.Parallel()
	boom := errors.New("cannot read drain records")
	f := &fsFigures{size: 220 * gib, free: 7 * gib}
	m := &fakeMetrics{}
	nc := &fakeNC{}
	n := 0
	committed := func(context.Context) (int64, int, error) {
		n++
		if n == 1 {
			return 3 * gib, 2, nil
		}
		return 0, 0, boom
	}
	r := NewRunner(NewSource(Config{Accounting: true, Share: 0.5, Reserve: 15 * gib}, "/c", fakeStat(f)), nc, "n", committed, m, quietLog(), time.Minute, time.Second, 0)
	if err := r.Pass(context.Background()); err != nil {
		t.Fatal(err)
	}
	err := r.Pass(context.Background())
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want the committed error", err)
	}
	if m.nComm != 1 || m.comm != 3*gib {
		t.Errorf("committed gauge written %d times, last %d; want once, 3Gi", m.nComm, m.comm)
	}
	if m.n != 2 {
		t.Errorf("budget gauges written %d times, want 2", m.n)
	}
}

// TestPassRunsUnderDeadline: a node API that never answers cannot hold a pass.
func TestPassRunsUnderDeadline(t *testing.T) {
	t.Parallel()
	f := &fsFigures{size: 220 * gib}
	nc := &blockingNC{}
	r := NewRunner(NewSource(Config{Accounting: true, Share: 0.5}, "/c", fakeStat(f)), nc, "n", zeroCommitted, &fakeMetrics{}, quietLog(), time.Minute, 50*time.Millisecond, 0)
	done := make(chan error, 1)
	go func() { done <- r.Pass(context.Background()) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("err = %v, want a deadline error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the pass outlived its deadline")
	}
}

type blockingNC struct{ fakeNC }

func (*blockingNC) CacheCapacity(ctx context.Context, _ string) (int64, bool, error) {
	<-ctx.Done()
	return 0, false, ctx.Err()
}

func TestNewRunnerDefaults(t *testing.T) {
	t.Parallel()
	r := NewRunner(NewSource(Config{}, "/c", nil), &fakeNC{}, "n", zeroCommitted, &fakeMetrics{}, quietLog(), 0, -1, 0)
	if r.interval != defaultInterval || r.deadline != defaultDeadline {
		t.Errorf("interval %v, deadline %v", r.interval, r.deadline)
	}
	// A zero interval would panic time.NewTicker.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r.Run(ctx)
}

// TestRunRetriesAfterFailedPass: a failed pass is tried again at the next tick.
func TestRunRetriesAfterFailedPass(t *testing.T) {
	t.Parallel()
	f := &fsFigures{size: 220 * gib}
	nc := &fakeNC{set: errors.New("denied"), setCh: make(chan struct{}, 8)}
	r := NewRunner(NewSource(Config{Accounting: true, Share: 0.5}, "/c", fakeStat(f)), nc, "n", zeroCommitted, &fakeMetrics{}, quietLog(), 5*time.Millisecond, time.Second, 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	for i := 0; i < 3; i++ {
		select {
		case <-nc.setCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d passes ran; Run gave up after a failure", i)
		}
	}
	cancel()
	<-done
}

func statusErr(code int32, reason metav1.StatusReason) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure, Code: code, Reason: reason}}
}

// TestPatchFailuresCounted: a budget the node cannot advertise is a metric,
// by reason, for a set and for a clear; successes and read failures are not.
func TestPatchFailuresCounted(t *testing.T) {
	t.Parallel()
	on := Config{Accounting: true, Share: 0.5, Reserve: 15 * gib}
	off := Config{Share: 0.5}
	for _, tt := range []struct {
		name string
		cfg  Config
		nc   *fakeNC
		want []string
	}{
		{"set denied by RBAC or the policy (403)", on, &fakeNC{set: statusErr(403, metav1.StatusReasonForbidden)}, []string{"denied"}},
		{"set invalid (422)", on, &fakeNC{set: statusErr(422, metav1.StatusReasonInvalid)}, []string{"denied"}},
		{"set fails otherwise", on, &fakeNC{set: errors.New("down")}, []string{"error"}},
		{"set server error", on, &fakeNC{set: statusErr(500, metav1.StatusReasonInternalError)}, []string{"error"}},
		{"set succeeds", on, &fakeNC{}, nil},
		{"read fails, no patch", on, &fakeNC{getErr: errors.New("down")}, nil},
		{"clear denied", off, &fakeNC{have: true, remove: statusErr(403, metav1.StatusReasonForbidden)}, []string{"denied"}},
		{"clear fails", off, &fakeNC{have: true, remove: errors.New("down")}, []string{"error"}},
		{"clear succeeds", off, &fakeNC{have: true}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := &fakeMetrics{}
			f := &fsFigures{size: 220 * gib}
			r := NewRunner(NewSource(tt.cfg, "/c", fakeStat(f)), tt.nc, "n", zeroCommitted, m, quietLog(), time.Minute, time.Second, 0)
			_ = r.Pass(context.Background())
			if !slices.Equal(m.failures, tt.want) {
				t.Errorf("failures = %v, want %v", m.failures, tt.want)
			}
		})
	}
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func fenceRunner(nc *fakeNC, m *fakeMetrics, c *clock) *Runner {
	f := &fsFigures{size: 220 * gib}
	r := NewRunner(NewSource(Config{Accounting: true, Share: 0.5, Reserve: 15 * gib}, "/c", fakeStat(f)), nc, "n", zeroCommitted, m, quietLog(), time.Minute, time.Second, 10*time.Minute)
	r.now = c.now
	return r
}

// TestFenceProbeCadence: the probe runs at start and then once per probe
// interval; passes in between advertise on the last verdict.
func TestFenceProbeCadence(t *testing.T) {
	t.Parallel()
	nc, m, c := &fakeNC{}, &fakeMetrics{}, &clock{time.Unix(1000, 0)}
	r := fenceRunner(nc, m, c)
	ctx := context.Background()
	if err := r.Pass(ctx); err != nil || nc.probes != 1 || len(nc.setBytes) != 1 {
		t.Fatalf("first pass: err %v, probes %d, sets %v", err, nc.probes, nc.setBytes)
	}
	for i := 0; i < 9; i++ {
		c.t = c.t.Add(time.Minute)
		nc.have, nc.bytes = false, 0 // the key keeps vanishing, so every pass advertises
		if err := r.Pass(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if nc.probes != 1 || len(nc.setBytes) != 10 {
		t.Errorf("within the interval: probes %d, sets %d, want 1 and 10", nc.probes, len(nc.setBytes))
	}
	c.t = c.t.Add(time.Minute)
	if err := r.Pass(ctx); err != nil || nc.probes != 2 {
		t.Errorf("at the interval: err %v, probes %d, want 2", err, nc.probes)
	}
	if !slices.Equal(m.states, []string{"fenced", "fenced"}) {
		t.Errorf("fence states = %v", m.states)
	}
	if len(m.failures) != 0 {
		t.Errorf("a denied probe counted as a failure: %v", m.failures)
	}
}

// TestFenceUnfencedFailsClosed: an allowed probe stops the advertisement and
// removes our key, until a later probe is denied.
func TestFenceUnfencedFailsClosed(t *testing.T) {
	t.Parallel()
	nc, m, c := &fakeNC{unfenced: true, have: true, bytes: 95 * gib}, &fakeMetrics{}, &clock{time.Unix(1000, 0)}
	r := fenceRunner(nc, m, c)
	ctx := context.Background()
	if err := r.Pass(ctx); !errors.Is(err, ErrUnfenced) {
		t.Fatalf("err = %v, want ErrUnfenced", err)
	}
	if len(nc.setBytes) != 0 || !slices.Equal(nc.calls, []string{"remove"}) {
		t.Errorf("calls = %v, sets = %v; want a removal and no set", nc.calls, nc.setBytes)
	}
	if !slices.Equal(m.states, []string{"unfenced"}) {
		t.Errorf("fence states = %v, want [unfenced]", m.states)
	}
	// An unfenced verdict is re-probed on every pass: no set, and a key that
	// came back is removed again.
	c.t = c.t.Add(time.Minute)
	nc.have = true
	if err := r.Pass(ctx); !errors.Is(err, ErrUnfenced) || nc.probes != 2 || len(nc.setBytes) != 0 || len(nc.calls) != 2 {
		t.Errorf("second pass: err %v, probes %d, calls %v", err, nc.probes, nc.calls)
	}
	// A later probe that is denied lifts it.
	nc.unfenced = false
	c.t = c.t.Add(time.Minute)
	nc.have = false
	if err := r.Pass(ctx); err != nil || nc.probes != 3 || len(nc.setBytes) != 1 {
		t.Errorf("after a denied probe: err %v, probes %d, sets %v", err, nc.probes, nc.setBytes)
	}
	if !slices.Equal(m.states, []string{"unfenced", "unfenced", "fenced"}) {
		t.Errorf("fence states = %v; want [unfenced unfenced fenced]", m.states)
	}
}

// TestFenceUnknownSkipsAndRetriesSoon: a probe that cannot tell leaves the
// gauge alone, advertises nothing, counts a failure, and is retried at the
// pass interval and not the probe interval.
func TestFenceUnknownSkipsAndRetriesSoon(t *testing.T) {
	t.Parallel()
	nc, m, c := &fakeNC{probeErr: errors.New("timeout")}, &fakeMetrics{}, &clock{time.Unix(1000, 0)}
	r := fenceRunner(nc, m, c)
	ctx := context.Background()
	if err := r.Pass(ctx); err == nil {
		t.Fatal("no error from an unknown probe")
	}
	if len(nc.setBytes) != 0 || !slices.Equal(m.states, []string{"unknown"}) || !slices.Equal(m.failures, []string{"unknown"}) {
		t.Errorf("sets %v, states %v, failures %v", nc.setBytes, m.states, m.failures)
	}
	// Under the pass interval nothing is probed, and nothing is advertised.
	c.t = c.t.Add(30 * time.Second)
	if err := r.Pass(ctx); !errors.Is(err, ErrFenceUnknown) || nc.probes != 1 || len(nc.setBytes) != 0 {
		t.Errorf("early pass: err %v, probes %d", err, nc.probes)
	}
	// At the pass interval it probes again, and a clean answer advertises.
	nc.probeErr = nil
	c.t = c.t.Add(time.Minute)
	if err := r.Pass(ctx); err != nil || nc.probes != 2 || len(nc.setBytes) != 1 {
		t.Errorf("retry: err %v, probes %d, sets %v", err, nc.probes, nc.setBytes)
	}
	if !slices.Equal(m.states, []string{"unknown", "fenced"}) {
		t.Errorf("fence states = %v", m.states)
	}
}

func TestFenceProbeIntervalDefault(t *testing.T) {
	t.Parallel()
	r := NewRunner(NewSource(Config{}, "/c", nil), &fakeNC{}, "n", zeroCommitted, &fakeMetrics{}, quietLog(), 0, 0, 0)
	if r.probeEvery != 10*time.Minute || DefaultProbeInterval != 10*time.Minute {
		t.Errorf("probe interval = %v, default %v, want 10m", r.probeEvery, DefaultProbeInterval)
	}
}

// TestFenceProbeCleanupRetriedEveryPass: an allowed probe whose key cannot be
// removed is logged, counted, and retried on the next pass, not after the
// probe interval.
func TestFenceProbeCleanupRetriedEveryPass(t *testing.T) {
	t.Parallel()
	boom := errors.New("cannot remove")
	nc := &fakeNC{unfenced: true, probeCleanErr: boom, removeProbeErr: boom}
	m, c := &fakeMetrics{}, &clock{time.Unix(1000, 0)}
	var logs bytes.Buffer
	r := fenceRunner(nc, m, c)
	r.log = slog.New(slog.NewTextHandler(&logs, nil))
	ctx := context.Background()
	if err := r.Pass(ctx); !errors.Is(err, boom) {
		t.Fatalf("first pass err = %v", err)
	}
	if !strings.Contains(logs.String(), "fence probe key could not be removed") {
		t.Errorf("no log of the failed cleanup: %s", logs.String())
	}
	if !slices.Equal(m.failures, []string{"error"}) {
		t.Errorf("failures = %v, want one error", m.failures)
	}
	// The probe re-runs each pass while unfenced; its own key removal now
	// works, the earlier leftover does not.
	nc.probeCleanErr = nil
	c.t = c.t.Add(time.Minute)
	if err := r.Pass(ctx); !errors.Is(err, boom) || nc.removeProbes != 1 || nc.probes != 2 {
		t.Errorf("second pass: err %v, cleanups %d, probes %d", err, nc.removeProbes, nc.probes)
	}
	nc.removeProbeErr = nil
	c.t = c.t.Add(time.Minute)
	if err := r.Pass(ctx); !errors.Is(err, ErrUnfenced) || errors.Is(err, boom) || nc.removeProbes != 2 {
		t.Errorf("third pass: err %v, cleanups %d", err, nc.removeProbes)
	}
	c.t = c.t.Add(time.Minute)
	_ = r.Pass(ctx)
	if nc.removeProbes != 2 {
		t.Errorf("cleanup kept being retried after it succeeded: %d", nc.removeProbes)
	}
}

// TestRunLogsUnknownVerdictOnce: while the verdict is unknown a failing pass
// is a Warn the first time and a Debug after; a changed verdict resets it.
func TestRunLogsUnknownVerdictOnce(t *testing.T) {
	t.Parallel()
	nc := &fakeNC{probeErr: errors.New("timeout"), setCh: make(chan struct{}, 64)}
	var mu sync.Mutex
	var logs bytes.Buffer
	h := slog.NewTextHandler(&lockedWriter{&mu, &logs}, &slog.HandlerOptions{Level: slog.LevelDebug})
	f := &fsFigures{size: 220 * gib}
	r := NewRunner(NewSource(Config{Accounting: true, Share: 0.5}, "/c", fakeStat(f)), nc, "n", zeroCommitted, &fakeMetrics{}, slog.New(h), 5*time.Millisecond, time.Second, 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := strings.Count(logs.String(), "level=DEBUG")
		mu.Unlock()
		if n >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("too few passes")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if w := strings.Count(logs.String(), "level=WARN"); w != 1 {
		t.Errorf("%d Warn lines, want 1:\n%s", w, logs.String())
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// TestFenceStateTransitions walks fenced -> unknown -> unfenced -> fenced and
// checks the state, the gauge and the failure counts at each step: an unknown
// probe is its own state and its own reason, never a denial.
func TestFenceStateTransitions(t *testing.T) {
	t.Parallel()
	nc, m, c := &fakeNC{}, &fakeMetrics{}, &clock{time.Unix(1000, 0)}
	r := fenceRunner(nc, m, c)
	ctx := context.Background()
	step := func(mutate func()) {
		c.t = c.t.Add(11 * time.Minute) // past the probe interval
		mutate()
		_ = r.Pass(ctx)
	}
	_ = r.Pass(ctx)
	step(func() { nc.probeErr = statusErr(403, metav1.StatusReasonForbidden) })
	step(func() { nc.probeErr, nc.unfenced = nil, true })
	step(func() { nc.unfenced = false })
	want := []string{"fenced", "unknown", "unfenced", "fenced"}
	if !slices.Equal(m.states, want) {
		t.Errorf("states = %v, want %v", m.states, want)
	}
	if !slices.Equal(m.failures, []string{"unknown"}) {
		t.Errorf("failures = %v, want one unknown and no denied", m.failures)
	}
}

// TestFenceProbeCleanupRetriedWhateverTheVerdict: a probe key that could not be
// removed is retried after the verdict has flipped to Fenced, is counted and
// warned about once when it keeps failing, and stops when it succeeds.
func TestFenceProbeCleanupRetriedWhateverTheVerdict(t *testing.T) {
	t.Parallel()
	denied := statusErr(403, metav1.StatusReasonForbidden)
	nc := &fakeNC{unfenced: true, probeCleanErr: errors.New("net"), removeProbeErr: denied}
	m, c := &fakeMetrics{}, &clock{time.Unix(1000, 0)}
	var logs bytes.Buffer
	r := fenceRunner(nc, m, c)
	r.log = slog.New(slog.NewTextHandler(&logs, nil))
	ctx := context.Background()
	_ = r.Pass(ctx) // allowed probe, key left
	// The binding is fixed: the next probe is denied, so the verdict is Fenced.
	nc.unfenced, nc.probeCleanErr = false, nil
	c.t = c.t.Add(11 * time.Minute)
	for i := 0; i < 3; i++ {
		_ = r.Pass(ctx)
		c.t = c.t.Add(time.Minute)
	}
	if r.verdict != Fenced || nc.removeProbes != 3 {
		t.Errorf("verdict %v, cleanup attempts %d; want Fenced and 3", r.verdict, nc.removeProbes)
	}
	if n := strings.Count(logs.String(), "still on the node"); n != 1 {
		t.Errorf("%d warnings about the stranded key, want 1", n)
	}
	if n := slices.Index(m.failures, "denied"); n < 0 {
		t.Errorf("failures = %v, want denied removals counted", m.failures)
	}
	nc.removeProbeErr = nil
	_ = r.Pass(ctx)
	_ = r.Pass(ctx)
	if nc.removeProbes != 4 {
		t.Errorf("cleanup attempts = %d, want it to stop after succeeding (4)", nc.removeProbes)
	}
}

// TestFenceUnfencedReprobesNextPass: an unfenced verdict is not held for the
// probe interval; the next pass probes again.
func TestFenceUnfencedReprobesNextPass(t *testing.T) {
	t.Parallel()
	nc, m, c := &fakeNC{unfenced: true}, &fakeMetrics{}, &clock{time.Unix(1000, 0)}
	r := fenceRunner(nc, m, c)
	ctx := context.Background()
	_ = r.Pass(ctx)
	c.t = c.t.Add(time.Minute)
	_ = r.Pass(ctx)
	if nc.probes != 2 {
		t.Errorf("probes = %d, want 2: unfenced must re-probe on the next pass", nc.probes)
	}
}

// TestFenceUnfencedThenFencedAdvertises: once the policy is enforced the next
// pass is denied and the node advertises, long before the probe interval.
func TestFenceUnfencedThenFencedAdvertises(t *testing.T) {
	t.Parallel()
	nc, m, c := &fakeNC{unfenced: true}, &fakeMetrics{}, &clock{time.Unix(1000, 0)}
	r := fenceRunner(nc, m, c)
	ctx := context.Background()
	if err := r.Pass(ctx); !errors.Is(err, ErrUnfenced) {
		t.Fatalf("first pass err = %v, want ErrUnfenced", err)
	}
	nc.unfenced = false
	c.t = c.t.Add(time.Minute)
	if err := r.Pass(ctx); err != nil || len(nc.setBytes) != 1 {
		t.Errorf("next pass: err %v, sets %v, want an advertisement", err, nc.setBytes)
	}
}

// TestFenceFencedWaitsProbeInterval: a fenced verdict is held for the whole
// probe interval.
func TestFenceFencedWaitsProbeInterval(t *testing.T) {
	t.Parallel()
	nc, m, c := &fakeNC{}, &fakeMetrics{}, &clock{time.Unix(1000, 0)}
	r := fenceRunner(nc, m, c)
	ctx := context.Background()
	_ = r.Pass(ctx)
	c.t = c.t.Add(9*time.Minute + 59*time.Second)
	_ = r.Pass(ctx)
	if nc.probes != 1 {
		t.Fatalf("probes = %d before the interval, want 1", nc.probes)
	}
	c.t = c.t.Add(time.Second)
	_ = r.Pass(ctx)
	if nc.probes != 2 {
		t.Errorf("probes = %d at the interval, want 2", nc.probes)
	}
}

// TestFenceUnfencedErrorLoggedOnTransition: the Error about the policy is
// logged once across repeated unfenced passes, again after a fenced pass in
// between, and repeats go to Debug.
func TestFenceUnfencedErrorLoggedOnTransition(t *testing.T) {
	t.Parallel()
	nc, m, c := &fakeNC{unfenced: true}, &fakeMetrics{}, &clock{time.Unix(1000, 0)}
	var logs bytes.Buffer
	r := fenceRunner(nc, m, c)
	r.log = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		_ = r.Pass(ctx)
		c.t = c.t.Add(time.Minute)
	}
	if n := strings.Count(logs.String(), "level=ERROR"); n != 1 {
		t.Errorf("%d Error lines across 4 unfenced passes, want 1:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), "level=DEBUG") {
		t.Errorf("repeats not logged at Debug:\n%s", logs.String())
	}
	nc.unfenced = false
	_ = r.Pass(ctx)
	nc.unfenced = true
	c.t = c.t.Add(11 * time.Minute)
	_ = r.Pass(ctx)
	if n := strings.Count(logs.String(), "level=ERROR"); n != 2 {
		t.Errorf("%d Error lines after fenced then unfenced again, want 2", n)
	}
}

// TestFenceUnfencedThenFencedLeavesNoProbeKey: the allowed probe's key is
// removed at once, and the denied probe on the next pass advertises.
func TestFenceUnfencedThenFencedLeavesNoProbeKey(t *testing.T) {
	t.Parallel()
	nc, m, c := &fakeNC{unfenced: true}, &fakeMetrics{}, &clock{time.Unix(1000, 0)}
	r := fenceRunner(nc, m, c)
	ctx := context.Background()
	_ = r.Pass(ctx)
	if r.probeKeyLeft {
		t.Error("probe key left after an allowed probe whose removal succeeded")
	}
	nc.unfenced = false
	c.t = c.t.Add(time.Minute)
	if err := r.Pass(ctx); err != nil || len(nc.setBytes) != 1 || r.probeKeyLeft || nc.removeProbes != 0 {
		t.Errorf("err %v, sets %v, keyLeft %v, cleanups %d", err, nc.setBytes, r.probeKeyLeft, nc.removeProbes)
	}
}

// TestFenceFencedThenUnfenced: a node that was advertising stops, its key is
// removed, the probe key is gone, and the Error is logged exactly once.
func TestFenceFencedThenUnfenced(t *testing.T) {
	t.Parallel()
	nc, m, c := &fakeNC{}, &fakeMetrics{}, &clock{time.Unix(1000, 0)}
	var logs bytes.Buffer
	r := fenceRunner(nc, m, c)
	r.log = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	ctx := context.Background()
	if err := r.Pass(ctx); err != nil || len(nc.setBytes) != 1 {
		t.Fatalf("first pass: err %v, sets %v", err, nc.setBytes)
	}
	nc.have, nc.bytes, nc.unfenced = true, nc.setBytes[0], true
	for i := 0; i < 3; i++ {
		c.t = c.t.Add(11 * time.Minute)
		if err := r.Pass(ctx); !errors.Is(err, ErrUnfenced) {
			t.Fatalf("pass %d: err %v, want ErrUnfenced", i, err)
		}
	}
	if len(nc.setBytes) != 1 || nc.calls[len(nc.calls)-1] != "remove" || r.probeKeyLeft {
		t.Errorf("sets %v, calls %v, keyLeft %v", nc.setBytes, nc.calls, r.probeKeyLeft)
	}
	if n := strings.Count(logs.String(), "level=ERROR"); n != 1 {
		t.Errorf("%d Error lines, want 1:\n%s", n, logs.String())
	}
}
