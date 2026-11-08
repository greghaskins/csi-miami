package cachecap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
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

func (n *fakeNC) RemoveCacheCapacity(_ context.Context, node string) error {
	n.nodes = append(n.nodes, node)
	n.calls = append(n.calls, "remove")
	return n.remove
}

type fakeMetrics struct {
	n, nComm                 int
	failures                 []string
	budget, comm, size, free int64
}

func (m *fakeMetrics) SetCacheCapacity(b, s, f int64) {
	m.n++
	m.budget, m.size, m.free = b, s, f
}

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
			r := NewRunner(NewSource(tt.cfg, "/c", fakeStat(f)), tt.nc, node, committed, m, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Minute, time.Second)
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
		func(context.Context) (int64, int, error) { return 0, 0, nil }, &fakeMetrics{}, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Hour, time.Second)
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
		r := NewRunner(NewSource(Config{Accounting: true, Share: 0.5}, "/c", failing), nc, "n", zeroCommitted, m, quietLog(), time.Minute, time.Second)
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
		r := NewRunner(NewSource(cfg, "/c", failing), nc, "n", zeroCommitted, m, quietLog(), time.Minute, time.Second)
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
		r := NewRunner(NewSource(cfg, "/c", stat), &fakeNC{}, "n", zeroCommitted, m, quietLog(), time.Minute, time.Second)
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
	r := NewRunner(NewSource(Config{Accounting: true, Share: 0.5, Reserve: 15 * gib}, "/c", fakeStat(f)), nc, "n", committed, m, quietLog(), time.Minute, time.Second)
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
	r := NewRunner(NewSource(Config{Accounting: true, Share: 0.5}, "/c", fakeStat(f)), nc, "n", zeroCommitted, &fakeMetrics{}, quietLog(), time.Minute, 50*time.Millisecond)
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
	r := NewRunner(NewSource(Config{}, "/c", nil), &fakeNC{}, "n", zeroCommitted, &fakeMetrics{}, quietLog(), 0, -1)
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
	r := NewRunner(NewSource(Config{Accounting: true, Share: 0.5}, "/c", fakeStat(f)), nc, "n", zeroCommitted, &fakeMetrics{}, quietLog(), 5*time.Millisecond, time.Second)
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
			r := NewRunner(NewSource(tt.cfg, "/c", fakeStat(f)), tt.nc, "n", zeroCommitted, m, quietLog(), time.Minute, time.Second)
			_ = r.Pass(context.Background())
			if !slices.Equal(m.failures, tt.want) {
				t.Errorf("failures = %v, want %v", m.failures, tt.want)
			}
		})
	}
}
