package cachecap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
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
	have        bool
	bytes       int64
	getErr, set error
	calls       []string
	nodes       []string
	setBytes    []int64
	setCh       chan struct{}
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
	return nil
}

type fakeMetrics struct {
	n                        int
	budget, comm, size, free int64
}

func (m *fakeMetrics) SetCacheCapacity(b, c, s, f int64) {
	m.n++
	m.budget, m.comm, m.size, m.free = b, c, s, f
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
			r := NewRunner(NewSource(tt.cfg, "/c", fakeStat(f)), tt.nc, node, committed, m, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Minute)
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
				if m.n != 1 || m.budget != Budget(tt.fs, tt.cfg) || m.comm != 3*gib || m.size != tt.fs || m.free != 7*gib {
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
		func(context.Context) (int64, int, error) { return 0, 0, nil }, &fakeMetrics{}, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Hour)
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
