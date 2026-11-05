package probe

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// clock is a settable time.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// script answers each read in turn: nil reads succeed, a non-nil error fails,
// and a nil entry in hang makes the read block until released.
type script struct {
	mu      sync.Mutex
	results []error
	reads   int
	hold    chan struct{} // when non-nil every read blocks on it
	started chan struct{}
}

func (s *script) read(_ context.Context, _ string) error {
	s.mu.Lock()
	i := s.reads
	s.reads++
	hold, started := s.hold, s.started
	var err error
	if i < len(s.results) {
		err = s.results[i]
	}
	s.mu.Unlock()
	if started != nil {
		started <- struct{}{}
	}
	if hold != nil {
		<-hold
	}
	return err
}

func (s *script) count() int { s.mu.Lock(); defer s.mu.Unlock(); return s.reads }

func newProber(t *testing.T, s *script, c *clock) *Prober {
	t.Helper()
	p := New(Options{Read: s.read, Timeout: 10 * time.Second, Now: c.now})
	t.Cleanup(p.Close)
	return p
}

func TestProbeReadsFirstSectorDirect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dev")
	want := bytes.Repeat([]byte{0xA5}, 8192)
	if err := os.WriteFile(path, want, 0o600); err != nil {
		t.Fatal(err)
	}
	var flags int
	old := openDevice
	openDevice = func(p string, f int, m uint32) (int, error) {
		flags = f
		// A temp directory on tmpfs or overlayfs may refuse O_DIRECT; the
		// flag is what is under test, so the open here drops it.
		return unix.Open(p, f&^unix.O_DIRECT, m)
	}
	defer func() { openDevice = old }()

	buf, err := readFirstSector(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if flags&unix.O_DIRECT == 0 {
		t.Errorf("open flags %#x lack O_DIRECT", flags)
	}
	if flags&(unix.O_WRONLY|unix.O_RDWR) != 0 {
		t.Errorf("open flags %#x are not read-only", flags)
	}
	if len(buf) != SectorSize || !bytes.Equal(buf, want[:SectorSize]) {
		t.Errorf("read %d bytes, not the first %d of the device", len(buf), SectorSize)
	}
}

func TestProbeOfAMissingDeviceFails(t *testing.T) {
	if err := Read(context.Background(), filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Fatal("reading a device that is not there succeeded")
	}
}

func TestTwoFailuresMarkFailed(t *testing.T) {
	boom := errors.New("input/output error")
	s := &script{results: []error{nil, boom, boom, nil}}
	c := &clock{t: time.Unix(1000, 0)}
	p := newProber(t, s, c)

	step := func() State {
		t.Helper()
		p.Observe("v", "/dev/x")
		p.Wait()
		return p.State("v")
	}
	if st := step(); st.Abnormal {
		t.Fatalf("a success is abnormal: %+v", st)
	}
	if st := step(); st.Abnormal {
		t.Fatalf("one failure is abnormal: %+v", st)
	}
	st := step()
	if !st.Abnormal || st.Message == "" {
		t.Fatalf("two consecutive failures: %+v, want abnormal with a message", st)
	}
	if st := step(); st.Abnormal {
		t.Fatalf("a success did not clear the condition: %+v", st)
	}
}

func TestAFailureThenASuccessDoesNotAccumulate(t *testing.T) {
	boom := errors.New("input/output error")
	s := &script{results: []error{boom, nil, boom, nil, boom}}
	p := newProber(t, s, &clock{t: time.Unix(1000, 0)})
	for i := range s.results {
		p.Observe("v", "/dev/x")
		p.Wait()
		if st := p.State("v"); st.Abnormal {
			t.Fatalf("probe %d: failures that are not consecutive made it abnormal", i)
		}
	}
}

func TestProbeTimeoutCountsAsFailure(t *testing.T) {
	s := &script{hold: make(chan struct{}), started: make(chan struct{}, 8)}
	c := &clock{t: time.Unix(1000, 0)}
	p := newProber(t, s, c)

	p.Observe("v", "/dev/x")
	<-s.started
	c.add(5 * time.Second)
	p.Observe("v", "/dev/x")
	if p.State("v").Abnormal {
		t.Fatal("a read inside its deadline counts as a failure")
	}
	c.add(6 * time.Second) // 11 s: past the deadline
	p.Observe("v", "/dev/x")
	if st := p.State("v"); st.Abnormal {
		t.Fatalf("one timeout is abnormal: %+v", st)
	}
	c.add(11 * time.Second)
	p.Observe("v", "/dev/x")
	st := p.State("v")
	if !st.Abnormal {
		t.Fatalf("two timeouts: %+v, want abnormal", st)
	}
	if s.count() != 1 {
		t.Errorf("%d reads started, want the one stuck read only", s.count())
	}

	// The stuck read returns well: the volume is fine, and probed again.
	close(s.hold)
	p.Wait()
	if st := p.State("v"); st.Abnormal {
		t.Fatalf("a late success did not clear the condition: %+v", st)
	}
}

func TestALateErrorAfterATimeoutIsNotCountedTwice(t *testing.T) {
	boom := errors.New("input/output error")
	s := &script{results: []error{boom}, hold: make(chan struct{}), started: make(chan struct{}, 1)}
	c := &clock{t: time.Unix(1000, 0)}
	p := newProber(t, s, c)
	p.Observe("v", "/dev/x")
	<-s.started
	c.add(11 * time.Second)
	p.Observe("v", "/dev/x") // failure 1: the timeout
	close(s.hold)
	p.Wait() // the read now returns its error
	if st := p.State("v"); st.Abnormal {
		t.Fatalf("one probe counted as two failures: %+v", st)
	}
}

func TestProbeNeverBlocksTheCaller(t *testing.T) {
	s := &script{hold: make(chan struct{}), started: make(chan struct{}, 1)}
	p := newProber(t, s, &clock{t: time.Unix(1000, 0)})
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Observe("v", "/dev/x")
		p.Observe("v", "/dev/x")
		_ = p.State("v")
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Observe waited for a read that does not return")
	}
	close(s.hold)
}

func TestOneProbeInFlightPerVolume(t *testing.T) {
	s := &script{hold: make(chan struct{}), started: make(chan struct{}, 8)}
	p := newProber(t, s, &clock{t: time.Unix(1000, 0)})
	for range 5 {
		p.Observe("a", "/dev/a")
	}
	<-s.started
	if n := s.count(); n != 1 {
		t.Fatalf("%d reads of one volume at once, want 1", n)
	}
	p.Observe("b", "/dev/b") // another volume is not held up by a's
	<-s.started
	if n := s.count(); n != 2 {
		t.Fatalf("%d reads for two volumes, want 2", n)
	}
	close(s.hold)
	p.Wait()
	p.Observe("a", "/dev/a")
	p.Wait()
	if n := s.count(); n != 3 {
		t.Errorf("%d reads, want a new one after the first returned", n)
	}
}

func TestForgetDropsTheState(t *testing.T) {
	boom := errors.New("input/output error")
	s := &script{results: []error{boom, boom}}
	p := newProber(t, s, &clock{t: time.Unix(1000, 0)})
	for range 2 {
		p.Observe("v", "/dev/x")
		p.Wait()
	}
	if !p.State("v").Abnormal {
		t.Fatal("not abnormal")
	}
	p.Forget("v")
	if st := p.State("v"); st.Abnormal || st.Known {
		t.Fatalf("after Forget: %+v", st)
	}
}
