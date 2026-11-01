package reconcile

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/mount/fake"
	"github.com/greghaskins/csi-miami/internal/observe"
)

// sim is a Mounter whose mount table changes the way the kernel's would, for
// the operations the reconciler uses. It wraps the fake, which supplies the
// error, block and busy configuration, the call record and the deadline
// behaviour; sim adds the effect of a call that succeeds. Tests that share a
// sim share a "kernel", so a second Reconciler over the same sim is a
// restarted process.
type sim struct {
	*fake.Mounter

	mu     sync.Mutex
	tbl    observe.Table
	nextID int
}

func newSim() *sim {
	s := &sim{Mounter: &fake.Mounter{}, nextID: 100}
	s.set(observe.Table{{ID: 1, ParentID: 0, Major: 8, Minor: 1, Root: "/", MountPoint: "/", Options: []string{"rw", "relatime"}, FSType: "ext4", Source: "/dev/sda1", SuperOptions: []string{"rw"}}})
	return s
}

// set replaces the table. The caller holds s.mu or is the only user.
func (s *sim) set(t observe.Table) {
	s.tbl = t
	s.SetTable(t)
}

// table returns a copy of the current table.
func (s *sim) table() observe.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append(observe.Table(nil), s.tbl...)
}

// addMount puts a mount at m.MountPoint as if something else had made it, a
// child of whatever mount covers that path, on a device of its own.
func (s *sim) addMount(mountPoint, fstype, source string) observe.Mount {
	s.mu.Lock()
	defer s.mu.Unlock()
	parent, _ := s.tbl.Resolve(mountPoint)
	m := observe.Mount{ID: s.nextID, ParentID: parent.ID, Major: 0, Minor: s.nextID, Root: "/", MountPoint: mountPoint, Options: []string{"rw"}, FSType: fstype, Source: source, SuperOptions: []string{"rw"}}
	s.nextID++
	s.set(append(s.tbl, m))
	return m
}

// clear forgets every mount but the root: a reboot.
func (s *sim) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set(s.tbl[:1:1])
}

// remove drops the top mount at path without any call, as if it vanished.
func (s *sim) remove(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeLocked(path)
}

func (s *sim) removeLocked(path string) bool {
	top, ok := s.tbl.At(path)
	if !ok {
		return false
	}
	var out observe.Table
	for _, m := range s.tbl {
		if m.ID != top.ID {
			out = append(out, m)
		}
	}
	s.set(out)
	return true
}

func rwOptions(ro bool) []string {
	if ro {
		return []string{"ro", "relatime"}
	}
	return []string{"rw", "relatime"}
}

func (s *sim) setReadOnly(id int, ro bool) {
	out := append(observe.Table(nil), s.tbl...)
	for i := range out {
		if out[i].ID == id {
			out[i].Options = rwOptions(ro)
		}
	}
	s.set(out)
}

// Bind follows the real Mounter's contract: it adopts a bind of source, sets
// its read-only state, refuses a different mount at target with ErrConflict,
// and otherwise makes the mount.
func (s *sim) Bind(ctx context.Context, root, source, target string, readOnly bool) error {
	if err := s.Mounter.Bind(ctx, root, source, target, readOnly); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.tbl.At(target); ok {
		same, err := s.tbl.IsBindOf(target, source)
		if err != nil {
			return err
		}
		if !same {
			return fmt.Errorf("bind %s: %w", target, mount.ErrConflict)
		}
		s.setReadOnly(cur.ID, readOnly)
		return nil
	}
	container, ok := s.tbl.Resolve(source)
	if !ok {
		return fmt.Errorf("bind %s: no mount contains the source", source)
	}
	parent, _ := s.tbl.Resolve(target)
	rel, err := filepath.Rel(container.MountPoint, source)
	if err != nil {
		return err
	}
	m := observe.Mount{
		ID: s.nextID, ParentID: parent.ID, Major: container.Major, Minor: container.Minor,
		Root: filepath.Join(container.Root, rel), MountPoint: target, Options: rwOptions(readOnly),
		FSType: container.FSType, Source: container.Source, SuperOptions: container.SuperOptions,
	}
	s.nextID++
	s.set(append(s.tbl, m))
	return nil
}

// Unmount removes the top mount at target, or fails with EBUSY if another
// mount lies inside it. Unmounting what is not mounted is a no-op.
func (s *sim) Unmount(ctx context.Context, target string, flags int, id int) error {
	if err := s.Mounter.Unmount(ctx, target, flags, id); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	top, ok := s.tbl.At(target)
	if !ok {
		return nil
	}
	for _, m := range s.tbl {
		if m.ParentID == top.ID && m.ID != top.ID {
			return fmt.Errorf("unmount %s: %w", target, unix.EBUSY)
		}
	}
	s.removeLocked(target)
	return nil
}

// calls returns the recorded calls that change mounts.
func (s *sim) mutating() []fake.Call {
	var out []fake.Call
	for _, c := range s.Calls() {
		if c.Op != fake.OpMounts && c.Op != fake.OpStatfs {
			out = append(out, c)
		}
	}
	return out
}

// ---- metrics, events, clock ----

type recMetrics struct {
	mu             sync.Mutex
	intents        map[string]int
	transports     map[string]bool
	up             map[string]bool
	conditions     map[string]int
	errors         map[string]int
	timeouts       map[string]int
	garbage        int
	loadFailures   int
	reconcileCount int
}

func newRecMetrics() *recMetrics {
	return &recMetrics{intents: map[string]int{}, transports: map[string]bool{}, up: map[string]bool{}, errors: map[string]int{}, timeouts: map[string]int{}}
}

func (m *recMetrics) SetIntents(kind string, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.intents[kind] = n
}

func (m *recMetrics) SetTransport(kind, id string, mounted bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.transports[kind+"/"+id] = mounted
}

func (m *recMetrics) SetTransportUp(kind, id string, up bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.up[kind+"/"+id] = up
}

func (m *recMetrics) SetVolumeConditions(counts map[string]int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conditions = counts
}

// upOf and conditionCount read what the latest pass reported.
func (m *recMetrics) upOf(kind, id string) (up, reported bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	up, reported = m.up[kind+"/"+id]
	return up, reported
}

func (m *recMetrics) conditionCount(condition string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.conditions[condition]
}

func (m *recMetrics) ObserveReconcile(time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconcileCount++
}

func (m *recMetrics) IncReconcileError(step string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors[step]++
}

func (m *recMetrics) IncObservationTimeout(op string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.timeouts[op]++
}

func (m *recMetrics) IncGarbageUnmount() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.garbage++
}

func (m *recMetrics) SetIntentLoadFailures(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loadFailures = n
}

func (m *recMetrics) errCount(step string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.errors[step]
}

type recEvent struct{ Volume, Reason string }

type recEvents struct {
	mu sync.Mutex
	ev []recEvent
}

func (e *recEvents) Event(volumeID, reason string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ev = append(e.ev, recEvent{volumeID, reason})
}

func (e *recEvents) all() []recEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]recEvent(nil), e.ev...)
}

func (e *recEvents) reasons(reason string) []recEvent {
	var out []recEvent
	for _, x := range e.all() {
		if x.Reason == reason {
			out = append(out, x)
		}
	}
	return out
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type atomicCount struct{ n atomic.Int64 }

func (c *atomicCount) add()     { c.n.Add(1) }
func (c *atomicCount) get() int { return int(c.n.Load()) }

// stackBind puts a bind of source at target as a new mount on top of whatever
// is there, as someone else mounting over the driver's bind would.
func (s *sim) stackBind(source, target string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	container, _ := s.tbl.Resolve(source)
	parent, _ := s.tbl.Resolve(target)
	rel, _ := filepath.Rel(container.MountPoint, source)
	s.set(append(s.tbl, observe.Mount{
		ID: s.nextID, ParentID: parent.ID, Major: container.Major, Minor: container.Minor,
		Root: filepath.Join(container.Root, rel), MountPoint: target, Options: rwOptions(false),
		FSType: container.FSType, Source: container.Source, SuperOptions: container.SuperOptions,
	}))
	s.nextID++
}
