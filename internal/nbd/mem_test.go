package nbd

import (
	"errors"
	"sync"
)

// memBackend is an in-memory Backend for the unit and smoke tests. failRead
// and failWrite make the next call at that offset fail once.
type memBackend struct {
	mu        sync.Mutex
	data      []byte
	flushes   int
	writes    int
	reads     int
	fuaWrites int
	failRead  map[int64]bool
	failWrite map[int64]bool
}

var errInjected = errors.New("injected backend failure")

func newMem(size int) *memBackend {
	return &memBackend{data: make([]byte, size), failRead: map[int64]bool{}, failWrite: map[int64]bool{}}
}

func (m *memBackend) ReadAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reads++
	if m.failRead[off] {
		delete(m.failRead, off)
		return 0, errInjected
	}
	return copy(p, m.data[off:]), nil
}

func (m *memBackend) WriteAt(p []byte, off int64, fua bool) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writes++
	if m.failWrite[off] {
		delete(m.failWrite, off)
		return 0, errInjected
	}
	if fua {
		m.fuaWrites++
	}
	return copy(m.data[off:], p), nil
}

func (m *memBackend) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushes++
	return nil
}

func (m *memBackend) Trim(off, n int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.data[off : off+n])
	return nil
}

func (m *memBackend) Size() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.data))
}
