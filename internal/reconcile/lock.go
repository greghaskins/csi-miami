package reconcile

import (
	"context"
	"sync"
)

// keyedLock is a mutex per key that can be abandoned: a waiter whose context
// ends gives up instead of queueing behind a holder that is waiting for a
// reconcile pass. A key's entry exists only while someone holds or waits for
// it, so the table does not grow with the number of volumes ever seen.
type keyedLock struct {
	mu sync.Mutex
	m  map[string]*lockEntry
}

type lockEntry struct {
	ch   chan struct{} // holds one token while the key is locked
	refs int           // holders plus waiters
}

// lock acquires key, or fails with ctx's error. The returned func releases it
// and must be called exactly once.
func (k *keyedLock) lock(ctx context.Context, key string) (func(), error) {
	k.mu.Lock()
	if k.m == nil {
		k.m = map[string]*lockEntry{}
	}
	e := k.m[key]
	if e == nil {
		e = &lockEntry{ch: make(chan struct{}, 1)}
		k.m[key] = e
	}
	e.refs++
	k.mu.Unlock()

	select {
	case e.ch <- struct{}{}:
		return func() {
			<-e.ch
			k.release(key, e)
		}, nil
	case <-ctx.Done():
		k.release(key, e)
		return nil, ctx.Err()
	}
}

func (k *keyedLock) release(key string, e *lockEntry) {
	k.mu.Lock()
	defer k.mu.Unlock()
	e.refs--
	if e.refs == 0 {
		delete(k.m, key)
	}
}

// refs returns how many holders and waiters key has. Tests use it to know
// that a caller is parked on the lock without sleeping.
func (k *keyedLock) refs(key string) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	if e := k.m[key]; e != nil {
		return e.refs
	}
	return 0
}
