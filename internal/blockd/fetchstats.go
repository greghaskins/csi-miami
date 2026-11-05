package blockd

import (
	"context"
	"errors"
	"sync"
	"time"
)

// FetchBuckets are the upper bounds, in seconds, of the chunk fetch duration
// histogram a Status carries: from a warm local store to a transport that
// answers at the edge of the request budget.
var FetchBuckets = []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60}

// FetchStats is the server's account of its fetches from the store, since it
// started. The node plugin cannot observe them (the server is another
// process), so it re-exports them from the status; a restart of the server
// resets them, which a counter's rate() already handles.
type FetchStats struct {
	Count   uint64   // fetches that reached the store, failed ones included
	Errors  uint64   // fetches that failed (a cancelled fetch is neither counted nor an error)
	Sum     float64  // total seconds of the counted fetches
	Buckets []uint64 // cumulative counts for FetchBuckets, in order
}

// fetchRecorder accumulates FetchStats.
type fetchRecorder struct {
	mu sync.Mutex
	s  FetchStats
}

// record notes one finished fetch. A fetch that ended because its context did
// (another fetch of the request failed first, or the device is going away) says
// nothing about the store and is dropped.
func (r *fetchRecorder) record(d time.Duration, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	sec := d.Seconds()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.s.Buckets == nil {
		r.s.Buckets = make([]uint64, len(FetchBuckets))
	}
	r.s.Count++
	r.s.Sum += sec
	if err != nil {
		r.s.Errors++
	}
	for i, le := range FetchBuckets {
		if sec <= le {
			r.s.Buckets[i]++
		}
	}
}

// snapshot returns a copy; Buckets always has len(FetchBuckets) entries.
func (r *fetchRecorder) snapshot() FetchStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.s
	out.Buckets = make([]uint64, len(FetchBuckets))
	copy(out.Buckets, r.s.Buckets)
	return out
}
