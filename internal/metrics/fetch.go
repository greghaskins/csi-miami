package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/greghaskins/csi-miami/internal/blockd"
)

// fetchCollector exposes each volume's chunk fetches. The fetches happen in
// the volume's block server, another process, which keeps the counts since it
// started and reports them in its status; the node re-exports the latest it
// read, per volume. They are a histogram and a counter that start over when
// the server restarts, which a rate() already treats as a counter reset.
type fetchCollector struct {
	durDesc, errDesc *prometheus.Desc

	mu   sync.Mutex
	vols map[string]blockd.FetchStats
}

func newFetchCollector() *fetchCollector {
	return &fetchCollector{
		durDesc: prometheus.NewDesc(NameChunkFetchDuration,
			"Duration of the volume's block server's fetches of a chunk from the store, failed ones included, since the server started.",
			[]string{"volume"}, nil),
		errDesc: prometheus.NewDesc(NameChunkFetchErrors,
			"Fetches of a chunk from the store that failed, since the volume's block server started.",
			[]string{"volume"}, nil),
		vols: map[string]blockd.FetchStats{},
	}
}

func (c *fetchCollector) set(volume string, s blockd.FetchStats) {
	c.mu.Lock()
	c.vols[volume] = s
	c.mu.Unlock()
}

func (c *fetchCollector) delete(volume string) {
	c.mu.Lock()
	delete(c.vols, volume)
	c.mu.Unlock()
}

// Describe implements prometheus.Collector.
func (c *fetchCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.durDesc
	ch <- c.errDesc
}

// Collect implements prometheus.Collector.
func (c *fetchCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for vol, s := range c.vols {
		// The bounds are the server's; a status from a server with other
		// bounds (a mixed upgrade) would mislabel them, so it is not exported.
		if len(s.Buckets) == len(blockd.FetchBuckets) {
			buckets := make(map[float64]uint64, len(s.Buckets))
			for i, le := range blockd.FetchBuckets {
				buckets[le] = s.Buckets[i]
			}
			if h, err := prometheus.NewConstHistogram(c.durDesc, s.Count, s.Sum, buckets, vol); err == nil {
				ch <- h
			}
		}
		ch <- prometheus.MustNewConstMetric(c.errDesc, prometheus.CounterValue, float64(s.Errors), vol)
	}
}
