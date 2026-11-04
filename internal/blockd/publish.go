package blockd

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greghaskins/csi-miami/internal/cache"
	"github.com/greghaskins/csi-miami/internal/chunk"
)

// uploadConcurrency bounds how many boundary chunks upload to the store at once.
const uploadConcurrency = 8

// storer is the slice of chunk.Store the server uses to publish a generation; a
// test wraps it to assert the publish order.
type storer interface {
	Current(ctx context.Context) (name string, ok bool, err error)
	ReadManifest(ctx context.Context, name string) (chunk.Manifest, error)
	Put(ctx context.Context, h chunk.Hash, r io.Reader, n int64) error
	WriteManifest(ctx context.Context, m chunk.Manifest) error
	Publish(ctx context.Context, name string) error
	Prune(ctx context.Context, keep int) (removedManifests, removedChunks int, err error)
}

var _ storer = (*chunk.Store)(nil)

// boundary snapshots the dirty set at a generation boundary. The caller holds
// the filesystem frozen (between FIFREEZE and FITHAW) so the snapshot is
// crash-consistent; afterwards a write to a frozen chunk copies it first, so the
// upload reads a stable image while the guest keeps writing.
func (s *server) boundary() error {
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	chunks, err := s.back.cache.SetBoundary()
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.boundarySet = chunks
	s.haveBound = true
	s.mu.Unlock()
	return nil
}

// publish uploads the boundary generation and makes it current. It returns
// ErrNoBoundary when no boundary is set and ErrRefused when the fencing check
// refused it (the dirty set is kept). Boundary, publish and the prune inside it
// are serialised, so no two generations are in flight at once.
func (s *server) publish(ctx context.Context, trigger string) (PublishReply, error) {
	s.publishMu.Lock()
	defer s.publishMu.Unlock()

	s.mu.Lock()
	have, boundary := s.haveBound, s.boundarySet
	s.mu.Unlock()
	if !have {
		return PublishReply{}, ErrNoBoundary
	}

	reply, err := s.doPublish(ctx, trigger, boundary)

	s.mu.Lock()
	s.haveBound = false
	s.boundarySet = nil
	s.lastPublish = time.Now()
	s.refused = reply.Refused
	switch {
	case reply.Refused:
		s.lastErr = ErrRefused.Error()
	case err != nil:
		s.lastErr = err.Error()
	default:
		s.lastErr = ""
	}
	s.mu.Unlock()
	return reply, err
}

// doPublish does the work of a publish: upload the boundary chunks, build and
// write the manifest, run the fencing check, rename current, then release the
// boundary and prune. On any failure before current is renamed it keeps the
// dirty set (ClearBoundary(false)) and publishes nothing.
func (s *server) doPublish(ctx context.Context, trigger string, boundary []uint32) (PublishReply, error) {
	if len(boundary) == 0 {
		s.back.cache.ClearBoundary(true) // nothing to publish
		return PublishReply{}, nil
	}

	size := s.back.Size()
	chunks := s.baseManifestChunks(int(size / s.cfg.Chunk))

	hashes, uploaded, err := s.uploadBoundary(ctx, boundary)
	if err != nil {
		s.back.cache.ClearBoundary(false)
		return PublishReply{}, err
	}
	for k, i := range boundary {
		chunks[i] = hashes[k]
	}

	s.mu.Lock()
	seq := s.seq + 1
	s.mu.Unlock()
	m := chunk.Manifest{
		Volume:   s.cfg.Volume,
		Epoch:    s.cfg.Epoch,
		Sequence: seq,
		Size:     size,
		Chunk:    s.cfg.Chunk,
		Time:     time.Now().UTC(),
		Chunks:   chunks,
	}

	// Fencing, after the slow upload and immediately before we commit: the
	// holder must still be ours and current must not already name a higher
	// epoch. On refusal nothing is written and the dirty set is kept.
	refused, err := s.refusedToPublish(ctx, m)
	switch {
	case err != nil:
		s.back.cache.ClearBoundary(false)
		return PublishReply{}, err
	case refused:
		s.back.cache.ClearBoundary(false)
		return PublishReply{Refused: true, Uploaded: uploaded}, ErrRefused
	}

	if err := s.store.WriteManifest(ctx, m); err != nil {
		s.back.cache.ClearBoundary(false)
		return PublishReply{}, err
	}
	if err := s.store.Publish(ctx, m.Name()); err != nil {
		s.back.cache.ClearBoundary(false)
		return PublishReply{}, err
	}

	// Committed. Advance the lineage, release the boundary as published, prune.
	s.back.setManifest(chunks)
	s.back.cache.SetBase(m.Name(), s.cfg.Epoch)
	s.back.cache.ClearBoundary(true)
	s.mu.Lock()
	s.seq = seq
	s.base = m.Name()
	s.mu.Unlock()
	if _, _, perr := s.store.Prune(ctx, generationsKeep); perr != nil {
		s.log.Warn("prune after publish failed", "volume", s.cfg.Volume, "err", perr)
	}
	s.log.Info("published generation", "volume", s.cfg.Volume, "trigger", trigger, "manifest", m.Name(), "uploaded", uploaded)
	return PublishReply{Name: m.Name(), Uploaded: uploaded}, nil
}

// baseManifestChunks is the base lineage's chunk hashes, padded with the zero
// hash to n chunks (a grown region past the base manifest reads as zeros).
func (s *server) baseManifestChunks(n int) []chunk.Hash {
	out := make([]chunk.Hash, n)
	s.back.mu.RLock()
	base := s.back.manifest
	for i := range out {
		if i < len(base) {
			out[i] = base[i]
		} else {
			out[i] = s.back.zeroHash
		}
	}
	s.back.mu.RUnlock()
	return out
}

// refusedToPublish is the fencing gate: the holder must still name our node at
// our epoch, and current must not already name a manifest at a higher epoch.
func (s *server) refusedToPublish(ctx context.Context, m chunk.Manifest) (bool, error) {
	mine, err := s.cfg.Holder.Mine(ctx)
	if err != nil {
		return false, err
	}
	if !mine {
		return true, nil
	}
	name, ok, err := s.store.Current(ctx)
	if err != nil {
		return false, err
	}
	if ok {
		ep, _, err := chunk.ParseManifestName(name)
		if err != nil {
			return false, err
		}
		if ep > m.Epoch {
			return true, nil
		}
	}
	return false, nil
}

// uploadBoundary uploads each boundary chunk under its hash, concurrently, and
// returns the hashes in boundary order and the bytes uploaded. A boundary chunk
// that is all zeros uploads the well-known zero chunk, which the store stores
// once.
func (s *server) uploadBoundary(ctx context.Context, boundary []uint32) ([]chunk.Hash, int64, error) {
	hashes := make([]chunk.Hash, len(boundary))
	var uploaded atomic.Int64

	ectx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	var firstErr error
	fail := func(e error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = e
			cancel()
		}
		mu.Unlock()
	}

	jobs := make(chan int)
	var wg sync.WaitGroup
	workers := min(uploadConcurrency, len(boundary))
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := cache.AlignedBuffer(int(s.cfg.Chunk))
			for k := range jobs {
				if ectx.Err() != nil {
					return
				}
				i := boundary[k]
				if err := s.back.cache.BoundaryChunk(i, buf); err != nil {
					fail(err)
					return
				}
				h := chunk.Sum(buf)
				if err := s.store.Put(ectx, h, bytes.NewReader(buf), s.cfg.Chunk); err != nil {
					fail(err)
					return
				}
				hashes[k] = h
				uploaded.Add(s.cfg.Chunk)
			}
		}()
	}
	for k := range boundary {
		select {
		case jobs <- k:
		case <-ectx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	return hashes, uploaded.Load(), firstErr
}
