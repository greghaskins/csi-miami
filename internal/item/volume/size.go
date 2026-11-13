package volume

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/deadline"
	"github.com/greghaskins/csi-miami/internal/sizerec"
	kinds "github.com/greghaskins/csi-miami/internal/volume"
)

// raiseSize returns v with its size raised to the largest size any record
// holds, never lowered. A PV's attributes are immutable and say the size the
// volume was created with, so a volume that has been grown needs the others:
//
//   - the intent's GrownSize, what NodeExpandVolume last asked for on this
//     node;
//   - the size record beside the volume in the store, which ControllerExpand
//     wrote before it answered (package sizerec), the one a stage on any node
//     finds;
//   - the size of the manifest `current` names, because a generation of a
//     grown device carries the grown size and the block server refuses to
//     start from a manifest larger than its configuration.
//
// A record that cannot be read stops the pass with an error and the stage is
// retried: starting at a smaller size is the one wrong answer. A missing
// manifest is not an error here (the server reports a store that is broken).
func (it *Item) raiseSize(ctx context.Context, v vol, vs *vstate, cur current) (vol, error) {
	size := max(v.size, chunk.RoundUp(v.in.GrownSize, v.chunk))

	rec, err := deadline.Run(ctx, it.cfg.Timeout, "reading the size record of "+v.name, &it.busy, "sizerec\x00"+v.name, func(context.Context) (int64, error) {
		n, _, err := sizerec.Read(v.holdDir, v.name)
		return n, err
	})
	if err != nil {
		return v, fmt.Errorf("volume item %s: reading the size record: %w", v.in.VolumeID, err)
	}
	size = max(size, chunk.RoundUp(rec, v.chunk))

	if cur.ok {
		if vs.manName != cur.name {
			n, err := it.manifestSize(ctx, v, cur.name)
			if err != nil {
				return v, err
			}
			vs.manName, vs.manSize = cur.name, n
		}
		size = max(size, vs.manSize)
	}

	// A class that caches the whole volume keeps doing so when it grows
	// (the same rule the footprint accounting uses).
	p := v.params
	p.Size, p.CacheSize = v.size, v.bound
	p = kinds.ServedParams(p, size)
	v.size, v.bound = p.Size, p.CacheSize
	return v, nil
}

// manifestSize reads the size recorded in the named manifest, 0 when the
// manifest is not there.
func (it *Item) manifestSize(ctx context.Context, v vol, name string) (int64, error) {
	n, err := deadline.Run(ctx, it.cfg.Timeout, "reading the manifest "+name+" of "+v.name, &it.busy, "manifest\x00"+v.name, func(ctx context.Context) (int64, error) {
		m, err := chunk.ReadManifestAt(ctx, v.storeDir, name)
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		if err != nil {
			return 0, err
		}
		return m.Size, nil
	})
	if err != nil {
		return 0, fmt.Errorf("volume item %s: reading the size of manifest %s: %w", v.in.VolumeID, name, err)
	}
	return n, nil
}
