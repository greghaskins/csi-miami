package blockd

import (
	"context"
	"path/filepath"

	"github.com/greghaskins/csi-miami/internal/holder"
)

// FileChecker is a non-acquiring holder.Checker. The block-server process does
// not own the holder file — the node plugin acquired it and handed the epoch to
// the server — so the server only fences: it re-reads the holder file every
// call and reports whether it still names this node at this epoch. A takeover
// by another node raises the epoch (or changes the node), and Mine then returns
// false, which refuses the publish.
type FileChecker struct {
	Dir   string // the directory the holder file lives in (beside the volume directory)
	Name  string // the volume name
	Node  string // this node
	Epoch uint64 // the epoch this server publishes under
}

// Mine reports whether the holder file still names this node at this epoch.
func (c FileChecker) Mine(ctx context.Context) (bool, error) {
	info, err := holder.Read(ctx, c.Dir, c.Name)
	if err != nil {
		return false, err
	}
	return info.Present && info.Node == c.Node && info.Epoch == c.Epoch, nil
}

var _ holder.Checker = FileChecker{}

// NewFileChecker builds a FileChecker for the volume whose store directory is
// storeDir: the holder file sits beside that directory, named by the volume.
func NewFileChecker(storeDir, volume, node string, epoch uint64) FileChecker {
	return FileChecker{Dir: filepath.Dir(filepath.Clean(storeDir)), Name: volume, Node: node, Epoch: epoch}
}
