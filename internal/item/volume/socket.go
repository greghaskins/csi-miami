package volume

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
)

// SocketPath is the control socket of the block server of the volume called
// name, in the cache dir. It is the one place the path is built: the item dials
// it and passes it to the unit's arguments. The name is the first 16 hex digits
// of the SHA-256 of the volume name, so the path is the same short length for
// every volume and fits sun_path (107 bytes) under long kubelet directories.
// The lock file and the drain record keep the volume's name; they are not
// sockets.
func SocketPath(cacheDir, name string) string {
	sum := sha256.Sum256([]byte(name))
	return filepath.Join(cacheDir, hex.EncodeToString(sum[:8])+".s")
}

// SocketPathLen is the length of SocketPath in cacheDir; it does not depend on
// the volume.
func SocketPathLen(cacheDir string) int {
	return len(SocketPath(cacheDir, "pvc-00000000-0000-0000-0000-000000000000"))
}
