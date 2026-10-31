package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/greghaskins/csi-miami/internal/mount"
	"github.com/greghaskins/csi-miami/internal/observe"
)

// DefaultResolveTimeout bounds name resolution and host path resolution when
// Options.ResolveTimeout is not positive.
const DefaultResolveTimeout = 5 * time.Second

// mountPointMode is the mode of a mount point directory the transport creates:
// plain state that only the driver, which runs as root, needs.
const mountPointMode = 0o700

// Transport is a Spec mounted on the host mount table at Dir. The mount table
// is the only record of whether it is mounted: nothing is cached, so a new
// Transport made after a restart sees what the old one mounted.
//
// A Transport does not serialize its callers. The Mounter it was made with
// admits one call per path at a time, and the reconciler is expected to run
// one operation per transport at a time.
type Transport interface {
	// Spec is the transport's Spec, in normal form.
	Spec() Spec

	// Dir is the mount point: Spec().MountPoint(root).
	Dir() string

	// Mounted reports, from the mount table only, whether the mount visible
	// at Dir is exactly this transport. A mount of something else, and a
	// mount hidden under another, are false; Mount reports them as a
	// conflict. For local, an error means the answer could not be found,
	// for example because the host path can no longer be resolved.
	Mounted(ctx context.Context) (bool, error)

	// Mount mounts the transport. It is a no-op when Mounted is true. When
	// something else is mounted at Dir, or is hidden under another mount
	// there, it fails with an error wrapping mount.ErrConflict and changes
	// nothing: the mount point is never stacked over.
	Mount(ctx context.Context) error

	// Unmount unmounts what is visible at Dir, and is a no-op when nothing
	// is. It never detaches lazily: a busy transport is an error for the
	// caller to report. It does not check that the mount is this transport,
	// so a mount left by an older option profile can be replaced.
	Unmount(ctx context.Context) error
}

// Resolver looks up the addresses of a host name. *net.Resolver implements it.
type Resolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

// Options are the parts of a Transport that tests replace and deployments
// tune. The zero value is the production configuration.
type Options struct {
	// Resolver looks up the server name of an nfs3 transport. Nil means
	// net.DefaultResolver. A server that is an IP literal is never looked up.
	Resolver Resolver

	// ResolveTimeout bounds one name lookup and one host path resolution.
	// Zero or negative means DefaultResolveTimeout. A lookup that exceeds it
	// fails with an error wrapping mount.ErrTimeout and the lookup is
	// abandoned, so a resolver or a filesystem that hangs cannot hang a mount.
	ResolveTimeout time.Duration

	// ResolveHostPath returns the resolved form of a local transport's host
	// path: absolute, cleaned, with no symlink, naming an existing
	// directory. Nil means ResolveDirectory. It runs under ResolveTimeout,
	// because filepath.EvalSymlinks can hang on a dead NFS mount.
	ResolveHostPath func(path string) (string, error)
}

// New returns the Transport for spec, mounted under root. It fails with
// InvalidArgument if spec is not valid and in normal form, or root is not an
// absolute, cleaned path, and with an error if m is nil. root must be
// resolved, with no symlink in it: the Mounter refuses a mount point reached
// through one.
func New(spec Spec, root string, m mount.Mounter, opts Options) (Transport, error) {
	dir, err := spec.MountPoint(root)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, errors.New("transport: a Mounter is required")
	}
	if opts.Resolver == nil {
		opts.Resolver = net.DefaultResolver
	}
	if opts.ResolveTimeout <= 0 {
		opts.ResolveTimeout = DefaultResolveTimeout
	}
	if opts.ResolveHostPath == nil {
		opts.ResolveHostPath = ResolveDirectory
	}
	b := base{spec: spec, dir: dir, m: m, opts: opts}
	if spec.Kind == KindLocal {
		return &local{base: b}, nil
	}
	return newNFS3(b), nil // MountPoint validated the Spec: the only other kind is nfs3
}

// ResolveDirectory resolves the symlinks in p and checks that the result is an
// existing directory. It is the default Options.ResolveHostPath. The driver
// never creates the directory, so a missing one is an error that wraps
// fs.ErrNotExist.
func ResolveDirectory(p string) (string, error) {
	resolved, err := filepath.EvalSymlinks(p)
	if err != nil {
		return "", err
	}
	fi, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if !fi.IsDir() {
		return "", fmt.Errorf("%q is not a directory", resolved)
	}
	return resolved, nil
}

// base holds what both kinds share.
type base struct {
	spec Spec
	dir  string
	m    mount.Mounter
	opts Options
}

func (b *base) Spec() Spec  { return b.spec }
func (b *base) Dir() string { return b.dir }

// Unmount is a no-op when the table shows no mount visible at Dir; otherwise
// it unmounts the top mount there, without MNT_DETACH.
func (b *base) Unmount(ctx context.Context) error {
	t, err := b.m.Mounts(ctx)
	if err != nil {
		return err
	}
	if _, ok := t.At(b.dir); !ok {
		return nil
	}
	return b.m.Unmount(ctx, b.dir, 0)
}

// dirState is what the mount table shows at a transport's Dir.
type dirState int

const (
	// dirEmpty: no mount at Dir.
	dirEmpty dirState = iota
	// dirMounted: a mount is visible at Dir.
	dirMounted
	// dirHidden: the table has a mount at Dir that nothing visible leads to,
	// because a mount above it covers the path.
	dirHidden
)

// inspect classifies Dir in t and returns the visible mount, if any.
func (b *base) inspect(t observe.Table) (observe.Mount, dirState) {
	if top, ok := t.At(b.dir); ok {
		return top, dirMounted
	}
	for _, m := range t {
		if m.MountPoint == b.dir {
			return observe.Mount{}, dirHidden
		}
	}
	return observe.Mount{}, dirEmpty
}

// conflict is the error for a mount point that is occupied by something that
// is not this transport. It names the file system type and the source the
// kernel reports, never any mount option.
func (b *base) conflict(top observe.Mount, state dirState, why string) error {
	if state == dirHidden {
		return fmt.Errorf("%w: %s has a mount that is hidden under another mount; clear it before mounting %s", mount.ErrConflict, b.dir, b.spec.Canonical())
	}
	return fmt.Errorf("%w: %s is mounted as %s from %q, which is not %s (%s)", mount.ErrConflict, b.dir, top.FSType, top.Source, b.spec.Canonical(), why)
}

// makeMountPoint creates Dir if it is missing. A mount point is a plain
// directory, not a mount.
func (b *base) makeMountPoint() error {
	if err := os.MkdirAll(b.dir, mountPointMode); err != nil {
		return fmt.Errorf("creating the mount point: %w", err)
	}
	return nil
}
