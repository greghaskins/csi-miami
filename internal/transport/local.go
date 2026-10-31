package transport

import (
	"context"
	"fmt"

	"github.com/greghaskins/csi-miami/internal/observe"
)

// local is a host directory bind-mounted read-write at Dir, so that every
// transport looks the same to the code above it.
type local struct {
	base
}

// resolve returns the resolved host path under the resolution deadline.
func (l *local) resolve(ctx context.Context) (string, error) {
	what := fmt.Sprintf("resolving local path %q", l.spec.Path)
	return within(ctx, l.opts.ResolveTimeout, what, func(context.Context) (string, error) {
		resolved, err := l.opts.ResolveHostPath(l.spec.Path)
		if err != nil {
			return "", fmt.Errorf("%s: %w", what, err)
		}
		return resolved, nil
	})
}

// adopts reports whether the mount visible at Dir is a bind of the resolved
// host path.
func (l *local) adopts(ctx context.Context, t observe.Table) (bool, error) {
	resolved, err := l.resolve(ctx)
	if err != nil {
		return false, err
	}
	return t.IsBindOf(l.dir, resolved)
}

func (l *local) Mounted(ctx context.Context) (bool, error) {
	t, err := l.m.Mounts(ctx)
	if err != nil {
		return false, err
	}
	if _, state := l.inspect(t); state != dirMounted {
		return false, nil
	}
	return l.adopts(ctx, t)
}

func (l *local) Mount(ctx context.Context) error {
	t, err := l.m.Mounts(ctx)
	if err != nil {
		return err
	}
	top, state := l.inspect(t)
	switch state {
	case dirHidden:
		return l.conflict(top, state, "")
	case dirMounted:
		ours, err := l.adopts(ctx, t)
		if err != nil {
			return err
		}
		if ours {
			return nil
		}
		return l.conflict(top, state, "it is not a bind of the host path")
	}
	resolved, err := l.resolve(ctx)
	if err != nil {
		return err
	}
	if err := l.makeMountPoint(); err != nil {
		return err
	}
	return l.m.Bind(ctx, resolved, l.dir, false)
}
