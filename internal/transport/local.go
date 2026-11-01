package transport

import (
	"context"
	"fmt"

	"github.com/greghaskins/csi-miami/internal/deadline"
	"github.com/greghaskins/csi-miami/internal/observe"
)

// local is a host directory bind-mounted read-write at Dir, so that every
// transport looks the same to the code above it.
type local struct {
	base
}

// resolve returns the resolved host path under the resolution deadline. The
// host path is the key of the call in flight: while one that timed out has not
// returned, the next fails with mount.ErrBusy instead of starting another.
func (l *local) resolve(ctx context.Context) (string, error) {
	what := fmt.Sprintf("resolving local path %q", l.spec.Path)
	return deadline.Run(ctx, l.opts.ResolveTimeout, what, l.busy, "hostpath\x00"+l.spec.Path, func(context.Context) (string, error) {
		resolved, err := l.opts.ResolveHostPath(l.spec.Path)
		if err != nil {
			return "", fmt.Errorf("%s: %w", what, err)
		}
		return resolved, nil
	})
}

// judge classifies Dir: a mount there is ours if it is a bind of the resolved
// host path. The path is resolved only when something is mounted at Dir.
func (l *local) judgeTable(ctx context.Context, t observe.Table) (verdict, error) {
	return l.judge(t, func(observe.Mount) (string, error) {
		resolved, err := l.resolve(ctx)
		if err != nil {
			return "", err
		}
		same, err := t.IsBindOf(l.dir, resolved)
		if err != nil {
			return "", err
		}
		if same {
			return "", nil
		}
		return "it is not a bind of the host path", nil
	})
}

func (l *local) State(ctx context.Context) (State, error) {
	t, err := l.m.Mounts(ctx)
	if err != nil {
		return StateEmpty, err
	}
	v, err := l.judgeTable(ctx, t)
	return v.state, err
}

func (l *local) Mounted(ctx context.Context) (bool, error) {
	s, err := l.State(ctx)
	return s == StateOurs, err
}

func (l *local) Mount(ctx context.Context) error {
	t, err := l.m.Mounts(ctx)
	if err != nil {
		return err
	}
	v, err := l.judgeTable(ctx, t)
	if err != nil {
		return err
	}
	switch v.state {
	case StateOurs:
		return nil
	case StateForeign, StateHidden:
		return l.conflict(v)
	}
	resolved, err := l.resolve(ctx)
	if err != nil {
		return err
	}
	if err := l.makeMountPoint(ctx); err != nil {
		return err
	}
	return l.m.Bind(ctx, "", resolved, l.dir, false)
}
