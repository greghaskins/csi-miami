// Package holder is the holder file that gives a chunk-backed volume strong
// single-writer exclusivity across nodes. The file sits on the NFSv3 export
// every node shares, beside the volume directory: O_EXCL create, link(2) and
// rename over an existing name are atomic on the server, and nothing else is
// assumed (there is no conditional rename and no cross-node lock). Each
// ownership has an epoch that only grows; the block server re-reads the file
// before it publishes a generation and refuses unless node and epoch are
// still its own, which is the fencing token.
//
// Clocks differ between nodes, so a holder's age is the server's own: the
// modification time of a probe file made now, minus the holder file's.
package holder

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/greghaskins/csi-miami/internal/deadline"
	"github.com/greghaskins/csi-miami/internal/lock"
)

var (
	// ErrHeld means another holder owns the volume and is not to be taken over.
	ErrHeld = errors.New("the volume is held by another node")
	// ErrLost means the holder file is no longer ours: another node took the
	// volume over, or the file is gone.
	ErrLost = errors.New("the holder file is no longer ours")
)

// CallTimeout bounds each file system call when the context has no earlier
// deadline.
const CallTimeout = 30 * time.Second

// Info is what the holder file says, and how old it is.
type Info struct {
	Node     string
	Epoch    uint64
	Renewals uint64
	// Age is on the server's clock: a probe file's modification time minus the
	// holder file's. Zero when not Present.
	Age     time.Duration
	Present bool
}

// Checker answers whether the holder file is still ours. *Holder implements it.
type Checker interface {
	Mine(ctx context.Context) (bool, error)
}

// Acquirer is what the volume item uses to look at and take the holder file.
type Acquirer interface {
	Read(ctx context.Context, dir, name string) (Info, error)
	Acquire(ctx context.Context, dir, name, node string, judge Judgement) (*Holder, error)
}

// Real is the Acquirer over the package functions.
type Real struct{}

// Read implements Acquirer.
func (Real) Read(ctx context.Context, dir, name string) (Info, error) { return Read(ctx, dir, name) }

// Acquire implements Acquirer.
func (Real) Acquire(ctx context.Context, dir, name, node string, judge Judgement) (*Holder, error) {
	return Acquire(ctx, dir, name, node, judge)
}

// file is the JSON content of the holder file.
type file struct {
	Node     string `json:"node"`
	Epoch    uint64 `json:"epoch"`
	Renewals uint64 `json:"renewals"`
}

// Seams for tests: the server's clock, and a point between the steps of an
// operation at which a test can play another node.
var (
	probe = lock.ProbeClock
	hook  = func(string) {}
)

// calls tracks the file system calls that hung: one outstanding per file.
var calls deadline.Set

// Path is the holder file of the volume name in dir: beside the volume's
// directory, not inside it.
func Path(dir, name string) string { return filepath.Join(dir, name+".holder") }

func step[T any](ctx context.Context, key, what string, fn func() (T, error)) (T, error) {
	return deadline.Run(ctx, CallTimeout, what, &calls, key, func(context.Context) (T, error) { return fn() })
}

func randomSuffix() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("holder: no randomness: " + err.Error()) // crypto/rand does not fail on supported systems
	}
	return hex.EncodeToString(b[:])
}

// snapshot is one fresh read of the file.
type snapshot struct {
	file
	mtime time.Time
	ok    bool
}

// load opens the file afresh (so that the client revalidates), stats and
// reads it. A missing file is ok == false with a nil error.
func load(ctx context.Context, dir, name string) (snapshot, error) {
	path := Path(dir, name)
	return step(ctx, path, "read the holder file", func() (snapshot, error) {
		f, err := os.Open(path) //nolint:gosec // built from a validated volume name
		if errors.Is(err, fs.ErrNotExist) {
			return snapshot{}, nil
		}
		if err != nil {
			return snapshot{}, err
		}
		defer func() { _ = f.Close() }()
		st, err := f.Stat()
		if err != nil {
			return snapshot{}, err
		}
		b, err := io.ReadAll(io.LimitReader(f, 4096))
		if err != nil {
			return snapshot{}, err
		}
		s := snapshot{mtime: st.ModTime(), ok: true}
		if err := json.Unmarshal(b, &s.file); err != nil {
			return snapshot{}, fmt.Errorf("the holder file is not valid: %w", err)
		}
		return s, nil
	})
}

// Read reads the holder file afresh. Present is false, with a nil error, when
// there is none.
func Read(ctx context.Context, dir, name string) (Info, error) {
	s, err := load(ctx, dir, name)
	if err != nil || !s.ok {
		return Info{}, err
	}
	// The probe is taken after the stat: a renewal landing in between can only
	// make the age smaller, never larger.
	now, err := step(ctx, dir+"\x00probe", "read the server's clock", func() (time.Time, error) { return probe(dir) })
	if err != nil {
		return Info{}, err
	}
	age := max(now.Sub(s.mtime), 0)
	return Info{Node: s.Node, Epoch: s.Epoch, Renewals: s.Renewals, Age: age, Present: true}, nil
}

// writeTemp writes content to a new temporary file beside the holder file and
// returns its path.
func writeTemp(ctx context.Context, dir, name string, content file) (string, error) {
	path := Path(dir, name) + ".tmp-" + randomSuffix()
	b, err := json.Marshal(content)
	if err != nil {
		return "", err
	}
	b = append(b, '\n')
	_, err = step(ctx, Path(dir, name), "write the holder file", func() (struct{}, error) {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644) //nolint:gosec // other nodes' users must read the holder file
		if err != nil {
			return struct{}{}, err
		}
		_, werr := f.Write(b)
		// Close is where an NFS client reports a failed write.
		cerr := f.Close()
		if err := errors.Join(werr, cerr); err != nil {
			_ = os.Remove(path)
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
	return path, err
}

// removeTemp removes a temporary file; it is best effort and bounded by its
// own deadline, since the context may be the reason we are here.
func removeTemp(path string) {
	ctx, cancel := context.WithTimeout(context.Background(), CallTimeout)
	defer cancel()
	_, _ = step(ctx, path, "remove a temporary holder file", func() (struct{}, error) {
		err := os.Remove(path)
		if errors.Is(err, fs.ErrNotExist) {
			err = nil
		}
		return struct{}{}, err
	})
}

func rename(ctx context.Context, dir, name, tmp string) error {
	_, err := step(ctx, Path(dir, name), "replace the holder file", func() (struct{}, error) {
		return struct{}{}, os.Rename(tmp, Path(dir, name))
	})
	return err
}

// Holder is a holder file we own, at the epoch we took it.
type Holder struct {
	dir, name, node string
	epoch           uint64
	lost            atomic.Bool
}

// Epoch is the epoch of this ownership, the fencing token.
func (h *Holder) Epoch() uint64 { return h.epoch }

func (h *Holder) mine(s snapshot) bool {
	return s.ok && s.Node == h.node && s.Epoch == h.epoch
}

// Acquire takes the holder file of name in dir for node. With no holder it
// creates the file at epoch 1, atomically (a temporary file linked to the
// final name, which fails if someone else won). With a holder it takes the
// file over, at epoch + 1, only if judge says so; otherwise it fails with
// ErrHeld.
func Acquire(ctx context.Context, dir, name, node string, judge Judgement) (*Holder, error) {
	if node == "" || strings.ContainsAny(name, "/\x00") || name == "" {
		return nil, errors.New("holder: node and name are required, and the name is one path element")
	}
	cur, err := load(ctx, dir, name)
	if err != nil {
		return nil, err
	}
	h := &Holder{dir: dir, name: name, node: node}
	if !cur.ok {
		return h.create(ctx)
	}
	if !judge.Takeover {
		return nil, fmt.Errorf("%w: %s at epoch %d (%s)", ErrHeld, cur.Node, cur.Epoch, judge.Reason)
	}
	return h.takeOver(ctx, cur)
}

func (h *Holder) create(ctx context.Context) (*Holder, error) {
	h.epoch = 1
	tmp, err := writeTemp(ctx, h.dir, h.name, file{Node: h.node, Epoch: 1})
	if err != nil {
		return nil, err
	}
	defer removeTemp(tmp)
	hook("created")
	_, err = step(ctx, Path(h.dir, h.name), "link the holder file", func() (struct{}, error) {
		return struct{}{}, os.Link(tmp, Path(h.dir, h.name))
	})
	if errors.Is(err, fs.ErrExist) {
		return nil, h.heldBy(ctx)
	}
	if err != nil {
		return nil, err
	}
	return h, h.confirm(ctx)
}

func (h *Holder) takeOver(ctx context.Context, judged snapshot) (*Holder, error) {
	h.epoch = judged.Epoch + 1
	tmp, err := writeTemp(ctx, h.dir, h.name, file{Node: h.node, Epoch: h.epoch})
	if err != nil {
		return nil, err
	}
	defer removeTemp(tmp)
	hook("written")
	// The file we replace must still be the one that was judged: the epoch
	// never decreases, and a holder renewed or another taker in between means
	// the judgement is out of date.
	now, err := load(ctx, h.dir, h.name)
	if err != nil {
		return nil, err
	}
	switch {
	case !now.ok:
		return nil, fmt.Errorf("%w: the holder file vanished while it was being taken over; try again", ErrHeld)
	case now.Epoch > judged.Epoch:
		return nil, fmt.Errorf("%w: epoch %d was taken by %s after the judgement at epoch %d; the epoch never decreases", ErrHeld, now.Epoch, now.Node, judged.Epoch)
	case now.Node != judged.Node || now.Renewals != judged.Renewals:
		return nil, fmt.Errorf("%w: %s renewed at epoch %d after the judgement", ErrHeld, now.Node, now.Epoch)
	}
	if err := rename(ctx, h.dir, h.name, tmp); err != nil {
		return nil, err
	}
	return h, h.confirm(ctx)
}

// confirm re-reads the file after we replaced or created it and checks that it
// is ours, which it is not if a taker replaced it right after us.
func (h *Holder) confirm(ctx context.Context) error {
	s, err := load(ctx, h.dir, h.name)
	if err != nil {
		return err
	}
	if !h.mine(s) {
		return h.heldBy(ctx)
	}
	return nil
}

func (h *Holder) heldBy(ctx context.Context) error {
	s, err := load(ctx, h.dir, h.name)
	if err != nil || !s.ok {
		return fmt.Errorf("%w: another node took it first", ErrHeld)
	}
	return fmt.Errorf("%w: %s at epoch %d", ErrHeld, s.Node, s.Epoch)
}

// Renew rewrites the holder file with one more renewal: a temporary file
// renamed over it. ErrLost when the file is no longer ours, before the write
// or after the rename (a takeover raced it); the Holder then stays lost.
func (h *Holder) Renew(ctx context.Context) error {
	if h.lost.Load() {
		return ErrLost
	}
	s, err := load(ctx, h.dir, h.name)
	if err != nil {
		return err
	}
	if !h.mine(s) {
		h.lost.Store(true)
		return ErrLost
	}
	tmp, err := writeTemp(ctx, h.dir, h.name, file{Node: h.node, Epoch: h.epoch, Renewals: s.Renewals + 1})
	if err != nil {
		return err
	}
	defer removeTemp(tmp)
	hook("renew-written")
	if err := rename(ctx, h.dir, h.name, tmp); err != nil {
		return err
	}
	hook("renew-renamed")
	after, err := load(ctx, h.dir, h.name)
	if err != nil {
		return err
	}
	if !h.mine(after) {
		h.lost.Store(true)
		return ErrLost
	}
	return nil
}

// Mine re-reads the file, every call, and reports whether it names our node
// at our epoch. The block server asks before it publishes a generation.
func (h *Holder) Mine(ctx context.Context) (bool, error) {
	if h.lost.Load() {
		return false, nil
	}
	s, err := load(ctx, h.dir, h.name)
	if err != nil {
		return false, err
	}
	return h.mine(s), nil
}

// Release removes the holder file only if it is still ours; otherwise, and
// when it is already gone, it does nothing. The Holder is finished either way.
func (h *Holder) Release(ctx context.Context) error {
	defer h.lost.Store(true)
	s, err := load(ctx, h.dir, h.name)
	if err != nil || !h.mine(s) {
		return err
	}
	_, err = step(ctx, Path(h.dir, h.name), "remove the holder file", func() (struct{}, error) {
		err := os.Remove(Path(h.dir, h.name))
		if errors.Is(err, fs.ErrNotExist) {
			err = nil
		}
		return struct{}{}, err
	})
	return err
}

var _ Checker = (*Holder)(nil)
var _ Acquirer = Real{}
