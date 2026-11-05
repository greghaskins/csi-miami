// Package holder is the holder file that gives a chunk-backed volume strong
// single-writer exclusivity across nodes. The file sits on the NFSv3 export
// every node shares, beside the volume directory: O_EXCL create, link(2) and
// rename over an existing name are atomic on the server, and nothing else is
// assumed (there is no conditional rename and no cross-node lock). Each
// ownership has an epoch that only grows; the block server re-reads the file
// before it publishes a generation and refuses unless node and epoch are
// still its own, which is the fencing token.
//
// An epoch is issued exactly once for the life of the volume. Before it writes
// epoch N into the holder file an acquirer must link(2) a durable claim file
// `<name>.holder.epoch-<N>` beside the holder; the link fails with EEXIST when
// another node already owns N, so the acquirer picks a higher N (the highest
// claimed plus one) and tries again. Release leaves the claim files in place,
// so the next first acquire does not reset to 1 but continues above the
// highest epoch ever claimed, and two holders can never share an epoch nor can
// an epoch move backwards. A successful acquire prunes claims older than
// current-1, keeping the current and previous epoch to bound the window.
//
// Clocks differ between nodes, so a holder's age is the server's own: the
// modification time of a probe file made now, minus the holder file's.
//
// A holder renews by touching, not by rewriting: futimens(UTIME_NOW) on a
// descriptor opened when the holder was loaded, so the NFS server stamps the
// time. The descriptor names the very inode we installed. A takeover renames
// a new inode over the name, so a stale holder's late renewal reaches only the
// inode that was replaced (ESTALE, or an inode that no longer is the name's)
// and can never put its own node and epoch back into the file.
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

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
	Adopt(ctx context.Context, dir, name, node string) (*Holder, error)
}

// Real is the Acquirer over the package functions.
type Real struct{}

// Read implements Acquirer.
func (Real) Read(ctx context.Context, dir, name string) (Info, error) { return Read(ctx, dir, name) }

// Acquire implements Acquirer.
func (Real) Acquire(ctx context.Context, dir, name, node string, judge Judgement) (*Holder, error) {
	return Acquire(ctx, dir, name, node, judge)
}

// Adopt implements Acquirer.
func (Real) Adopt(ctx context.Context, dir, name, node string) (*Holder, error) {
	return Adopt(ctx, dir, name, node)
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
	// touch sets the holder file's modification time to the server's now
	// (UTIME_NOW: the NFS client sends SET_TO_SERVER_TIME, so the stamp stays
	// on the server's clock) through the descriptor kept since the file was
	// loaded.
	touch = futimensNow
)

// calls tracks the file system calls that hung: one outstanding per file.
var calls deadline.Set

// futimensNow is futimens(f, UTIME_NOW, UTIME_NOW): utimensat(2) on the
// descriptor with a NULL path, which x/sys/unix has no wrapper for.
func futimensNow(f *os.File) error {
	ts := [2]unix.Timespec{{Nsec: unix.UTIME_NOW}, {Nsec: unix.UTIME_NOW}}
	_, _, errno := unix.Syscall6(unix.SYS_UTIMENSAT, f.Fd(), 0, uintptr(unsafe.Pointer(&ts[0])), 0, 0, 0) //nolint:gosec // fixed-size timespec pair, valid for the call
	if errno != 0 {
		return errno
	}
	return nil
}

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
	s, f, err := loadFile(ctx, dir, name)
	if f != nil {
		_ = f.Close()
	}
	return s, err
}

// loadFile is load that keeps the descriptor it read from open, for the caller
// to close: a caller that takes the file as its own keeps it to touch. The
// descriptor is returned (non-nil) only with ok == true and a nil error.
func loadFile(ctx context.Context, dir, name string) (snapshot, *os.File, error) {
	path := Path(dir, name)
	type result struct {
		s snapshot
		f *os.File
	}
	r, err := step(ctx, path, "read the holder file", func() (result, error) {
		f, err := os.Open(path) //nolint:gosec // built from a validated volume name
		if errors.Is(err, fs.ErrNotExist) {
			return result{}, nil
		}
		if err != nil {
			return result{}, err
		}
		keep := false
		defer func() {
			if !keep {
				_ = f.Close()
			}
		}()
		st, err := f.Stat()
		if err != nil {
			return result{}, err
		}
		b, err := io.ReadAll(io.LimitReader(f, 4096))
		if err != nil {
			return result{}, err
		}
		s := snapshot{mtime: st.ModTime(), ok: true}
		if err := json.Unmarshal(b, &s.file); err != nil {
			return result{}, fmt.Errorf("the holder file is not valid: %w", err)
		}
		// A holder file names a node at an epoch that starts at 1; an empty
		// node or a zero epoch is corruption, not a usable holder and not an
		// absence.
		if s.Node == "" || s.Epoch == 0 {
			return result{}, fmt.Errorf("the holder file is corrupt: node %q, epoch %d", s.Node, s.Epoch)
		}
		keep = true
		return result{s, f}, nil
	})
	if err != nil {
		// A call that timed out may still return later with a descriptor
		// nobody will close; os.File's finalizer closes it.
		return snapshot{}, nil, err
	}
	return r.s, r.f, nil
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
	now, err := step(ctx, Path(dir, name)+":probe", "read the server's clock", func() (time.Time, error) { return probe(dir) })
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

// link hard-links oldpath to newpath atomically; EEXIST (fs.ErrExist) means
// the name already exists and the link did nothing.
func link(ctx context.Context, key, what, oldpath, newpath string) error {
	_, err := step(ctx, key, what, func() (struct{}, error) {
		return struct{}{}, os.Link(oldpath, newpath)
	})
	return err
}

// claimPath is the durable per-epoch claim file beside the holder file.
func claimPath(dir, name string, epoch uint64) string {
	return fmt.Sprintf("%s.epoch-%d", Path(dir, name), epoch)
}

// claimPrefix is the common prefix of every claim file for name in dir.
func claimPrefix(name string) string { return name + ".holder.epoch-" }

// maxClaimedEpoch is the highest epoch any node has ever claimed for name in
// dir, or 0 when none has. Unparsable names are ignored.
func maxClaimedEpoch(ctx context.Context, dir, name string) (uint64, error) {
	prefix := claimPrefix(name)
	return step(ctx, Path(dir, name)+":claims", "list the epoch claims", func() (uint64, error) {
		ents, err := os.ReadDir(dir)
		if err != nil {
			return 0, err
		}
		var mx uint64
		for _, e := range ents {
			n := e.Name()
			if !strings.HasPrefix(n, prefix) {
				continue
			}
			v, err := strconv.ParseUint(n[len(prefix):], 10, 64)
			if err != nil {
				continue
			}
			if v > mx {
				mx = v
			}
		}
		return mx, nil
	})
}

// pruneClaims removes claim files older than keepFrom, best effort: the epoch
// is already ours, so a failure to prune is not a failure to acquire.
func pruneClaims(ctx context.Context, dir, name string, keepFrom uint64) {
	prefix := claimPrefix(name)
	_, _ = step(ctx, Path(dir, name)+":prune", "prune old epoch claims", func() (struct{}, error) {
		ents, err := os.ReadDir(dir)
		if err != nil {
			return struct{}{}, nil
		}
		for _, e := range ents {
			n := e.Name()
			if !strings.HasPrefix(n, prefix) {
				continue
			}
			v, err := strconv.ParseUint(n[len(prefix):], 10, 64)
			if err != nil || v >= keepFrom {
				continue
			}
			_ = os.Remove(filepath.Join(dir, n))
		}
		return struct{}{}, nil
	})
}

// claimEpoch reserves an epoch for this holder with a durable per-epoch claim
// file: it links a temporary file to <name>.holder.epoch-<N> for N one above
// the highest claimed (and above floor, the epoch being superseded). EEXIST
// means another node took N, so it raises the floor past N and tries the next.
// The returned epoch has been claimed by no one else, ever.
func (h *Holder) claimEpoch(ctx context.Context, floor uint64) (uint64, error) {
	for {
		mx, err := maxClaimedEpoch(ctx, h.dir, h.name)
		if err != nil {
			return 0, err
		}
		n := max(mx, floor) + 1
		tmp, err := writeTemp(ctx, h.dir, h.name, file{Node: h.node, Epoch: n})
		if err != nil {
			return 0, err
		}
		cp := claimPath(h.dir, h.name, n)
		err = link(ctx, cp, "claim the epoch", tmp, cp)
		removeTemp(tmp)
		if errors.Is(err, fs.ErrExist) {
			floor = n // guarantee progress even if the listing lagged the claim
			hook("claim-lost")
			continue
		}
		if err != nil {
			return 0, err
		}
		hook("claimed")
		return n, nil
	}
}

// Holder is a holder file we own, at the epoch we took it.
type Holder struct {
	dir, name, node string
	epoch           uint64
	lost            atomic.Bool

	// fmu guards fd, the descriptor on the inode this holder installed (or
	// adopted), which Renew touches. It is closed by Release, by Close and
	// when Renew finds the holder lost.
	fmu sync.Mutex
	fd  *os.File
}

// setFile keeps f as the descriptor to touch, closing any earlier one.
func (h *Holder) setFile(f *os.File) {
	h.fmu.Lock()
	defer h.fmu.Unlock()
	if h.fd != nil {
		_ = h.fd.Close()
	}
	h.fd = f
}

// Close closes the descriptor Renew touches. It is idempotent and does not
// release the holder file: the ownership stays in the store.
func (h *Holder) Close() error {
	h.fmu.Lock()
	defer h.fmu.Unlock()
	if h.fd == nil {
		return nil
	}
	err := h.fd.Close()
	h.fd = nil
	return err
}

// Epoch is the epoch of this ownership, the fencing token.
func (h *Holder) Epoch() uint64 { return h.epoch }

func (h *Holder) mine(s snapshot) bool {
	return s.ok && s.Node == h.node && s.Epoch == h.epoch
}

// sameHolderAs reports whether this fresh snapshot is the very holder info
// describes: same node, epoch and renewal count.
func (s snapshot) sameHolderAs(info Info) bool {
	return s.ok && s.Node == info.Node && s.Epoch == info.Epoch && s.Renewals == info.Renewals
}

// Acquire takes the holder file of name in dir for node. With no holder it
// creates the file at an epoch above every one ever claimed for the volume,
// atomically (a temporary file linked to the final name, which fails if
// someone else won). With a holder it takes the file over only if judge says
// so, and only if the holder it finds is still exactly the one judge judged
// (same node, epoch and renewals); otherwise it fails with ErrHeld, and the
// next pass re-judges the holder as it stands.
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
	// The judgement is bound to the holder it judged: a renewal or a takeover
	// landing between the judge read and here means the holder is no longer
	// the one that was judged stale, so we refuse rather than break a holder
	// that may now be live.
	if !judge.Info.Present || !cur.sameHolderAs(judge.Info) {
		return nil, fmt.Errorf("%w: the holder changed since it was judged (now %s at epoch %d, %d renewals); re-judge", ErrHeld, cur.Node, cur.Epoch, cur.Renewals)
	}
	return h.takeOver(ctx, judge.Info)
}

// Adopt returns a Holder for a holder file that already names node, carrying
// the file's existing epoch: after a plugin restart the ownership continues
// where it was, so Renew, Mine and Release work as on an acquired Holder.
// Adopt never claims or increments an epoch; only a takeover issues a new one.
// It fails with ErrHeld when the file names another node (at whatever epoch),
// with ErrLost when there is no holder file (nothing to adopt; Acquire it), and
// with load's error when the file is corrupt or unreadable.
func Adopt(ctx context.Context, dir, name, node string) (*Holder, error) {
	if node == "" || strings.ContainsAny(name, "/\x00") || name == "" {
		return nil, errors.New("holder: node and name are required, and the name is one path element")
	}
	cur, f, err := loadFile(ctx, dir, name)
	if err != nil {
		return nil, err
	}
	if !cur.ok {
		return nil, fmt.Errorf("%w: there is no holder file to adopt", ErrLost)
	}
	if cur.Node != node {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s at epoch %d", ErrHeld, cur.Node, cur.Epoch)
	}
	return &Holder{dir: dir, name: name, node: node, epoch: cur.Epoch, fd: f}, nil
}

func (h *Holder) create(ctx context.Context) (*Holder, error) {
	epoch, err := h.claimEpoch(ctx, 0)
	if err != nil {
		return nil, err
	}
	h.epoch = epoch
	tmp, err := writeTemp(ctx, h.dir, h.name, file{Node: h.node, Epoch: epoch})
	if err != nil {
		return nil, err
	}
	defer removeTemp(tmp)
	hook("created")
	err = link(ctx, Path(h.dir, h.name), "link the holder file", tmp, Path(h.dir, h.name))
	if errors.Is(err, fs.ErrExist) {
		return nil, h.heldBy(ctx)
	}
	if err != nil {
		return nil, err
	}
	if err := h.confirm(ctx); err != nil {
		return nil, err
	}
	pruneClaims(ctx, h.dir, h.name, epoch-1)
	return h, nil
}

func (h *Holder) takeOver(ctx context.Context, judged Info) (*Holder, error) {
	// Claim an epoch strictly above the one being superseded. The claim is
	// durable, so even a takeover we then abandon cannot be reissued.
	epoch, err := h.claimEpoch(ctx, judged.Epoch)
	if err != nil {
		return nil, err
	}
	h.epoch = epoch
	tmp, err := writeTemp(ctx, h.dir, h.name, file{Node: h.node, Epoch: epoch})
	if err != nil {
		return nil, err
	}
	defer removeTemp(tmp)
	hook("written")
	// The file we replace must still be the one that was judged: the epoch
	// never decreases, and a holder renewed or another taker in between means
	// the judgement is out of date. (NFSv3 has no conditional rename, so a one
	// round-trip window remains between this check and the rename, accepted by
	// the design: a taker that loses it is caught by its own confirm, and the
	// fencing token stops either from publishing.)
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
	if err := h.confirm(ctx); err != nil {
		return nil, err
	}
	pruneClaims(ctx, h.dir, h.name, epoch-1)
	return h, nil
}

// confirm re-reads the file after we replaced or created it and checks that it
// is ours, which it is not if a taker replaced it right after us.
//
// The file read is kept open as the descriptor Renew touches. It is the inode
// the name has now; one that replaces it after this read is a taker's, and
// Renew finds that by comparing inodes.
func (h *Holder) confirm(ctx context.Context) error {
	s, f, err := loadFile(ctx, h.dir, h.name)
	if err != nil {
		return err
	}
	if !h.mine(s) {
		if f != nil {
			_ = f.Close()
		}
		return h.heldBy(ctx)
	}
	h.setFile(f)
	return nil
}

func (h *Holder) heldBy(ctx context.Context) error {
	s, err := load(ctx, h.dir, h.name)
	if err != nil || !s.ok {
		return fmt.Errorf("%w: another node took it first", ErrHeld)
	}
	return fmt.Errorf("%w: %s at epoch %d", ErrHeld, s.Node, s.Epoch)
}

// Renew refreshes the holder file's modification time. It reads the file
// afresh and requires it to still name this node at this epoch, requires the
// name to still be the inode this holder installed (a fresh read of the path
// against the descriptor kept from the load), and then sets the time through
// that descriptor with futimens(UTIME_NOW), so the NFS server stamps it, and
// looks at the name once more. It writes no content and renames nothing.
//
// A takeover renames a new inode over the name, so a holder that lost the
// volume can only touch the inode that was replaced: the taker's file is never
// altered, whatever the order of the two. ESTALE on the touch (the replaced
// inode is gone on the server), an inode that is not the name's, or content
// that is not ours is ErrLost, and the Holder then stays lost with its
// descriptor closed. Any other error is returned as it is and the ownership
// is as it was.
func (h *Holder) Renew(ctx context.Context) error {
	if h.lost.Load() {
		return ErrLost
	}
	h.fmu.Lock()
	defer h.fmu.Unlock()
	if h.fd == nil {
		// Nothing was installed or adopted by this value: it cannot renew.
		return ErrLost
	}
	lose := func() error {
		h.lost.Store(true)
		_ = h.fd.Close()
		h.fd = nil
		return ErrLost
	}
	s, f, err := loadFile(ctx, h.dir, h.name)
	if err != nil {
		return err
	}
	if f != nil {
		defer func() { _ = f.Close() }()
	}
	if !h.mine(s) {
		return lose()
	}
	same, err := sameInode(h.fd, f)
	if err != nil {
		return err
	}
	if !same {
		return lose()
	}
	hook("renew-checked")
	_, err = step(ctx, Path(h.dir, h.name), "touch the holder file", func() (struct{}, error) {
		return struct{}{}, touch(h.fd)
	})
	switch {
	case errors.Is(err, syscall.ESTALE):
		return lose()
	case err != nil:
		return err
	}
	// A takeover that renamed its inode in after the check leaves the touch on
	// the inode it replaced (which a local file system still accepts): the name
	// must still be ours after it.
	after, err := step(ctx, Path(h.dir, h.name), "stat the holder file", func() (os.FileInfo, error) {
		return os.Stat(Path(h.dir, h.name))
	})
	if errors.Is(err, fs.ErrNotExist) {
		return lose()
	}
	if err != nil {
		return err
	}
	mine, err := h.fd.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(mine, after) {
		return lose()
	}
	return nil
}

// sameInode reports whether a and b are descriptors on the same file.
func sameInode(a, b *os.File) (bool, error) {
	sa, err := a.Stat()
	if err != nil {
		if errors.Is(err, syscall.ESTALE) {
			return false, nil
		}
		return false, err
	}
	sb, err := b.Stat()
	if err != nil {
		return false, err
	}
	return os.SameFile(sa, sb), nil
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
	defer func() { _ = h.Close() }()
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
