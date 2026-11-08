package volume

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/greghaskins/csi-miami/internal/cache"
	"github.com/greghaskins/csi-miami/internal/crypt"
	"github.com/greghaskins/csi-miami/internal/ext4"
	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/intent"
	"github.com/greghaskins/csi-miami/internal/item"
	"github.com/greghaskins/csi-miami/internal/nbd"
	"github.com/greghaskins/csi-miami/internal/observe"
	"github.com/greghaskins/csi-miami/internal/prereq"
	"github.com/greghaskins/csi-miami/internal/safefs"
	"github.com/greghaskins/csi-miami/internal/systemd"
)

// fencedReason is the judgement's reason for a holder file that names this
// node while the store has moved on to a newer epoch.
const fencedReason = "own ownership fenced by a newer generation"

// Converge runs the seven layers; see the package comment.
func (it *Item) Converge(ctx context.Context, in intent.Intent, obs item.Observed) error {
	v, err := it.volOf(in)
	if err != nil {
		return err
	}
	if err := it.checkPrereq(v, obs.Prereq); err != nil {
		return err
	}
	if err := it.checkBudget(ctx, v); err != nil {
		return err
	}
	st := it.state(in.VolumeID)
	st.fz.Lock()
	st.tearingDown, st.gone = false, false // a restage after an unstage that failed or finished
	st.fz.Unlock()

	// Layer 2, first half: the holder. It comes before anything is read from
	// the store that decides what to mount and at which epoch.
	h, err := it.ensureHolder(ctx, v, st)
	if err != nil {
		return err
	}

	// Then `current`, fresh, and the stage-time fence: if the store has moved
	// to a newer epoch than the one we hold, our ownership is already fenced,
	// whatever the holder file says, and no server may be started at it.
	cur, err := it.readCurrent(ctx, v)
	if err != nil {
		return err
	}
	// The size is the largest any record holds, so a restage never brings the
	// volume up smaller than it was grown to.
	if v, err = it.raiseSize(ctx, v, st, cur); err != nil {
		return err
	}
	if cur.ok && cur.epoch > h.Epoch() {
		if err := it.refuseIfBound(v, obs.Mounts); err != nil {
			return err
		}
		if h, err = it.refence(ctx, v, st, cur); err != nil {
			return err
		}
		if obs.Mounts, err = it.cfg.Mounter.Mounts(ctx); err != nil {
			return fmt.Errorf("volume item %s: %w: %w", in.VolumeID, item.ErrObserve, err)
		}
	}

	// The other direction of the same fence: a server that runs under an epoch
	// other than the one we hold (we lost the volume, and took it back at a
	// higher epoch) refuses every publish and serves a cache of another
	// lineage. It is stopped and restarted at the held epoch, and its cache is
	// dropped, exactly as for a fenced ownership.
	if it.serverEpochStale(ctx, v, h) {
		if err := it.refuseIfBound(v, obs.Mounts); err != nil {
			return err
		}
		// Stop it; whether its cache is kept is decideCache's call when the
		// server is started again, the same rule as at any other start.
		if err := it.stopStack(ctx, v, st); err != nil {
			return err
		}
		if obs.Mounts, err = it.cfg.Mounter.Mounts(ctx); err != nil {
			return fmt.Errorf("volume item %s: %w: %w", in.VolumeID, item.ErrObserve, err)
		}
	}

	if err := it.ensureBinary(); err != nil {
		return fmt.Errorf("volume item %s: %w", in.VolumeID, err)
	}
	dev, err := it.ensureServer(ctx, v, st, h, cur)
	if err != nil {
		return err
	}
	blk, err := it.ensureMapping(ctx, v, dev, cur, obs.Keys)
	if err != nil {
		return err
	}
	st.blk = blk
	if err := it.ensureMounted(ctx, v, st, blk, it.devPath(dev.Index), cur, obs.Mounts); err != nil {
		return err
	}
	return it.ensureSize(ctx, v, st, blk)
}

// ---- layer 1: prerequisites ----

// checkPrereq refuses a volume on a node whose probe found a feature missing
// that this volume needs, before anything is started or created. The cipher
// and the mapper matter only to an encrypted volume.
func (it *Item) checkPrereq(v vol, rep prereq.Report) error {
	need := map[string]bool{prereq.FeatureNBD: true, prereq.FeatureSystemdSocket: true, prereq.FeatureCacheDir: true}
	if v.params.Encryption {
		need[prereq.FeatureDeviceMapper] = true
		need[prereq.FeatureAdiantum] = strings.Contains(v.params.Cipher, "adiantum")
	}
	var missing []string
	for _, f := range rep.Features {
		if f.Err != nil && need[f.Name] {
			missing = append(missing, f.Name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("volume item %s: %w: %s", v.in.VolumeID, item.ErrPrerequisiteMissing, strings.Join(missing, ", "))
	}
	return nil
}

// ---- layer 2: holder ----

// ensureHolder returns the volume's holder: renewed when this process already
// holds it, otherwise adopted when the holder file names this node (a plugin
// restart continues the ownership at its epoch) or acquired.
func (it *Item) ensureHolder(ctx context.Context, v vol, st *vstate) (*holder.Holder, error) {
	if st.h != nil {
		err := st.h.Renew(ctx)
		switch {
		case err == nil:
			return st.h, nil
		case !errors.Is(err, holder.ErrLost):
			// The transport did not answer; the ownership is as it was.
			return nil, fmt.Errorf("volume item %s: renewing the holder file: %w", v.in.VolumeID, err)
		}
		it.log.Warn("the holder file is no longer ours", "volume", v.in.VolumeID)
		st.h = nil
	}
	h, err := it.acquire(ctx, v, current{})
	if err != nil {
		return nil, err
	}
	st.h = h
	return h, nil
}

// acquire adopts or acquires the holder file. cur is what the store's `current`
// says when the caller has read it, and is only used for the fence rule.
func (it *Item) acquire(ctx context.Context, v vol, cur current) (*holder.Holder, error) {
	info, err := it.cfg.Holder.Read(ctx, v.holdDir, v.name)
	if err != nil {
		return nil, fmt.Errorf("volume item %s: reading the holder file: %w", v.in.VolumeID, err)
	}
	if info.Present && info.Node == it.cfg.Node && (!cur.ok || cur.epoch <= info.Epoch) {
		h, err := it.cfg.Holder.Adopt(ctx, v.holdDir, v.name, it.cfg.Node)
		switch {
		case err == nil:
			return h, nil
		case errors.Is(err, holder.ErrHeld):
			return nil, fmt.Errorf("volume item %s: %w: %w", v.in.VolumeID, item.ErrHolderContended, err)
		case !errors.Is(err, holder.ErrLost):
			return nil, fmt.Errorf("volume item %s: adopting the holder file: %w", v.in.VolumeID, err)
		}
		// The file vanished between the read and the adoption: acquire it.
	}
	j := it.judge(ctx, v, info, cur)
	h, err := it.cfg.Holder.Acquire(ctx, v.holdDir, v.name, it.cfg.Node, j)
	switch {
	case err == nil:
		if j.Takeover && j.Info.Present && j.Info.Node != it.cfg.Node && it.cfg.Takeover != nil {
			it.cfg.Takeover(v.in.VolumeID, j)
		}
		return h, nil
	case errors.Is(err, holder.ErrHeld):
		return nil, fmt.Errorf("volume item %s: %w: %w (%s)", v.in.VolumeID, item.ErrHolderContended, err, j.Reason)
	}
	return nil, fmt.Errorf("volume item %s: acquiring the holder file: %w", v.in.VolumeID, err)
}

// judge decides whether the holder found may be replaced. This item's one rule
// comes first: a holder file that names this node at an epoch older than the
// store's `current` is a fenced ownership of our own (another node took the
// volume over and published, and a late renewal put our older claim back), and
// the only way out is to take our own file over, at an epoch above everything
// claimed. Otherwise only a holder that names another node is put to
// Config.Judge; a holder that names this node and is not fenced is adopted
// before this is reached, and a missing file needs no judgement.
func (it *Item) judge(ctx context.Context, v vol, info holder.Info, cur current) holder.Judgement {
	if info.Present && info.Node == it.cfg.Node && cur.ok && cur.epoch > info.Epoch {
		return holder.Judgement{Takeover: true, Reason: fencedReason, Info: info}
	}
	var j holder.Judgement
	if it.cfg.Judge != nil && info.Present && info.Node != it.cfg.Node {
		j = it.cfg.Judge(ctx, v.in.VolumeID, info)
	}
	if j.Takeover && !j.Info.Present {
		j.Info = info
	}
	return j
}

// refence handles the stage-time fence: the epoch we hold is below `current`'s.
// Whatever serves at the fenced epoch is stopped (it refuses every publish
// anyway), the cache is discarded (its lineage is not the newest), and the
// volume is acquired afresh at an epoch above `current`'s, to be started at.
func (it *Item) refence(ctx context.Context, v vol, st *vstate, cur current) (*holder.Holder, error) {
	it.log.Warn("our ownership is fenced by a newer generation; starting over at a new epoch", "volume", v.in.VolumeID, "currentEpoch", cur.epoch)
	// The cache is not discarded here: when the server is started again at
	// the new epoch, decideCache keeps it if current proves it is the newest
	// data and removes it, reporting any unpublished chunks, if not.
	if err := it.stopStack(ctx, v, st); err != nil {
		return nil, err
	}
	if st.h != nil {
		_ = st.h.Close()
	}
	st.h = nil
	h, err := it.acquire(ctx, v, cur)
	if err != nil {
		return nil, err
	}
	if h.Epoch() <= cur.epoch {
		return nil, fmt.Errorf("volume item %s: %w: epoch %d is not above the current generation's %d", v.in.VolumeID, item.ErrHolderContended, h.Epoch(), cur.epoch)
	}
	st.h = h
	return h, nil
}

// stopStack stops everything raised above the store: the mount, the mapping,
// the server and its device. The cache stays.
func (it *Item) stopStack(ctx context.Context, v vol, st *vstate) error {
	// Whatever read of the old device the probe has in flight is not about the
	// stack that replaces this one (ProbeDevice's stack number).
	it.mu.Lock()
	st.stack++
	it.mu.Unlock()
	if err := it.unmountAndClose(ctx, v, st); err != nil {
		return err
	}
	return it.stopServerAndUnit(ctx, v)
}

// refuseIfBound refuses to tear the stage down while it is still bound into
// pods (another mount of the same device): the fence is real, but unmounting
// under a running pod is for the operator and the pod's end, not for a
// reconcile pass. The condition is publish-refused; the next pass looks again,
// and the teardown happens once the binds are gone.
func (it *Item) refuseIfBound(v vol, mounts observe.Table) error {
	own := it.ownsStage(v)
	top, mounted := mounts.At(v.stage)
	for _, m := range mounts {
		if m.MountPoint == v.stage {
			continue
		}
		// A bind of the stage is the same device again: by device numbers
		// when the stage is mounted, and by the device it is made from either
		// way, so a stage already unmounted does not hide a pod's bind.
		sameNumbers := mounted && m.ID != top.ID && m.FSType == top.FSType && m.Major == top.Major && m.Minor == top.Minor
		if ours, _ := own(mounts, m); sameNumbers || ours {
			return fmt.Errorf("volume item %s: %w: the ownership is fenced but the stage is still bound into a pod; leaving it until the binds are gone", v.in.VolumeID, item.ErrPublishRefused)
		}
	}
	return nil
}

// serverEpochStale reports whether the volume's unit is running and its server
// says it publishes under an epoch other than h's. A server that does not
// answer is not stale: layer 4 reports it.
func (it *Item) serverEpochStale(ctx context.Context, v vol, h *holder.Holder) bool {
	us, err := it.cfg.Systemd.State(ctx, v.unit)
	if err != nil || !us.Present || us.Active != "active" {
		return false
	}
	st, err := it.status(ctx, v)
	return err == nil && st.Epoch != h.Epoch()
}

// ---- layer 3: binary ----

// binPath is the host copy of the binary the units execute.
func (it *Item) binPath() string {
	return filepath.Join(it.cfg.BinDir, "miami-"+it.cfg.BinaryHash)
}

// binMemo is what ensureBinary has verified: the running binary's hash, keyed
// by the file it was computed from, and the host copy's identity once it was
// found to match.
type binMemo struct {
	srcKey, dstKey fileKey
	srcSum         [sha256.Size]byte
	haveSrc, dstOK bool
}

// fileKey identifies a file's content cheaply: stat fields only.
type fileKey struct {
	size int64
	mod  time.Time
}

func keyOf(fi os.FileInfo) fileKey { return fileKey{fi.Size(), fi.ModTime()} }

// hashFile is the SHA-256 of a file.
func hashFile(f *os.File) ([sha256.Size]byte, error) {
	var sum [sha256.Size]byte
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return sum, err
	}
	copy(sum[:], h.Sum(nil))
	return sum, nil
}

// ensureBinary makes the host copy: present with the content of the running
// binary, else copied to a temporary name and renamed into place with mode
// 0755. The comparison is by content hash, not by name and size: two builds
// can be the same size, and a unit must run the binary that is staging it. The
// hashes are computed once per file identity, not on every pass. A copy that
// matches is never rewritten, so a running unit's binary is never changed
// under it.
func (it *Item) ensureBinary() error {
	dst := it.binPath()
	src, err := os.Open(it.cfg.Exe)
	if err != nil {
		return fmt.Errorf("opening the running binary: %w", err)
	}
	defer func() { _ = src.Close() }()
	si, err := src.Stat()
	if err != nil {
		return fmt.Errorf("the running binary: %w", err)
	}
	m := &it.bin
	if !m.haveSrc || m.srcKey != keyOf(si) {
		if m.srcSum, err = hashFile(src); err != nil {
			return fmt.Errorf("hashing the running binary: %w", err)
		}
		m.srcKey, m.haveSrc, m.dstOK = keyOf(si), true, false
		hexSum := hex.EncodeToString(m.srcSum[:])
		it.srcHex.Store(&hexSum)
		if _, err := src.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("the running binary: %w", err)
		}
	}
	if di, err := os.Stat(dst); err == nil && di.Mode().IsRegular() && di.Mode().Perm()&0o111 != 0 {
		if m.dstOK && m.dstKey == keyOf(di) {
			return nil
		}
		if f, err := os.Open(dst); err == nil { //nolint:gosec // the item's own host copy
			sum, herr := hashFile(f)
			_ = f.Close()
			if herr == nil && sum == m.srcSum {
				m.dstKey, m.dstOK = keyOf(di), true
				return nil
			}
		}
	}
	if err := os.MkdirAll(it.cfg.BinDir, 0o755); err != nil { //nolint:gosec // the host directory the units execute from
		return fmt.Errorf("making the binary directory: %w", err)
	}
	tmp, err := os.CreateTemp(it.cfg.BinDir, ".miami-copy-*")
	if err != nil {
		return fmt.Errorf("copying the binary: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := io.Copy(tmp, src); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("copying the binary: %w", err)
	}
	if err := tmp.Chmod(0o755); err != nil { //nolint:gosec // an executable the unit runs
		_ = tmp.Close()
		return fmt.Errorf("copying the binary: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("copying the binary: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("copying the binary: %w", err)
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return fmt.Errorf("installing the binary: %w", err)
	}
	if di, err := os.Stat(dst); err == nil {
		m.dstKey, m.dstOK = keyOf(di), true
	}
	return nil
}

// ---- layer 4: unit and device ----

// unitFor is the block server's unit: the host binary's blockd subcommand with
// the volume's geometry and the epoch it publishes under, restarted by systemd
// a second after it fails. No key is anywhere in it.
func (it *Item) unitFor(v vol, h *holder.Holder) systemd.Unit {
	itoa := func(n int64) string { return strconv.FormatInt(n, 10) }
	return systemd.Unit{
		Name:        v.unit,
		Description: "csi-miami block server for " + v.name,
		ExecStart: []string{
			it.binPath(), "blockd",
			"-volume", v.name, "-mount", v.mount, "-store", v.storeDir, "-cache-dir", v.cacheDir, "-control", v.socket,
			"-size", itoa(v.size), "-chunk-size", itoa(v.chunk), "-cache-bound", itoa(v.bound),
			"-epoch", strconv.FormatUint(h.Epoch(), 10), "-node", it.cfg.Node,
		},
		Restart:      "on-failure",
		RestartDelay: RestartDelay,
		Sandbox:      blockdSandbox(it.cfg.CacheDir, v.holdDir),
	}
}

// blockdSandbox confines the block server, which runs as root on files from a
// shared export. It writes in the node's cache directory (the volume's cache,
// the lock file beside it and the control socket are all in it) and, on the
// transport, below the directory that holds the volume's directory and its
// holder file (the fencing check makes a probe file there); it reads everything
// else.
//
// The capabilities are CAP_NET_ADMIN and CAP_SYS_ADMIN for the nbd netlink
// calls and the device node, CAP_DAC_OVERRIDE and CAP_FOWNER for root's access
// to files on the cache and the transport that other uids own. It calls
// nothing that needs CAP_CHOWN or CAP_SYS_RESOURCE, nor CAP_DAC_READ_SEARCH
// (which adds nothing over DAC_OVERRIDE and enables open_by_handle_at).
// Because CAP_SYS_ADMIN could remount the read-only views, the system calls of
// the mount, module, reboot, swap, raw I/O, clock, debug and obsolete groups
// are denied.
func blockdSandbox(cacheDir, holdDir string) *systemd.Sandbox {
	return &systemd.Sandbox{
		ReadWritePaths: []string{cacheDir, holdDir},
		Capabilities: []int{
			unix.CAP_SYS_ADMIN, unix.CAP_NET_ADMIN,
			unix.CAP_DAC_OVERRIDE, unix.CAP_FOWNER,
		},
		DenySyscalls:    []string{"@mount", "@module", "@reboot", "@swap", "@raw-io", "@clock", "@debug", "@obsolete"},
		AddressFamilies: []string{"AF_UNIX", "AF_NETLINK"},
	}
}

// ensureServer makes the volume's nbd device served by a running block server
// and returns the device. It observes before it acts (I8): a unit that is
// active whose control socket names the volume's device is adopted without a
// call. A unit that is not running is started, and when the device is still
// there the new server re-points it (decision 51), so the mapping and the
// mount above are never touched. Only a start with no device in the kernel
// decides what to do with a node-local cache.
func (it *Item) ensureServer(ctx context.Context, v vol, st *vstate, h *holder.Holder, cur current) (nbd.Device, error) {
	// Every start, the unmount and drain restarts included, is at the size
	// the records hold (Converge raised its own copy already; this is a no-op
	// then).
	v, err := it.raiseSize(ctx, v, st, cur)
	if err != nil {
		return nbd.Device{}, err
	}
	// A cache never exists without the record that lets a node with no intent
	// find the volume again (see Orphans); a server is not started without it.
	// The unit executes the host copy of the binary; a plugin that has only
	// drained or unstaged since it started has not made it yet.
	if err := it.ensureBinary(); err != nil {
		return nbd.Device{}, fmt.Errorf("volume item %s: %w: %w", v.in.VolumeID, item.ErrServerDown, err)
	}
	if err := it.saveDrainRecord(v); err != nil {
		return nbd.Device{}, fmt.Errorf("volume item %s: %w: writing the drain record: %w", v.in.VolumeID, item.ErrServerDown, err)
	}
	us, err := it.cfg.Systemd.State(ctx, v.unit)
	if err != nil {
		return nbd.Device{}, fmt.Errorf("volume item %s: %w: %w", v.in.VolumeID, item.ErrServerDown, err)
	}
	dev, haveDev, err := it.findDevice(v)
	if err != nil {
		return nbd.Device{}, err
	}
	if us.Present {
		switch us.Active {
		case "active":
			if haveDev {
				ss, serr := it.status(ctx, v)
				switch {
				case serr != nil:
					return nbd.Device{}, fmt.Errorf("volume item %s: %w: the unit is active and its control socket does not answer: %w", v.in.VolumeID, item.ErrServerDown, serr)
				case ss.Index != dev.Index:
					return nbd.Device{}, fmt.Errorf("volume item %s: %w: the unit serves device %d, the volume's device is %d", v.in.VolumeID, item.ErrServerDown, ss.Index, dev.Index)
				}
				// systemd restarts a crashed server inside its restart delay, so
				// the unit is often active again by the next pass; the main
				// process is what shows it was a different one.
				if st.pid != 0 && us.MainPID != 0 && us.MainPID != st.pid {
					it.log.Info("the block server was restarted", "volume", v.in.VolumeID)
					it.event(v.in.VolumeID, ReasonServerRestarted)
				}
				if us.MainPID != 0 {
					st.pid = us.MainPID
				}
				return dev, nil
			}
			return nbd.Device{}, fmt.Errorf("volume item %s: %w: the unit is active and the volume has no device", v.in.VolumeID, item.ErrServerDown)
		case "activating", "deactivating", "reloading":
			// Systemd is restarting (or stopping) it: wait for the next pass.
			return nbd.Device{}, fmt.Errorf("volume item %s: %w: the unit is %s (%s)", v.in.VolumeID, item.ErrServerDown, us.Active, us.Sub)
		}
	}

	if !haveDev {
		if err := it.decideCache(v, cur); err != nil {
			return nbd.Device{}, err
		}
	} else if _, ierr := cache.ReadIndex(v.cacheDir); errors.Is(ierr, cache.ErrNoIndex) {
		// A device is still there but the cache that served it is not. A
		// server cannot re-point it without its cache, and this item never
		// builds one for a device that exists: the volume waits for a person.
		return nbd.Device{}, fmt.Errorf("volume item %s: %w: the volume's device exists but its cache does not", v.in.VolumeID, item.ErrServerDown)
	}
	// systemd resolves ReadWritePaths itself, so they must not be redirected: a
	// symlink on the export in the path of the volume's directory would make a
	// node directory writable in the unit. Both are opened without following a
	// link anywhere and must resolve to themselves.
	for _, p := range []string{v.holdDir, v.storeDir} {
		if err := safefs.CheckResolves(p); err != nil {
			return nbd.Device{}, fmt.Errorf("volume item %s: %w: %w", v.in.VolumeID, item.ErrServerDown, err)
		}
	}
	err = it.cfg.Systemd.Start(ctx, it.unitFor(v, h))
	switch {
	case errors.Is(err, systemd.ErrExecStartDiffers):
		// The binary changed under a unit that is running (a plugin upgrade
		// raced this start). The running unit is the volume's server; the new
		// binary is used at the next stage.
		it.log.Info("the unit runs a different binary; adopting it", "volume", v.in.VolumeID)
		if it.serverEpochStale(ctx, v, h) {
			// Adopting is for a different binary; a different epoch is a
			// fenced server. Stop it so that the next pass starts one at the
			// held epoch.
			if serr := it.stopStack(ctx, v, st); serr != nil {
				return nbd.Device{}, serr
			}
			return nbd.Device{}, fmt.Errorf("volume item %s: %w: the running unit published under another epoch and was stopped", v.in.VolumeID, item.ErrServerDown)
		}
	case err != nil:
		return nbd.Device{}, fmt.Errorf("volume item %s: %w: starting the unit: %w", v.in.VolumeID, item.ErrServerDown, err)
	}
	want := -1
	if haveDev {
		want = dev.Index
	}
	dev, err = it.waitServing(ctx, v, want)
	if err != nil {
		return nbd.Device{}, err
	}
	// The unit was started onto a device that was already there: this is the
	// restart of a server that died under a staged volume (decision 51).
	if haveDev {
		it.log.Info("the block server was started again for its surviving device", "volume", v.in.VolumeID)
		it.event(v.in.VolumeID, ReasonServerRestarted)
	}
	st.pid = 0
	if now, serr := it.cfg.Systemd.State(ctx, v.unit); serr == nil && now.Active == "active" {
		st.pid = now.MainPID
	}
	return dev, nil
}

// findDevice looks for the nbd device whose backend is the volume.
func (it *Item) findDevice(v vol) (nbd.Device, bool, error) {
	devs, err := it.cfg.NBD.Devices()
	if err != nil {
		return nbd.Device{}, false, fmt.Errorf("volume item %s: listing the nbd devices: %w", v.in.VolumeID, err)
	}
	for _, d := range devs {
		if d.Backend == v.name {
			return d, true, nil
		}
	}
	return nbd.Device{}, false, nil
}

// status asks the volume's block server for its status.
func (it *Item) status(ctx context.Context, v vol) (statusT, error) {
	cctx, cancel := context.WithTimeout(ctx, it.cfg.Timeout)
	defer cancel()
	c, err := it.cfg.Dial(cctx, v.socket)
	if err != nil {
		return statusT{}, err
	}
	defer func() { _ = c.Close() }()
	st, err := c.Status(cctx)
	if err != nil {
		return statusT{}, err
	}
	if err := it.identity(v, st); err != nil {
		return statusT{}, err
	}
	return st, nil
}

// errWrongServer marks the ErrServerDown of a server that answers for another
// volume. The unstage and the drain return it as it is, not as a retryable
// incomplete unstage: waiting does not change whose server it is.
var errWrongServer = errors.New("the server on the control socket is not this volume's")

// identity refuses a status that names another volume: the socket is this
// volume's, so the server behind it is not the one to act on. Every path that
// acts on a server (converge, generation, final sync, stop, drain) reads a
// status first and passes it through here, directly or through status.
func (it *Item) identity(v vol, st statusT) error {
	if st.Volume != v.name {
		return fmt.Errorf("volume item %s: %w: %w: it serves %q", v.in.VolumeID, item.ErrServerDown, errWrongServer, st.Volume)
	}
	return nil
}

// waitServing waits until the block server answers on its control socket
// naming a device that exists in the kernel with the volume as its backend,
// and, when the device already existed (want >= 0), naming that same device.
func (it *Item) waitServing(ctx context.Context, v vol, want int) (nbd.Device, error) {
	end := time.Now().Add(it.cfg.StartTimeout)
	var last error
	for {
		st, err := it.status(ctx, v)
		if err == nil {
			dev, ok, derr := it.findDevice(v)
			switch {
			case derr != nil:
				err = derr
			case ok && st.Index == dev.Index && (want < 0 || want == dev.Index):
				return dev, nil
			default:
				err = fmt.Errorf("the server names device %d, the volume's device is %d (found: %t, wanted: %d)", st.Index, dev.Index, ok, want)
			}
		}
		last = err
		if time.Now().After(end) {
			return nbd.Device{}, fmt.Errorf("volume item %s: %w: not serving %s after starting: %w", v.in.VolumeID, item.ErrServerDown, it.cfg.StartTimeout, last)
		}
		select {
		case <-ctx.Done():
			return nbd.Device{}, fmt.Errorf("volume item %s: waiting for the block server: %w", v.in.VolumeID, ctx.Err())
		case <-time.After(it.cfg.PollEvery):
		}
	}
}

// ---- the node-local cache ----

// reuse says whether a cache that survived (a reboot, a crashed server) may be
// served again, with the store's `current` as the witness:
//
//   - `current` exists and is still the manifest the cache was built from
//     (its base): a manifest name is never reused and `current` only ever
//     takes a new name, so nothing has been published since the cache began,
//     whoever's epoch it was built under. This is a reboot inside the first
//     interval after a stage.
//   - `current` exists and its epoch is the cache's: an epoch is issued to one
//     node only, so a generation at that epoch was published from this very
//     cache directory (the directory is per volume and per node, and removed at
//     unstage), and the directory is at least as new as `current`. Anyone
//     else's publish carries another epoch and moves `current` off this one.
//   - `current` is absent and the cache descends from nothing: nobody ever
//     published, so nothing in the store is newer.
//
// The epoch held now is deliberately not the test. The holder file can be put
// back to an older claim of ours by a late renewal that raced a takeover, which
// would make a stale cache look current; and it is too strict when another
// node took over and never published, leaving our cache the newest data there
// is. A cache reused under a newer held epoch just publishes at that epoch on
// the base it descends from.
func reuse(idx cache.Index, cur current) bool {
	if cur.ok {
		return cur.epoch == idx.Epoch || cur.name == idx.Base
	}
	return idx.Base == ""
}

// decideCache runs before a server is started with no device in the kernel:
// it keeps a reusable cache and removes any other, so that the server builds a
// fresh one from `current`.
func (it *Item) decideCache(v vol, cur current) error {
	idx, err := cache.ReadIndex(v.cacheDir)
	switch {
	case errors.Is(err, cache.ErrNoIndex):
		// Nothing to reuse; a directory left half made is cleared.
		if _, serr := os.Stat(v.cacheDir); serr == nil {
			return it.discardCache(v, "the directory has no index")
		}
		return nil
	case err != nil:
		// An index that cannot be read may be holding unpublished writes that
		// only a person can judge. It is not removed and no server is started
		// over it.
		it.log.Warn("the node-local cache index is unreadable; leaving the cache alone", "volume", v.in.VolumeID, "error", err)
		return fmt.Errorf("volume item %s: %w: the cache index is unreadable (%v); the cache is left for an operator", v.in.VolumeID, item.ErrServerDown, err)
	case reuse(idx, cur):
		return nil
	}
	if len(idx.Dirty) > 0 {
		// Writes that were never published are discarded: the store has
		// moved on without them.
		it.lost(v, int64(len(idx.Dirty))*v.chunk, "the node-local cache is not descended from the store's current generation")
	}
	return it.discardCache(v, "the store's current generation is not this cache's")
}

// discardCache removes the volume's node-local cache directory. It is the
// item's own directory, so a half-made one with no index goes with it.
func (it *Item) discardCache(v vol, why string) error {
	if _, err := os.Stat(v.cacheDir); err == nil {
		it.log.Info("removing the node-local cache", "volume", v.in.VolumeID, "reason", why)
		if err := cache.Remove(v.cacheDir); err != nil {
			if rerr := os.RemoveAll(v.cacheDir); rerr != nil {
				return fmt.Errorf("volume item %s: removing the cache: %w", v.in.VolumeID, rerr)
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("volume item %s: the cache: %w", v.in.VolumeID, err)
	}
	// The block server's lock lives beside the directory, not in it, so that it
	// survives the directory's removal and the next server cannot lock a
	// directory it does not yet see. Nothing holds it now: every caller got
	// here after the unit was stopped (and the orphan device disconnected), or
	// before one was started. The next server makes its own.
	if err := os.Remove(v.cacheDir + ".lock"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("volume item %s: removing the cache lock: %w", v.in.VolumeID, err)
	}
	return nil
}

// ---- layer 5: LUKS mapping ----

// dmPresent reports whether a device-mapper device called name exists, from
// the names sysfs shows.
func (it *Item) dmPresent(name string) (bool, error) {
	names, err := filepath.Glob(filepath.Join(it.cfg.Sysfs, "block", "dm-*", "dm", "name"))
	if err != nil {
		return false, err
	}
	for _, n := range names {
		b, err := os.ReadFile(n) //nolint:gosec // sysfs
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return false, err
		}
		if strings.TrimSpace(string(b)) == name {
			return true, nil
		}
	}
	return false, nil
}

// ensureMapping returns the block device the file system lives on: the nbd
// device of an unencrypted volume, else the LUKS mapping, which is adopted
// when it exists and otherwise formatted (the volume's first stage, when there
// is no generation anywhere) and opened with the request's key. A wrong key
// that a previous key repairs is rotated to the new one first.
func (it *Item) ensureMapping(ctx context.Context, v vol, dev nbd.Device, cur current, keys item.Keys) (string, error) {
	nbdPath := it.devPath(dev.Index)
	if !v.params.Encryption {
		return nbdPath, nil
	}
	name := crypt.Name(v.name)
	present, err := it.dmPresent(name)
	if err != nil {
		return "", fmt.Errorf("volume item %s: looking for the mapping: %w", v.in.VolumeID, err)
	}
	if present {
		return crypt.MapperPath(name), nil
	}
	if len(keys.Key) == 0 {
		return "", fmt.Errorf("volume item %s: %w", v.in.VolumeID, item.ErrKeyRequired)
	}
	isLUKS, err := crypt.IsLUKS(ctx, it.cfg.Tools, nbdPath)
	if err != nil {
		return "", fmt.Errorf("volume item %s: %w", v.in.VolumeID, err)
	}
	if !isLUKS {
		if cur.ok {
			return "", fmt.Errorf("volume item %s: the device holds no LUKS header but the volume has published generations; not formatting it", v.in.VolumeID)
		}
		if err := crypt.Format(ctx, it.cfg.Tools, nbdPath, keys.Key, crypt.Params{Cipher: v.params.Cipher, KeySize: v.params.KeySize}); err != nil {
			return "", fmt.Errorf("volume item %s: %w", v.in.VolumeID, err)
		}
	}
	err = crypt.Open(ctx, it.cfg.Tools, nbdPath, name, keys.Key)
	if errors.Is(err, crypt.ErrBadKey) {
		if len(keys.PreviousKey) == 0 {
			return "", fmt.Errorf("volume item %s: %w", v.in.VolumeID, item.ErrBadKey)
		}
		if rerr := crypt.Rotate(ctx, it.cfg.Tools, nbdPath, keys.PreviousKey, keys.Key); rerr != nil {
			if errors.Is(rerr, crypt.ErrBadKey) {
				return "", fmt.Errorf("volume item %s: %w", v.in.VolumeID, item.ErrBadKey)
			}
			return "", fmt.Errorf("volume item %s: rotating the key: %w", v.in.VolumeID, rerr)
		}
		err = crypt.Open(ctx, it.cfg.Tools, nbdPath, name, keys.Key)
		if errors.Is(err, crypt.ErrBadKey) {
			return "", fmt.Errorf("volume item %s: %w", v.in.VolumeID, item.ErrBadKey)
		}
	}
	if err != nil {
		return "", fmt.Errorf("volume item %s: %w", v.in.VolumeID, err)
	}
	return crypt.MapperPath(name), nil
}

// ---- layer 6: file system and mount ----

// sameDevice reports whether a mount source names the block device blk,
// directly or through a symlink (a mapper path and its /dev/dm-N).
func sameDevice(source, blk string) bool {
	if source == blk {
		return true
	}
	a, aerr := filepath.EvalSymlinks(source)
	b, berr := filepath.EvalSymlinks(blk)
	return aerr == nil && berr == nil && a == b
}

// ensureMounted adopts the ext4 mount at the stage path when it is this
// volume's, else makes it: a blank device (see below) gets a file system (only when the
// volume has no published generation, so a lost mapping can never be formatted
// over data), an existing one is checked, then mounted. A mount that is there
// is never checked, stacked over or removed. The first time this process sees
// a mount it did not make it thaws it: a plugin that crashed in the middle of
// a generation must not leave the file system frozen.
func (it *Item) ensureMounted(ctx context.Context, v vol, st *vstate, blk, nbdPath string, cur current, mounts observe.Table) error {
	if top, ok := mounts.At(v.stage); ok {
		if top.FSType != "ext4" || !sameDevice(top.Source, blk) {
			return fmt.Errorf("volume item %s: %w: %s is mounted as %s from %q", v.in.VolumeID, item.ErrStageConflict, v.stage, top.FSType, top.Source)
		}
		return it.thawOnce(v, st)
	}
	// Whether the device is blank is judged on the raw nbd device, never on the
	// mapping: a fresh LUKS format writes only its header, so the start of the
	// payload is zero until a file system is made through the mapping, and then
	// it is ciphertext, which is never zero. That one witness also covers a crash
	// between the format and the mkfs.
	area := ext4.Area{Dev: nbdPath}
	if v.params.Encryption {
		off, err := crypt.PayloadOffset(ctx, it.cfg.Tools, nbdPath)
		if err != nil {
			return fmt.Errorf("volume item %s: %w", v.in.VolumeID, err)
		}
		area.Offset = off
	}
	has, err := ext4.HasFilesystem(ctx, it.cfg.Tools, blk, area)
	if errors.Is(err, ext4.ErrDataWithoutSignature) {
		// A crash inside mkfs lands here: it zaps the old superblock, so the
		// payload is written and there is no signature. A person decides.
		what := "the volume"
		if v.params.Encryption {
			what = "the encrypted volume"
		}
		return fmt.Errorf("volume item %s: %s holds data but no file system signature; if this volume was never used, delete the claim and recreate it: %w", v.in.VolumeID, what, err)
	}
	if err != nil {
		return fmt.Errorf("volume item %s: %w", v.in.VolumeID, err)
	}
	if !has {
		if cur.ok {
			return fmt.Errorf("volume item %s: the device holds no file system but the volume has published generations; not formatting it", v.in.VolumeID)
		}
		if err := ext4.Mkfs(ctx, it.cfg.Tools, blk); err != nil {
			return fmt.Errorf("volume item %s: %w", v.in.VolumeID, err)
		}
		st.resizedTo = v.size // a file system just made fills its device
	} else if err := ext4.Fsck(ctx, it.cfg.Tools, blk); err != nil {
		return fmt.Errorf("volume item %s: %w", v.in.VolumeID, err)
	}
	if err := it.cfg.Mounter.Mount(ctx, blk, v.stage, "ext4", unix.MS_NOSUID|unix.MS_NODEV, ext4.MountOptions); err != nil {
		return fmt.Errorf("volume item %s: mounting the file system: %w", v.in.VolumeID, err)
	}
	st.thawed.Store(true)
	return nil
}

// thawOnce issues FITHAW on a mount this process found already there, once.
// EINVAL, "not frozen", is the normal answer and is the reason the call is
// harmless; it is made once so that it can never end a generation's freeze.
func (it *Item) thawOnce(v vol, st *vstate) error {
	if st.thawed.Load() {
		return nil // checked outside fz: a generation's freeze must not stall a pass
	}
	st.fz.Lock()
	defer st.fz.Unlock()
	if st.thawed.Load() {
		return nil
	}
	if err := it.cfg.Thaw(v.stage); err != nil {
		return fmt.Errorf("volume item %s: thawing the adopted mount: %w", v.in.VolumeID, err)
	}
	st.thawed.Store(true)
	return nil
}

// ---- layer 7: size ----

// ensureSize brings the volume to the size the record says, in order: the
// block server's device, the LUKS mapping, the file system. The device is
// grown when the server reports it smaller. The mapping and the file system
// are resized whenever this process has not yet brought them to this size;
// cryptsetup resize and resize2fs both do nothing to something already full
// size, so this finishes a grow that was interrupted at any step (and the
// price of not guessing from how full the file system looks is one call each
// per size per plugin start).
func (it *Item) ensureSize(ctx context.Context, v vol, vs *vstate, blk string) error {
	st, err := it.status(ctx, v)
	if err != nil {
		return fmt.Errorf("volume item %s: %w: %w", v.in.VolumeID, item.ErrServerDown, err)
	}
	if st.Size < v.size {
		if err := it.grow(ctx, v); err != nil {
			return err
		}
	}
	if vs.resizedTo == v.size {
		return nil
	}
	if v.params.Encryption {
		if err := crypt.Resize(ctx, it.cfg.Tools, crypt.Name(v.name)); err != nil {
			return fmt.Errorf("volume item %s: %w", v.in.VolumeID, err)
		}
	}
	if err := ext4.Resize(ctx, it.cfg.Tools, blk); err != nil {
		return fmt.Errorf("volume item %s: %w", v.in.VolumeID, err)
	}
	vs.resizedTo = v.size
	return nil
}

// grow raises the block server's device to the volume's size.
func (it *Item) grow(ctx context.Context, v vol) error {
	cctx, cancel := context.WithTimeout(ctx, it.cfg.Timeout)
	defer cancel()
	c, err := it.cfg.Dial(cctx, v.socket)
	if err != nil {
		return fmt.Errorf("volume item %s: %w: %w", v.in.VolumeID, item.ErrServerDown, err)
	}
	defer func() { _ = c.Close() }()
	if err := c.Grow(cctx, v.size); err != nil {
		return fmt.Errorf("volume item %s: growing the device: %w", v.in.VolumeID, err)
	}
	return nil
}
