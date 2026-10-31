package intent

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	recordSuffix = ".json"
	tempInfix    = recordSuffix + ".tmp-"
	dirPerm      = 0o700
	filePerm     = 0o600

	// tempAttempts bounds the retries when a random temp name collides.
	tempAttempts = 5
)

// Store keeps one record per volume in a directory. It is safe for
// concurrent use. It caches nothing: Load reads the directory every time.
//
// Use one Store per directory per process and share the instance: the mutex
// that makes writes and Load safe against each other lives in the Store, so
// two Stores on one directory give no such guarantee.
type Store struct {
	dir string

	// mu serialises Save, Update, Delete, Get and Load, so that Load's
	// removal of stale temp files can never delete the temp file of a Save
	// in flight, and an Update is one atomic read-modify-write.
	mu sync.Mutex
}

// LoadFailure is a record that could not be loaded. The file is left in
// place.
type LoadFailure struct {
	// Path is the file that failed.
	Path string
	// VolumeID is the volume ID recovered from the file name with Unescape,
	// or empty if the name does not unescape. It is a hint for events and
	// metric labels; the ID inside a record is the authoritative one, and a
	// record that cannot be read has none.
	VolumeID string
	// Err says what was wrong. It never contains a field value from the
	// record.
	Err error
}

// Loaded is the result of Load.
type Loaded struct {
	// Intents holds every valid record, by VolumeID.
	Intents map[string]Intent
	// Failures holds one entry per file that could not be loaded, in file
	// name order. The volumes behind them have no intent.
	Failures []LoadFailure
}

// Open returns a Store on dir, creating it (and any missing parents) with
// mode 0700 if it is missing. The creation is made durable by syncing the
// directories that gained an entry. An existing directory that is looser
// than 0700 is tightened to 0700.
func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("intent store: directory is required")
	}
	// Remember which directories do not exist yet, outermost last.
	var created []string
	for d := filepath.Clean(dir); ; d = filepath.Dir(d) {
		if _, err := os.Stat(d); err == nil {
			break
		} else if !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("intent store: %w", err)
		}
		created = append(created, d)
		if filepath.Dir(d) == d {
			break
		}
	}
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("intent store: %w", err)
	}
	for _, d := range created {
		if err := syncDir(filepath.Dir(d)); err != nil {
			return nil, err
		}
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("intent store: %w", err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("intent store: %s is not a directory", dir)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		if err := os.Chmod(dir, dirPerm); err != nil {
			return nil, fmt.Errorf("intent store: %w", err)
		}
	}
	return &Store{dir: dir}, nil
}

// Dir returns the directory the Store keeps its records in.
func (s *Store) Dir() string { return s.dir }

func (s *Store) fileName(volumeID string) string {
	return Escape(volumeID) + recordSuffix
}

// Save validates the Intent and writes it atomically and durably, replacing
// any record for the same volume, including a record that failed to load and
// was kept for inspection: that is the repair path. After a crash at any
// point the record is either the old one or the new one.
//
// An error from Save before the rename means the old record (if any) is
// untouched. An error that says the directory sync failed means the rename
// happened: the new record is in place, but may not survive a power loss.
func (s *Store) Save(in Intent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveLocked(in)
}

func (s *Store) saveLocked(in Intent) error {
	if err := in.Validate(); err != nil {
		return fmt.Errorf("intent store: invalid intent: %w", err)
	}
	data, err := json.MarshalIndent(in, "", "  ")
	if err != nil {
		return fmt.Errorf("intent store: encode: %w", err)
	}
	data = append(data, '\n')
	return writeAtomic(s.dir, s.fileName(in.VolumeID), data)
}

// Get returns the record for a volume. A missing record is (zero, false,
// nil). A record that cannot be read or is invalid is an error, because the
// caller must not mistake it for "no intent"; Save replaces it.
func (s *Store) Get(volumeID string) (Intent, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getLocked(volumeID)
}

func (s *Store) getLocked(volumeID string) (Intent, bool, error) {
	name := s.fileName(volumeID)
	in, err := readRecord(filepath.Join(s.dir, name), name)
	if errors.Is(err, fs.ErrNotExist) {
		// A missing record is "no intent" only if the directory is there.
		if _, serr := os.Stat(s.dir); serr != nil {
			return Intent{}, false, fmt.Errorf("intent store: %w", serr)
		}
		return Intent{}, false, nil
	}
	if err != nil {
		return Intent{}, false, err
	}
	return in, true, nil
}

// Update changes the record for a volume in one step: read it, apply fn,
// validate the result and write it atomically, all under the Store's lock, so
// concurrent Updates of one volume cannot lose each other's changes.
//
// If no record exists, fn receives a zero Intent with only Version set and
// must fill in the rest, VolumeID included. fn must not change VolumeID of an
// existing record. If fn returns an error, nothing is written and Update
// returns that error unchanged, so a caller can use a sentinel to abort. A
// record that exists but cannot be loaded is an error, not a fresh start:
// repair it with Save.
func (s *Store) Update(volumeID string, fn func(*Intent) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	in, ok, err := s.getLocked(volumeID)
	if err != nil {
		return err
	}
	if !ok {
		in = Intent{Version: Version}
	}
	if err := fn(&in); err != nil {
		return err
	}
	if in.VolumeID != volumeID {
		return errors.New("intent store: update must not change or omit the volume ID")
	}
	return s.saveLocked(in)
}

// Delete removes the record for a volume. A missing record is success. The
// directory is synced after the unlink so the removal survives a crash.
func (s *Store) Delete(volumeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := os.Remove(filepath.Join(s.dir, s.fileName(volumeID)))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("intent store: delete: %w", err)
	}
	return syncDir(s.dir)
}

// Load reads every *.json record in the directory and removes stale temp
// files left by a crash (best effort). A record that cannot be read or is
// invalid is reported in Failures and left on disk; Load does not log, so the
// caller decides when to report a failure and should export len(Failures).
//
// An error from Load means the intents are unknown, not that there are none:
// the directory could not be read. The caller must not garbage-collect mounts
// on it. Only a nil error with the record absent from Intents says that a
// volume has no intent, and a volume named in Failures has none either, but
// for a reason a human should look at.
func (s *Store) Load() (Loaded, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return Loaded{}, fmt.Errorf("intent store: %w", err)
	}
	out := Loaded{Intents: make(map[string]Intent)}
	for _, e := range entries {
		name := e.Name()
		full := filepath.Join(s.dir, name)
		switch {
		case isTemp(name):
			_ = os.Remove(full)
		case strings.HasSuffix(name, recordSuffix):
			in, err := readRecord(full, name)
			if err != nil {
				id, _ := Unescape(strings.TrimSuffix(name, recordSuffix))
				out.Failures = append(out.Failures, LoadFailure{Path: full, VolumeID: id, Err: err})
				continue
			}
			out.Intents[in.VolumeID] = in
		}
	}
	return out, nil
}

func isTemp(name string) bool {
	return strings.HasPrefix(name, ".") && strings.Contains(name, tempInfix)
}

// readRecord reads and checks one record. The volume ID inside the record is
// authoritative; the file name is only checked against it. A missing file
// returns an error for which errors.Is(err, fs.ErrNotExist) holds.
func readRecord(full, name string) (Intent, error) {
	data, err := os.ReadFile(full) // #nosec G304 -- a file in the store directory
	if err != nil {
		return Intent{}, err
	}
	var in Intent
	if err := json.Unmarshal(data, &in); err != nil {
		return Intent{}, fmt.Errorf("decode: %w", err)
	}
	if err := in.Validate(); err != nil {
		return Intent{}, err
	}
	if want := Escape(in.VolumeID) + recordSuffix; name != want {
		return Intent{}, fmt.Errorf("the volume ID in the record belongs in file %q, not %q", want, name)
	}
	return in, nil
}

// writeAtomic writes data to dir/name through a temp file in dir: create
// exclusively, write, fsync, rename over the target, fsync the directory.
//
// The temp file is removed if any step before the rename fails. Once the
// rename has happened the new record is the file, so a failing directory sync
// returns an error but removes nothing.
func writeAtomic(dir, name string, data []byte) error {
	f, tmp, err := createTemp(dir, name)
	if err != nil {
		return err
	}
	target := filepath.Join(dir, name)
	if err := writeAndRename(f, tmp, target, data); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("record %s is in place but may not be durable: %w", target, err)
	}
	return nil
}

// writeAndRename is the part of writeAtomic that must clean up after itself.
func writeAndRename(f *os.File, tmp, target string, data []byte) error {
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("intent store: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("intent store: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("intent store: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, target); err != nil {
		return fmt.Errorf("intent store: rename to %s: %w", target, err)
	}
	return nil
}

// createTemp creates ".<name>.tmp-<random>" in dir, exclusively, mode 0600.
func createTemp(dir, name string) (*os.File, string, error) {
	for range tempAttempts {
		var r [8]byte
		if _, err := rand.Read(r[:]); err != nil {
			return nil, "", fmt.Errorf("intent store: %w", err)
		}
		tmp := filepath.Join(dir, "."+name+".tmp-"+hex.EncodeToString(r[:]))
		f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, filePerm) // #nosec G304 -- a new file in the store directory
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return nil, "", fmt.Errorf("intent store: create temp file: %w", err)
		}
		return f, tmp, nil
	}
	return nil, "", fmt.Errorf("intent store: no unused temp file name in %s", dir)
}

// syncDir fsyncs a directory so that renames, creations and unlinks in it
// are durable.
func syncDir(dir string) error {
	d, err := os.Open(dir) // #nosec G304 -- a directory of the store
	if err != nil {
		return fmt.Errorf("intent store: open directory: %w", err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("intent store: sync directory %s: %w", dir, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("intent store: close directory %s: %w", dir, err)
	}
	return nil
}
