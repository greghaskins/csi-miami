package intent

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
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
type Store struct {
	dir string

	// mu serialises Save, Delete and Load, so that Load's removal of stale
	// temp files can never delete the temp file of a Save in flight.
	mu sync.Mutex
}

// LoadFailure is a record that could not be loaded. The file is left in
// place.
type LoadFailure struct {
	Path string
	Err  error
}

// Loaded is the result of Load.
type Loaded struct {
	// Intents holds every valid record, by VolumeID.
	Intents map[string]Intent
	// Failures holds one entry per file that could not be loaded. The
	// volumes behind them have no intent.
	Failures []LoadFailure
}

// Open returns a Store on dir, creating it with mode 0700 if it is missing.
func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("intent store: directory is required")
	}
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("intent store: %w", err)
	}
	return &Store{dir: dir}, nil
}

// Dir returns the directory the Store keeps its records in.
func (s *Store) Dir() string { return s.dir }

func (s *Store) path(volumeID string) string {
	return filepath.Join(s.dir, Escape(volumeID)+recordSuffix)
}

// Save validates the Intent and writes it atomically and durably, replacing
// any record for the same volume. After a crash at any point the record is
// either the old one or the new one.
func (s *Store) Save(in Intent) error {
	if err := in.Validate(); err != nil {
		return fmt.Errorf("intent for %q: %w", in.VolumeID, err)
	}
	data, err := json.MarshalIndent(in, "", "  ")
	if err != nil {
		return fmt.Errorf("intent for %q: %w", in.VolumeID, err)
	}
	data = append(data, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	return writeAtomic(s.dir, Escape(in.VolumeID)+recordSuffix, data)
}

// Delete removes the record for a volume. A missing record is success. The
// directory is synced after the unlink so the removal survives a crash.
func (s *Store) Delete(volumeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := os.Remove(s.path(volumeID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("intent store: delete: %w", err)
	}
	return syncDir(s.dir)
}

// Load reads every *.json record in the directory and removes stale temp
// files left by a crash. The error is only for a directory that cannot be
// read; a record that cannot be read or is invalid is reported in Failures,
// logged, and left on disk.
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
			if err := os.Remove(full); err != nil && !errors.Is(err, fs.ErrNotExist) {
				slog.Warn("intent store: cannot remove stale temp file", "path", full, "err", err)
			}
		case strings.HasSuffix(name, recordSuffix):
			in, err := readRecord(full, name)
			if err != nil {
				slog.Warn("intent store: record not loaded; file kept for inspection", "path", full, "err", err)
				out.Failures = append(out.Failures, LoadFailure{Path: full, Err: err})
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
// authoritative; the file name is only checked against it.
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
		return Intent{}, fmt.Errorf("volumeID %q belongs in file %q, not %q", in.VolumeID, want, name)
	}
	return in, nil
}

// writeAtomic writes data to dir/name through a temp file in dir: create
// exclusively, write, fsync, rename over the target, fsync the directory.
func writeAtomic(dir, name string, data []byte) (err error) {
	f, tmp, err := createTemp(dir, name)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("intent store: write %s: %w", tmp, err)
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("intent store: sync %s: %w", tmp, err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("intent store: close %s: %w", tmp, err)
	}
	target := filepath.Join(dir, name)
	if err = os.Rename(tmp, target); err != nil {
		return fmt.Errorf("intent store: rename to %s: %w", target, err)
	}
	// The rename has happened: from here the new record is the file, so a
	// directory sync failure must not remove anything.
	if serr := syncDir(dir); serr != nil {
		return serr
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

// syncDir fsyncs a directory so that renames and unlinks in it are durable.
func syncDir(dir string) error {
	d, err := os.Open(dir) // #nosec G304 -- the store's own directory
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
