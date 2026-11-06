package holder

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The holder file sits on a shared export, where a writer is not trusted for
// integrity: a link or an oversized file there must not make this process read
// outside the export, and file content must not reach an error.

const secret = "TOPSECRET-token-abc"

func plantSecret(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(p, []byte(secret+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func noSecret(t *testing.T, err error) {
	t.Helper()
	if err != nil && strings.Contains(err.Error(), secret) {
		t.Fatalf("error carries file content: %v", err)
	}
}

func TestHolderFileSymlinkIsNotFollowed(t *testing.T) {
	e := newEnv(t)
	if err := os.Symlink(plantSecret(t), Path(e.dir, "v")); err != nil {
		t.Fatal(err)
	}
	info, err := Read(ctx, e.dir, "v")
	if err == nil || info.Present {
		t.Fatalf("Read followed a symlink: %+v, %v", info, err)
	}
	noSecret(t, err)
	if _, err := Acquire(ctx, e.dir, "v", "n1", Judgement{}); err == nil {
		t.Fatal("Acquire took over through a symlink")
	}
}

func TestHolderFileContentIsNotEchoed(t *testing.T) {
	e := newEnv(t)
	for name, body := range map[string]string{
		"not json":     secret + "\n",
		"node content": `{"node":"` + secret + ` ","epoch":1}`,
		"no epoch":     `{"node":"` + secret + `"}`,
	} {
		if err := os.WriteFile(Path(e.dir, "v"), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Read(ctx, e.dir, "v")
		if err == nil {
			t.Fatalf("%s: accepted", name)
		}
		noSecret(t, err)
	}
}

func TestHolderFileIsSizeCapped(t *testing.T) {
	e := newEnv(t)
	body := `{"node":"n1","epoch":1,"pad":"` + strings.Repeat("x", 1<<20) + `"}`
	if err := os.WriteFile(Path(e.dir, "v"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(ctx, e.dir, "v"); err == nil {
		t.Fatal("a 1 MiB holder file was read")
	}
}

func TestHolderFileMustBeRegular(t *testing.T) {
	e := newEnv(t)
	if err := os.Mkdir(Path(e.dir, "v"), 0o750); err != nil {
		t.Fatal(err)
	}
	if _, err := Read(ctx, e.dir, "v"); err == nil {
		t.Fatal("a directory was read as a holder file")
	}
}

func TestSymlinkedHolderDirectoryIsRefused(t *testing.T) {
	e := newEnv(t)
	link := filepath.Join(t.TempDir(), "export")
	if err := os.Symlink(e.dir, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(ctx, link, "v", "n1", Judgement{}); err == nil {
		t.Fatal("Acquire worked through a symlinked directory")
	}
	if len(e.names()) != 0 {
		t.Fatalf("files written through the link: %v", e.names())
	}
}

// Takeover and release act on the name: a link planted where the holder file
// is gets replaced or removed itself, and its target is untouched.
func TestTakeoverAndReleaseLeaveALinkTargetAlone(t *testing.T) {
	e := newEnv(t)
	h, err := Acquire(ctx, e.dir, "v", "n1", Judgement{})
	if err != nil {
		t.Fatal(err)
	}
	target := plantSecret(t)
	if err := os.Remove(Path(e.dir, "v")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, Path(e.dir, "v")); err != nil {
		t.Fatal(err)
	}
	if err := h.Renew(ctx); !errors.Is(err, ErrLost) && err == nil {
		t.Fatalf("Renew over a symlinked name = %v, want ErrLost or an error", err)
	}
	_ = h.Release(ctx)
	if b, _ := os.ReadFile(target); !bytes.Equal(b, []byte(secret+"\n")) { //nolint:gosec // a test path
		t.Fatalf("link target changed: %q", b)
	}
}

// A symlink in the middle of the holder directory's path is refused for every
// operation, and the probe file the age check makes is not created behind it.
func TestIntermediateSymlinkInTheHolderDirectoryIsRefused(t *testing.T) {
	newEnv(t)
	node := t.TempDir()
	link := filepath.Join(t.TempDir(), "ns")
	if err := os.Symlink(node, link); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(link, "x")
	if err := os.Mkdir(filepath.Join(node, "x"), 0o750); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(ctx, dir, "v", "n1", Judgement{}); err == nil {
		t.Error("Acquire worked through a symlinked directory in the path")
	}
	if _, err := Read(ctx, dir, "v"); err == nil {
		t.Error("Read worked through a symlinked directory in the path")
	}
	if ents, _ := os.ReadDir(filepath.Join(node, "x")); len(ents) != 0 {
		t.Errorf("files created behind the link: %v", ents)
	}
}
