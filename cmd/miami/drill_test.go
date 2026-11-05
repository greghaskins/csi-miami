package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/greghaskins/csi-miami/internal/chunk"
)

// docDrill is the shell block of docs/restore.md without the lines that need a
// key and a mount: the lines the script runs.
func docDrill(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile("../../docs/restore.md")
	if err != nil {
		t.Fatal(err)
	}
	_, rest, _ := strings.Cut(string(b), "```sh\n")
	block, _, _ := strings.Cut(rest, "```")
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(block), "\n") {
		if strings.HasPrefix(l, "cd ") || strings.HasPrefix(l, "cryptsetup ") || strings.HasPrefix(l, "mount ") {
			continue
		}
		out = append(out, l)
	}
	return out
}

func scriptDrill(t *testing.T) []string {
	t.Helper()
	b, err := os.ReadFile("../../hack/restore-drill.sh")
	if err != nil {
		t.Fatal(err)
	}
	_, rest, _ := strings.Cut(string(b), "# drill-begin\n")
	block, _, _ := strings.Cut(rest, "# drill-end")
	return strings.Split(strings.TrimSpace(block), "\n")
}

func TestDrillScriptRunsTheDocumentedDrill(t *testing.T) {
	t.Parallel()
	doc, script := docDrill(t), scriptDrill(t)
	if len(doc) != 4 {
		t.Fatalf("the documented drill has %d runnable lines, want 4: %q", len(doc), doc)
	}
	if strings.Join(doc, "\n") != strings.Join(script, "\n") {
		t.Fatalf("hack/restore-drill.sh differs from docs/restore.md:\ndoc:\n%s\nscript:\n%s", strings.Join(doc, "\n"), strings.Join(script, "\n"))
	}
}

func TestDrillScriptMatchesRestore(t *testing.T) {
	t.Parallel()
	for _, tool := range []string{"bash", "sha256sum", "sed", "grep", "head", "cat"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed", tool)
		}
	}
	// Build a store whose zero chunks are not files (as a first generation of
	// a sparse volume leaves them) next to one where the zero chunk is stored.
	for _, storeZero := range []bool{false, true} {
		ts := newTestStore(t)
		a, b := ts.put(t, 'a'), ts.put(t, 'b')
		zero := chunk.ZeroHash(testMiB)
		if storeZero {
			if err := ts.s.Put(t.Context(), zero, bytes.NewReader(make([]byte, testMiB)), testMiB); err != nil {
				t.Fatal(err)
			}
		}
		ts.manifest(t, 1, 1, false, a, zero, zero)
		ts.manifest(t, 1, 2, true, a, zero, b, zero, a)

		work := t.TempDir()
		drillOut := filepath.Join(work, "drill.img")
		cmd := exec.Command("../../hack/restore-drill.sh", ts.dir, drillOut) //nolint:gosec // test fixture
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("storeZero=%v: drill: %v\n%s", storeZero, err, out)
		}
		miamiOut := filepath.Join(work, "miami.img")
		if code, _, stderr := runMiami("restore", "-store", ts.dir, "-out", miamiOut); code != 0 {
			t.Fatalf("storeZero=%v: restore exit %d: %s", storeZero, code, stderr)
		}
		d, err := os.ReadFile(drillOut) //nolint:gosec // test fixture
		if err != nil {
			t.Fatal(err)
		}
		m, err := os.ReadFile(miamiOut) //nolint:gosec // test fixture
		if err != nil {
			t.Fatal(err)
		}
		if len(d) != 5*int(testMiB) || !bytes.Equal(d, m) {
			t.Fatalf("storeZero=%v: drill and restore differ (%d and %d bytes)", storeZero, len(d), len(m))
		}
		if ents, _ := os.ReadDir(ts.dir); hasEntry(ents, "volume.img") {
			t.Errorf("storeZero=%v: the drill left volume.img in the store", storeZero)
		}
	}
}

func TestDrillStopsOnACorruptChunk(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("sha256sum"); err != nil {
		t.Skip("sha256sum is not installed")
	}
	ts := newTestStore(t)
	a := ts.put(t, 'a')
	ts.manifest(t, 1, 1, true, a)
	if err := os.WriteFile(ts.s.ChunkPath(a), []byte("rot"), 0o644); err != nil { //nolint:gosec // test fixture
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "drill.img")
	if err := exec.Command("../../hack/restore-drill.sh", ts.dir, out).Run(); err == nil { //nolint:gosec // test fixture
		t.Fatal("the drill succeeded on a corrupt chunk")
	}
	if _, err := os.Stat(out); err == nil {
		t.Fatal("the drill left an image after a failed verification")
	}
}

func hasEntry(ents []os.DirEntry, name string) bool {
	for _, e := range ents {
		if e.Name() == name {
			return true
		}
	}
	return false
}
