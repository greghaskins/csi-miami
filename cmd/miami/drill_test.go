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

// shellBlock is the drill's shell block of a markdown file, without the lines
// that need a key and a mount: the lines the script runs.
func shellBlock(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // test fixture
	if err != nil {
		t.Fatal(err)
	}
	_, rest, ok := strings.Cut(string(b), "```sh\ncd pvc-<uuid>\n")
	if !ok {
		t.Fatalf("%s has no drill block", path)
	}
	block, _, _ := strings.Cut(rest, "```")
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(block), "\n") {
		if strings.HasPrefix(l, "cryptsetup ") || strings.HasPrefix(l, "mount ") {
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
	doc := shellBlock(t, "../../docs/restore.md")
	spec := shellBlock(t, "../../docs/design/2026-11-03-chunk-backed-volumes-design.md")
	script := scriptDrill(t)
	if len(doc) != 3 {
		t.Fatalf("the documented drill has %d runnable lines, want 3: %q", len(doc), doc)
	}
	if strings.Join(doc, "\n") != strings.Join(spec, "\n") {
		t.Fatalf("docs/restore.md differs from the design's 5.1:\ndoc:\n%s\nspec:\n%s", strings.Join(doc, "\n"), strings.Join(spec, "\n"))
	}
	if strings.Join(doc, "\n") != strings.Join(script, "\n") {
		t.Fatalf("hack/restore-drill.sh differs from docs/restore.md:\ndoc:\n%s\nscript:\n%s", strings.Join(doc, "\n"), strings.Join(script, "\n"))
	}
}

func requireDrillTools(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"bash", "sha256sum", "sed", "grep", "xargs", "cat"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not installed", tool)
		}
	}
}

// drillAndRestore runs the drill script and `miami restore` on the store and
// requires the same bytes, of the given length.
func drillAndRestore(t *testing.T, dir string, wantLen int) {
	t.Helper()
	work := t.TempDir()
	drillOut := filepath.Join(work, "drill.img")
	if out, err := exec.Command("../../hack/restore-drill.sh", dir, drillOut).CombinedOutput(); err != nil { //nolint:gosec // test fixture
		t.Fatalf("drill: %v\n%s", err, out)
	}
	miamiOut := filepath.Join(work, "miami.img")
	if code, _, stderr := runMiami("restore", "-store", dir, "-out", miamiOut); code != 0 {
		t.Fatalf("restore exit %d: %s", code, stderr)
	}
	d, err := os.ReadFile(drillOut) //nolint:gosec // test fixture
	if err != nil {
		t.Fatal(err)
	}
	m, err := os.ReadFile(miamiOut) //nolint:gosec // test fixture
	if err != nil {
		t.Fatal(err)
	}
	if len(d) != wantLen || !bytes.Equal(d, m) {
		t.Fatalf("drill and restore differ (%d and %d bytes, want %d)", len(d), len(m), wantLen)
	}
	if ents, _ := os.ReadDir(dir); hasEntry(ents, "volume.img") {
		t.Error("the drill left volume.img in the store")
	}
}

func TestDrillScriptMatchesRestore(t *testing.T) {
	t.Parallel()
	requireDrillTools(t)
	ts := newTestStore(t)
	a, b := ts.put(t, 'a'), ts.put(t, 'b')
	zero := chunk.ZeroHash(testMiB)
	if err := ts.s.EnsureZero(t.Context(), testMiB); err != nil { // as every writer does
		t.Fatal(err)
	}
	ts.manifest(t, 1, 1, false, a, zero, zero)
	ts.manifest(t, 1, 2, true, a, zero, b, zero, a)
	drillAndRestore(t, ts.dir, 5*int(testMiB))
}

func TestDrillRunsOnAnImportedStore(t *testing.T) {
	t.Parallel()
	requireDrillTools(t)
	img := luks2Image(int(4 * testMiB)) // one data chunk, three zero chunks
	vol := volumeDir(t)
	if code, _, stderr := runMiami("import", "-image", writeImage(t, img), "-store", vol, "-chunk-size", "1048576"); code != 0 {
		t.Fatalf("import exit %d: %s", code, stderr)
	}
	drillAndRestore(t, vol, 4*int(testMiB))
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
