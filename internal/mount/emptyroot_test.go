package mount

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The empty root opens a bind source with no root above it, which is right
// only for the local transport's validated host path. Pin it as the one
// non-test caller that passes one.
func TestEmptyRootHasOneCaller(t *testing.T) {
	t.Parallel()
	re := regexp.MustCompile(`\.Bind\([^,]+,\s*"",`)
	var found []string
	err := filepath.WalkDir("..", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return err
		}
		b, rerr := os.ReadFile(p) //nolint:gosec // walking this repository's own sources
		if rerr != nil {
			return rerr
		}
		if re.Match(b) {
			found = append(found, filepath.ToSlash(p))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0] != "../transport/local.go" {
		t.Errorf("callers of Bind with an empty root: %v, want only ../transport/local.go", found)
	}
}
