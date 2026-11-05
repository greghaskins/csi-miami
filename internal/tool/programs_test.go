package tool_test

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/greghaskins/csi-miami/internal/tool"
)

// TestProgramsAreTheImagesTools: the programs the code starts, tool.Programs
// and hack/tools.txt (what the image is built from and the smoke test starts)
// are one list.
func TestProgramsAreTheImagesTools(t *testing.T) {
	t.Parallel()

	f, err := os.Open("../../hack/tools.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var listed []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		path := strings.Fields(line)[0]
		if filepath.Dir(path) != "/sbin" {
			t.Errorf("hack/tools.txt: %s is not under /sbin", path)
		}
		listed = append(listed, filepath.Base(path))
	}
	want := slices.Clone(tool.Programs)
	sort.Strings(want)
	sort.Strings(listed)
	if !slices.Equal(want, listed) {
		t.Errorf("hack/tools.txt names %v, tool.Programs is %v", listed, want)
	}

	// Every command a package that starts programs builds, an argument list
	// whose first element is a literal, names one of them.
	execed := map[string]bool{}
	for _, dir := range []string{"../crypt", "../ext4"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, file := range files {
			if strings.HasSuffix(file, "_test.go") {
				continue
			}
			tree, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			ast.Inspect(tree, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok || len(lit.Elts) == 0 {
					return true
				}
				if at, ok := lit.Type.(*ast.ArrayType); !ok || at.Len != nil {
					return true
				}
				if id, ok := lit.Type.(*ast.ArrayType).Elt.(*ast.Ident); !ok || id.Name != "string" {
					return true
				}
				if b, ok := lit.Elts[0].(*ast.BasicLit); ok && b.Kind == token.STRING {
					name, _ := strconv.Unquote(b.Value)
					execed[name] = true
				}
				return true
			})
		}
	}
	for name := range execed {
		if !slices.Contains(tool.Programs, name) {
			t.Errorf("the code builds a command for %q, which is not in tool.Programs (and so not in the image)", name)
		}
	}
	for _, name := range tool.Programs {
		if !execed[name] {
			t.Errorf("tool.Programs lists %q but no command for it is built in internal/crypt or internal/ext4", name)
		}
	}
}
