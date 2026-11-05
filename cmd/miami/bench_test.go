package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestBenchUsageErrors(t *testing.T) {
	for _, args := range [][]string{
		{"bench"},
		{"bench", "--stack", "nbd", "--dir", "/x"},
		{"bench", "--stack", "none", "--dir", "/x", "--size", "lots"},
		{"bench", "--stack", "none", "--dir", "/x", "--scale", "huge"},
		{"bench", "--compare", "only-one.json"},
	} {
		var o, e bytes.Buffer
		if code := run(args, &o, &e); code != 2 {
			t.Errorf("%v: exit %d (%s), want 2", args, code, e.String())
		}
	}
}

func TestBenchNoneJSONAndCompare(t *testing.T) {
	dir := t.TempDir()
	var o, e bytes.Buffer
	if code := run([]string{"bench", "--stack", "none", "--dir", dir, "--scale", "small", "--rounds", "1", "--json"}, &o, &e); code != 0 {
		t.Fatalf("exit %d: %s", code, e.String())
	}
	if !strings.Contains(o.String(), `"stack": "none"`) {
		t.Errorf("json lacks the stack:\n%s", o.String())
	}
}

func TestBenchRefusesNonEmptyDir(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "keep"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	var o, e bytes.Buffer
	if code := run([]string{"bench", "--stack", "none", "--dir", dir}, &o, &e); code != 1 || !strings.Contains(e.String(), "keep") {
		t.Errorf("exit %d, stderr %q", code, e.String())
	}
}

func TestBenchRefusesNonRoot(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root")
	}
	var o, e bytes.Buffer
	code := run([]string{"bench", "--stack", "loop", "--dir", t.TempDir()}, &o, &e)
	if code != 1 || !strings.Contains(e.String(), "must run as root") {
		t.Errorf("exit %d, stderr %q", code, e.String())
	}
}

func TestBenchCompareExit(t *testing.T) {
	dir := t.TempDir()
	write := func(name, stack string, secs int) string {
		p := filepath.Join(dir, name)
		body := `{"stack":"` + stack + `","rounds":1,"phases":[{"name":"p","seconds":[` + strconv.Itoa(secs) + `]}]}`
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	loop := write("loop.json", "loop", 2)
	good := write("nbd.json", "nbd", 3)
	bad := write("nbd-slow.json", "nbd", 4)
	var o, e bytes.Buffer
	if code := run([]string{"bench", "--compare", good, loop}, &o, &e); code != 0 || !strings.Contains(o.String(), "G1: PASS") {
		t.Errorf("good: exit %d\n%s%s", code, o.String(), e.String())
	}
	o.Reset()
	if code := run([]string{"bench", "--compare", loop, bad}, &o, &e); code != 1 || !strings.Contains(o.String(), "G1: FAIL") {
		t.Errorf("bad: exit %d\n%s%s", code, o.String(), e.String())
	}
}
