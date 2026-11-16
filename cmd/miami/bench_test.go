package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
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
		body := `{"stack":"` + stack + `","rounds":2,"phases":[{"name":"p","seconds":[9,` + strconv.Itoa(secs) + `]}]}`
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

func TestBenchOutRefusesExistingFile(t *testing.T) {
	out := filepath.Join(t.TempDir(), "r.json")
	if err := os.WriteFile(out, []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}
	var o, e bytes.Buffer
	args := []string{"bench", "--stack", "none", "--dir", t.TempDir(), "--scale", "small", "--rounds", "1", "--out", out}
	if code := run(args, &o, &e); code != 1 || !strings.Contains(e.String(), "refusing to overwrite") {
		t.Errorf("exit %d, stderr %q", code, e.String())
	}
	b, _ := os.ReadFile(out) //nolint:gosec // the test's own temp path
	if string(b) != "mine" {
		t.Error("the existing file was changed")
	}
}

func TestBenchParameterFlags(t *testing.T) {
	f, err := parseBenchFlags([]string{"--stack", "nbd", "--dir", "/d", "--store", "/s", "--size", "8Gi",
		"--chunk-size", "16Mi", "--cipher", "aes-xts-plain64", "--cache-bound", "2Gi", "--generation-interval", "90s"})
	if err != nil {
		t.Fatal(err)
	}
	o := f.opts
	if o.ChunkSize != 16<<20 || o.Cipher != "aes-xts-plain64" || o.CacheBound != 2<<30 || o.GenerationInterval != 90*time.Second {
		t.Errorf("parsed %+v", o)
	}

	// The defaults are what the bench did before the flags: zero values that
	// Run reads as the driver's defaults.
	f, err = parseBenchFlags([]string{"--stack", "nbd", "--dir", "/d", "--store", "/s"})
	if err != nil {
		t.Fatal(err)
	}
	if o := f.opts; o.ChunkSize != 0 || o.Cipher != "" || o.CacheBound != 0 || o.GenerationInterval != 0 {
		t.Errorf("defaults %+v, want zero values", o)
	}
	f, err = parseBenchFlags([]string{"--stack", "loop", "--dir", "/d", "--cipher", "xchacha12,aes-adiantum-plain64"})
	if err != nil || f.opts.Cipher != "xchacha12,aes-adiantum-plain64" {
		t.Errorf("loop with a cipher: %+v %v", f.opts, err)
	}
}

func TestBenchParameterUsageErrors(t *testing.T) {
	nbd := []string{"bench", "--stack", "nbd", "--dir", "/d", "--store", "/s", "--size", "8Gi"}
	for _, extra := range [][]string{
		{"--chunk-size", "3Mi"},             // not a power of two
		{"--chunk-size", "512Ki"},           // below the minimum
		{"--chunk-size", "lots"},            // not a size
		{"--cipher", "rot13"},               // not a cipher the driver offers
		{"--cache-bound", "1Mi"},            // below the (default 4 MiB) chunk size
		{"--cache-bound", "-1"},             // negative
		{"--generation-interval", "-1s"},    // negative
		{"--generation-interval", "500ms"},  // below a second
		{"--generation-interval", "sooner"}, // not a duration
	} {
		var o, e bytes.Buffer
		args := append(append([]string{}, nbd...), extra...)
		if code := run(args, &o, &e); code != 2 {
			t.Errorf("%v: exit %d (%s), want 2", extra, code, e.String())
		}
	}
	// The nbd-only parameters on another stack, and a cipher on none.
	for _, args := range [][]string{
		{"bench", "--stack", "loop", "--dir", "/d", "--chunk-size", "8Mi"},
		{"bench", "--stack", "loop", "--dir", "/d", "--cache-bound", "1Gi"},
		{"bench", "--stack", "loop", "--dir", "/d", "--generation-interval", "1m"},
		{"bench", "--stack", "none", "--dir", "/d", "--cipher", "aes-xts-plain64"},
	} {
		var o, e bytes.Buffer
		if code := run(args, &o, &e); code != 2 {
			t.Errorf("%v: exit %d (%s), want 2", args, code, e.String())
		}
	}
}
