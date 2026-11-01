package intent

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// The mode is informational for a version 1 reader: it is written only when
// set, so a record of a volume without one is byte for byte what it was (the
// golden tests pin that), and one with it round-trips.
func TestModeRoundTripsAndIsOmittedWhenAbsent(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	with := goldenIntent()
	with.Mode = "2775"
	mustSave(t, s, with)

	b, err := os.ReadFile(filepath.Join(dir, Escape(with.VolumeID)+".json")) // #nosec G304 -- test file
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"mode": "2775"`) {
		t.Errorf("record = %s; want it to hold the mode", b)
	}
	got := mustLoad(t, s)
	if len(got.Failures) != 0 || !reflect.DeepEqual(got.Intents[with.VolumeID], stored(with)) {
		t.Fatalf("loaded %+v, failures %v; want %+v", got.Intents, got.Failures, stored(with))
	}
	if c := got.Intents[with.VolumeID].Context(); c.Mode != "2775" || c.DirMode() != 0o2775 {
		t.Errorf("Context() = %+v", c)
	}

	without := goldenIntent()
	if strings.Contains(string(readGolden(t)), "mode") {
		t.Fatal("the golden record holds a mode")
	}
	if without.Mode != "" || without.Context().DirMode() != 0o755 {
		t.Errorf("an intent without a mode: %q, %o", without.Mode, without.Context().DirMode())
	}
}

func TestValidateMode(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"", "0755", "0777", "2775", "0000"} {
		in := goldenIntent()
		in.Mode = mode
		if err := in.Validate(); err != nil {
			t.Errorf("mode %q: %v", mode, err)
		}
	}
	for _, mode := range []string{"755", "0o755", "0758", "12345", " 0755", "-755"} {
		in := goldenIntent()
		in.Mode = mode
		err := in.Validate()
		if err == nil || !strings.Contains(err.Error(), "mode") {
			t.Errorf("mode %q: Validate() = %v; want an error naming mode", mode, err)
		}
		if err != nil && strings.Contains(err.Error(), mode) && mode != "" {
			t.Errorf("mode %q: the error repeats the value: %v", mode, err)
		}
	}
}
