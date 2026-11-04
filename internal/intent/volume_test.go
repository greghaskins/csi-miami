package intent

import (
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

var updateGolden = flag.Bool("update-volume-golden", false, "rewrite testdata/v1_volume.json")

// volumeIntent is the Intent that testdata/v1_volume.json encodes.
func volumeIntent() Intent {
	const pv = "pvc-0a1b2c3d-1111-2222-3333-444455556666"
	return Intent{
		Version:  Version,
		VolumeID: "volume:" + pv,
		Kind:     volume.KindVolume,
		Transport: transport.Spec{
			Kind:   transport.KindNFS3,
			Server: "nfs.example.com",
			Export: "/export/data",
		},
		StagePath:  "/var/lib/kubelet/plugins/example/stage/" + pv,
		AccessMode: "SINGLE_NODE_WRITER",
		Epoch:      3,
		Volume: &volume.VolumeParams{
			ChunkSize: 4 << 20, CacheSize: 1 << 30, Size: 1 << 30,
			Encryption: true, Cipher: "xchacha12,aes-adiantum-plain64", KeySize: 256,
			Generations: volume.GenerationParams{
				Interval: 5 * time.Minute, MaxAge: time.Hour, Keep: 3, DirtyWatermark: 512 << 20,
			},
		},
	}
}

func TestVolumeIntentLoads(t *testing.T) {
	t.Parallel()
	path := filepath.Join("testdata", "v1_volume.json")
	if *updateGolden {
		s, dir := openStore(t)
		mustSave(t, s, volumeIntent())
		b, err := os.ReadFile(filepath.Join(dir, Escape(volumeIntent().VolumeID)+".json")) // #nosec G304 -- test file
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, b, 0o600); err != nil { // #nosec G306 G703 -- testdata
			t.Fatal(err)
		}
	}
	golden, err := os.ReadFile(path) // #nosec G304 -- testdata
	if err != nil {
		t.Fatal(err)
	}
	s, dir := openStore(t)
	want := stored(volumeIntent())
	if err := os.WriteFile(filepath.Join(dir, Escape(want.VolumeID)+".json"), golden, 0o600); err != nil {
		t.Fatal(err)
	}
	got := mustLoad(t, s)
	if len(got.Failures) != 0 || !reflect.DeepEqual(got.Intents[want.VolumeID], want) {
		t.Fatalf("loaded %+v, failures %v; want %+v", got.Intents, got.Failures, want)
	}
	// Saving the same intent writes the golden bytes.
	s2, dir2 := openStore(t)
	mustSave(t, s2, volumeIntent())
	written, err := os.ReadFile(filepath.Join(dir2, Escape(want.VolumeID)+".json")) // #nosec G304 -- test file
	if err != nil {
		t.Fatal(err)
	}
	if string(written) != string(golden) {
		t.Fatalf("Save wrote\n%s\nbut the golden file holds\n%s", written, golden)
	}
}

// TestIntentGoldenBytesUnchanged: a directory record carries neither the
// volume nor the epoch field, so the version 1 golden bytes stay as they
// were.
func TestIntentGoldenBytesUnchanged(t *testing.T) {
	t.Parallel()
	b, err := goldenIntent().MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"volume"`, `"epoch"`} {
		if strings.Contains(string(b), key) {
			t.Errorf("a directory record has %s: %s", key, b)
		}
	}
	s, dir := openStore(t)
	mustSave(t, s, goldenIntent())
	written, err := os.ReadFile(filepath.Join(dir, Escape(goldenIntent().VolumeID)+".json")) // #nosec G304 -- test file
	if err != nil {
		t.Fatal(err)
	}
	if string(written) != string(readGolden(t)) {
		t.Fatalf("the directory record's bytes changed:\n%s", written)
	}
}

func TestVolumeKindNeedsVolumeParams(t *testing.T) {
	t.Parallel()
	in := volumeIntent()
	in.Volume = nil
	if in.Validate() == nil {
		t.Error("a volume record without parameters validates")
	}
	d := goldenIntent()
	d.Volume = volumeIntent().Volume
	if d.Validate() == nil {
		t.Error("a directory record with volume parameters validates")
	}
}
