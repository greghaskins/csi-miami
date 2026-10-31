package intent

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/greghaskins/csi-miami/internal/transport"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// goldenIntent is the Intent that testdata/v1.json encodes. The golden file
// pins the exact bytes of a version 1 record. If a test here fails because
// the file or the encoding had to change, that is a format change: make the
// version decision (see Version) in the same commit, and keep the old file as
// a record that must still load.
//
// The record deliberately holds shapes that a stricter boundary rule could
// refuse later (a mixed-case host name, a subdir shaped like a PV name):
// records already on disk must keep loading.
func goldenIntent() Intent {
	const pv = "pvc-0a1b2c3d-1111-2222-3333-444455556666"
	return Intent{
		Version:  Version,
		VolumeID: "directory:" + pv,
		Kind:     volume.KindDirectory,
		Transport: transport.Spec{
			Kind:   transport.KindNFS3,
			Server: "Nfs.Example.COM",
			Export: "/export/data",
		},
		Subdir:    pv,
		StagePath: "/var/lib/kubelet/plugins/example/stage/" + pv,
		Publishes: []Publish{
			{TargetPath: "/var/lib/kubelet/pods/uid-a/volumes/mount", ReadOnly: false},
			{TargetPath: "/var/lib/kubelet/pods/uid-b/volumes/mount", ReadOnly: true},
		},
		MountFlags: []string{"noatime", "nodiratime"},
		AccessMode: "MULTI_NODE_MULTI_WRITER",
	}
}

// stored is what Save leaves on disk for a record written for the first time:
// the same Intent with the store's generation.
func stored(in Intent) Intent {
	in.Generation = 1
	return in
}

func readGolden(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The generation was added to version 1 after the format shipped, as an
// informational field a version 1 reader ignores (see Version), so Version
// stays 1. testdata/v1_before_generation.json is the record as it was written
// before: it must keep loading, with generation 0.
func TestRecordWrittenBeforeTheGenerationFieldStillLoads(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	want := goldenIntent() // generation 0
	old, err := os.ReadFile(filepath.Join("testdata", "v1_before_generation.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(old), "generation") {
		t.Fatal("the pre-generation record has a generation")
	}
	if err := os.WriteFile(filepath.Join(dir, Escape(want.VolumeID)+".json"), old, 0o600); err != nil { // #nosec G703 -- a test file in a temp dir
		t.Fatal(err)
	}
	got := mustLoad(t, s)
	if len(got.Failures) != 0 || !reflect.DeepEqual(got.Intents[want.VolumeID], want) {
		t.Fatalf("loaded %+v, failures %v; want %+v", got.Intents, got.Failures, want)
	}
	// And the next write continues from it.
	if err := s.Save(got.Intents[want.VolumeID]); err != nil {
		t.Fatal(err)
	}
	if again, _, _ := s.Get(want.VolumeID); again.Generation != 1 {
		t.Errorf("generation after the first write over an old record = %d, want 1", again.Generation)
	}
}

func TestGoldenRecordLoads(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	want := stored(goldenIntent())
	if err := os.WriteFile(filepath.Join(dir, Escape(want.VolumeID)+".json"), readGolden(t), 0o600); err != nil {
		t.Fatal(err)
	}

	got := mustLoad(t, s)
	if len(got.Failures) != 0 {
		t.Fatalf("golden record failed to load: %v", got.Failures)
	}
	if !reflect.DeepEqual(got.Intents[want.VolumeID], want) {
		t.Fatalf("golden record loaded as\n%+v\nwant\n%+v", got.Intents[want.VolumeID], want)
	}
	one, ok, err := s.Get(want.VolumeID)
	if err != nil || !ok || !reflect.DeepEqual(one, want) {
		t.Fatalf("Get = %+v, %v, %v", one, ok, err)
	}
}

func TestGoldenRecordBytesArePinned(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	want := goldenIntent()
	if err := s.Save(want); err != nil {
		t.Fatal(err)
	}
	written, err := os.ReadFile(filepath.Join(dir, Escape(want.VolumeID)+".json")) // #nosec G304 -- test file
	if err != nil {
		t.Fatal(err)
	}
	if string(written) != string(readGolden(t)) {
		t.Fatalf("Save wrote\n%s\nbut the golden file holds\n%s\nA change here is a format change: see Version.", written, readGolden(t))
	}
}

func TestUnknownFieldsAreTolerated(t *testing.T) {
	t.Parallel()
	s, dir := openStore(t)
	want := stored(goldenIntent())
	golden := string(readGolden(t))
	withExtra := strings.Replace(golden, `"version": 1,`, `"version": 1,
  "addedLater": {"note": "informational"},`, 1)
	withExtra = strings.Replace(withExtra, `"kind": "nfs3",`, `"kind": "nfs3",
    "alsoNew": true,`, 1)
	if strings.Count(withExtra, "addedLater") != 1 || strings.Count(withExtra, "alsoNew") != 1 {
		t.Fatal("test setup: unknown fields were not inserted")
	}
	if err := os.WriteFile(filepath.Join(dir, Escape(want.VolumeID)+".json"), []byte(withExtra), 0o600); err != nil {
		t.Fatal(err)
	}
	got := mustLoad(t, s)
	if len(got.Failures) != 0 || !reflect.DeepEqual(got.Intents[want.VolumeID], want) {
		t.Fatalf("record with unknown fields: intents %+v, failures %v", got.Intents, got.Failures)
	}
}

// A relaxed shape must validate: Validate is structural, not the boundary
// policy of the volume package.
func TestValidateIsNotTheBoundaryPolicy(t *testing.T) {
	t.Parallel()
	in := goldenIntent()
	if err := in.Validate(); err != nil {
		t.Fatalf("golden intent does not validate: %v", err)
	}
	// An unusual but structurally sound volume name and host.
	in.VolumeID = "directory:Name With Spaces~and+more"
	in.Transport.Server = "[2001:db8::1]"
	if err := in.Validate(); err != nil {
		t.Fatalf("Validate = %v, want nil: name and host rules belong to the boundary", err)
	}
}
