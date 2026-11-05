package volume

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"

	"github.com/greghaskins/csi-miami/internal/chunk"
	"github.com/greghaskins/csi-miami/internal/sizerec"
)

// unitSize is the -size the first started unit was given.
func (e *env) unitSize() int64 {
	e.t.Helper()
	if len(e.sd.units) == 0 {
		e.t.Fatal("no unit was started")
	}
	args := e.sd.units[0].ExecStart
	n, err := strconv.ParseInt(args[slices.Index(args, "-size")+1], 10, 64)
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

// writeManifest puts a manifest of the given size in the volume's store.
func (e *env) writeManifest(name string, size int64) {
	e.t.Helper()
	dir := filepath.Join(e.v.storeDir, "manifests")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		e.t.Fatal(err)
	}
	var epoch, seq uint64
	epoch, seq, err := chunk.ParseManifestName(name)
	if err != nil {
		e.t.Fatal(err)
	}
	m := chunk.Manifest{Volume: e.v.name, Epoch: epoch, Sequence: seq, Size: size, Chunk: testChunk, Chunks: make([]chunk.Hash, size/testChunk)}
	if err := os.WriteFile(filepath.Join(dir, name), m.Marshal(), 0o600); err != nil {
		e.t.Fatal(err)
	}
}

// A NodeExpandVolume raises the intent's GrownSize; the converge that follows
// grows the stack with the steps a stage already has: the block server's
// device, the mapping, then the mounted file system.
func TestGrownSizeInTheIntentGrowsTheStack(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	e.mustConverge() // staged at full size
	e.rec.reset()
	e.in.GrownSize = 2 * testSize
	e.mustConverge()
	e.rec.inOrder(t, "ctl.Grow", "tool cryptsetup resize", "tool resize2fs")
	if e.ctl.size != 2*testSize {
		t.Errorf("the device is %d, want %d", e.ctl.size, 2*testSize)
	}
	// Repeating it changes nothing more.
	e.rec.reset()
	e.mustConverge()
	e.rec.none(t, "ctl.Grow", "tool cryptsetup resize", "tool resize2fs")
}

// The regress scenario: a volume grown, published, unstaged and staged again
// comes back with a PV whose attributes still say the size it was created
// with. Every restage takes the largest size any record holds.
func TestRestageNeverComesBackSmaller(t *testing.T) {
	tests := []struct {
		name  string
		setup func(e *env)
		want  int64
	}{
		{"nothing grown", func(*env) {}, testSize},
		{"a size record beside the volume", func(e *env) {
			if _, err := sizerec.Grow(e.v.holdDir, e.v.name, 3*testSize); err != nil {
				t.Fatal(err)
			}
		}, 3 * testSize},
		{"a published manifest that is larger", func(e *env) {
			e.writeManifest("1.1", 4*testSize)
			e.curName = "1.1"
		}, 4 * testSize},
		{"the intent's grown size", func(e *env) { e.in.GrownSize = 5 * testSize }, 5 * testSize},
		{"a record smaller than the manifest", func(e *env) {
			if _, err := sizerec.Grow(e.v.holdDir, e.v.name, 3*testSize); err != nil {
				t.Fatal(err)
			}
			e.writeManifest("1.1", 6*testSize)
			e.curName = "1.1"
		}, 6 * testSize},
		{"a record smaller than the context", func(e *env) {
			if _, err := sizerec.Grow(e.v.holdDir, e.v.name, testSize/2); err != nil {
				t.Fatal(err)
			}
		}, testSize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			tt.setup(e)
			e.mustConverge()
			if got := e.unitSize(); got != tt.want {
				t.Errorf("the server was started at %d, want %d", got, tt.want)
			}
		})
	}
}

func TestMalformedSizeRecordStopsTheStage(t *testing.T) {
	e := newEnv(t)
	if err := os.WriteFile(sizerec.Path(e.v.holdDir, e.v.name), []byte("garbage\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := e.converge()
	if !errors.Is(err, sizerec.ErrMalformed) {
		t.Fatalf("Converge = %v, want the malformed record", err)
	}
	e.rec.none(t, "systemd.Start")
}

// A restage of a running volume whose store records a larger size than the
// device grows the device, the mapping and the file system to it.
func TestStageFindsGrowthThatWasRecordedButNeverDone(t *testing.T) {
	e := newEnv(t)
	e.seedRunning()
	if _, err := sizerec.Grow(e.v.holdDir, e.v.name, 2*testSize); err != nil {
		t.Fatal(err)
	}
	e.mustConverge()
	e.rec.inOrder(t, "ctl.Grow", "tool cryptsetup resize", "tool resize2fs")
}
