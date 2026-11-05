package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/greghaskins/csi-miami/internal/chunk"
)

// ext4Image is size bytes with an ext4 superblock: the magic and a feature
// set that includes extents.
func ext4Image(size int) []byte {
	b := make([]byte, size)
	binary.LittleEndian.PutUint16(b[1024+56:], 0xEF53)
	binary.LittleEndian.PutUint32(b[1024+0x60:], 0x40|0x200)
	return b
}

// luks2Image is size bytes starting with a LUKS2 header magic and version.
func luks2Image(size int) []byte {
	b := make([]byte, size)
	copy(b, "LUKS\xba\xbe")
	binary.BigEndian.PutUint16(b[6:], 2)
	return b
}

func writeImage(t *testing.T, data []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "disk.img")
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func volumeDir(t *testing.T) string {
	t.Helper()
	d := filepath.Join(t.TempDir(), "pvc-1234")
	if err := os.Mkdir(d, 0o755); err != nil { //nolint:gosec // test fixture
		t.Fatal(err)
	}
	return d
}

func TestDetectFormat(t *testing.T) {
	t.Parallel()
	ext2 := ext4Image(4096)
	binary.LittleEndian.PutUint32(ext2[1024+0x60:], 0)
	luks1 := luks2Image(4096)
	binary.BigEndian.PutUint16(luks1[6:], 1)
	for _, tc := range []struct {
		name string
		data []byte
		want imageClass
	}{
		{"luks2", luks2Image(4096), classEncrypted},
		{"ext4", ext4Image(4096), classPlain},
		{"luks1", luks1, classUnknown},
		{"ext2 without ext4 features", ext2, classUnknown},
		{"zeros", make([]byte, 4096), classUnknown},
		{"short", []byte("LUKS"), classUnknown},
		{"empty", nil, classUnknown},
	} {
		if got := detectFormat(tc.data); got != tc.want {
			t.Errorf("%s: class %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestImportChunksImageAndPublishesEpochZero(t *testing.T) {
	t.Parallel()
	img := ext4Image(int(2*testMiB + testMiB/2)) // not a whole number of chunks
	for i := range img[testMiB:] {
		img[int(testMiB)+i] = byte(i%251 + 1)
	}
	vol := volumeDir(t)

	code, stdout, stderr := runMiami("import", "-image", writeImage(t, img), "-store", vol, "-chunk-size", "1048576")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "plain") || !strings.Contains(stdout, "0.1") {
		t.Errorf("report does not name the class and manifest: %q", stdout)
	}

	s, err := chunk.Open(vol)
	if err != nil {
		t.Fatal(err)
	}
	cur, ok, err := s.Current(context.Background())
	if err != nil || !ok || cur != "0.1" {
		t.Fatalf("current = %q, %v, %v; want 0.1", cur, ok, err)
	}
	m, err := s.ReadManifest(context.Background(), cur)
	if err != nil {
		t.Fatal(err)
	}
	if m.Volume != "pvc-1234" || m.Size != 3*testMiB || m.Chunk != testMiB || len(m.Chunks) != 3 {
		t.Fatalf("manifest = %+v", m)
	}

	// The padded tail reads as zeros, so the image is unchanged to the fs.
	out := filepath.Join(t.TempDir(), "back.img")
	if code, _, stderr := runMiami("restore", "-store", vol, "-out", out); code != 0 {
		t.Fatalf("restore exit %d: %s", code, stderr)
	}
	got, _ := os.ReadFile(out) //nolint:gosec // test fixture
	want := append(append([]byte{}, img...), make([]byte, testMiB/2)...)
	if !bytes.Equal(got, want) {
		t.Fatal("the restored image is not the imported one, zero padded")
	}
}

func TestImportStoresTheZeroChunkOnceAndSparse(t *testing.T) {
	t.Parallel()
	img := luks2Image(int(4 * testMiB)) // chunk 0 has the header, 1 to 3 are zero
	vol := volumeDir(t)
	code, stdout, stderr := runMiami("import", "-image", writeImage(t, img), "-store", vol, "-chunk-size", "1048576")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "encrypted") {
		t.Errorf("report does not name the encrypted class: %q", stdout)
	}
	s, _ := chunk.Open(vol)
	m, err := s.ReadManifest(context.Background(), "0.1")
	if err != nil {
		t.Fatal(err)
	}
	zero := chunk.ZeroHash(testMiB)
	if m.Chunks[0] == zero || m.Chunks[1] != zero || m.Chunks[2] != zero || m.Chunks[3] != zero {
		t.Fatalf("manifest chunks = %v", m.Chunks)
	}
	if err := s.Get(context.Background(), zero, io.Discard); err != nil {
		t.Fatalf("the manifest names the zero chunk but it does not verify in the store: %v", err)
	}
	var files int
	_ = filepath.WalkDir(filepath.Join(vol, "chunks"), func(_ string, d os.DirEntry, _ error) error {
		if !d.IsDir() {
			files++
		}
		return nil
	})
	if files != 2 { // the header chunk, and the zero chunk once for three regions
		t.Fatalf("%d chunk files, want 2", files)
	}
}

func TestImportRefusesAnExistingVolume(t *testing.T) {
	t.Parallel()
	img := writeImage(t, ext4Image(int(testMiB)))
	for name, setup := range map[string]func(t *testing.T, vol string){
		"current": func(t *testing.T, vol string) {
			must(t, os.WriteFile(filepath.Join(vol, "current"), []byte("1.1\n"), 0o644)) //nolint:gosec // test fixture
		},
		"manifest": func(t *testing.T, vol string) {
			must(t, os.MkdirAll(filepath.Join(vol, "manifests"), 0o755)) //nolint:gosec // test fixture
			must(t, os.WriteFile(filepath.Join(vol, "manifests", "1.1"), []byte("x"), 0o644))
		},
		"holder file": func(t *testing.T, vol string) {
			must(t, os.WriteFile(vol+".holder", []byte("x"), 0o644)) //nolint:gosec // test fixture
		},
		"epoch claim": func(t *testing.T, vol string) {
			must(t, os.WriteFile(vol+".holder.epoch-3", []byte("x"), 0o644)) //nolint:gosec // test fixture
		},
	} {
		vol := volumeDir(t)
		setup(t, vol)
		before := dirNames(t, vol)
		code, _, stderr := runMiami("import", "-image", img, "-store", vol, "-chunk-size", "1048576")
		if code != 1 || !strings.Contains(stderr, "already") {
			t.Errorf("%s: exit %d, stderr %q", name, code, stderr)
		}
		if got := dirNames(t, vol); got != before {
			t.Errorf("%s: the volume directory changed: %q -> %q", name, before, got)
		}
	}
}

func TestImportRefusesUnknownFormat(t *testing.T) {
	t.Parallel()
	vol := volumeDir(t)
	code, _, stderr := runMiami("import", "-image", writeImage(t, make([]byte, testMiB)), "-store", vol, "-chunk-size", "1048576")
	if code != 1 || !strings.Contains(stderr, "LUKS2") {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	if got := dirNames(t, vol); got != "" {
		t.Fatalf("a refused import left %q in the volume directory", got)
	}
}

func TestImportRefusesBadInput(t *testing.T) {
	t.Parallel()
	img := writeImage(t, ext4Image(int(testMiB)))
	vol := volumeDir(t)
	for name, args := range map[string][]string{
		"relative image":  {"-image", "disk.img", "-store", vol},
		"traversal store": {"-image", img, "-store", vol + "/../x"},
		"no image":        {"-store", vol},
		"no store":        {"-image", img},
		"directory image": {"-image", filepath.Dir(img), "-store", vol},
		"missing image":   {"-image", filepath.Join(filepath.Dir(img), "absent"), "-store", vol},
		"bad chunk size":  {"-image", img, "-store", vol, "-chunk-size", "3000000"},
		"bad volume name": {"-image", img, "-store", vol, "-volume", "a\nb"},
	} {
		if code, _, _ := runMiami(append([]string{"import"}, args...)...); code == 0 {
			t.Errorf("%s: accepted", name)
		}
	}
	if got := dirNames(t, vol); got != "" {
		t.Fatalf("refused imports left %q behind", got)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func dirNames(t *testing.T, dir string) string {
	t.Helper()
	var names []string
	_ = filepath.WalkDir(dir, func(p string, _ os.DirEntry, _ error) error {
		if p != dir {
			names = append(names, strings.TrimPrefix(p, dir))
		}
		return nil
	})
	return strings.Join(names, ",")
}

func TestImportWithNoZeroRegionStoresNoZeroChunk(t *testing.T) {
	t.Parallel()
	img := ext4Image(int(testMiB))
	for i := 2048; i < len(img); i++ {
		img[i] = 7
	}
	vol := volumeDir(t)
	if code, _, stderr := runMiami("import", "-image", writeImage(t, img), "-store", vol, "-chunk-size", "1048576"); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	s, _ := chunk.Open(vol)
	if ok, _ := s.Has(chunk.ZeroHash(testMiB)); ok {
		t.Fatal("a zero chunk was stored for a manifest that does not name one")
	}
}

// A holder that stages the volume and publishes between import's checks and
// its publish must not be rolled back to the import.
func TestImportPublishCannotOverwriteALiveCurrent(t *testing.T) {
	t.Parallel()
	vol := volumeDir(t)
	s, err := chunk.Open(vol)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	live := chunk.Manifest{Volume: "pvc-1234", Epoch: 1, Sequence: 1, Size: testMiB, Chunk: testMiB, Chunks: []chunk.Hash{chunk.ZeroHash(testMiB)}}
	must(t, s.WriteManifest(ctx, live))
	must(t, s.Publish(ctx, "1.1"))
	imp := live
	imp.Epoch = 0
	must(t, s.WriteManifest(ctx, imp))
	if err := s.PublishFirst(ctx, "0.1"); err == nil {
		t.Fatal("the import's publish replaced a live current")
	}
	if b, _ := os.ReadFile(filepath.Join(vol, "current")); string(b) != "1.1\n" { //nolint:gosec // test fixture
		t.Fatalf("current = %q, want 1.1", b)
	}
}

func TestImportInterruptedIsRefusedWithTheWayOut(t *testing.T) {
	t.Parallel()
	vol := volumeDir(t)
	s, _ := chunk.Open(vol)
	must(t, s.WriteManifest(context.Background(), chunk.Manifest{Volume: "pvc-1234", Size: testMiB, Chunk: testMiB, Chunks: []chunk.Hash{chunk.ZeroHash(testMiB)}, Sequence: 1}))
	code, _, stderr := runMiami("import", "-image", writeImage(t, ext4Image(int(testMiB))), "-store", vol, "-chunk-size", "1048576")
	if code != 1 || !strings.Contains(stderr, "interrupted") || !strings.Contains(stderr, filepath.Join(vol, "manifests", "0.1")) {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	must(t, os.Remove(filepath.Join(vol, "manifests", "0.1")))
	if code, _, stderr := runMiami("import", "-image", writeImage(t, ext4Image(int(testMiB))), "-store", vol, "-chunk-size", "1048576"); code != 0 {
		t.Fatalf("retry after removing the manifest: exit %d: %s", code, stderr)
	}
}

func TestImportCapacityDerivesTheChunkSizeAndSizesTheDevice(t *testing.T) {
	t.Parallel()
	vol := volumeDir(t)
	img := writeImage(t, ext4Image(int(testMiB)))
	// 300 GiB needs chunks of 8 MiB to stay within 65536 lines (4 MiB up to 256).
	const capacity = 300 << 30
	code, stdout, stderr := runMiami("import", "-image", img, "-store", vol, "-capacity", "322122547200")
	if code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	s, _ := chunk.Open(vol)
	m, err := s.ReadManifest(context.Background(), "0.1")
	if err != nil {
		t.Fatal(err)
	}
	if m.Chunk != chunk.DefaultChunkSize(capacity) || m.Chunk != 8<<20 || m.Size != chunk.RoundUp(capacity, m.Chunk) {
		t.Fatalf("chunk %d size %d", m.Chunk, m.Chunk)
	}
	if !strings.Contains(stdout, "chunkSize 8388608") {
		t.Errorf("report does not name the chunkSize: %q", stdout)
	}
	if code, _, _ := runMiami("import", "-image", img, "-store", volumeDir(t), "-capacity", "1000"); code == 0 {
		t.Error("a capacity below the image size was accepted")
	}
}

// A racing "holder" makes the volume live (current = 1.1, created exclusively)
// as soon as the import's manifest appears, which is after the import's last
// check and before its publish. Whoever creates current first wins: the
// import must either win outright or fail, and must never replace 1.1.
func TestImportRaceWithALiveHolderNeverRollsBack(t *testing.T) {
	t.Parallel()
	img := writeImage(t, ext4Image(int(8*testMiB)))
	var raced int
	for range 60 {
		vol := volumeDir(t)
		done := make(chan bool)
		go func() {
			for {
				ents, _ := os.ReadDir(filepath.Join(vol, "manifests"))
				for _, e := range ents {
					if e.Name() == "0.1" {
						f, err := os.OpenFile(filepath.Join(vol, "current"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644) //nolint:gosec // test fixture
						if err != nil {
							done <- false
							return
						}
						_, _ = f.WriteString("1.1\n")
						_ = f.Close()
						done <- true
						return
					}
				}
				if _, err := os.Stat(filepath.Join(vol, "current")); err == nil {
					done <- false
					return
				}
			}
		}()
		code, _, stderr := runMiami("import", "-image", img, "-store", vol, "-chunk-size", "1048576")
		holderWon := <-done
		b, _ := os.ReadFile(filepath.Join(vol, "current")) //nolint:gosec // test fixture
		if holderWon {
			raced++
			if string(b) != "1.1\n" {
				t.Fatalf("import rolled a live current back to %q", b)
			}
			if code == 0 {
				t.Fatalf("import reported success but the holder's current stands: %s", stderr)
			}
		} else if code != 0 || string(b) != "0.1\n" {
			t.Fatalf("import won but exit %d, current %q: %s", code, b, stderr)
		}
	}
	t.Logf("holder won %d of 60", raced)
}
