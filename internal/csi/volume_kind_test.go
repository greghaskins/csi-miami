package csi

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"

	"github.com/greghaskins/csi-miami/internal/holder"
	"github.com/greghaskins/csi-miami/internal/volume"
)

// volumeReq is a valid CreateVolume request for a volume of kind volume.
func (e *env) volumeReq(name string, bytes int64, extra ...string) *csi.CreateVolumeRequest {
	r := e.createReq(name, append([]string{"kind", "volume"}, extra...)...)
	r.VolumeCapabilities = []*csi.VolumeCapability{mountCap(csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER)}
	r.CapacityRange = &csi.CapacityRange{RequiredBytes: bytes}
	return r
}

func TestCreateVolumeRoundsUpAndRecordsSize(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const want = 5 * (4 << 20) // 17 MiB rounds up to five 4 MiB chunks
	resp, err := e.ctrl.CreateVolume(context.Background(), e.volumeReq(nameA, 17<<20))
	if err != nil {
		t.Fatal(err)
	}
	v := resp.GetVolume()
	if v.GetCapacityBytes() != want {
		t.Errorf("capacity = %d, want %d", v.GetCapacityBytes(), want)
	}
	if got := v.GetVolumeContext()["miami/size"]; got != "20971520" {
		t.Errorf("miami/size = %q, want 20971520", got)
	}
	if got := v.GetVolumeContext()["miami/chunkSize"]; got != "4194304" {
		t.Errorf("miami/chunkSize = %q", got)
	}
	if v.GetVolumeId() != "volume:"+nameA {
		t.Errorf("volume ID = %q", v.GetVolumeId())
	}
	ctx, err := volume.DecodeContext(v.GetVolumeContext())
	if err != nil || ctx.Volume.Size != want || !ctx.Volume.Encryption {
		t.Errorf("context does not decode as a resolved volume: %+v, %v", ctx.Volume, err)
	}
}

func TestCreateVolumeRoundUpPastTheLimit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	r := e.volumeReq(nameA, 17<<20)
	r.CapacityRange.LimitBytes = 18 << 20
	if _, err := e.ctrl.CreateVolume(context.Background(), r); codeOf(err) != codes.OutOfRange {
		t.Errorf("code = %v, want OutOfRange", codeOf(err))
	}
}

func TestCreateVolumeMakesOnlyTheDirectory(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	if _, err := e.ctrl.CreateVolume(context.Background(), e.volumeReq(nameA, 1<<30)); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(e.mountDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != nameA || !entries[0].IsDir() {
		t.Fatalf("the transport holds %v, want only the directory %s", entries, nameA)
	}
	inner, _ := os.ReadDir(filepath.Join(e.mountDir(), nameA))
	if len(inner) != 0 {
		t.Errorf("the volume directory holds %v, want it empty: the first stage formats", inner)
	}
}

func TestCreateVolumeInvalidArguments(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	tests := []struct {
		name string
		req  *csi.CreateVolumeRequest
	}{
		{"no capacity", e.volumeReq(nameA, 0)},
		{"local transport", func() *csi.CreateVolumeRequest {
			r := e.volumeReq(nameA, 1<<30)
			r.Parameters = e.localParams()
			r.Parameters["kind"] = "volume"
			return r
		}()},
		{"chunk not a power of two", e.volumeReq(nameA, 1<<30, "chunkSize", "5Mi")},
		{"chunk too small for the size", e.volumeReq(nameA, 1<<40, "chunkSize", "4Mi")},
		{"multi node mode", func() *csi.CreateVolumeRequest {
			r := e.volumeReq(nameA, 1<<30)
			r.VolumeCapabilities = rwx()
			return r
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := e.ctrl.CreateVolume(context.Background(), tt.req); codeOf(err) != codes.InvalidArgument {
				t.Errorf("code = %v (%v), want InvalidArgument", codeOf(err), err)
			}
		})
	}
}

func TestVolumeAcceptsSingleNodeModesOnly(t *testing.T) {
	t.Parallel()
	ok := []csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER,
	}
	no := []csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		csi.VolumeCapability_AccessMode_UNKNOWN,
	}
	for _, m := range ok {
		if p := capabilityProblemFor(volume.KindVolume, mountCap(m)); p != "" {
			t.Errorf("%v: %s", m, p)
		}
	}
	for _, m := range no {
		if p := capabilityProblemFor(volume.KindVolume, mountCap(m)); p == "" {
			t.Errorf("%v accepted for a volume", m)
		}
	}
	// The directory kind keeps its wider set.
	if p := capabilityProblemFor(volume.KindDirectory, mountCap(csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER)); p != "" {
		t.Errorf("directory: %s", p)
	}
}

// createdVolume creates a volume of kind volume with its PV registered and
// returns its ID and the directory the holder file sits in.
func createdVolume(t *testing.T, e *env) (id, parent string) {
	t.Helper()
	resp, err := e.ctrl.CreateVolume(context.Background(), e.volumeReq(nameA, 1<<30))
	if err != nil {
		t.Fatal(err)
	}
	e.pvs.Add(pvFor(resp))
	return resp.GetVolume().GetVolumeId(), e.mountDir()
}

func TestDeleteVolumeRefusesLiveHolder(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id, parent := createdVolume(t, e)
	h, err := holder.Acquire(context.Background(), parent, nameA, "node-a", holder.Judgement{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = e.ctrl.DeleteVolume(context.Background(), deleteReq(id))
	if codeOf(err) != codes.Unavailable {
		t.Fatalf("code = %v (%v), want Unavailable while a holder is live", codeOf(err), err)
	}
	mustExist(t, filepath.Join(parent, nameA))

	// A holder that has not renewed for longer than the takeover timeout is a
	// dead node's: the delete goes on and takes the holder files with it.
	old := time.Now().Add(-2 * volume.DefaultTakeoverTimeout)
	if err := os.Chtimes(holder.Path(parent, nameA), old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(id)); err != nil {
		t.Fatalf("delete past the takeover timeout: %v", err)
	}
	mustNotExist(t, filepath.Join(parent, nameA))
	mustNotExist(t, holder.Path(parent, nameA))
	_ = h
}

// A takeoverTimeout set on the class decides what "live" means: a holder aged
// past the default but inside the configured value still blocks the delete.
func TestDeleteVolumeHonoursTheClassTakeoverTimeout(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	resp, err := e.ctrl.CreateVolume(context.Background(), e.volumeReq(nameA, 1<<30, "takeoverTimeout", "30m"))
	if err != nil {
		t.Fatal(err)
	}
	e.pvs.Add(pvFor(resp))
	id, parent := resp.GetVolume().GetVolumeId(), e.mountDir()
	if _, err := holder.Acquire(context.Background(), parent, nameA, "node-a", holder.Judgement{}); err != nil {
		t.Fatal(err)
	}
	age := func(d time.Duration) {
		old := time.Now().Add(-d)
		if err := os.Chtimes(holder.Path(parent, nameA), old, old); err != nil {
			t.Fatal(err)
		}
	}
	age(10 * time.Minute) // past the 5m default, inside 30m
	if _, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(id)); codeOf(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable: the class says 30m", codeOf(err))
	}
	age(40 * time.Minute)
	if _, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(id)); err != nil {
		t.Fatalf("delete past the configured timeout: %v", err)
	}
}

func TestDeleteVolumeWithoutHolder(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id, parent := createdVolume(t, e)
	if _, err := e.ctrl.DeleteVolume(context.Background(), deleteReq(id)); err != nil {
		t.Fatal(err)
	}
	mustNotExist(t, filepath.Join(parent, nameA))
}
