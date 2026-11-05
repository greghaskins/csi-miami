package csi

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// wireCondition decodes field 2 of a NodeGetVolumeStatsResponse as the
// kubelet's older CSI bindings do: a VolumeCondition {abnormal=1, message=2}.
func wireCondition(t *testing.T, b []byte) (found, abnormal bool, message string) {
	t.Helper()
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			t.Fatal(protowire.ParseError(n))
		}
		b = b[n:]
		if num != volumeConditionField || typ != protowire.BytesType {
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				t.Fatal(protowire.ParseError(n))
			}
			b = b[n:]
			continue
		}
		msg, n := protowire.ConsumeBytes(b)
		if n < 0 {
			t.Fatal(protowire.ParseError(n))
		}
		b = b[n:]
		found = true
		for len(msg) > 0 {
			num, typ, n := protowire.ConsumeTag(msg)
			msg = msg[n:]
			switch {
			case num == 1 && typ == protowire.VarintType:
				v, n := protowire.ConsumeVarint(msg)
				msg, abnormal = msg[n:], v != 0
			case num == 2 && typ == protowire.BytesType:
				v, n := protowire.ConsumeString(msg)
				msg, message = msg[n:], v
			default:
				t.Fatalf("unexpected field %d in the condition", num)
			}
		}
	}
	return found, abnormal, message
}

func TestNodeGetVolumeStatsReportsTheCondition(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := unix.Statfs_t{Bsize: 4096, Blocks: 1000, Bfree: 400, Bavail: 300, Files: 100, Ffree: 60}
	req := &csi.NodeGetVolumeStatsRequest{VolumeId: nodeVol, VolumePath: podTarget}

	for _, tc := range []struct {
		name     string
		abnormal bool
		msg      string
	}{
		{"abnormal", true, "the volume's device did not answer a read of its first sector"},
		{"normal", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &fakeRec{record: stagedRecord(), abnormal: tc.abnormal, abnormalMsg: tc.msg}
			resp, err := newTestNode(t, rec, &fakeStatfs{st: st}).NodeGetVolumeStats(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			// Over the wire, as the kubelet receives it.
			b, err := proto.Marshal(resp)
			if err != nil {
				t.Fatal(err)
			}
			found, abnormal, msg := wireCondition(t, b)
			if found != tc.abnormal || abnormal != tc.abnormal || msg != tc.msg {
				t.Errorf("on the wire: found %v, abnormal %v, message %q; want abnormal %v, %q", found, abnormal, msg, tc.abnormal, tc.msg)
			}
			// The usage is still there for a client that knows nothing of it.
			var back csi.NodeGetVolumeStatsResponse
			if err := proto.Unmarshal(b, &back); err != nil {
				t.Fatal(err)
			}
			if u := back.GetUsage(); len(u) != 2 || u[0].GetTotal() != 1000*4096 {
				t.Errorf("usage = %v", u)
			}
		})
	}
}

func TestVolumeConditionCapabilityIsAdvertised(t *testing.T) {
	t.Parallel()
	if volumeConditionCapability != 4 {
		t.Errorf("VOLUME_CONDITION is value 4 of the node capability enum, have %d", volumeConditionCapability)
	}
}
