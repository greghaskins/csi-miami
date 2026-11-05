package csi

import (
	"context"
	"strings"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	"github.com/greghaskins/csi-miami/internal/probe"
)

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
		{"abnormal", true, probe.MessageFailed},
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
			var back csi.NodeGetVolumeStatsResponse
			if err := proto.Unmarshal(b, &back); err != nil {
				t.Fatal(err)
			}
			c := back.GetVolumeCondition()
			if c == nil || c.GetAbnormal() != tc.abnormal || c.GetMessage() != tc.msg {
				t.Errorf("condition = %v, want abnormal %v, %q", c, tc.abnormal, tc.msg)
			}
			if u := back.GetUsage(); len(u) != 2 || u[0].GetTotal() != 1000*4096 {
				t.Errorf("usage = %v", u)
			}
		})
	}
}

func TestProbeMessagesCarryTheGuidance(t *testing.T) {
	t.Parallel()
	for _, m := range []string{probe.MessageFailed, probe.MessageTimeout} {
		if !strings.Contains(m, "restart the volume's pod once the store is reachable and the cache has drained") {
			t.Errorf("message %q lacks the guidance", m)
		}
	}
}
