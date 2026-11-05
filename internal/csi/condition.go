package csi

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/encoding/protowire"
)

// The CSI specification this driver is built against (v1.13) removed
// VolumeCondition, but the kubelet's bindings still read it: a
// NodeGetVolumeStatsResponse field 2 holding a message {abnormal = 1, message =
// 2}, and the node capability VOLUME_CONDITION, value 4. The generated types
// here have neither, so the field is written as an unknown field of the
// response, which protobuf marshals as it is, and the capability is the enum
// value the generated type has no name for (an open enum keeps it). A client
// that does not know the field skips it.
const (
	volumeConditionField                                         = protowire.Number(2)
	volumeConditionCapability csi.NodeServiceCapability_RPC_Type = 4
)

// conditioner is implemented by a node reconciler that knows whether a
// volume's device answers its active probe. The node service asks it when the
// reconciler can say.
type conditioner interface {
	// VolumeCondition returns whether the volume is abnormal and why, in fixed
	// text that names no path.
	VolumeCondition(volumeID string) (abnormal bool, message string)
}

// withCondition sets the response's volume condition as the wire form above.
func withCondition(resp *csi.NodeGetVolumeStatsResponse, abnormal bool, message string) {
	var inner []byte
	inner = protowire.AppendTag(inner, 1, protowire.VarintType)
	v := uint64(0)
	if abnormal {
		v = 1
	}
	inner = protowire.AppendVarint(inner, v)
	if message != "" {
		inner = protowire.AppendTag(inner, 2, protowire.BytesType)
		inner = protowire.AppendString(inner, message)
	}
	var raw []byte
	raw = protowire.AppendTag(raw, volumeConditionField, protowire.BytesType)
	raw = protowire.AppendBytes(raw, inner)
	resp.ProtoReflect().SetUnknown(raw)
}
