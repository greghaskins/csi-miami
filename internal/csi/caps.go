package csi

import (
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/greghaskins/csi-miami/internal/volume"
)

// acceptedAccessModes are the access modes a directory volume supports: the
// transport's own POSIX semantics serve any number of readers and writers on
// any number of nodes, so every mode that a directory can honour is accepted.
var acceptedAccessModes = map[csi.VolumeCapability_AccessMode_Mode]bool{
	csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER:       true,
	csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER: true,
	csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:  true,
	csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY:   true,
}

// volumeAccessModes are the access modes a volume of kind volume supports in
// M2: one node at a time (the holder admits one). Multi-node modes come with
// the re-export milestone.
var volumeAccessModes = map[csi.VolumeCapability_AccessMode_Mode]bool{
	csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER:       true,
	csi.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER: true,
}

// capabilityProblem is capabilityProblemFor a directory volume.
func capabilityProblem(c *csi.VolumeCapability) string {
	return capabilityProblemFor(volume.KindDirectory, c)
}

// capabilityProblemFor returns why c is not a capability a volume of the
// kind has, or "" if it is. A capability must be a mount capability (a directory is not
// a block device), with no file system type (a directory is not formatted; the
// transport's file system is what it is), and with an access mode from
// acceptedAccessModes.
//
// Mount flags are not examined: the driver owns the mount options (the
// transport's profile), so what a StorageClass puts there has no effect.
func capabilityProblemFor(kind volume.Kind, c *csi.VolumeCapability) string {
	if c == nil {
		return "a volume capability is empty"
	}
	switch at := c.GetAccessType().(type) {
	case *csi.VolumeCapability_Mount:
		if fs := at.Mount.GetFsType(); fs != "" {
			return "fsType is not supported: a directory volume is not formatted"
		}
	case *csi.VolumeCapability_Block:
		return "block access is not supported: only mount access is"
	default:
		return "a volume capability must have an access type (mount)"
	}
	mode := c.GetAccessMode().GetMode()
	accepted := acceptedAccessModes
	if kind == volume.KindVolume {
		accepted = volumeAccessModes
	}
	if !accepted[mode] {
		if c.GetAccessMode() == nil {
			return "a volume capability must have an access mode"
		}
		return fmt.Sprintf("access mode %s is not supported", mode)
	}
	return ""
}

// checkCapabilities returns InvalidArgument naming the first capability that
// a directory volume does not have.
func checkCapabilities(caps []*csi.VolumeCapability) error {
	return checkCapabilitiesFor(volume.KindDirectory, caps)
}

// checkCapabilitiesFor is checkCapabilities for a volume of the kind.
func checkCapabilitiesFor(kind volume.Kind, caps []*csi.VolumeCapability) error {
	if len(caps) == 0 {
		return status.Error(codes.InvalidArgument, "volume capabilities are required")
	}
	for _, c := range caps {
		if p := capabilityProblemFor(kind, c); p != "" {
			return status.Error(codes.InvalidArgument, p)
		}
	}
	return nil
}
