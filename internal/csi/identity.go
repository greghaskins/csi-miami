package csi

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// DriverName is the CSI driver name, as registered with Kubernetes in the
// CSIDriver object.
const DriverName = "miami.csi.greghaskins.com"

// TopologyKeyNode is the key of the accessible topology a local transport's
// volume reports, and of the topology segment a node reports: the node's
// name.
const TopologyKeyNode = DriverName + "/node"

// Identity is the CSI identity service.
type Identity struct {
	csi.UnimplementedIdentityServer
	version string
}

var _ csi.IdentityServer = (*Identity)(nil)

// NewIdentity returns the identity service. version is reported as the
// driver's vendor version.
func NewIdentity(version string) *Identity {
	return &Identity{version: version}
}

// GetPluginInfo reports the driver name and version.
func (i *Identity) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: DriverName, VendorVersion: i.version}, nil
}

// GetPluginCapabilities reports the controller service and volume
// accessibility constraints (topology).
func (i *Identity) GetPluginCapabilities(context.Context, *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	service := func(t csi.PluginCapability_Service_Type) *csi.PluginCapability {
		return &csi.PluginCapability{Type: &csi.PluginCapability_Service_{
			Service: &csi.PluginCapability_Service{Type: t},
		}}
	}
	return &csi.GetPluginCapabilitiesResponse{Capabilities: []*csi.PluginCapability{
		service(csi.PluginCapability_Service_CONTROLLER_SERVICE),
		service(csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS),
	}}, nil
}

// Probe reports ready. The service has no dependency whose absence it could
// report here: a controller that cannot reach the API server or a transport
// says so on the call that needs it.
func (i *Identity) Probe(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}
