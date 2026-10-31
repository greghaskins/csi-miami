// Package volume defines the identity and the context of a driver volume.
//
// A volume ID is "<kind>:<name>"; it is opaque to everything except the code
// that reads the kind and name back with ParseID. The volume context is the
// map CreateVolume returns, which Kubernetes stores on the PV and sends on
// every node call; Context encodes to and decodes from it, under keys that
// all start with "miami/".
//
// StorageClass parameters are parsed by ParseParameters, the subdir template
// is expanded by ResolveSubdir, and NewContext does both and validates the
// result in one call.
//
// Every validation error carries the gRPC code InvalidArgument and names the
// parameter or field and the rule that failed, never the whole input.
package volume
