// Package transport describes where a driver directory comes from.
//
// A Spec is the value a StorageClass (and later a PV's volume context)
// carries: a kind, an address and a path. Validate says whether a Spec is
// well formed, Canonical gives one deterministic line for it, and ID is a
// short hash of that line, so two Specs that mean the same place always
// share an ID and therefore a mount point.
//
// Every validation error carries the gRPC code InvalidArgument and names the
// field and the rule that failed.
package transport
