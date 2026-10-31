// Package transport describes where a driver directory comes from, and mounts
// it.
//
// A Spec is the value a StorageClass (and later a PV's volume context)
// carries: a kind, an address and a path. Validate says whether a Spec is
// well formed, Canonical gives one deterministic line for it, and ID is a
// short hash of that line, so two Specs that mean the same place always
// share an ID and therefore a mount point. Every validation error carries the
// gRPC code InvalidArgument and names the field and the rule that failed.
//
// A Transport is a Spec made real: "nfs3" is mount(2) of an NFSv3 export with
// the one option profile the driver owns, "local" is a read-write bind of a
// host directory. Both are mounted at <root>/transports/<ID> on the host
// mount table, so they survive a restart of the process. The kernel is the
// only truth about whether a transport is mounted: Mounted reads the mount
// table, and answers true only for a mount that is exactly this transport,
// which is how a fresh process adopts what its predecessor mounted. Any other
// mount at the mount point is a conflict (mount.ErrConflict), never adopted
// and never stacked over.
//
// Adoption is exact about the source: the kernel prints the source of a shared
// NFS superblock under the spelling of whichever mount came first, so a given
// server is named one way across all StorageClasses on a cluster (the normal
// form of a Spec already collapses case). Naming one server by both a host
// name and an address is unsupported, and shows as a mount conflict.
//
// The driver is IPv4 only: a server is a host name or a dotted IPv4 literal,
// an IPv6 literal is rejected, and a host name must resolve to an IPv4 address.
//
// Nothing here execs a helper. Name resolution and host path resolution run
// under a deadline of their own, and mounting goes through a mount.Mounter,
// which bounds every system call the same way.
package transport
