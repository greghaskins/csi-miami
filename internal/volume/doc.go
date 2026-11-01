// Package volume defines the identity and the context of a driver volume.
//
// A volume ID is "<kind>:<name>"; it is opaque to everything except the code
// that reads the kind and name back with ParseID. The name is always
// pvc-<uuid>, in lower-case hex, the shape the external-provisioner gives a
// PV (IsPVName): NewID and ParseID refuse any other, and so do MatchesID and
// Dir, which validate the ID they are given, so a static PV keeps or is given
// such a name. (DecodeContext never sees a name: the volume context does not
// carry one.) The volume context is the
// map CreateVolume returns, which Kubernetes stores on the PV and sends on
// every node call; Context encodes to and decodes from it, under keys that
// all start with "miami/".
//
// A volume's directory is always <transport mount>/<subdir>/<name>: subdir is
// an optional per-class prefix, and name is the volume's own name from its
// ID, never from a parameter. Context.Dir is the one place that path is
// computed. Because the name is always the last element, no template can make
// two volumes share a directory, and because no subdir element may look like
// a PV name while every volume name does, no subdir element can be a volume
// and no volume can nest inside another's.
//
// The optional mode parameter is the mode of the volume's own directory when
// the driver creates it (ParseMode: up to four octal digits, 0755 by default).
// The context carries it in the canonical four-digit form under miami/mode,
// and only when the StorageClass sets it, so that the node, which creates a
// local volume's directory, sees it too.
//
// StorageClass parameters are parsed by ParseParameters, the subdir template
// is expanded by ResolveSubdir, and NewContext does both and validates the
// result in one call. DecodeContext accepts exactly what NewContext
// produces and never repairs a value.
//
// Every validation error carries the gRPC code InvalidArgument and names the
// parameter or field and the rule that failed, never the whole input.
package volume
