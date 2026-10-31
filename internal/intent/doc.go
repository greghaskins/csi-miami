// Package intent persists what should be true on this node for each volume.
//
// An Intent is a record of desire, never of observation: it names a volume,
// its transport, its stage path and its publish targets, and says nothing
// about what the kernel currently mounts. Observers report that; the
// reconciler compares the two.
//
// # Store
//
// A Store keeps one JSON file per volume in a single directory; use one Store
// per directory per process and share it. Save writes atomically and durably
// (temp file, fsync, rename, fsync of the directory), so a crash leaves the
// old record or the new one, never a partial one. Update is a locked
// read-modify-write for callers that change part of a record. Load reads the
// directory afresh each time and never fails because of one bad record: a
// record that cannot be read or is invalid is reported in Loaded.Failures and
// left on disk for inspection, and its volume simply has no intent. A later
// Save of that volume overwrites the kept file, which is the repair path.
// Temp files left behind by a crash are removed by Load.
//
// The package does not log. Load returns the failures; the caller logs when
// the set of failed paths changes and exports the count (T7 turns it into
// miami_intent_load_failures_total). A Load error means the intents are
// unknown, not that there are none, and must never trigger garbage
// collection.
//
// The ID inside a record is authoritative. File names are derived from it
// with Escape, and a record whose ID does not match its file name is a load
// failure. Unescape exists for diagnostics only; LoadFailure.VolumeID is its
// best-effort result.
//
// # Format and validation
//
// The on-disk format belongs to this package: it is written and read through
// private structs with explicit keys, and a golden file under testdata/ pins
// it. See Version for the rules for changing it. Unknown fields are ignored on
// read, so a downgrade does not tear volumes down.
//
// Validate checks structure only: the things the reconciler needs in order
// to act on a record safely (known kinds, absolute cleaned paths, publish
// targets that do not nest, a CSI access mode name). The boundary policy,
// meaning the stricter rules that decide what a CSI request may ask for, is
// applied at CSI time by the volume and transport packages. Records already
// on disk are never re-judged by it, so tightening that policy cannot make a
// live volume's record fail to load and its mounts look like garbage.
//
// Errors name the field and the rule, never a field's value, so that a field
// which later carries a secret cannot leak through an error or a log line.
package intent
