// Package intent persists what should be true on this node for each volume.
//
// An Intent is a record of desire, never of observation: it names a volume,
// its transport, its stage path and its publish targets, and says nothing
// about what the kernel currently mounts. Observers report that; the
// reconciler compares the two.
//
// A Store keeps one JSON file per volume in a single directory. Save writes
// atomically and durably (temp file, fsync, rename, fsync of the directory),
// so a crash leaves the old record or the new one, never a partial one.
// Load reads the directory afresh each time and never fails because of one
// bad record: a record that cannot be read or is invalid is reported in
// Loaded.Failures and left on disk for inspection, and its volume simply has
// no intent. The caller counts the failures. Temp files left behind by a
// crash are removed by Load.
//
// The ID inside a record is authoritative. File names are derived from it
// with Escape, and a record whose ID does not match its file name is a load
// failure. Unescape exists for diagnostics only.
package intent
