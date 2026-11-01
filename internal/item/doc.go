// Package item defines what a kind of volume has to do between its transport
// and its publish targets.
//
// A volume is three layers. The transport yields a directory on the node, the
// item makes a thing inside it real (and, if the kind needs one, a mount at
// the stage path), and the publishes are binds into pods of the directory the
// item names (Item.PublishSource). The reconciler owns the outer two and
// drives them for every item kind alike; an Item owns the middle, behind
// methods that must each decide from the mount table, never from memory:
// Converge takes one idempotent step toward the volume being real, and
// Teardown undoes it.
//
// This package holds the interface, the errors every item kind shares, and
// UnmountBind, the one way to take down a mount: only if it is a bind of what
// it should be a bind of. The directory kind lives in
// internal/item/directory and makes no mount at the stage path; M2 adds image.
package item
