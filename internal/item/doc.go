// Package item defines what a kind of volume has to do between its transport
// and its stage path.
//
// A volume is three layers. The transport yields a directory on the node,
// the item turns a thing inside it into a mount at the stage path, and the
// publishes are binds of the stage path into pods. The reconciler owns the
// outer two and drives them for every item kind alike; an Item owns the
// middle, behind two methods that must each be idempotent and must each
// decide from the mount table, never from memory: Converge takes one step
// toward the stage path being a mount of the volume, and Teardown undoes it.
//
// This package holds the interface, the errors every item kind shares, and
// the helper that unmounts a path safely. The directory kind lives in
// internal/item/directory; M2 adds image.
package item
