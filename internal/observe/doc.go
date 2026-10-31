// Package observe reads the node's mount table from the kernel.
//
// /proc/self/mountinfo is the only source of mounted state: nothing here
// execs findmnt or mount, and nothing caches. Parse decodes the file format
// (including the octal escapes the kernel uses in paths and sources and any
// number of optional fields), and Table answers the questions the reconciler
// asks of it: what is mounted at a path, what is mounted below a directory
// and in what order to unmount it, and whether one path is a bind mount of
// another.
//
// Reading the file does not touch the mounted filesystems, so it cannot hang
// on a dead NFS server. Callers that stat or statfs a mount point must do so
// under a deadline; internal/mount provides that.
package observe
