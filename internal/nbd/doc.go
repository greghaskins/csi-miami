// Package nbd serves a block device to the kernel over the NBD protocol and
// creates, resizes and removes the kernel's nbd devices.
//
// The protocol is implemented here rather than imported: the maintained Go
// library lacks flush, trim and FUA. proto.go holds the wire values and the
// negotiation (fixed newstyle with NO_ZEROES, NBD_OPT_GO and NBD_OPT_INFO,
// simple replies only); server.go the transmission loop, a single reader
// handing requests to a bounded pool of workers whose replies are written
// under one mutex. A backend error fails its request alone with EIO; a request
// larger than MaxRequest is refused with EINVAL without its payload being
// held in memory. Payload buffers come from size-class pools, so the steady
// state allocates nothing per request.
//
// Devices are created through the nbd generic netlink family (netlink.go),
// so their number is not bounded by the module's static default. The kernel
// speaks only the transmission phase, so Connect runs the client half of the
// handshake on the socket before handing its descriptor to the kernel: the
// usual arrangement is a socketpair with Serve on one end and Connect given
// the other. Each connection carries a request timeout and a dead-connection
// timeout, so a dead server gives the kernel I/O errors, never an unbounded
// wait. Every netlink call is bounded by its context and abandoned, never
// joined, when the context ends; a Connect abandoned that way may still have
// created its device, which Devices then shows by its backend string.
//
// Devices (sysfs.go) observes the devices through sysfs: the serving pid, the
// backend identifier given on connect (which names the volume) and the size.
package nbd
