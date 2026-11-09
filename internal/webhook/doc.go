// Package webhook is the mutation behind the cache-capacity admission
// webhook (design 2026-11-07-cache-capacity-scheduling, section 4.4).
//
// A pod that mounts a volume of kind volume claims node-local cache space on
// whichever node it lands on. Mutator.Mutate works out that space from the
// pod's claims (the PVC and its PV, or the StorageClass and the request when
// nothing is bound yet), sums it, and answers with a JSON patch that sets the
// extended resource volume.CacheResource as both request and limit of the
// pod's first regular container, so the scheduler places the pod only where
// the node advertises the room.
//
// Three rules shape the code:
//
//   - Accounting is best effort and never gates admission. Every failure of
//     the webhook's own (a lookup error, a pod it cannot read, a panic) allows
//     the pod with the cache resource stripped and is counted as error; only a
//     pod it cannot decode at all is allowed as it came. The node's stage check
//     is the net.
//   - Nothing a user wrote survives. The resource is removed from every
//     container but the first regular one, and replaced there, so a small
//     number set by hand cannot under-request.
//   - The patch touches the resource's key and nothing else.
//
// The cost of the second rule is on the error path: the strip runs whenever
// the webhook fails, so on a re-invocation (another mutator changed the pod)
// a transient error in the second call removes the number the first call set.
// That pod is then scheduled with no reservation. It is rare, heals with the
// next pod, and is backstopped by the node's stage check; change either side
// only with the other in view.
//
// The footprint itself is computed by package volume, the one place the
// formula is spelled, with the rule the node uses for an expanded volume
// (volume.ServedFootprint), so the two cannot disagree.
package webhook
