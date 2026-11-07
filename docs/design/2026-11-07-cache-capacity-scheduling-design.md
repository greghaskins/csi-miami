# Scheduling node-local cache capacity

**Status:** Approved by the owner on 2026-11-07 (decision 66); the implementation plan follows
**Created:** 2026-11-07
**Reads with:** `2026-11-03-chunk-backed-volumes-design.md` (4.3, the cache; decisions 54, 59), `2026-10-30-architecture-brief.md` (decision log), `docs/operating-volumes.md` (node layout, monitoring)

## 1. Intent

Every volume keeps a node-local cache under the kubelet directory, bounded per
volume by its class's `cacheSize` (default: the volume's size). Nothing adds
those bounds up per node. The scheduler places pods by CPU and memory, the
node plugin stages whatever it is asked to, and on a worker with one disk the
sum of the caches it ends up holding can exceed the disk. The kubelet then sees
`nodefs` pressure and evicts pods node-wide, including ones that have nothing to
do with volumes, and the caches cannot shrink to help because dirty chunks are
pinned until published. The failure arrives late, hits bystanders, and is
invisible to the scheduler that caused it.

This design makes cache space a scheduled resource: a node advertises how much
cache it has, a pod that mounts volumes asks for what its volumes will cache,
and the scheduler keeps the sum on each node under the budget. The node plugin
keeps a check of its own as defence in depth, with a clear error, a metric and
an alert.

## 2. Goals, non-goals, anti-goals

Goals:
- A pod whose volumes would not fit a node's cache budget is not scheduled
  there; it stays Pending with the scheduler's reason, like a pod that does not
  fit in memory.
- A stage that would exceed the budget anyway (a pod admitted before this
  shipped, a webhook that was down) is refused by the node with an error an
  operator can act on, never by the kubelet evicting bystanders.
- The budget, what is committed against it and the cache file system's free
  space are metrics with alerts.
- One formula for a volume's footprint, in the driver, used by the webhook and
  the node check alike.

Non-goals:
- Accounting the caches of RWX volumes (M3): only the holder node caches, the
  holder is not known at admission, and M3's design decides this.
- Changing how a volume's cache is bounded or evicted; this design counts what
  decision 54 and the cache already do.
- Exact accounting of several pods sharing one RWO volume on one node (section
  4.5: counted conservatively).
- Moving the cache off the kubelet directory, or splitting the disk.

Anti-goals:
- A webhook outage must not stop unrelated pods from being created
  (section 4.4, failure policy).
- The node plugin must not gain the right to change other nodes' capacity
  (section 4.9).
- No pod may be evicted, deleted or rescheduled by the driver (decision 53
  stands).

## 3. Constraints and facts

- An extended resource is an opaque integer quantity a node advertises in
  `status.capacity`; the kubelet copies it to `allocatable`, the scheduler sums
  the requests of the pods on the node against it, and it cannot be
  overcommitted: a container that requests one must set `limits` equal to
  `requests`. The kubelet enforces nothing else about it; nothing needs a
  device plugin. A `resource.Quantity` such as `30Gi` is an integer and is
  valid; fractional values are not.
- A pod-level extended-resource request does not exist; the request lives on
  one container. The scheduler's effective request is the larger of the init
  containers' and the sum of the regular containers', so a request on a regular
  container is what counts.
- Kubernetes 1.36 (k0s) in the owner's cluster; bound ServiceAccount tokens
  carry `authentication.kubernetes.io/node-name` in `userInfo.extra`, which a
  ValidatingAdmissionPolicy can read.
- A volume's cache is at most `cacheSize` of chunk files (clean chunks are
  evicted LRU, dirty chunks are pinned until published, writes fail with
  `ErrFull` beyond the bound), plus the boundary copies made during a publish
  (`b/`, copy-on-write of chunks rewritten while an upload runs, at most the
  boundary's size, which the dirty watermark bounds), plus `index.json`.
- `cacheSize` defaults to the volume's size; `generations.dirtyWatermark`
  defaults to half the cache; both are StorageClass parameters and are also in
  the PV's volume context (`miami/cacheSize`, `miami/generations.dirtyWatermark`,
  `miami/chunkSize`) once provisioned.
- The owner's workers have one disk each, 220 to 465 GiB, holding the OS,
  images, logs, emptyDirs and the caches. The kubelet's own `nodefs` eviction
  thresholds apply to that disk.

## 4. Design

### 4.1 The footprint of a volume

`footprint(size, params) = min(size, cacheSize) + dirtyWatermark + indexSlack`,
rounded up to a whole MiB, where `size` is the PV's capacity (or the PVC's
request rounded up to whole chunks, when the PV does not exist yet),
`cacheSize` and `dirtyWatermark` are the class's values or their defaults, and
`indexSlack` is 16 MiB (the index at 65,536 lines, twice, for the atomic
rewrite). With the defaults that is 1.5 × the volume's size: the cache full of
dirty chunks while a publish holds boundary copies of half of them. For a lazy
volume with a small bound it is 1.5 × the bound.

The formula is one function in `internal/volume`, `CacheFootprint`, taking the
same `VolumeParams` the item already has, and it is what the webhook requests,
what the node advertises against, and what the stage check sums. A test pins
the formula so the three never drift.

`size` is the size the item serves (`raiseSize`: the largest of the context,
the size record and the current manifest), so an expanded volume's footprint
grows with it. The pod's request does not: a request is fixed at admission,
and expansion is an operator's act. Accepted: the node's commitment then
exceeds what the scheduler reserved by the growth, the next stage of that
volume anywhere requests the new footprint, and the commitment metric (4.8)
shows the excess meanwhile.

### 4.2 The resource

Name `miami.csi.greghaskins.com/cache`; unit bytes, written with binary
suffixes (`30Gi`), always a whole number of MiB. A pod carries it as
`requests` and `limits` of equal value on one container (4.4).

### 4.3 Node capacity

**Who sets it.** The node plugin, for its own node, on every reconcile pass:
it computes the budget, reads the node's current `status.capacity`, and
patches `status.capacity["miami.csi.greghaskins.com/cache"]` when it differs
(a JSON patch on the `status` subresource). It re-patches after a kubelet
restart or a node re-registration, where the entry can be lost, because every
pass compares. Capacity is set, never allocatable: the kubelet derives
allocatable from capacity for an extended resource.

**The budget.** `budget = max(0, share × size(cacheFS) − reserve)`, where
`size(cacheFS)` is the capacity of the file system holding the cache directory
(`statfs` on `-cache-dir`), `share` is `-cache-budget-share` (default 0.5) and
`reserve` is `-cache-budget-reserve` (default 15Gi). `-cache-budget` set to an
absolute value overrides the formula. A budget of zero, computed or explicit,
is a real answer: the node advertises `0` and refuses every new volume, which
is what a disk too small for any cache should do. Turning accounting off is a
separate switch, `-cache-accounting=false`: the node then removes its key from
`status.capacity` (a JSON patch `remove`, its own key only) and skips the stage
check, so a flag turned off leaves no stale capacity behind. The budget is
static for a given disk and
flag set: it is deliberately not "free space minus what miami uses", which
would fluctuate with other tenants and make the scheduler churn; the free-space
metric and alert (4.8) catch real contention. The reserve exists so the budget
sits below the kubelet's `nodefs` eviction threshold with room for images and
logs; an overlay sizes it for its disks.

**Narrowing the write (4.9).** The node plugin's ClusterRole gains `patch` on
`nodes/status`. A ValidatingAdmissionPolicy shipped in the base binds that
ServiceAccount's node-status updates to two rules: the object's name equals
`request.userInfo.extra["authentication.kubernetes.io/node-name"][0]` (the
bound token's node), and `object.status.capacity` differs from
`oldObject.status.capacity` in no key but ours. A compromised node plugin can
then change one number on its own node, nothing else. The facts this rests
on: the DaemonSet's token is the kubelet's projected, bound token (every pod's
is, since 1.22), the `node-name` extra on such tokens is GA since 1.32, and
ValidatingAdmissionPolicy is GA since 1.30; the owner's cluster is 1.36. On a
cluster older than 1.32 the policy's first rule cannot be evaluated and the
base's policy fails closed (the patch is refused, the node advertises nothing,
the stage check still runs on the flags' budget); no controller-side fallback
is built for that case.

**Alternative considered:** the controller patching every node's capacity
from a budget it computes from `status.capacity.ephemeral-storage`. One
principal and no per-node token trick, but it cannot see a cache directory
that an overlay moved to another file system, and it gives one pod write
access to every node's status. Rejected in favour of the per-node patch under
the policy; it stays the fallback if the token extra is unavailable.

### 4.4 The webhook

**Where it runs.** In the controller Deployment, as a second listener
(`-webhook-addr`, default `:9812`), because the controller already holds the
cluster-wide read access the lookups need, is the one place that knows the
class parameters' defaults, and is a single replica whose restart is quick. A
`MutatingWebhookConfiguration` in the base matches pods, `CREATE` only,
`sideEffects: None`, `matchPolicy: Equivalent`, `reinvocationPolicy:
IfNeeded`, `timeoutSeconds: 10`, with a `namespaceSelector` that excludes
`kube-system` and `miami-system` (so the control plane and the driver itself
never wait on it) and any namespace labelled
`miami.csi.greghaskins.com/cache-accounting: disabled`.

**What it does.** For each pod volume:
- `persistentVolumeClaim`: GET the PVC. If it is bound, GET the PV; a PV of
  this driver with `miami/kind: volume` yields a footprint from the PV's
  capacity and volume context. If it is unbound (`WaitForFirstConsumer`, or
  `Immediate` and not yet bound), GET its StorageClass (the named one, or the
  cluster default when the field is unset); a class of this provisioner with
  `kind: volume` yields a footprint from the PVC's request and the class's
  parameters, through the same `WithSize` the controller uses at
  `CreateVolume`, so the rounding matches.
- `ephemeral.volumeClaimTemplate`: as an unbound PVC, from the template (a
  template naming no class resolves the cluster default, as a PVC would).
  Inline CSI ephemeral volumes do not arise: the CSIDriver allows only the
  `Persistent` lifecycle.
- Anything else, or a PVC or class of another driver: nothing. A PVC
  annotation never overrides a class parameter; the class and the request are
  the inputs, as at `CreateVolume`.
- A PVC that does not exist yet (a StatefulSet's claim template before its
  controller made the claim, a race with a client that creates the pod first):
  nothing for that volume, a counter (`miami_webhook_unresolved_total`) and
  a log line. The stage check (4.8) is the backstop. Denying would break
  ordinary StatefulSet rollouts, which is worse than one unaccounted pod.

The footprints are summed and set as both `requests` and `limits` of the
resource on the pod's first regular container, replacing any value already
there and removing the resource from every other container (so a
re-invocation, another mutator reordering containers, or a pod spec copied
from an admitted pod all end with one computed number in one place, and a user
cannot under-request by setting a small value by hand). Init and ephemeral
containers get nothing. A pod with no volume of this kind is admitted
unchanged, except that a stray request of the resource is removed.

**Reaching it.** The controller runs on the host network, so the webhook's
port is a host port: `9812` is one of the four the driver already reserves
(`9810` to `9813`), and a ClusterIP Service selecting the controller pod gives
the API server a stable name for it (a Service selects host-network pods like
any other; its endpoint is the node's address and the host port).

**TLS.** The base makes `kubectl apply -k` work with no other components: the
controller generates a CA and a serving certificate into a Secret
(`miami-webhook-tls` in `miami-system`) on first start if the Secret is absent,
serves with it, and asserts the `caBundle` of the one
`MutatingWebhookConfiguration` it is named in on every reconcile pass, not only
at start, so a bundle wiped by a re-apply of the base (which ships the field
empty) heals within a pass. A GitOps tool that applies the base must ignore
that field's drift (Argo CD: an `ignoreDifferences` entry for
`webhooks[].clientConfig.caBundle`), and the controller exports
`miami_webhook_config_ok` (1 when the configuration carries its bundle) so a
wipe that lasts is visible, since with `Ignore` a missing bundle means every
pod skips accounting silently. `-webhook-tls-secret` points at a Secret someone
else manages (a cert-manager `Certificate` in an overlay, with the CA injector
annotation on the webhook configuration instead of the controller's patch, and
no drift to ignore); the owner's cluster has cert-manager and would use it.

**Failure policy.** `Ignore`, with the stage check as the net. With `Fail`,
the controller being down (an upgrade, a crash loop, a node drain) stops every
pod creation in every namespace the selector matches, which is the whole
cluster minus two namespaces; that is a bigger outage than the thing this
design prevents. With `Ignore`, a pod admitted while the webhook was down
carries no request, is scheduled without accounting, and is refused at stage
by the node if it would overcommit, with the clear error below; an alert on
`apiserver_admission_webhook_fail_open_count` (and the webhook's own
availability) says the accounting had a gap. This is the owner's call (6).

**Could a MutatingAdmissionPolicy (CEL) replace the webhook?** Not today. The
footprint needs the PVC, then the PV or the StorageClass, and CEL admission
policies can read only the request, the pod and one param object; they cannot
look a PVC up. If a future release adds resource lookups to CEL, the policy
could replace the webhook with no change to the resource or the node side.

### 4.5 Several pods sharing one RWO volume on one node

Two pods of one node may mount the same RWO volume (`SINGLE_NODE_MULTI_WRITER`,
a Deployment with two replicas landing together). Each carries the footprint,
the node caches once, and the scheduler reserves twice. The webhook cannot know
at admission whether another pod of the same claim will land on the same node,
and the reservation is released when either pod ends, so there is no correct
place to subtract. Accepted as conservative: it costs packing on a node, never
safety, and the stage check counts volumes, not pods, so it never refuses for
this reason. A per-claim de-duplication would need the webhook to see
scheduling, which is the wrong layer.

### 4.6 Pods that exist before this ships

They carry no request. Their caches consume the budget without the scheduler
knowing, so new pods can be placed onto nodes that are already committed. The
stage check (4.8) refuses the overcommit at the node, and an operator recreates
the old pods (a rolling restart of each Deployment and StatefulSet) to bring
them under accounting; the design does not do that for them (decision 53). The
gap is visible without new privileges: a recording rule compares the node's
`miami_cache_committed_bytes` with the sum of
`kube_pod_container_resource_requests{resource="miami.csi.greghaskins.com/cache"}`
for the pods on that node, and the difference is the unaccounted commitment.

**Alternative considered:** the node plugin advertising `budget − unaccounted`,
which needs it to list the pods on its node and match them to its staged
volumes. Accurate, but it is `pods list` cluster-wide for a DaemonSet and a
capacity figure that moves as pods come and go; rejected for a one-time
transition.

### 4.7 RWX volumes (M3)

Out of scope here. M3's design decides whether non-holder pods carry a request
(conservative, every node reserves) or none (only the holder's stage check
guards). Until M3 ships no RWX volume exists, and the webhook requests nothing
for a PVC whose access mode is many-node.

### 4.8 The node's own check, metrics and alerts

**The stage check.** In `Converge`, at layer 4 and only there, immediately
before `ensureServer`, and only when the volume is new to this node: no nbd
device carries its name and its cache directory has no index (the state in
which `decideCache` would build a fresh cache). An adopted mount, a server
restart, a reboot with a surviving cache, a drain and a teardown never reach
the check: their cache already occupies the disk and refusing them would
strand data. The item sums the footprints of every volume this node holds
(its intent records of the kind, plus the drain records of caches awaiting a
drain, each with the size and parameters `volOf` derives) and adds the new
one; if the sum exceeds the budget the stage fails with
`ErrCacheBudgetExceeded`, condition
`cache-budget-exceeded`, event `CacheBudgetExceeded` (Warning, fixed text), and
an error naming the three numbers: "the node's cache budget is X, Y is
committed to N volumes, and this volume needs Z". It is classed retryable
(`Unavailable`): the budget frees when another volume unstages, and kubelet
retries. The pod stays `ContainerCreating` with the event; moving it is the
scheduler's or the operator's act, never the driver's.

The check counts caches, the scheduler counts pods, and the two differ for a
moment by design: after a clean unstage the cache is deleted at once, but a
cache awaiting an orphan drain (a node that came back after its pod moved)
stays committed on the node until the drain publishes and removes it, while
the scheduler already sees that pod's reservation released. A new volume placed
into that window is refused retryably and proceeds when the drain ends; that
is the intended outcome, not a bug, and the drain's own stuck alert covers a
drain that never ends.

**Metrics** (node, per pass): `miami_cache_budget_bytes`,
`miami_cache_committed_bytes` (the sum above, staged volumes only),
`miami_cache_fs_size_bytes` and `miami_cache_fs_free_bytes` (`statfs` of the
cache directory's file system), and the webhook's `miami_webhook_mutations_total`
by outcome and `miami_webhook_unresolved_total`.

**Alerts** (shipped as examples in `docs/operating-volumes.md`, wired in the
owner's overlay): `miami_cache_committed_bytes > miami_cache_budget_bytes` (an
unaccounted overcommit exists on the node); `miami_cache_fs_free_bytes <
reserve` (the disk is where the kubelet will act next); the unaccounted
recording rule above being non-zero for longer than a rollout; and
`apiserver_admission_webhook_fail_open_count` rising for the webhook.

### 4.9 The privilege increase, and how it is narrowed

| Principal | New permission | Narrowing |
|---|---|---|
| node plugin | `nodes/status` patch | ValidatingAdmissionPolicy: own node only (bound-token `node-name`), one capacity key only |
| controller | `persistentvolumeclaims` get (all namespaces), `storageclasses` get | reads only; both already readable by the sidecars |
| controller | `mutatingwebhookconfigurations` get and patch | `resourceNames` of the one object; only when self-signed TLS is used |
| controller | `secrets` get, create and update in `miami-system` | `resourceNames: [miami-webhook-tls]`; absent when `-webhook-tls-secret` names a managed Secret |

Nothing new reads Secrets outside the driver's namespace, and nothing new can
delete or evict anything.

### 4.10 Rollout order

1. The driver release with the footprint function, the node's advertisement
   and check, the webhook and the policy. All of it is on by default in the
   base (the webhook with `Ignore`, the advertisement with the default share
   and reserve): a base that ships the check off ships the problem. An overlay
   that must opt out sets `-cache-budget 0`, which advertises nothing and
   disables the check, and labels its namespaces out of the webhook.
2. In the cluster: the node plugins advertise and begin checking; existing
   pods are unaccounted (4.6); the recording rule shows the gap.
3. Rolling restarts of the workloads with volumes, in the order the operator
   prefers; the gap closes.
4. The alerts.

## 5. Alternatives rejected

- **CSI storage capacity tracking** (`storageCapacity: true`,
  `CSIStorageCapacity` objects per topology segment): consulted at provisioning
  only, so a pod rescheduled to another node is not checked against that node;
  and it needs topology on volumes, which pins them. Rejected by the owner.
- **A static per-node limit on the number of volumes**: wrong unit, and blind
  to lazy volumes.
- **Making the stage check the only mechanism** (option 1 alone): correct but
  late; the pod is already placed, its other volumes may already be staged, and
  the fix is an operator moving it. Kept as the net, not the fence.
- **Dynamic budget from free space**: churns the scheduler and makes placement
  depend on other tenants' usage at the moment of advertisement.

## 6. Decision and questions

**Decision 66.** Node cache capacity is a scheduled resource: the node plugin
advertises `miami.csi.greghaskins.com/cache` on its own node as its budget
(`share × cache file system − reserve`, or an absolute flag), a mutating webhook
in the controller adds a request equal to the limit, the sum of the footprints
`min(size, cacheSize) + dirtyWatermark + 16 MiB` of the volumes a pod mounts,
on its first container, and the node refuses a stage that would exceed the
budget (`cache-budget-exceeded`, retryable) as defence in depth, with the
budget, the commitment and the file system's free space as metrics. CSI storage
capacity tracking is rejected (provisioning-time only; pins topology). The
webhook fails open; RWX volumes wait for M3; shared RWO claims count
conservatively.

**Settled by the owner (2026-11-07),** each as the design recommends:
1. The webhook fails open (`Ignore`); the stage check is the net, and the
   fail-open count is alerted on.
2. The node plugin patches its own node's capacity, narrowed by the admission
   policy; the controller does not patch nodes.
3. The base defaults are `share 0.5` and `reserve 15Gi`. The owner's overlay
   sets absolute budgets: `-cache-budget 100Gi` on the 220 GiB workers and
   `200Gi` on the two 466 GiB workers (with the defaults those would be about
   95 and 218 GiB, and the 220 GiB nodes already carry 20 to 100 GiB of
   other usage, so the absolute figures leave the kubelet's own eviction line
   clear).
4. Pods sharing one RWO claim on a node are counted conservatively.
5. Pods that predate the rollout stay unaccounted until recreated; the stage
   check protects the node and the recording rule shows the gap.
6. The owner's cluster uses cert-manager and the CA injector; the base keeps
   the self-signed default.

## 7. Left to the builder

The exact JSON patch and conflict handling for the node status update; the
webhook's handler layout, admission review version (`v1`), and its tests
against envtest; the CEL of the policy; the flag names' final spelling; the
rounding helper; the dashboard panels; the Service manifest for the webhook;
and the wording of the event and the runbook section in
`docs/operating-volumes.md`.
