# Operating chunk-backed volumes

For the person who runs a cluster with `kind: volume` classes. The design and
its reasons are in
[the spec](design/2026-11-03-chunk-backed-volumes-design.md); this page is
what to do and what to look at. Placeholders: `<pv>` is the PV's name
(`pvc-<uuid>`), `<node>` a node name, `<epoch>` a holder epoch, `<kubelet>`
the kubelet directory.

## Lifecycle in one paragraph

`NodeStageVolume` takes the holder file of the volume (one node at a time),
reads `current`, starts the volume's block server as a transient systemd unit,
formats LUKS2 on the first stage or opens it with the Secret's key, checks the
file system with `e2fsck -p` (or makes it, on a blank device that has never
published), and mounts ext4 at the stage path; publish is a bind of that
mount. While it is staged the block server serves an nbd device from a
node-local cache, and the node publishes a **generation** (freeze the file
system, mark a boundary, upload the changed chunks, then the manifest, then
`current`) every `generations.interval` while there are unpublished writes,
when they reach `generations.dirtyWatermark`, when the PV's
`miami.csi.greghaskins.com/publish` annotation changes value (read about once a
minute), and always at unstage. `NodeUnstageVolume` unmounts, closes the
mapping, publishes the final generation, reads `current` back from the store
and checks that it names that generation and that the server has nothing dirty
and no publish running; only then does it stop the server, delete the cache
and release the holder file. Anything short of that fails the unstage as
retryable and leaves the server, the cache and the holder where they are
(decision 52). Returning to a node fetches the volume again.

## Runbook: a volume that cannot leave its node

A volume moves only after its holder has synced it. That is the point, and it
means two situations need a person: a node that is down, and an unstage that
fails and does not clear (the store is gone for good, the cache index is
corrupt). In both a pod scheduled elsewhere waits as `holder-contended`, and
its stage error names the holder and the way out. The old node's log carries
the same for a stuck unstage: `the unstage cannot finish ... an operator sets
the PV annotation miami.csi.greghaskins.com/force-detach to "<node>@<epoch>"`;
the `UnstageStuck` event follows after 30 minutes.

**What is lost.** Whatever the old holder had not published: its writes since
its last generation, which are the dirty chunks in its node-local cache. The
volume restages on the new node from `current`, a crash-consistent image
(every generation is taken under a freeze), and ext4 replays its journal. A
node that comes back later cannot undo this: its publishes are refused by the
epoch fence, and its unstage discards what it held and succeeds
(`GenerationLost` event). Be sure the old node's pod is not still the one
doing the writing you care about.

1. Delete the pod; force it if its node cannot answer
   (`kubectl delete pod <pod> --force --grace-period=0`).
2. Read the holder: the waiting pod's stage error, the old node's log, or the
   file `<pv>.holder` beside the volume's directory on the store (JSON with the
   node and the epoch), which gives `<node>@<epoch>`.
3. Set the annotation to exactly that value:

   ```sh
   kubectl annotate pv <pv> miami.csi.greghaskins.com/force-detach='<node>@<epoch>'
   ```

   The annotation names one holder at one epoch. It is ignored when it does
   not match the holder file as read at that moment (a stale epoch after the
   volume has moved), the driver never removes it, and once the holder has
   been replaced it names nobody. The node plugin and the controller only read
   it.
4. Wait for the attachment to move. With `attachRequired`, a ReadWriteOnce
   volume is not attached to the new node until the attach-detach controller
   has detached it from the old one, and that controller force-detaches after
   its unmount wait, about six minutes, which also applies on a healthy node
   once the pod object is gone. On a real cluster a pod elsewhere therefore
   reaches `holder-contended` only after the attachment has moved; until then
   it shows a multi-attach error and the driver is not involved.
5. The new node's stage retry reads the annotation, takes over the holder at a
   higher epoch and raises an `EmergencyDetach` Warning event on the PV that
   names the abandoned `<node>@<epoch>` and the trigger. The volume mounts
   from `current`.
6. The old node's next unstage retry finds the holder taken, reports
   `GenerationLost`, discards its cache and finishes.

The standard taint does the same for every volume of a dead node: put
`node.kubernetes.io/out-of-service` on the node (any effect; the Kubernetes
form is `kubectl taint nodes <node> node.kubernetes.io/out-of-service=nodeshutdown:NoExecute`)
only after the node is really shut down. The driver reads the taint and never
writes it. Both triggers are independent of how old the holder is and of the
node's readiness; nothing else takes a volume over.

**A cache that no holder file claims.** A surviving cache directory
(`<cache-dir>/<pv>`) whose volume has no holder file, and no proof that the
volume was given up, is kept, and a warning says so once per plugin process.
The force-detach annotation acts on a holder file, and there is none; removing
the cache directory by hand gives up its unpublished writes. The cache is
discarded without asking only on proof: the holder file names another node,
`current` has moved past the cache, or the volume's PV no longer exists.

**Caches from before the drain record.** Each cache has a record beside it,
`<cache-dir>/<pv>.drain.json`, written before its server first starts; it is
what lets a node that has lost its intent records (a crash, then the pod
moved) find the volume again and drain it. A cache left by a build that
predates the record is not drained until the volume is staged on that node
again.

**A node that comes back with a volume it no longer stages** drains it on its
own, at plugin start and on later passes: it serves the cache without a key
or a mount, publishes, verifies against the store as an unstage does, stops
the server, deletes the cache and releases the holder. The generation it
publishes is a crash image of what had reached the cache. A drain that keeps
failing retries with a growing delay (1 minute doubling to 10) and raises
`DrainStuck` after 30 minutes. The waiting pod elsewhere mounts as soon as
the holder is released.

## Delete

`DeleteVolume` is refused (`Unavailable`) while any holder file exists beside
the volume, even a stale one: a holder means a node still owns unsynced data.
It proceeds after the holder unstages or drains, or when the PV carries the
same annotation naming the holder's `<node>@<epoch>`; the error says which.
Deleting the directory also removes the holder files and the `<pv>.size`
record.

## Read-only ext4 after the cache filled

When the cache is smaller than the volume and fills with unpublished chunks
while the store is unreachable, the block server fails writes with an I/O
error rather than block (no kernel thread waits on the NAS), and ext4
(`errors=remount-ro`) goes read-only. It stays read-only until it is
remounted, and the unstage that remounting needs must publish a final
generation, which needs the store. So: restore the store, wait for the cache
to drain (watch `miami_generation_dirty_bytes` fall), then restart the pod.
The driver reports it and does nothing else (decision 53; it has no pod-delete
permission). The signals are `miami_generation_dirty_bytes` approaching the
cache bound and the `GenerationOverdue` event (and the `generation-overdue`
condition). The probe may also report it (`VolumeUnhealthy`,
`miami_volume_probe_ok`, the `VolumeCondition`, described below), but it reads
one sector and a full cache that still holds that chunk serves the read, so do
not rely on it.

## A stage that refuses

- **The cache index is unreadable.** The stage fails with `server-down` and
  says the cache is left for an operator: an index that cannot be read may
  hold unpublished writes only a person can judge, and no server is started
  over it. Removing `<cache-dir>/<pv>` by hand gives those writes up; the
  next stage builds a fresh cache from `current`. Moving the volume instead is
  the force-detach runbook above; the old node's unstage then keeps failing
  on the unreadable index until its cache directory is removed too.
- **The device holds data but no LUKS header, or no file system signature.**
  The driver formats a device only when the volume has never published and the
  start of the device (or of the LUKS payload) reads as zeros. A device that
  has data and no header, or a published volume whose device reads as blank,
  is refused and never formatted over. If the claim was never used by a
  workload (a crash inside the first format is the usual cause), delete the
  claim and recreate it. Otherwise do not format anything by hand: restore the
  previous generation (see [restore.md](restore.md)) or force-detach and stage
  elsewhere.
- **`key-required` / `bad-key`.** The class's Secret is missing, empty or
  wrong; see the key in the README. A key that is shorter than 32 bytes or
  longer than 4096 bytes is refused at stage and publish with
  `InvalidArgument` (the message gives the length, never the key); make one
  with `openssl rand -base64 48`.
- **`cache-budget-exceeded`** (event `CacheBudgetExceeded`, Warning, on the PV;
  the pod stays `ContainerCreating`). The node's cache budget cannot hold this
  volume besides the ones it already commits to. The error names three
  numbers: the budget, the commitment with its volume count, and what this
  volume needs. Only a volume new to the node is ever refused; an adopted
  mount, a server restart, a reboot with a surviving cache, a drain and a
  teardown never are. It is retryable (`Unavailable`): kubelet retries, and
  the stage goes through when something unstages or a drain ends. To act:
  - Find what is committed: `miami_cache_committed_bytes` against
    `miami_cache_budget_bytes` for the node, and the node's volumes in
    `miami_cache_bytes{volume=...}`, one series per volume the node holds a
    cache for. The sum of footprints is not exported on its own, only the
    total; each volume's footprint is `min(size, cacheSize) + dirtyWatermark +
    16 MiB` from its class and size. A node whose committed figure stays above the sum
    of its served volumes' footprints holds caches awaiting a discard, for
    example a volume taken over by another node whose old holder has not
    discarded its cache yet. Such a cache still occupies disk and is
    deliberately counted; the `publish-refused` condition and event on that
    node show it.
  - A pod admitted before accounting shipped carries no request, so the
    scheduler could place it on a committed node. Recreate such pods (a
    rolling restart of each Deployment and StatefulSet) so the webhook gives
    them a request; this is step 3 of the rollout under Upgrades.
  - The scheduler can run ahead of the node. It frees a deleted pod's request
    at once; the node's committed figure falls only after unstage and drain. A
    pod placed in that gap can see one `CacheBudgetExceeded` refusal before it
    runs, and then runs on kubelet's retry. This is not a fault.
  - To raise the budget, set `-cache-budget` (an absolute quantity, for
    example `<budget>`) or change `-cache-budget-share` and
    `-cache-budget-reserve` on the node DaemonSet. `-cache-budget 0` is a
    budget of zero: it advertises `0` and refuses every fresh volume.
    `-cache-accounting=false` is the off switch: the node removes its key and
    skips the check.
  - Applying the new base while volumes are already staged refuses nothing for
    them; only fresh stages are checked.
  - Two pods that share one RWO claim on a node are counted twice by the
    scheduler and once by the node. This costs packing, never safety.

## A compromised key

Rotating the key (`previousKey`, see the README) changes a keyslot of the
LUKS header, not the volume's master key, and does not re-encrypt anything.
Older generations in the store, NAS snapshots and offsite copies carry older
headers that the old key still opens, so a key that may have leaked is not
fixed by rotation. To recover:

1. Create a new claim from an encrypted class with a new key (a new Secret
   entry, 32 bytes or more), so the new volume has a new master key.
2. Mount both volumes in one pod and copy the data across (`rsync -a`), then
   switch the workload to the new claim.
3. Delete the old claim, then delete or expire every copy of the old volume's
   store: older generations, NAS snapshots and the offsite copy. Until they
   are gone, anyone with the old key and a copy can read what was in them.

## Restarts

A block server that is killed (a crash, `SIGKILL`) is restarted by systemd a
second later (`Restart=on-failure`). The new process finds the volume's nbd
device still there, matches it by its backend identifier and re-points the
same device with `NBD_CMD_RECONFIGURE` (decision 51): the device, the dm-crypt
mapping and the ext4 mount survive, so the pod keeps running and nothing needs
a key or `e2fsck`. Requests that arrive while the connection is dead wait up
to the nbd dead-connection timeout (60 s) and then fail with EIO; the restart
is well inside it. Unpublished writes are in the cache and are served again.

The `ServerRestarted` event (Normal) means "the blockd unit restarted": the
plugin saw a different main process under the volume's unit, or started the
unit again onto a surviving device. It misses a restart that happened while
the plugin was down. A restart of the
plugin pod changes nothing: it adopts the unit, the device, the mapping and the
mount. The server's own output goes to the host journal
(`journalctl -u miami-blockd-<pv>.service`).

A plugin upgrade does not touch a served volume. The new binary is copied to
`<kubelet>/plugins/miami.csi.greghaskins.com/bin/miami-<hash>`, and a volume
runs it from its next stage; `miami_volumes_served{state="outdated-binary"}`
counts those still on an older build.

## Expansion

Growth is online and grow-only. `ControllerExpandVolume` rounds the size up to
whole chunks and records it in `<pv>.size` beside the volume's directory on the
store (a PV's attributes are immutable, so the volume context cannot carry
it); the record only grows, and is never repaired if it cannot be parsed,
because a smaller size must not come from a record that could not be read. A
stage takes the largest of the PV's size, that record and the size of the
manifest `current` names, so no restage comes back smaller. The node then
grows the nbd device, `cryptsetup resize` and `resize2fs` on the mounted file
system. A request to shrink is `OutOfRange`.

## Node layout

Everything the volume kind keeps on a node is under
`<kubelet>/plugins/miami.csi.greghaskins.com/`:

| Path | What |
|---|---|
| `cache/<pv>/` | the volume's cache: `c/` one file per chunk (O_DIRECT where the file system allows it), `b/` the boundary copies, `index.json` the dirty set and the manifest the cache is based on |
| `cache/<pv>.lock`, `cache/<pv>.drain.json` | the server's lock, and the drain record |
| `cache/<16 hex>.s` | the server's control socket, named by a hash of the volume name |
| `bin/miami-<hash>` | the copy of the binary the units run |
| `intents/` | the node's intent records |
| `transports/` | the NFS mounts |

The cache must be on local disk, never on a transport. To put the caches on a
dedicated disk, mount it and pass `-cache-dir` to `miami node`; the plugin in
its container and the block server on the host must see the directory at the
same absolute path, so it must lie under the kubelet directory or under a
directory named with `-shared-dir` (repeatable, and the DaemonSet must mount
that directory at the same path). `-bin-dir` follows the same rule. A layout
that breaks either rule, or a cache directory whose control socket would not
fit a unix socket path, is reported as the `cache-dir` feature missing and the
node serves no volumes.

The socket check is on the path `<cache-dir>/<16 hex>.s`, at most 107 bytes
(`sun_path` is 108 with its NUL), so the cache directory itself may be at most
88 bytes. The hashed names make the length the same for every volume. The
default cache directory under the common layouts is 56 bytes (stock kubelet),
60 (k0s), 74 (k3s) and 81 (microk8s), all within the 88-byte limit (the socket
paths are 75, 79, 93 and 100 bytes, within 107).

## Monitoring

The node serves `/metrics` on `:9810` (`-metrics-addr`), the controller on
`:9811`. Families the volume kind adds, on the node:

| Family | Labels | Meaning |
|---|---|---|
| `miami_volumes_served` | `state`: `serving`, `outdated-binary`, `down` | staged volumes by the state of their block server |
| `miami_generation_age_seconds` | `volume` | seconds since the newest published generation (since the node first saw the volume, if its server has not published yet) |
| `miami_generation_dirty_bytes` | `volume` | bytes written that no published generation holds |
| `miami_generation_publish_duration_seconds` | | histogram, freeze to upload, successful generations only |
| `miami_generation_publishes_total` | `result`: `ok`, `refused`, `failed` | refused is the fencing check saying another node owns the volume |
| `miami_cache_bytes` | `volume`, `state`: `clean`, `dirty` | bytes in the node-local cache |
| `miami_chunk_fetch_duration_seconds`, `miami_chunk_fetch_errors_total` | `volume` | the server's fetches from the store; they start over when the server restarts |
| `miami_volume_probe_ok` | `volume` | 1 if the device answers a read of its first sector, 0 after two failures in a row |
| `miami_volume_condition` | `condition` | volumes in each condition after the last pass (below) |
| `miami_node_capability` | `feature` | 1 if the node has `nbd`, `device-mapper`, `adiantum`, `systemd-socket`, `cache-dir` |
| `miami_transport_up` | `kind`, `id` | the transport answered a bounded `statfs` |

The conditions of a volume: `ok`, `prerequisite-missing`, `holder-contended`,
`server-down`, `key-required`, `bad-key`, `generation-overdue`,
`publish-refused`, `probe-failed`, `cache-budget-exceeded`, plus the transport
and stage conditions the directory kind has.

**Alerting.** While a volume's block server is down, its per-volume
`miami_generation_age_seconds`, `miami_generation_dirty_bytes`,
`miami_cache_bytes` and chunk-fetch series are **absent**, not stale, so an
alert written only on generation age never fires for the volume that needs it
most. Alert on the server states as well:

- `miami_volumes_served{state="down"} > 0`, and
  `miami_volume_condition{condition="server-down"} > 0` (a volume whose stage
  failed on the server is counted by the condition, not by the state);
- generation age **with** dirty bytes:
  `miami_generation_age_seconds > 3600 and miami_generation_dirty_bytes > 0`
  (age grows for a volume that is idle and clean; the driver itself reports
  overdue only when there are unpublished writes);
- `miami_volume_probe_ok == 0`, `miami_volume_condition{condition="holder-contended"} > 0`
  for longer than a rollout takes, and `increase(miami_generation_publishes_total{result="failed"}[1h]) > 0`.

**Events**, on the PV (`kubectl describe pv <pv>`); the text is fixed per
reason and carries the node, never an error (the node's log has that):

| Reason | Type | Means |
|---|---|---|
| `PublishRefused` | Warning | the store refused a generation because the volume is no longer held by that node; its unpublished writes are kept until unstage |
| `GenerationLost` | Warning | unpublished writes were discarded on the node: an unstage after an abandonment, or a cache that was not descended from `current` |
| `EmergencyDetach` | Warning | the holder's unpublished data was abandoned; names `<node>@<epoch>` and the trigger |
| `GenerationOverdue` | Warning | the newest generation is older than the maximum age and the node holds unpublished writes |
| `VolumeUnhealthy` | Warning | the device failed two probes; restart the pod once the store is reachable and the cache has drained |
| `KeyRequired` | Warning | the stage request carried no key the volume needs |
| `MountFailed`, `TransportUnavailable` | Warning | the volume or its transport could not be brought up; see the node's log |
| `ServerRestarted` | Normal | the blockd unit restarted; the device and mounts were kept |
| `UnstageStuck`, `DrainStuck` | Normal | an unstage or a drain has retried for 30 minutes; the log beside it has the cause and the way out |

### Cache accounting

The node plugin advertises `miami.csi.greghaskins.com/cache` on its own node
(its budget), a mutating webhook in the controller gives each pod that mounts
volumes a request for their footprint, and the node refuses a fresh stage that
would exceed the budget. The footprint of a volume is
`min(size, cacheSize) + dirtyWatermark + 16 MiB`, at the size the volume is
served at (an expanded volume counts its grown size). The node's committed
figure is the stage check's own sum over all drain records: it includes caches
awaiting a drain or a discard, and a fresh stage leaves out the volume's own
record. Host ports in use: node 9810 (metrics) and 9812 (health), controller
9811 (metrics), 9813 (health) and 9814 (webhook).

On the node:

| Family | Labels | Meaning |
|---|---|---|
| `miami_cache_budget_bytes` | | the advertised budget |
| `miami_cache_committed_bytes` | | the footprint committed on the node |
| `miami_cache_fs_size_bytes`, `miami_cache_fs_free_bytes` | | `statfs` of the cache directory's file system |
| `miami_cache_capacity_patch_failures_total` | `reason`: `denied`, `error`, `unknown` | a budget the node computed but could not advertise; `unknown` counts fence probes that could not tell |
| `miami_cache_fence_state` | `state`: `fenced`, `unfenced`, `unknown` | one series at 1: the verdict of the fence probe (see the README) |

On the controller:

| Family | Labels | Meaning |
|---|---|---|
| `miami_webhook_mutations_total` | `outcome`: `mutated`, `unchanged`, `stripped`, `error` | pods the webhook handled; `error` is its own failure |
| `miami_webhook_unresolved_total` | | volumes whose claim did not exist yet, so nothing was requested for them |
| `miami_webhook_config_ok` | | 1 when the webhook configuration carries its `caBundle`; registered in self-signed mode only, absent with the cert-manager component |

The webhook never fails admission. On any error of its own (its 7 second
deadline, a pod with more than 64 claims, a panic) it admits the pod with its
resource removed and counts `error`; the node's stage check is then the net.
A request body is bounded at 8 MiB.

The fence probe runs at start and then every `-cache-fence-probe-interval`
(default 10 minutes) while fenced, and on every pass (`-reconcile-interval`)
while unfenced or unknown. Its denials are by design: about one per node per
probe interval while fenced, up to one per pass while unknown. Do not chase
them in `apiserver_validating_admission_policy_check_total`.

Alerts, as examples (the thresholds are placeholders for your overlay; `<reserve>`
is the node's `-cache-budget-reserve` in bytes):

```yaml
- alert: MiamiCacheOvercommitted
  expr: miami_cache_committed_bytes > miami_cache_budget_bytes
  for: 10m    # a stage is refused at the budget, so a sustained excess is unaccounted pods
- alert: MiamiCacheDiskLow
  expr: miami_cache_fs_free_bytes < <reserve>
  for: 10m    # ignores a transient spike (an image pull); the kubelet acts on sustained pressure
- alert: MiamiCacheFenceNotInForce
  # every fresh install shows unfenced or unknown for one pass by design: the
  # first probe can beat the policy's enforcement, so require it to persist
  expr: miami_cache_fence_state{state!="fenced"} == 1
  for: 10m
- alert: MiamiWebhookFailingOpen
  expr: rate(apiserver_admission_webhook_fail_open_count{name=~".*miami.*"}[5m]) > 0
  for: 10m
- alert: MiamiWebhookConfigMissing    # self-signed mode only
  expr: miami_webhook_config_ok == 0
  for: 10m
- alert: MiamiUnaccountedCache
  expr: miami_cache_unaccounted_bytes > 0
  for: 2h    # longer than a rollout takes
```

The last alert uses a recording rule for the commitment that no pod accounts
for (pods that predate the rollout):

```yaml
- record: miami_cache_unaccounted_bytes
  expr: |
    clamp_min(
      miami_cache_committed_bytes
        - on(node) (
          sum by (node) (
            kube_pod_container_resource_requests{resource="miami_csi_greghaskins_com_cache"}
          )
          or on(node) (0 * sum by (node) (miami_cache_committed_bytes))
        ),
      0)
```

The `or` term gives a node with no accounted pod at all (every pod predates the
rollout, the case this rule exists for) a right-hand side of zero; without it
the subtraction drops that node and the rule yields nothing there.

kube-state-metrics sanitises the resource name by turning every character
that is not a letter, digit or underscore into `_`, so
`miami.csi.greghaskins.com/cache` is
`miami_csi_greghaskins_com_cache`; check the label value your version emits.
The node plugin's gauges carry no `node` label of their own, so the scrape
configuration must attach one, or the `on(node)` join matches nothing. With
Prometheus-style pod discovery, a relabel rule such as:

```yaml
- source_labels: [__meta_kubernetes_pod_node_name]
  target_label: node
```

An absolute budget is a node argument (`-cache-budget <budget>`, a quantity
such as `50Gi`, an example only); with the defaults (`share 0.5`, `reserve 15Gi`)
the budget is half the cache file system less 15Gi.

## VolumeCondition

The node reports whether a volume's device is abnormal in
`NodeGetVolumeStats`, from the active probe (a direct read of the first sector
of the mapper, or the nbd device of a plain volume, failing or not returning
twice in a row). `VolumeCondition` is an alpha field of the CSI spec, and the
kubelet acts on it only with the `CSIVolumeHealth` feature
gate, alpha and off by default. The driver pins the CSI spec module at
v1.12.0 because v1.13.0 replaced that field with a new volume-health API
(`NodeGetVolumeHealth` and its messages, KEP-1432). As of this writing the
Kubernetes `release-1.36` branch still builds against spec v1.9.0 and the
`master` branch has moved to v1.13.0; the driver will migrate when the kubelets
it runs under adopt the new API. Until then the same verdict is also in
`miami_volume_probe_ok`, the `probe-failed` condition and the `VolumeUnhealthy`
event, which do not depend on the gate. A volume generation that is overdue is
reported by its own condition and event, not through `VolumeCondition`.

## Upgrades

A node plugin upgrade is a DaemonSet rollout and touches no served volume.
Roll back with care: a binary older than the one that wrote an intent record
reads it, ignores the fields it does not know, and writes it back without them.
`unstagingSince` (when an unstage began, so its stuck-warning counts across
restarts) is the one such field that only affects reporting; a binary older
than the volume kind drops the volume parameters, epoch and grown size too and
cannot serve the volume. Do not run an older plugin on a node with staged
volumes of the newer kind. Block servers stay on the binary they started with
until the volume is next staged.

**Cache accounting rollout.** Apply the driver release (all of accounting is on
by default in the base); then the node plugins advertise and begin checking
while existing pods are unaccounted, and the `miami_cache_unaccounted_bytes`
rule above shows the gap; then restart the workloads with volumes in the order
you prefer so the webhook gives them a request; then enable the alerts.
Applying the new base with volumes already staged refuses nothing for them.

A GitOps tool must ignore `webhooks[].clientConfig.caBundle` of the
`miami-cache` MutatingWebhookConfiguration (the Argo CD `ignoreDifferences`
entry is in the README), in both TLS modes. The cert-manager component is
opt-in; `-cache-accounting=false` turns the node's side off, and labelling a
namespace `miami.csi.greghaskins.com/cache-accounting=disabled` takes it out
of the webhook (admin-only, as in the README).

**Switching a running self-signed install to the cert-manager component.**
cert-manager's cainjector merges a new CA into an existing `caBundle` and drops
only expired entries. After enabling the component, once: empty
`webhooks[].clientConfig.caBundle` (the cainjector refills it with the managed
CA alone) and delete the Secret `miami-webhook-tls`, which holds the old CA's
key. Admission works either way; this only retires the old trust anchor. The
reverse switch needs nothing, because the controller replaces the bundle
rather than merging.

## Restore

A volume is its newest complete manifest, and restoring needs no driver: see
[restore.md](restore.md). For an encrypted volume the machine doing the
restore needs `cryptsetup` and the namespace's key; `miami restore` assembles
the image and `miami import` writes an existing image back as the first
generation of a new volume.
