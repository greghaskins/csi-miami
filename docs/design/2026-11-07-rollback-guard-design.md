# Rollback guard and deliberate rollback: a design addendum

**Status:** Draft, for the owner's review
**Created:** 2026-11-07
**Reads with:** `2026-11-03-chunk-backed-volumes-design.md` (sections 4.2 to 4.7
and decisions 36 to 60), `2026-10-30-architecture-brief.md`,
`docs/operating-volumes.md`, `docs/restore.md`
**Adds:** decisions 61 and 62, appended to the logs of the spec and the brief.
Nothing in the spec is rewritten.

This addendum closes one gap the chunk-backed design leaves open: a volume's
`current` moving backwards without anyone having asked for it. The direction is
decided; this document records it so a builder can plan the work in M3 without
going back to the owner. Nothing in it is specific to one environment.

## 1. The problem

A volume's history is a sequence of generations, `<epoch>.<seq>`, ordered by
epoch and then sequence, and `current` names the newest. Every object in the
store is genuine, so if `current` is made to name an older manifest, whether
because the export was restored from a NAS snapshot, restored from an offsite
copy, or an old copy of the directory was synced back over it, nothing today
notices. Each chunk still hashes to its name, the manifest still names its own
volume, and the older epoch is not above any epoch that the fence compares
against, because the fence compares only what the publisher and the holder file
say.

The consequence arrives at the next stage. A node that has no reusable cache
(the usual case, since a clean unstage deletes the cache, 4.7) reads the old
`current` and mounts old data. The filesystem is consistent, the pod starts, and
every write since the restored point is gone without a message. The next
generation then lands on top of the old one and the loss becomes permanent once
the newer manifests are pruned.

## 2. What already holds

- **Chunks are verified by hash** on every read and in every restore, so a
  corrupt or substituted chunk is caught.
- **A manifest must name its own volume**, and the block server refuses another
  volume's manifest.
- **The epoch fence** (4.6) stops a stale holder from making its writes
  authoritative; it says nothing about a store that has gone backwards as a
  whole.
- **`miami restore --manifest <epoch.seq>`** rebuilds any retained generation
  offline (5.1), so older data is always recoverable on purpose.

What is missing is a memory of how far the volume has got that does not live in
the store a rollback rewinds.

## 3. Why manifest authentication does not solve it

A MAC over each manifest, keyed from the volume key, would let a node tell a
genuine manifest from a forged one. A replayed old manifest is genuine: it was
signed when it was written and still verifies. Signing defends against a
malicious writer on the export, who is outside the current threat model (the
export is the owner's).

Manifest signing is therefore deferred until the export is shared with an
untrusted party. The question to settle then is whether plain (unencrypted)
volumes get a key for it at all, since today only an encrypted volume has one;
that belongs to whichever decision introduces the shared export. It is not
needed for anything below.

## 4. Decision 61: the rollback guard

### 4.1 The mark

Each volume has a **high-water mark**: the newest generation known to have been
published, as `<epoch>.<seq>`. It is kept outside the store, on the PV, as the
annotation

```
miami.csi.greghaskins.com/high-water: <epoch>.<seq>
```

Outside the store is the point: a restore of the export rewinds the manifests,
`current`, the holder file and the epoch claims together, and cannot reach the
Kubernetes API. The mark only moves forward, by compare-and-set against the
PV's `resourceVersion`, and a value lower than the one present is never written.
An absent annotation, as on a PV from before the guard, means no mark; the
first publish after the upgrade sets it, so existing volumes are covered from
their next generation without a migration step.

### 4.2 The check

`NodeStageVolume` reads `current` (4.7) and, before anything that serves,
mounts or discards data, compares the generation the stage would serve with the
mark. If that generation is older, the stage fails retryably and changes
nothing: no holder takeover beyond what was already done, no server started, no
cache discarded. The failure is named, in three places:

- the `rollback-detected` condition on the volume;
- a `RollbackDetected` Warning event on the PV;
- the stage error, which says what happened and how to proceed, in the style of
  `holder-contended`: the store's `current` names `<cur>`, a generation `<mark>`
  was published before it, the store has probably been restored from an older
  copy, writes after `<cur>` are not in it; to use the older data set
  `accept-generation='<cur>'` on the PV (section 5); to recover the newer data,
  restore the store from a copy that holds `<mark>` or run `miami restore
  --manifest <mark>` against one.

**Interaction with cache reuse (decision 57).** The guard compares the data the
stage would serve, not `current` alone. Cache reuse is decided separately and
is unchanged: a surviving cache is reused when `current` still names its base
or carries its epoch. The two combine as follows.

| What the stage would serve | Against the mark | Result |
|---|---|---|
| a reused cache, base at or above the mark | not older | stage proceeds; the next publish writes a generation above `current` and so repairs the store |
| a reused cache, base below the mark | older | refused; cache untouched |
| `current` (no cache, or a cache that fails the reuse test) | older | refused; **the cache is not discarded** |
| `current` | not older | as today |

The third row is the one that matters: a cache that fails the reuse test only
because `current` went backwards would otherwise be discarded as "a newer
lineage exists", which is the rollback destroying the one copy of the newer data
that survived. The guard therefore runs before the reuse test discards
anything. A cache held back this way is kept like any other cache no holder file
claims (`docs/operating-volumes.md`); it is discarded only by the existing proofs
or by an accepted rollback (section 5).

A rolled-back store also rewinds the epoch claims, so a later acquisition could
issue an epoch that retained newer manifests already use. The mark gives a
floor: an acquisition claims an epoch above the mark's epoch whenever the mark
is present. This keeps manifest names unique across a lineage that was rewound
and continued.

### 4.3 When the mark is written

Two ways to keep the mark current were weighed.

- **On every publish.** After `current` is renamed, patch the PV. The mark is
  exact. It costs one API write per generation per volume (at the default five
  minutes, about twelve an hour per active volume, bursting to the one-minute
  floor), and every publishing node needs the new privilege continuously.
- **At a verified unstage plus a bounded interval.** Write when the final
  generation is verified (4.7), and otherwise at most once per `markInterval`
  (default 5 m) after a publish. Cheaper and quieter. It misses a rollback that
  rewinds only the generations published since the last write, at most one
  interval's worth.

**Recommendation (for the owner to confirm): the second.** The threats in
section 1 are restores from a snapshot or a backup, which rewind by hours or
days, not by minutes, so the window costs nothing against them. A verified
unstage is exact for the common case, a volume that moves between nodes. The
cost is a bounded blind spot after a node is lost: the generations its holder
published after its last write are not marked when the emergency detach of 4.6
hands the volume on. That is the same few minutes of exposure the loss window of
4.5 already accepts for the data itself. The per-publish form buys exactness
inside that window at the price of an API write on every generation and is the
fallback if the owner prefers it; the check, the annotation and the decision
below are the same either way. A failed patch never fails a publish or an
unstage; it is retried at the next trigger and counted in a metric.

### 4.4 The privilege

The node plugin's role today has `get` on persistentvolumes and nodes and
nothing else (4.6). The mark needs **`patch` on `persistentvolumes`**. PVs are
cluster-scoped and created dynamically, so `resourceNames` cannot narrow the
rule, and a node plugin that can patch can in principle patch any PV on any
field. That is acceptable for the same reason the existing `get` is: the node
plugin already runs as root on the host and already holds the volume keys of
every volume it stages, so a compromised node plugin can reach more than a PV's
metadata.

It can be narrowed anyway. A ValidatingAdmissionPolicy keyed on the plugin's
ServiceAccount can require that the object's `spec` and `labels` equal the old
object's and that the annotations differ only in the keys the driver writes
(`high-water` here; the others in decision 62 are the operator's, not the
plugin's). The base ships no admission policy (section 8 of the spec); the
operating guide carries this one as an example for a cluster to adopt.

## 5. Decision 62: deliberate rollback

Sometimes going back is the intent: a bad upgrade of the application, a
corrupted database. The operator says so, exactly, as with force-detach:

```
kubectl annotate pv <pv> miami.csi.greghaskins.com/accept-generation='<epoch>.<seq>'
```

The value is matched exactly against the generation `current` names at stage
time. The guard then lets that one stage proceed, on the data `current` names;
a cache whose lineage is newer is discarded by the existing rule, deliberately,
with `GenerationLost`. An annotation that does not match (a stale one, or one
for another generation) is ignored, so it cannot excuse a later, different
rollback. The driver never removes it, and it becomes inert once the volume
moves on.

The next publish lands as a **new, higher generation** (a fresh epoch above the
mark's, 4.2) on top of the accepted one, and the mark moves with it. A
rollback is thus a forward step in history, never a rewrite of it.

### 5.1 Retention

Prune-by-count (4.2, 4.5) keeps the last `generations.keep` manifests by order.
After a rollback that rule would drop the rolled-back-over generations first
(they sit below the new epoch), so rollback would lose data after all. The rule
is amended:

- When a rollback is accepted, the node records the **held range** on the PV
  before it stages, as the annotation
  `miami.csi.greghaskins.com/held-generations: <from>..<to>`, where `<from>` is
  the accepted generation and `<to>` is the mark at that moment.
- Manifests inside the range, and chunks they reference, are never pruned and
  do not count toward `generations.keep`. The rest of the volume's history is
  pruned as before.
- The operator releases them by removing the annotation; the next prune then
  treats them like any other generation. Until then they cost storage, and the
  operating guide says so.
- A rollback can therefore be undone: point `current` at a held generation
  (offline with the restore tooling, or by the same accept step against it) and
  the volume goes forward from there.

A held range survives its own restore: it lives on the PV, so if the store is
restored again to a copy that lacks those manifests, the guard fires and the
message names them.

### 5.2 Clone from a generation is the primary way back

The recommended way to look at, or return to, older data is not to rewind the
volume but to **clone from a generation**: a new PVC whose volume starts from
an older generation's manifest, with the original untouched. That is already
planned for M4 (snapshots and clones), and this addendum does not design it.
It only makes sure the guard and the retention rule fit:

- A snapshot's pin (M4: "publish a generation now and pin it from pruning") and
  the held range above are the same mechanism, a set of manifests prune must
  leave alone. M3 should build it once, as a pin that a rollback, and later a
  snapshot, can add to.
- A clone is a new volume and starts with its own mark, set to the generation it
  starts from, so a clone is never refused for being older than its source.
- Deliberate rollback of the live volume remains for the case where the same
  PVC must hold the old data.

## 6. Non-goals

- **Manifest signing** (section 3).
- **Cross-volume deduplication.** The mark, the held range and the pins are per
  volume; a chunk shared between volumes would need a reference count outside
  this design.
- **Detecting chunk tampering.** Chunk names are their hashes; the guard adds
  nothing there.
- **Detecting a rollback no node ever saw.** The mark records what was
  published; a volume that never published has nothing to roll back to.

## 7. Where it lands

An M3-sized item, not part of M2's merged scope. Touched areas:

- **Node RBAC:** `patch` on `persistentvolumes` in the node role; an example
  admission policy in the operating guide.
- **The reconcile stage path:** the check of 4.2 ahead of the cache-reuse test,
  the condition, event and error text, the epoch floor, and the accept step with
  the held range recorded first.
- **Recording the mark:** the block server's publish or the reconciler, after
  the rename of `current` and at the verified unstage, with the rate limit.
  Which of the two owns it is the builder's, following where the `publish`
  annotation is already read.
- **Retention:** prune honours the held range and the pin mechanism of 5.2.
- **Docs:** `docs/operating-volumes.md` (a runbook for `RollbackDetected`, the
  accept step, releasing the held range, the example policy) and the metrics
  list.
- **e2e:** publish two generations, copy the older manifest's name over `current`
  to simulate a store rollback, restage on a node with no cache and expect the
  refusal with the named condition and event and the cache untouched on a node
  that has one; then set `accept-generation` and expect the stage to succeed, a
  new higher generation after a write, the mark moved, and the generations
  newer than the rollback point still present after enough publishes to exceed
  `generations.keep`.

## 8. Left to the builder

- The exact annotation grammar for the held range, within section 5.1.
- The default of `markInterval` and the name of the metric for failed patches.
- Whether the mark is written by the block server or the reconciler.
- The text of the condition, event and error, within 4.2.
