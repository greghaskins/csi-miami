# Cache capacity in scheduling (decision 66) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Status:** Implemented (T1 to T9, 2026-11-09). Go-ahead was the owner's approval of the design (given 2026-11-07); the design carries the amendments.
**Created:** 2026-11-08

**Goal:** a node advertises its cache budget as the extended resource `miami.csi.greghaskins.com/cache`, a mutating webhook makes each pod request the footprint of the volumes it mounts, and the node plugin refuses a fresh stage that would exceed the budget.

**Architecture:** one pure function, `volume.CacheFootprint`, is the only definition of a volume's footprint; the webhook (controller), the advertisement (node) and the stage check (volume item) all call it. The node plugin patches its own node's `status.capacity` under a ValidatingAdmissionPolicy that pins the patch to its own node and one key; the webhook is a second listener in the controller with `failurePolicy: Ignore`, and the stage check is the net under it.

**Tech stack:** Go 1.27, client-go dynamic client (as `internal/kube` does today), `k8s.io/api/admission/v1`, `golang.org/x/sys/unix` (`Statfs`), controller-runtime `envtest` (admission policy and webhook tests), kustomize, CEL (admission policy), the kind e2e harness.

**Spec:** `2026-11-07-cache-capacity-scheduling-design.md` (decision 66; the plan argues from it, executors read both). **Reads with:** `2026-11-03-chunk-backed-volumes-design.md` (4.3, decisions 54, 59), `2026-11-03-m2-implementation-plan.md` (the conventions this plan copies), `docs/operating-volumes.md`.

## Global Constraints

- Resource name `miami.csi.greghaskins.com/cache`; unit bytes, always a whole number of MiB, written with binary suffixes (`30Gi`); a pod carries `requests` equal to `limits` on its first regular container only (design 4.2, 4.4).
- Footprint, verbatim: `footprint(size, params) = min(size, cacheSize) + dirtyWatermark + indexSlack`, `indexSlack` 16 MiB, rounded up to a whole MiB; `size` is the PV capacity, or the PVC request rounded up to whole chunks through `Context.WithSize` when no PV exists yet (design 4.1).
- Budget, verbatim: `budget = max(0, share x size(cacheFS) - reserve)`; `-cache-budget-share` default 0.5, `-cache-budget-reserve` default 15Gi, `-cache-budget` absolute overrides the formula; a budget of zero is a real answer (advertise `0`, refuse every new volume); `-cache-accounting=false` removes the node's own key and skips the check (design 4.3).
- The stage check runs at the top of `Converge`, before the holder layer, and only for a volume new to the node (no nbd device with its name and no cache index), at its raised size. Adopt, server restart, reboot with a surviving cache, drain and teardown are never refused (design 4.8).
- Refusal is `ErrCacheBudgetExceeded`, condition `cache-budget-exceeded`, event `CacheBudgetExceeded` (Warning, fixed text), class `Unavailable`; the error names the budget, the commitment with its volume count, and the volume's need (design 4.8).
- Webhook: `MutatingWebhookConfiguration`, pods, `CREATE` only, `sideEffects: None`, `matchPolicy: Equivalent`, `reinvocationPolicy: IfNeeded`, `timeoutSeconds: 10`, `failurePolicy: Ignore`; `namespaceSelector` excludes `kube-system`, `miami-system` and any namespace labelled `miami.csi.greghaskins.com/cache-accounting: disabled`. Nothing PVC- or annotation-driven overrides a class parameter (design 4.4).
- Admission policy: `ValidatingAdmissionPolicy` with `failurePolicy: Fail` and a binding with `validationActions: [Deny]`. The node plugin's ClusterRole gains `patch` on `nodes/status` and nothing else new (design 4.3, 4.9).
- TLS: base is self-signed with the Secret `miami-webhook-tls` in `miami-system`; `-webhook-tls-secret` names a Secret someone else manages; cert-manager is an optional kustomize component, never a base dependency (design 4.4, 6.6).
- Metrics (names verbatim): node `miami_cache_budget_bytes`, `miami_cache_committed_bytes`, `miami_cache_fs_size_bytes`, `miami_cache_fs_free_bytes`; controller `miami_webhook_mutations_total` (label `outcome`), `miami_webhook_unresolved_total`, `miami_webhook_config_ok`.
- Everything from the M2 plan's constraints still holds: nothing environment-specific in the repository (node names `kind-worker`, no hostnames, no owner budgets; the owner's absolute budgets live in the owner's overlay, not here); commit messages with an imperative subject and a body that says why, no co-author or session trailer; one branch `plan/cache-capacity` for the plan and then one branch per task `cache/tNN-<slug>` from `origin/main`, merged with a merge commit after review, never rebased or force-pushed; `make check` green before every push; events carry fixed text and never an error string; metrics carry no path or address.
- RWX volumes are out of scope (design 4.7): the webhook requests nothing for a PVC whose access modes include a many-node mode.

## Review Focus

Failure modes the design implies and no task's happy-path tests would exercise, most likely first. Each has its test in the task named.

1. A volume that is not this driver's, not of kind `volume`, or a PVC that is `Terminating`, `Lost` or a many-node claim: the pod is admitted unchanged, no request, no error, no counter other than `unchanged`; the one exception is a claim of our own class whose `NewContext` rejects its parameters, counted `unresolved` (T5).
2. A pod whose author set the resource by hand (a small request, on an init container, on a later container, with `limits` different from `requests`), a pod whose containers another mutator reordered, and a pod with no regular container: one computed number ends up on the first regular container and nowhere else; re-invocation changes nothing (T5).
3. A disk smaller than the reserve, a share that rounds to a fraction of a MiB, and a flag change from budget to `-cache-accounting=false`: the node advertises `0` (not nothing) in the first case, a whole-MiB figure in the second, and leaves no key behind in the third (T2, T3).
4. A footprint sum that overflows `int64` (many large volumes, or a hand-edited PV): it saturates at a whole-MiB maximum and the stage check refuses; it never wraps to a small number that admits (T1, T4, T5).
5. The self-signed serving certificate is expired, near expiry or unparseable in the Secret, or the `caBundle` is wiped by a re-apply of the base: the controller regenerates or reasserts within one sync pass and `miami_webhook_config_ok` shows the gap meanwhile (T6).
6. A refused fresh stage must acquire and write nothing (no holder file, no drain record): the pod may move to another node and nothing about the refusal may strand the volume; and an expanded volume is checked at its raised size (T4).

---

## 1. Exit criteria

The change is done when, on the kind e2e cluster over the nfs3 transport and in the unit, envtest and deploy suites:

1. `volume.CacheFootprint` is pinned by a table test and is the only place the formula appears.
2. Each worker's `status.capacity` carries the resource equal to the node plugin's budget, and removes it when accounting is switched off.
3. The node plugin's token can patch its own node's capacity key and nothing else; it is refused on another node, on another capacity key, on any other status or metadata field, and with an unbound token. The entry removed by hand returns within a pass.
4. A pod mounting a bound claim, an unbound `WaitForFirstConsumer` claim and an ephemeral claim template is admitted with `requests == limits ==` the sum of the footprints on its first container only.
5. A pod that does not fit the remaining budget of any worker stays Pending with `Insufficient miami.csi.greghaskins.com/cache` and is scheduled once a pod ends.
6. With the webhook bypassed, a fresh stage over the budget is refused retryably with the event `CacheBudgetExceeded` and is admitted once another volume unstages; adopt, plugin restart, node reboot with a surviving cache and orphan drain are never refused.
7. With the controller down, pod creation still succeeds (fail open) and the stage check holds the line.
8. `docs/operating-volumes.md` carries the alerts, the recording rule and the runbook of design 4.6 and 4.8.

Amended 2026-11-09: the reboot with a surviving cache and the orphan drain at a full budget in criterion 6 are covered by unit tests only (`internal/item/volume`, `budget_test.go`); the kind suite does not reboot a node or stage into a full budget with a drain pending. The second half of `TestCacheStageCheckNeverRefusesExisting` says itself that it does not test what the test's name claims.

## 2. Repository layout

| Path | Change |
|---|---|
| `internal/volume/footprint.go` | new: `CacheFootprint`, `AddFootprint`, `CacheResource` |
| `internal/cachecap/` | new: `Budget`, `Source`, `Runner` and `Pass` (the node's advertisement loop), the `NodeCapacity` seam |
| `internal/kube/capacity.go` | new: node capacity read, JSON patch set and remove on `nodes/status` |
| `internal/kube/{claims.go,kube.go}` | new `claims.go`: PVC, StorageClass and default-class reads; `PV` gains `CapacityBytes` in `kube.go` (the only definition) |
| `internal/item/volume/{budget.go,layers.go,volume.go}` | the committed sum and the stage check |
| `internal/item/item.go`, `internal/reconcile/{errors.go,converge.go}`, `internal/kube/events.go` | `ErrCacheBudgetExceeded`, the condition, the event |
| `internal/metrics` | the node cache gauges and the webhook metrics |
| `internal/webhook/` | new: mutator, resolver, HTTP server, self-signed TLS, `caBundle` assertion |
| `cmd/miami/{node.go,controller.go}` | flags and wiring |
| `deploy/kustomize/base/` | webhook configuration, Service, admission policy and binding, RBAC, controller args and port, golden |
| `deploy/kustomize/components/cert-manager/` | new optional component |
| `deploy/policy_test.go`, `deploy/policy_envtest_test.go` | exact RBAC, ports, rendered policy; the policy against a real API server |
| `test/e2e/cache_*_test.go`, `hack/e2e-kind.sh`, `deploy/kustomize/overlays/e2e` | the criteria above on kind |
| `docs/operating-volumes.md`, `README.md` | alerts, recording rule, runbook |

## 3. Interfaces shared by the tasks

```go
// internal/volume (T1)
const CacheResource = "miami.csi.greghaskins.com/cache"
const MaxFootprint int64 = math.MaxInt64 &^ (1<<20 - 1) // saturation value, whole MiB
func CacheFootprint(v VolumeParams) int64            // bytes, whole MiB; 0 when v.Size <= 0 (unresolved)
func AddFootprint(total, f int64) int64              // saturating at MaxFootprint

// internal/cachecap (T2, T3)
type NodeCapacity interface { // *kube.Client implements it (T3)
    CacheCapacity(ctx context.Context, node string) (bytes int64, present bool, err error)
    SetCacheCapacity(ctx context.Context, node string, bytes int64) error
    RemoveCacheCapacity(ctx context.Context, node string) error
}
type Config struct {
    Accounting   bool
    Share        float64 // (0, 1]
    Reserve      int64   // bytes
    Absolute     int64   // bytes, meaningful only when HaveAbsolute
    HaveAbsolute bool
}
func Budget(fsSize int64, c Config) int64 // whole MiB rounded down, never negative; Absolute wins
type FSStat func(dir string) (size, free int64, err error)
type Source struct{ /* Config, cache dir, FSStat, latest figures */ }
func NewSource(c Config, cacheDir string, stat FSStat) *Source
func (s *Source) Current(ctx context.Context) (budget int64, enabled bool, err error) // statfs each call; the stage check and the Runner share it
func (s *Source) Stats() (fsSize, fsFree int64) // the figures of the latest statfs Current made (0, 0 before any); the Runner's fs gauges read them
type CapacityMetrics interface{ SetCacheCapacity(budget, committed, fsSize, fsFree int64) } // the metrics registry (T2)
type Runner struct{ /* Source, NodeCapacity, node name, committed func, metrics, logger */ }
func NewRunner(src *Source, nc NodeCapacity, node string, committed func(context.Context) (int64, int, error), m CapacityMetrics, log *slog.Logger, interval time.Duration) *Runner
func (r *Runner) Pass(ctx context.Context) error // compute, set metrics, compare, patch when different
func (r *Runner) Run(ctx context.Context)        // Pass every interval until ctx ends

// internal/item/volume (T4)
type Config struct { /* existing fields */
    // CacheBudget is the node's budget; nil, or enabled false, turns the stage check off.
    CacheBudget func(ctx context.Context) (budget int64, enabled bool, err error)
}
func (it *Item) Committed(ctx context.Context) (bytes int64, volumes int, err error) // sum over drain records
var item.ErrCacheBudgetExceeded error // internal/item/item.go

// internal/webhook (T5, T6)
type Claims interface {
    GetPVC(ctx context.Context, namespace, name string) (*kube.PVC, error)       // wraps kube.ErrNotFound
    GetPV(ctx context.Context, name string) (*kube.PV, error)                    // wraps kube.ErrNotFound
    GetStorageClass(ctx context.Context, name string) (*kube.StorageClass, error) // wraps kube.ErrNotFound
    DefaultStorageClass(ctx context.Context) (*kube.StorageClass, error)         // nil, nil when the cluster has none
}
type Metrics interface { // implemented by the metrics registry built with WithWebhook() (T5)
    Outcome(outcome string) // miami_webhook_mutations_total{outcome}: mutated, unchanged, stripped, error
    Unresolved()            // miami_webhook_unresolved_total
}
type Mutator struct{ Claims Claims; Metrics Metrics; Log *slog.Logger }
func (m *Mutator) Mutate(ctx context.Context, req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse
```

---

## 4. Tasks

Branch per task from `origin/main`; T1 first; T3 to T5 consume its functions (T2 consumes nothing from T1). Tasks marked **security-sensitive** get the strongest-model review before merge; the architect reviews this plan and those PRs.

### Task 1: CacheFootprint

**Files:**
- Create: `internal/volume/footprint.go`
- Test: `internal/volume/footprint_test.go`

**Interfaces:**
- Consumes: `volume.VolumeParams` (`Size`, `CacheSize`, `Generations.DirtyWatermark`) as `Context.WithSize` resolves it.
- Produces: `CacheResource`, `MaxFootprint`, `CacheFootprint`, `AddFootprint` (section 3). Nothing else in the repository may spell the formula.

- [ ] **Step 1: Write the failing tests** in `footprint_test.go`.
  `TestCacheFootprintPinned`, a table in MiB-exact numbers (this is the pin; a change to it is a design change):

  | size | cacheSize | dirtyWatermark | want |
  |---|---|---|---|
  | 10Gi | 10Gi | 5Gi | 15376Mi (15Gi + 16Mi) |
  | 100Gi | 8Gi | 4Gi | 12304Mi (a lazy volume: 1.5 x the bound + 16Mi) |
  | 1Gi | 10Gi | 5Gi | 6160Mi (`min(size, cacheSize)` is the size) |
  | 4Mi | 4Mi | 2Mi | 22Mi |
  | 1Gi | 1Gi | 1000000 B | 1041Mi (rounds up: 1091519040 B is 1040.96 MiB) |

  `TestCacheFootprintMatchesWithSize`: for sizes 1 GiB, 100 GiB and 1 TiB and class params `{}`, `{cacheSize: 8Gi}`, `{cacheSize: 8Gi, generations.dirtyWatermark: 1Gi}`, run `NewContext(params)` then `WithSize(size)` and assert `CacheFootprint(*ctx.Volume)` equals the table formula computed from the resolved fields (the webhook relies on exactly this path).
  `TestCacheFootprintUnresolvedIsZero` (`Size == 0` gives 0, never a negative or a partial number).
  `TestCacheFootprintSaturates` (`Size`, `CacheSize` and watermark near `1<<62` give `MaxFootprint`, which is a multiple of 1 MiB and positive) and `TestAddFootprintSaturates` (`AddFootprint(MaxFootprint, 1<<20) == MaxFootprint`; `AddFootprint(3<<20, 5<<20) == 8<<20`) - review focus 4.

- [ ] **Step 2: Run to verify failure**: `go test ./internal/volume -run 'CacheFootprint|AddFootprint' -v`; expect FAIL, `undefined: CacheFootprint`.
- [ ] **Step 3: Implement** `CacheFootprint(v VolumeParams) int64` in `internal/volume/footprint.go`. Missing `CacheSize` means `Size` and missing watermark means `max(CacheSize/2, 1)`, the same defaults `WithSize` applies; do the sum in `uint64`/`math/bits` so an overflow saturates, then round up to 1 MiB. `AddFootprint` is the same saturating add. Doc-comment both with the design section so the "one formula" claim is findable.
- [ ] **Step 4: Run to verify pass**: `go test ./internal/volume -race`; expect PASS.
- [ ] **Step 5: Commit** (`Add CacheFootprint, the one formula for a volume's cache footprint`).

**Accept when** the pinned table passes and `grep -rn '16 << 20\|indexSlack' --include=*.go --exclude=*_test.go .` finds the constant only in this file (the pinned test may restate it).

### Task 2: Node budget, flags, metrics and the advertisement loop

**Files:**
- Create: `internal/cachecap/{budget.go,source.go,runner.go}`, `internal/cachecap/cachecap_test.go`
- Modify: `internal/metrics/metrics.go` (option `WithCacheCapacity()`, `SetCacheCapacity(budget, committed, fsSize, fsFree int64)`), `internal/metrics/metrics_test.go`; `cmd/miami/node.go` (flags, validation, `nodeConfig` fields, wiring), `cmd/miami/node_test.go`

**Interfaces:**
- Consumes: nothing from T1. The committed figure arrives from T4 as a `func(context.Context) (int64, int, error)`; until then the wiring passes a func returning zero. The real `NodeCapacity` is T3's `*kube.Client`; until then the wiring passes a no-op `NodeCapacity` (a small type in `cmd/miami` whose `CacheCapacity` reports absent and whose set and remove do nothing), so T2 builds, runs and tests alone and T3 replaces it.
- Produces: `cachecap.Config`, `Budget`, `FSStat`, `Source` (with `Current` and `Stats`), `Runner` with its `NewRunner` constructor, `NodeCapacity`, `CapacityMetrics` (section 3); node flags `-cache-accounting` (bool, true), `-cache-budget-share` (float, 0.5), `-cache-budget-reserve` (quantity string, `15Gi`), `-cache-budget` (quantity string, empty means "use the formula"; `0` is an explicit zero).

- [ ] **Step 1: Write the failing tests.**
  `TestBudgetFormula`: a 250Gi fs, share 0.5, reserve 15Gi gives 110Gi; a 500Gi fs gives 235Gi; a 20Gi fs gives 0 (negative clamps); share 0.5 on a 10Gi+700KiB fs with reserve 0 rounds down to whole MiB (review focus 3).
  `TestBudgetAbsoluteWins`: `HaveAbsolute` with 64Gi returns 64Gi on a 20Gi fs and on a 500Gi fs; `Absolute: 0, HaveAbsolute: true` returns 0 with `enabled` true from `Source.Current` (zero is an answer).
  `TestSourceAccountingOff`: `Current` returns `enabled == false`.
  `TestSourceStats`: `Stats` returns `(0, 0)` before any `Current`, then the size and free bytes of the latest `FSStat` call, and they change when the fake `FSStat` changes.
  `TestRunnerPass` with a fake `NodeCapacity` and a fake `FSStat`:
  absent key gets `SetCacheCapacity(node, budget)`; equal key gets no call; different key gets one call; budget 0 sets `0` and does not remove (review focus 3); accounting off with the key present calls `RemoveCacheCapacity` once and with the key absent calls nothing; a `CacheCapacity` or `SetCacheCapacity` error is returned and the metrics are still set; the node name in every call is the configured one.
  `TestNodeFlagsCacheBudget`: defaults parse to share 0.5, reserve 15Gi, accounting true, no absolute; `-cache-budget=64Gi` sets it; `-cache-budget=0` sets `HaveAbsolute` with 0; rejected with exit 2: share 0 or above 1, negative reserve, `-cache-budget=-1`, an unparseable quantity.
  `TestCacheMetrics`: the four gauges are registered only with `WithCacheCapacity()` and a call to `SetCacheCapacity` shows in the text exposition.
- [ ] **Step 2: Run to verify failure**: `go test ./internal/cachecap ./internal/metrics ./cmd/miami -run 'Budget|Source|Runner|CacheMetrics|CacheBudget' -v`; expect FAIL (packages and symbols undefined).
- [ ] **Step 3: Implement** `Budget`, `Source` (`Current` calls `FSStat(cacheDir)` each time; `RealFSStat` uses `unix.Statfs` with `Blocks*Bsize` and `Bavail*Bsize`), `NewRunner`, `Runner.Pass` (it calls `Current`, then reads `Stats` for the fs gauges) and `Runner.Run` (ticker at `-reconcile-interval`, first pass immediately, a failed pass logged and retried at the next tick; the loop is its own goroutine, never inside the reconciler's pass, so a slow API server cannot stall volume work). Quantity comparison with `resource.Quantity.Cmp`. Register the gauges and add the flags; build the `Runner` with `NewRunner` and the no-op `NodeCapacity` and start it in `serveNodeWith` beside the reconciler and stop it in the same order as the reconciler. When `-cache-accounting=false`, do not register the gauges.
- [ ] **Step 4: Run to verify pass**: `go test ./internal/cachecap ./internal/metrics ./cmd/miami -race`; expect PASS.
- [ ] **Step 5: Commit** (`Compute the node's cache budget and run its advertisement loop`).

### Task 3: Node-status patch, admission policy and node RBAC (security-sensitive)

**Files:**
- Create: `internal/kube/capacity.go`, `internal/kube/capacity_test.go`, `deploy/kustomize/base/validatingadmissionpolicy-node-capacity.yaml`, `deploy/kustomize/base/validatingadmissionpolicybinding-node-capacity.yaml`, `deploy/policy_envtest_test.go` (tag `envtest`)
- Modify: `deploy/kustomize/base/clusterrole-node.yaml` and `kustomization.yaml`; `deploy/policy_test.go` (`TestNodeRBACExact`, `TestRBACRuleSets`, the object-kind check at line ~1124); `deploy/testdata/base.golden.yaml` (`-update`); `Makefile` (`envtest` target runs `./deploy` too); `cmd/miami/node.go` (pass the real `*kube.Client` as the `NodeCapacity`, replacing T2's no-op type, which is deleted)

**Interfaces:**
- Consumes: T2's `cachecap.NodeCapacity`, `volume.CacheResource` (T1).
- Produces: `(*kube.Client).CacheCapacity`, `SetCacheCapacity`, `RemoveCacheCapacity`; the policy `miami-node-capacity` and its binding.

- [ ] **Step 1: Write the failing client tests** in `capacity_test.go` against `k8s.io/client-go/dynamic/fake`:
  `TestSetCacheCapacityPatch` asserts the request is a JSON patch on subresource `status` of `nodes/<name>` with body exactly `[{"op":"add","path":"/status/capacity/miami.csi.greghaskins.com~1cache","value":"95Gi"}]` (the `/` of the key escaped as `~1`; `add` replaces an existing member); `TestRemoveCacheCapacityPatch` body is one `remove` of the same path; `TestCacheCapacityReads` returns `present=false` when the key is missing and the exact byte count when set, and an error (never `present=false`) when the API answers an error or the value is not a quantity; a patch error carries no URL (use `reasonOf`).
- [ ] **Step 2: Write the failing policy tests** in `deploy/policy_envtest_test.go`. Start envtest, render the base with the existing `base(t)`, and create only the `ValidatingAdmissionPolicy` and binding from it (the shipped YAML is what is tested). Poll with a known-denied patch until the policy enforces before the matrix. Impersonate `system:serviceaccount:miami-system:miami-node` with extra `authentication.kubernetes.io/node-name: [node-a]` against nodes `node-a` and `node-b` created in the test:

  | request | want |
  |---|---|
  | add the resource key on `node-a` | allowed |
  | remove the key on `node-a` | allowed |
  | add the key on `node-b` | denied (another node) |
  | add `example.com/other` capacity on `node-a` | denied (another key) |
  | change `cpu` capacity on `node-a` | denied |
  | change `status.allocatable`, `status.conditions`, `status.images` | denied each |
  | change `metadata.labels` or `metadata.annotations` through the status endpoint | denied each |
  | no `node-name` extra (an unbound token) | denied |
  | `node-name` extra naming a node that does not exist | denied |
  | admin (`system:masters`) patches any of the above | allowed (the policy scopes by the plugin's username with a `matchCondition`) |
  | `system:node:node-a` (the kubelet) patches capacity, conditions and labels on `node-a` | allowed; the policy matches no rule for it (pins the `matchConditions` scope) |

  Also `TestPolicyAndBindingShip`: the binding names the policy, `validationActions == [Deny]`, the policy's `failurePolicy == Fail`; removing either file from `kustomization.yaml` fails the test (a policy with no binding enforces nothing).
- [ ] **Step 3: Run to verify failure**: `go test ./internal/kube -run CacheCapacity -v` and `make envtest`; expect FAIL (undefined; policy missing).
- [ ] **Step 4: Implement** the three client methods on `*kube.Client` in `capacity.go` (the dynamic client's `Patch(ctx, name, types.JSONPatchType, body, metav1.PatchOptions{}, "status")`). Write the policy: match `nodes/status` `UPDATE` with `matchConditions` limiting it to the plugin's username; validations (CEL, expression text is the builder's, the three rules are not): (1) `request.userInfo.extra['authentication.kubernetes.io/node-name']` exists, has one value, and equals `object.metadata.name`; (2) `object.status.capacity` and `oldObject.status.capacity` differ in no key but `miami.csi.greghaskins.com/cache` (a key may be added, changed or removed, every other key present on both sides with an equal value); (3) everything else is unchanged (if whole-`status` equality exceeds the CEL cost estimate, which under `Fail` would deny every plugin patch, fall back now to a bounded list of fields: `allocatable`, `conditions`, `addresses`, `daemonEndpoints`, `nodeInfo`, `images`, `volumesInUse`, `volumesAttached`; the envtest must run against the shipped expression so an over-cost policy fails the test): the rest of `status` and `metadata.labels`, `annotations`, `finalizers` and `ownerReferences` (the status endpoint resets `spec` but not metadata, so rule 3 is needed for "one number on its own node, nothing else"). Add `patch` on `nodes/status` to the node ClusterRole (its header comment updated: the plugin now writes one capacity key, narrowed by the policy). Regenerate the golden.
- [ ] **Step 5: Run to verify pass**: `go test ./internal/kube ./deploy -race` and `make envtest`; expect PASS, with every denied row denied for the reason of its row (assert the message names the rule).
- [ ] **Step 6: Commit** (`Let the node plugin patch its own node's cache capacity under an admission policy`).

**Accept when** the matrix passes against a real API server and the rendered node ClusterRole equals the old rules plus `nodes/status: [patch]`.

### Task 4: The stage check

**Files:**
- Create: `internal/item/volume/budget.go`, `internal/item/volume/budget_test.go`
- Modify: `internal/item/volume/{volume.go (Config, doc),layers.go (Converge)}`, `internal/item/item.go`, `internal/reconcile/{errors.go,converge.go}` and their tests, `internal/kube/events.go` (+ test), `cmd/miami/{node.go,volumekind.go}` (wire `Config.CacheBudget` from `cachecap.Source.Current` and the Runner's committed func from `Item.Committed`), `docs/design/2026-11-03-chunk-backed-volumes-design.md` (condition table, one line)

**Interfaces:**
- Consumes: `volume.CacheFootprint`, `volume.AddFootprint` (T1); `cachecap.Source.Current` (T2).
- Produces: `Config.CacheBudget`, `(*Item).Committed`, `item.ErrCacheBudgetExceeded`, `reconcile.ConditionCacheBudgetExceeded = "cache-budget-exceeded"` (added to `Conditions`), `kube.ReasonCacheBudgetExceeded = "CacheBudgetExceeded"` (a Warning reason with fixed text, no error string).

The check runs at the top of `Converge`, after `checkPrereq` and `volOf` and before `ensureHolder`. Predicate: no nbd device whose backend is the volume's name AND `cache.ReadIndex(v.cacheDir)` is `ErrNoIndex`; any other volume passes untouched. Before checking, it raises `v.size` with `raiseSize` (the size record and the current manifest: two bounded reads) and checks the raised size; a store read error is returned as itself, never as a refusal. Failure rules: a store read (the size record, the current manifest) returns its error as itself, never as a refusal; a statfs error (`CacheBudget` returning an error) and an unreadable cache-directory listing fail open, with a log line saying why. A refusal acquires nothing and writes nothing: no holder, no drain record, no cache directory. `Committed` is the plain sum of `CacheFootprint` over every drain record in `CacheDir`, with no exclusion by name (`discardCache` keeps `<pv>.drain.json`, and a fresh volume has no record yet); a record is written before every server start, so the records cover every staged volume and every cache awaiting a drain, and the intent store is not read (departure 8, section 6). Records are keyed by volume name, one per volume, and a clean release removes the record, so a restaged volume is never counted twice. A record `Orphans` would skip is skipped and warned about once. An unreadable cache-directory listing, like a statfs error, fails open with a log line.

- [ ] **Step 1: Write the failing tests** in `budget_test.go` with the existing `newEnv`/`makeCache`/`seedRunning` helpers:
  `TestFreshStageUnderBudgetProceeds`; `TestFreshStageOverBudgetRefused` (error `errors.Is` `item.ErrCacheBudgetExceeded`, the text names the budget, the committed bytes with their volume count and the need, no server started, no unit, no cache directory, no holder (`holderInfo().Present == false`) AND no drain record - review focus 6);
  never refused, each with a budget of 1 byte: `TestAdoptNeverRefused` (unit active and device present), `TestServerRestartNeverRefused` (device present, unit gone), `TestRebootWithSurvivingCacheNeverRefused` (index present, no device, `reuse` true), `TestDrainNeverRefused` (the orphan drain path), `TestTeardownNeverRefused`;
  `TestExpandedVolumeIsCheckedAtItsRaisedSize` (the intent says 1Gi, the size record or manifest says 3Gi: the need is the 3Gi footprint and the refusal text says so); `TestStoreReadErrorIsNotARefusal` (a failing size-record or manifest read returns that error, not `ErrCacheBudgetExceeded`; statfs is covered by `TestBudgetErrorFailsOpenWithLog`); `TestCommittedListingErrorFailsOpen` (an unreadable `CacheDir` logs and admits); `TestCommittedKeepsRecordOfDiscardedCache` (a kept `<pv>.drain.json` still counts);
  `TestRefusedThenAdmittedAfterUnstage` (two volumes fill the budget, a third is refused with `ErrCacheBudgetExceeded`; one is torn down; the third's next `Converge` succeeds);
  `TestCommittedCountsDrainRecordsOfUnstagedCaches` (a record with no intent counts);
  `TestCommittedSkipsUnusableRecordWithWarning`;
  `TestCommittedSaturates` (records whose footprints overflow give `MaxFootprint`, and the check refuses);
  `TestBudgetZeroRefusesEveryFreshStage`; `TestAccountingOffSkipsCheck` (`CacheBudget` nil, and returning `enabled=false`); `TestBudgetErrorFailsOpenWithLog` (a statfs error does not refuse: the check is defence in depth, and the log says why).
  In `reconcile`: `TestConditionOfCacheBudgetExceeded` (condition string, class `Unavailable`, gRPC `Unavailable`, listed in `Conditions`); in `kube`: the event reason is a Warning with fixed text and carries no error string.
- [ ] **Step 2: Run to verify failure**: `go test ./internal/item/volume ./internal/reconcile ./internal/kube -run 'Budget|Committed|Fresh|NeverRefused|Expanded|StoreRead|Refused|AccountingOff' -v`; expect FAIL.
- [ ] **Step 3: Implement** `Committed` and `checkBudget(ctx, v) error` in `budget.go`; call it from `Converge` as above and, on refusal, only raise the event through `it.event` (nothing was acquired or written, so nothing is undone). The error is `fmt.Errorf("volume item %s: %w: the node's cache budget is %s, %s is committed to %d volumes, and this volume needs %s", ...)` wrapping `item.ErrCacheBudgetExceeded`, sizes printed as `resource.Quantity` strings. Map the sentinel in `conditionOf` and `ClassUnavailable`.
- [ ] **Step 4: Run to verify pass**: `go test ./internal/item/... ./internal/reconcile ./internal/kube ./cmd/miami -race`; expect PASS.
- [ ] **Step 5: Commit** (`Refuse a fresh stage that would exceed the node's cache budget`).

### Task 5: The webhook's mutation (security-sensitive)

**Files:**
- Create: `internal/webhook/{mutate.go,resolve.go,doc.go}`, `internal/webhook/mutate_test.go` (unit, fake `Claims`), `internal/webhook/mutate_envtest_test.go` (tag `envtest`, real API server), `internal/kube/claims.go`, `internal/kube/claims_test.go`
- Modify: `internal/kube/kube.go` (`PV.CapacityBytes`, decoded from `spec.capacity.storage`), `internal/metrics/metrics.go` (`WithWebhook()`: `miami_webhook_mutations_total{outcome}`, `miami_webhook_unresolved_total`; outcomes `mutated`, `unchanged`, `stripped`, `error`), `Makefile` (`envtest` runs `./internal/webhook` too)

**Interfaces:**
- Consumes: `volume.CacheFootprint`, `AddFootprint`, `CacheResource` (T1); `volume.NewContext`, `Context.WithSize`, `volume.DecodeContext`, `chunk.RoundUp` (existing).
- Produces: `webhook.Claims`, `webhook.Metrics` (section 3; the registry from `WithWebhook()` implements it, and tests use a fake), `webhook.Mutator`, `kube.PVC{Namespace, Name, StorageClassName string, StorageClassSet bool, VolumeName string, RequestBytes int64, AccessModes []string, Phase string, Terminating bool}`, `kube.StorageClass{Name, Provisioner string, Parameters map[string]string}`, `kube.PV.CapacityBytes` (defined only in `internal/kube/kube.go`; the webhook reads it and declares no PV type), and `*kube.Client` methods satisfying `Claims`.

Per volume (design 4.4): `persistentVolumeClaim` bound (PVC `volumeName` set, phase `Bound`): GET the PV; a PV of this driver with `miami/kind: volume` gives `Context` through `DecodeContext(pv.VolumeAttributes)` with its size raised to `chunk.RoundUp(pv.CapacityBytes, chunk)` when larger. Unbound: GET its class (the named one, else `DefaultStorageClass`); provisioner `miami.csi.greghaskins.com` and `NewContext(class.Parameters)` of kind `volume` then `WithSize(pvc.RequestBytes)`. `ephemeral.volumeClaimTemplate` is resolved like an unbound PVC from the template (request and class name), in the pod's namespace. Everything else, a many-node access mode, a `Terminating` or `Lost` claim: nothing. A claim that does not exist: nothing, `miami_webhook_unresolved_total` +1, one log line. Any other lookup error: the pod is allowed unchanged, outcome `error`. The sum is `AddFootprint`; it is written as `requests` and `limits` on the first regular container, removed from every other container (init, regular and ephemeral), and a pod with no volume of this kind has only a stray request removed. The response is a JSON patch (`patchType: JSONPatch`).

- [ ] **Step 1: Write the failing unit tests** in `mutate_test.go`, asserting on the patched pod (apply the response patch with `jsonpatch`-style application to the request's pod, then compare):
  `TestBoundPVCRequestsPVFootprint` (a 10Gi PV with context defaults: `15376Mi` on container 0, requests and limits equal);
  `TestUnboundWFFCUsesClassAndRequest` (10Gi request, class `cacheSize: 8Gi`: footprint equals `CacheFootprint` of `NewContext(...).WithSize(10Gi)`, not a hand-typed number);
  `TestDefaultClassWhenUnset` (no `storageClassName`, `DefaultStorageClass` returns a miami class);
  `TestEphemeralTemplate`; `TestSumsTwoVolumes`;
  `TestExpandedPVUsesCapacity` (PV capacity larger than the context size raises the footprint);
  `TestMissingPVCIsUnaccounted` (allowed, no patch, `unresolved_total` 1);
  `TestLookupErrorIsAllowedUnchanged` (outcome `error`);
  review focus 1, each allowed unchanged with outcome `unchanged`: a PVC of another driver, a PV of kind `directory`, a class of another provisioner, a `Terminating` PVC, a `ReadWriteMany` claim, `TestLostPVCIsUnchanged` (a `Lost` claim); except that a claim of our own class whose parameters `NewContext` rejects is allowed unchanged and counted `unresolved` (`unresolved_total` +1, never an admission error; also an envtest case with a real class);
  review focus 2: `TestStrayRequestReplaced` (container 0 has `requests: 1Gi, limits: 2Gi` by hand: both become the computed value), `TestStrayRequestRemovedFromOtherContainers` (set on container 1 and on an init container: removed there, one computed value on container 0), `TestReorderedContainersLandOnFirst` (the same pod with containers swapped yields the number on the new first one only), `TestReinvocationIsIdempotent` (mutate the mutated pod: an empty patch), `TestPodWithoutVolumesStripsStrayRequest` (outcome `stripped`), `TestNoRegularContainer` (init containers only: allowed unchanged);
  review focus 4: `TestFootprintOverflowSaturates` (PVs near `1<<62`: the request is `MaxFootprint`, a valid `resource.Quantity`);
  `TestPatchPreservesOtherResources` (cpu and memory requests on container 0 survive).
  Client tests in `claims_test.go`: PVC, PV capacity, StorageClass and default-class decoding (`storageclass.kubernetes.io/is-default-class: "true"`; none gives `nil, nil`; two defaults pick the newest `creationTimestamp`, as the API server's admission does), and `ErrNotFound` only for the API server's own NotFound.
- [ ] **Step 2: Write the failing envtest cases** in `mutate_envtest_test.go` (real API server, real PVC/PV/StorageClass objects created through the typed client, the `Mutator` called with the `AdmissionRequest` built from real pods): bound claim, unbound WFFC claim with an explicit class, unbound claim on the default class, ephemeral template, missing PVC, container stripping, re-invocation on the patched pod. Each asserts the same numbers as the unit table.
- [ ] **Step 3: Run to verify failure**: `go test ./internal/webhook ./internal/kube -run 'Claims|Mutat|PVC|Ephemeral|Stray|Reinvocation' -v` and `make envtest`; expect FAIL.
- [ ] **Step 4: Implement** the `kube` reads in `claims.go` (dynamic client, same error discipline as `GetPV`), `Mutator.Mutate` and the volume resolver. Build the patch from the decoded pod's container indexes, one `add`/`replace`/`remove` per touched resource key so unrelated `resources` are untouched.
- [ ] **Step 5: Run to verify pass**: `go test ./internal/webhook ./internal/kube -race` and `make envtest`; expect PASS.
- [ ] **Step 6: Commit** (`Add the pod mutation that requests a pod's cache footprint`).

### Task 6: Webhook serving, self-signed TLS, `caBundle` healing and controller wiring (security-sensitive)

**Files:**
- Create: `internal/webhook/{server.go,tls.go,cabundle.go}`, `internal/webhook/{server_test.go,tls_test.go,cabundle_test.go}`
- Modify: `cmd/miami/controller.go`, `cmd/miami/controller_test.go`, `internal/kube/` (a `Secrets` and `WebhookConfigs` seam: `GetSecret`, `CreateSecret`, `UpdateSecret` in the driver namespace; `GetMutatingWebhookConfiguration`, `PatchCABundle`), `internal/metrics/metrics.go` (`miami_webhook_config_ok` gauge)

**Interfaces:**
- Consumes: `webhook.Mutator` (T5).
- Produces: controller flags `-webhook-addr` (default `:9814`; empty turns the webhook off), `-webhook-tls-secret` (empty means self-signed into `miami-webhook-tls`; a name means read-only, someone else's), `-webhook-config` (default `miami-cache`, the `MutatingWebhookConfiguration` whose `caBundle` is asserted), `-webhook-service` (default `miami-webhook`, for the certificate's names), `-webhook-sync-interval` (default `30s`), and the namespace from `POD_NAMESPACE`; `webhook.Server` with `Run(ctx)`.

The design says `:9812` for the listener; that port is the node's health port and both pods are on the host network, so the default is `:9814` (see the ambiguity list). The self-signed material: ECDSA P-256, a CA valid 10 years and a serving certificate valid 1 year for `<service>.<ns>.svc` and `<service>.<ns>.svc.cluster.local`; regenerated when absent, unparseable, signed by another CA, naming other SANs or within 30 days of expiry; the CA is kept across serving-certificate renewals so a `caBundle` stays valid. Each sync pass reloads the serving certificate for `tls.Config.GetCertificate` and asserts the `caBundle` of every entry in the configuration `webhooks[]`. With `-webhook-tls-secret` set, the controller never writes a Secret or the configuration and serves from that Secret once it exists (retrying each pass). It reads the Secret's data keys `tls.crt`, `tls.key` and `ca.crt` (the keys cert-manager writes), and a Secret missing any of them is not served from.

- [ ] **Step 1: Write the failing tests.**
  `server_test.go`: `TestServerRejectsNonPostAndOversizedBody` (405; a body above 1 MiB is rejected, not read to the end), `TestServerRoundTripsAdmissionReview` (a `v1` review in, a `v1` review out with the request's `uid`), `TestServerRejectsOtherAdmissionVersions`, `TestServerServesOnlyMutatePath`, `TestServerTLSMinVersion12`.
  `tls_test.go` (review focus 5): `TestBootstrapCreatesSecretWhenAbsent`, `TestBootstrapReusesValidSecret` (no write), `TestBootstrapRegeneratesExpired`, `TestBootstrapRegeneratesNearExpiry` (29 days), `TestBootstrapRegeneratesGarbage`, `TestBootstrapKeepsCAWhenRenewingServingCert`, `TestSANsAreTheServiceNames`, `TestManagedSecretIsNeverWritten` (a fake that fails on any create/update; the managed path only reads), `TestUpdateConflictRetriesThenSucceeds`.
  `cabundle_test.go`: `TestAssertsBundleOnEveryWebhookEntry`, `TestWipedBundleIsRestoredWithinOnePass`, `TestUnchangedBundleIsNotPatched`, `TestConfigOKGauge` (0 while the bundle is empty or differs, 1 after), `TestMissingConfigurationLogsAndCountsNotOK`, `TestPatchTouchesOnlyCABundle` (the patch body names `clientConfig.caBundle` fields only).
  `controller_test.go`: `TestControllerFlagsWebhook` (defaults; `-webhook-addr=` empty disables the webhook and touches no Secret; bad address exits 2), `TestControllerServesWebhookAndCSI` (both listeners up, both stop on cancel).
- [ ] **Step 2: Run to verify failure**: `go test ./internal/webhook ./cmd/miami -run 'Server|Bootstrap|Bundle|ConfigOK|Webhook' -v`; expect FAIL.
- [ ] **Step 3: Implement** `Server`, the TLS bootstrap and the `caBundle` assertion; wire them into `serveControllerWith` beside the CSI server and the metrics server, stopping in the reverse order. The webhook's failure to start (a port in use) is fatal like the metrics server's; a transient API error in a sync pass is logged and the pass retried, never fatal. The `caBundle` patch is a JSON patch with one `replace` per `webhooks[i]/clientConfig/caBundle`, nothing else.
- [ ] **Step 4: Run to verify pass**: `go test ./internal/webhook ./internal/kube ./internal/metrics ./cmd/miami -race`; expect PASS.
- [ ] **Step 5: Commit** (`Serve the cache webhook with self-signed TLS and a self-healing caBundle`).

### Task 7: Webhook manifests, controller RBAC and the cert-manager component (security-sensitive)

**Files:**
- Create: `deploy/kustomize/base/{mutatingwebhookconfiguration-cache.yaml,service-webhook.yaml}`, `deploy/kustomize/components/cert-manager/{kustomization.yaml,certificate.yaml}`
- Modify: `deploy/kustomize/base/{clusterrole-controller.yaml,role-controller.yaml,deployment-controller.yaml,kustomization.yaml}`, `deploy/policy_test.go` (`TestRBACRuleSets`, `TestControllerRBACExact`, the object-kind check at line ~1124 (which gains `MutatingWebhookConfiguration` beside T3's admission-policy kinds), `TestControllerLeaseRole`, `TestControllerDeployment`, `TestPortsDistinct`, `TestBaseGolden`, a new `TestWebhookConfiguration`, `TestCertManagerComponent`), `deploy/testdata/base.golden.yaml`

**Interfaces:**
- Consumes: the controller flags of T6; T3's pattern of exact-rule tests.
- Produces: the rendered base the e2e task applies.

- [ ] **Step 1: Write the failing deploy tests.**
  `TestWebhookConfiguration`: exactly one `MutatingWebhookConfiguration` with one webhook; `failurePolicy == Ignore`, `sideEffects == None`, `matchPolicy == Equivalent`, `reinvocationPolicy == IfNeeded`, `timeoutSeconds == 10`, `admissionReviewVersions == [v1]`; rules are `CREATE` on `pods` only; the `namespaceSelector` is a `NotIn` of `kube-system` and `miami-system` (by `kubernetes.io/metadata.name`) and a `NotIn [disabled]` on `miami.csi.greghaskins.com/cache-accounting`; `clientConfig.service` is `miami-webhook` in `miami-system`, path `/mutate`, and `caBundle` is empty in the base.
  `TestControllerRBACExact`, as an exact set, so a widening fails: the ClusterRole keeps its rules (PVC `get/list/watch/update`, StorageClass `get/list/watch`, PV `get/list/watch/create/patch/delete` already cover the webhook's reads; assert the webhook needs no new read) and gains `admissionregistration.k8s.io/mutatingwebhookconfigurations` `get`, `patch` with `resourceNames: [miami-cache]` and nothing broader; the controller Role in `miami-system` gains `secrets` `get`, `update` with `resourceNames: [miami-webhook-tls]` and `secrets` `create` without `resourceNames` (a `create` rule cannot be limited by name, so it is the one rule that is namespace-wide; this is in the review checklist); no `secrets` verb on any ClusterRole; the node ClusterRole is unchanged from T3.
  `TestControllerDeployment`: the miami container args carry `--webhook-addr=:9814` and `--webhook-config=miami-cache`, a `webhook` containerPort 9814, `POD_NAMESPACE` is available to it.
  `TestPortsDistinct` updated: the controller declares metrics 9811, health 9813 and webhook 9814 (three ports, no overlap with the node's 9810 and 9812).
  `TestCertManagerComponent`: rendering `components/cert-manager` over the base removes the controller's webhook-configuration RBAC and the Secret `create` and `update` rules, but keeps `get` on the named managed TLS Secret `miami-webhook-tls-managed` (`resourceNames`, nothing broader), because the managed Secret must be readable (data keys `tls.crt`, `tls.key`, `ca.crt`); the `Certificate`'s `secretName` is that same name; adds `--webhook-tls-secret=miami-webhook-tls-managed` to the controller args, adds the `cert-manager.io/inject-ca-from` annotation on the configuration, and a `Certificate` whose `dnsNames` are the Service's two names.
  `TestBaseGolden` fails until regenerated.
- [ ] **Step 2: Run to verify failure**: `go test ./deploy -v`; expect FAIL.
- [ ] **Step 3: Write the manifests.** Service `miami-webhook` (443 to port 9814, selector the controller's two labels); the webhook configuration; the RBAC rules above with header comments in the style of the existing files; controller args and port; the component as a kustomize `Component` (kind `Component`, not a base dependency). Regenerate the golden with `go test ./deploy -update` and read the diff.
- [ ] **Step 4: Run to verify pass**: `go test ./deploy -race`, `make deploy-check`; expect PASS.
- [ ] **Step 5: Commit** (`Ship the cache webhook, its Service and the controller's narrow rights`).

### Task 8: End to end on kind

**Files:**
- Create: `test/e2e/cache_helpers_test.go`, `test/e2e/cache_capacity_test.go`, `test/e2e/cache_policy_test.go`, `test/e2e/cache_stage_test.go`
- Modify: `hack/e2e-kind.sh` (the `volume_tests` list and the pass check), `deploy/kustomize/overlays/e2e/kustomization.yaml` (node arg `--cache-budget=4Gi` and a short `--reconcile-interval=5s`), `deploy/policy_test.go` (`TestE2EOverlay` expects the two args)

**Interfaces:**
- Consumes: the whole stack; the harness helpers `newVolumeNamespace`, `createVolumePVC`, `createPod`, `waitPodReady`, `pluginOff`, `rebootNode`, `restartController` (existing).
- Produces: the eight tests below, all nfs3-only like the other volume tests (skipped with a reason for local).

The overlay budget is 4Gi on both workers; a 1Gi volume with defaults has the footprint `1552Mi` (1Gi + 512Mi + 16Mi), so two fit (3104Mi) and three do not. Namespaces for the stage-check tests are labelled `miami.csi.greghaskins.com/cache-accounting: disabled` so the webhook is bypassed and the node check is what refuses.

- [ ] **Step 1: Write the failing tests.**
  `TestCacheCapacityAdvertised`: each worker's `status.capacity[miami.csi.greghaskins.com/cache]` is `4Gi`; the node plugin's `/metrics` `miami_cache_budget_bytes` equals it; the control plane (no plugin) has none.
  `TestCacheWebhookRequestsFootprint`: a pod on a bound 1Gi claim, one on an unbound `WaitForFirstConsumer` claim and one with an ephemeral template each carry `requests == limits == 1552Mi` on container 0 and no such key on a sidecar container; a pod with two such claims carries `3104Mi`.
  `TestCacheSchedulerHoldsTheLine`: four pods with one 1Gi volume each fill the two workers (two per 4Gi budget); a fifth stays Pending with `FailedScheduling` mentioning `Insufficient miami.csi.greghaskins.com/cache`; delete one pod and the fifth is scheduled and runs.
  `TestCacheStageCheckRefusesAndRecovers`: in the labelled namespace, three pods pinned to one worker by `nodeName` (the scheduler is bypassed): two run, the third stays `ContainerCreating` with the event `CacheBudgetExceeded` and the node condition metric `cache-budget-exceeded` at 1; its holder file is not held by that node (read through the NFS container, as `TestVolumeTakeover` does); deleting one of the two lets the third run.
  `TestCacheStageCheckNeverRefusesExisting`: at full budget (two volumes staged), the node plugin is deleted and restarted (`pluginOff`/`pluginOn`) and a staged volume's pod is deleted and recreated on the same node with the surviving cache: all proceed, no `CacheBudgetExceeded` event (adopt, restart, reboot with cache, as far as kind can show; the item tests carry the rest).
  `TestCacheWebhookFailsOpen`: scale the controller to zero (`restartController` style helper), create a pod with a volume claim already bound and assert admission first: the pod exists with no request, before the controller returns; then restore the controller and assert, separately, the stage outcome the budget dictates; `apiserver_admission_webhook_fail_open_count` is not asserted (not scraped in kind).
  `TestCachePolicyNegative` (the security test): obtain a token for `miami-node` bound to the node plugin pod on `kind-worker` (`kubectl create token miami-node -n miami-system --bound-object-kind Pod --bound-object-name <pod>` through the harness's client), build a client from it, and assert: own node, own key, `4Gi` to `3Gi` allowed (and restored by the plugin within two passes); the other worker's capacity key refused with the policy's message; `example.com/other` capacity on its own node refused; a label added through `nodes/status` refused; a token bound to the pod of the other worker used against `kind-worker` refused; an unbound `miami-node` token (no `--bound-object-kind`) refused.
  `TestCacheRepatchAfterRemoval`: remove the key from a worker by hand with an admin `remove` patch on `nodes/status`, and the key is back at `4Gi` within three reconcile intervals (the overlay's 5 s); then repeat with a stale lower value written by hand.
- [ ] **Step 2: Run to verify failure**: `go vet -tags e2e ./test/e2e` and `go test -c -tags e2e ./test/e2e`, then `E2E_RUN='TestCache' hack/e2e-kind.sh` with `E2E_TRANSPORT=nfs3`; expect the build to fail first (helpers absent) and then the tests to FAIL against the old image.
- [ ] **Step 3: Implement** the helpers and tests, add the eight names to `volume_tests` in `hack/e2e-kind.sh` (the listing check and the pass check), set the overlay's node args. Keep the pods tiny (`stay` pods of the harness) and the volumes 1Gi.
- [ ] **Step 4: Run to verify pass**: `E2E_TRANSPORT=nfs3 E2E_IMAGE=<ref> hack/e2e-kind.sh` end to end; expect `e2e: all N criteria passed for nfs3` with N raised by 8 and every M1 and M2 test still passing.
- [ ] **Step 5: Commit** (`Test cache capacity end to end on kind, including the policy's refusals`).

**Accept when** the full nfs3 leg is green on both architectures in CI and `TestCachePolicyNegative` shows each refusal by name in the log.

### Task 9: Alerts, recording rule, runbook and README

**Files:**
- Modify: `docs/operating-volumes.md` (host ports: list 9814 beside 9810 to 9813; Monitoring, "A stage that refuses", Upgrades/rollout), `README.md` (one paragraph under the volume kind, and the reserved host ports list gains 9814), `docs/design/2026-11-07-cache-capacity-scheduling-design.md` (Status line: implemented, with this plan linked; and departures 1, 2, 3 and 5 of this plan's section 6 written into the design as amendments), this plan's Status

**Interfaces:**
- Consumes: the metric and condition names of T2 (node gauges), T4 (condition, event), T5 (`miami_webhook_mutations_total{outcome}`, `miami_webhook_unresolved_total`) and T6 (`miami_webhook_config_ok`).
- Produces: the operator text of design 4.6, 4.8 and 4.10.

- [ ] **Step 1: Write the content.** Monitoring: the four node gauges and three webhook metrics with one line each; the four alerts as PromQL examples (`miami_cache_committed_bytes > miami_cache_budget_bytes`; `miami_cache_fs_free_bytes < <reserve>`; the unaccounted recording rule `miami_cache_committed_bytes - on(node) sum by (node) (kube_pod_container_resource_requests{resource="miami_csi_greghaskins_com_cache"})` with the metric-name note for how kube-state-metrics sanitises the resource name, non-zero for longer than a rollout; `rate(apiserver_admission_webhook_fail_open_count{name=~".*miami.*"}[5m]) > 0`); the owner's absolute-budget example uses placeholder numbers only. Runbook: `cache-budget-exceeded` (what it means, that kubelet retries, how to find the committed volumes, recreate old pods for rollout step 3, how to raise `-cache-budget`), the GitOps `ignoreDifferences` entry for `webhooks[].clientConfig.caBundle`, the cert-manager component, `-cache-accounting=false` and the namespace opt-out label, a line that applying the new base with volumes already staged refuses nothing for them (only fresh stages are checked), and the sharing-RWO-claims note (counted twice by the scheduler, once by the node).
- [ ] **Step 1b: Amend the design** so it stays truthful: in `docs/design/2026-11-07-cache-capacity-scheduling-design.md`, 4.4 gives the webhook listener `:9814` (not `:9812`, the node's health port; both pods are host-network); 4.10 says `-cache-budget 0` advertises `0` and refuses every fresh volume, and `-cache-accounting=false` is the off switch; 4.3 and 4.9 say the admission policy also pins `metadata.labels`, `annotations`, `finalizers` and `ownerReferences` (the status endpoint does not reset metadata); 4.8 says `miami_cache_committed_bytes` is the stage check's own sum over all drain records. Each is marked as an amendment with this plan's date.
- [ ] **Step 2: Verify** every metric, flag, condition and event name in the text against the code: `grep -rn` each in `internal/ cmd/ deploy/`; every name found, none invented.
- [ ] **Step 3: Run** `make check`; expect green.
- [ ] **Step 4: Commit** (`Document cache accounting: alerts, the unaccounted-pods rule and the runbook`).

---

## 5. Design coverage

| Design | Task |
|---|---|
| 4.1 footprint, one formula | T1 (consumed by T4, T5) |
| 4.2 the resource | T1 (const), T5 |
| 4.3 budget, advertisement, flags, accounting switch | T2, T3 |
| 4.3 / 4.9 admission policy and node RBAC | T3 |
| 4.4 webhook behaviour, reaching it, failure policy | T5, T6, T7 |
| 4.4 TLS, `caBundle`, cert-manager | T6, T7 |
| 4.5 shared RWO (conservative) | T5 (each pod counts each claim), T9 (documented) |
| 4.6 pre-rollout pods, recording rule | T4 (check protects), T9 (rule) |
| 4.7 RWX out of scope | T5 (many-node claim: nothing) |
| 4.8 stage check, metrics, alerts | T4, T2 (gauges), T6 (webhook metrics), T9 (alerts) |
| 4.9 privilege table | T3 (node), T7 (controller) |
| 4.10 rollout order | T9 |
| 6 owner's answers 1 to 6 | T7 (`Ignore`), T3 (own-node patch), T2 (defaults), T9 (RWO, pre-rollout), T6/T7 (TLS) |
| Requirements file: negative cases on kind | T8 |

## 6. Where the plan departs from, or settles, the design

1. The webhook's default port is `:9814`, not `:9812`: `9812` is the node DaemonSet's health port and both pods are host-network, so `TestPortsDistinct` would fail (T6, T7).
2. 4.10 says `-cache-budget 0` "advertises nothing and disables the check"; 4.3 says an explicit zero advertises `0` and refuses every volume, and `-cache-accounting=false` is the off switch. The plan follows 4.3, the later and more specific text (T2).
3. The admission policy pins more than "one capacity key": the status endpoint does not reset `metadata`, so labels and annotations are pinned too (T3).
4. Controller RBAC: the PVC and StorageClass reads the design lists as new already exist in `clusterrole-controller.yaml` (the provisioner's), so only the webhook-configuration patch and the Secret rules are new. `secrets create` cannot carry `resourceNames`; it is the one namespace-wide rule, in the driver's namespace only (T7).
5. `miami_cache_committed_bytes` is the stage check's own sum (all drain records), not "staged volumes only": an alert on committed over budget and the check must agree (T4).
6. The advertisement runs on its own ticker at `-reconcile-interval`, not inside the reconciler's pass, so a slow API server cannot stall volume work (T2).
7. Stage-check placement (reviewer-approved amendment): design 4.8's "immediately before `ensureServer`" becomes "at the top of `Converge`, before the holder layer", so a refusal acquires and writes nothing; the design is amended in this PR.
8. Spec 4.8 sums intent records plus drain records; `Committed` sums drain records only. A drain record is written before every server start, so it covers every staged volume and every cache awaiting a drain, and the intent store need not be read (architect-approved in plan review; T4).
9. Dashboard panels (design 7): deployment-side PR, shipped with the migration spec, not in this driver plan.
