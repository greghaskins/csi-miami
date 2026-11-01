#!/usr/bin/env bash
# The deploy check: applies the e2e overlay on a kind cluster of one control
# plane and two workers and waits for the driver to be up.
#
#   hack/kind.sh --image <ref>
#
# <ref> is a driver image already in the local Docker daemon: `make image`
# builds one, CI pulls the content-addressed per-arch tag. It is loaded into
# the kind nodes and never pulled by them.
#
# It passes when: the controller Deployment is available; the node DaemonSet's
# desired count equals its ready count, which is the number of nodes (it
# tolerates every taint); every node's CSINode lists the driver; and each pod's
# /healthz and /metrics, fetched through the API server's pod proxy, answer
# with miami_ metrics present. Nothing here waits for a fixed time: waits are
# `kubectl wait`, or a loop with a deadline and a one-second interval for the
# leases and the metrics, which appear only after the first calls.
#
# The cluster is deleted at the end. On failure, first, DEPLOY_ARTIFACT_DIR
# (if set) receives `kind export logs`, both pods' logs, `kubectl describe` of
# the pods and each node's /proc/1/mountinfo. KEEP_CLUSTER=1 keeps the cluster.
#
# Needs docker, kind and kubectl (pinned in mise.toml), curl-free. The cluster
# steps are in hack/lib/kind.sh, which hack/e2e-kind.sh shares.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
# shellcheck source=hack/lib/kind.sh
source hack/lib/kind.sh

driver="$kind_driver"
ns="$kind_ns"
cluster="${KIND_CLUSTER_NAME:-miami-deploy-check}"
timeout="${DEPLOY_TIMEOUT:-300s}"
metrics_deadline="${DEPLOY_METRICS_DEADLINE:-120}"

image=""
while [ $# -gt 0 ]; do
  case "$1" in
    --image) image="${2:?--image needs a value}"; shift 2 ;;
    *) echo "kind: unknown argument: $1" >&2; exit 2 ;;
  esac
done
if [ -z "$image" ]; then
  echo "usage: hack/kind.sh --image <ref>" >&2
  exit 2
fi
kind_require_tools || exit 1
kind_init "$cluster" "$timeout"

cleanup() {
  status=$?
  trap - EXIT
  if [ "$status" -ne 0 ] && [ -n "${DEPLOY_ARTIFACT_DIR:-}" ]; then
    echo "kind: failed; collecting logs into $DEPLOY_ARTIFACT_DIR" >&2
    kind_collect "$DEPLOY_ARTIFACT_DIR" || true
  fi
  kind_delete
  exit "$status"
}
trap cleanup EXIT

kind_create
kind_apply "$image"
kind_wait

# The sidecars' leader election takes a Lease each, which shows that the
# controller's RBAC covers them: the provisioner's is named for the driver, the
# attacher's has a prefix.
lease_name="${driver//./-}"
for lease in "$lease_name" "external-attacher-leader-$lease_name"; do
  end=$((SECONDS + metrics_deadline))
  until kubectl -n "$ns" get lease "$lease" >/dev/null 2>&1; do
    if [ "$SECONDS" -gt "$end" ]; then
      echo "kind: no lease $lease within ${metrics_deadline}s" >&2
      exit 1
    fi
    sleep 1
  done
  echo "kind: leader election lease $lease exists"
done

# scrape <component> <port>: /healthz answers, and /metrics has miami_ series.
scrape() {
  local component="$1" port="$2" pod base body
  for pod in $(kubectl -n "$ns" get pods -l "app.kubernetes.io/component=$component" -o jsonpath='{.items[*].metadata.name}'); do
    base="/api/v1/namespaces/${ns}/pods/${pod}:${port}/proxy"
    kubectl get --raw "${base}/healthz" >/dev/null
    echo "kind: $pod /healthz answered 200"
    local end=$((SECONDS + metrics_deadline))
    until body="$(kubectl get --raw "${base}/metrics")" && grep -q '^miami_' <<<"$body"; do
      if [ "$SECONDS" -gt "$end" ]; then
        echo "kind: $pod /metrics has no miami_ series after ${metrics_deadline}s" >&2
        exit 1
      fi
      sleep 1
    done
    echo "kind: $pod /metrics answered 200 with $(grep -c '^miami_' <<<"$body") miami_ series, for example:"
    # awk reads all of its input: `grep | head -3` can die of SIGPIPE when head
    # leaves early, which pipefail turns into a failed check.
    awk '/^miami_/ && n++ < 3 { print "kind:   " $0 }' <<<"$body"
  done
}
scrape controller 9811
scrape node 9810

echo "kind: deploy check passed"
