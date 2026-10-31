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
# with miami_ metrics present. Nothing here sleeps for a fixed time: waits are
# `kubectl wait`, or one loop with a deadline for the metrics, which appear
# only after the first calls.
#
# The cluster is deleted at the end. On failure, first, DEPLOY_ARTIFACT_DIR
# (if set) receives `kind export logs`, both pods' logs, `kubectl describe` of
# the pods and each node's /proc/1/mountinfo. KEEP_CLUSTER=1 keeps the cluster.
#
# Needs docker, kind and kubectl (pinned in mise.toml), curl-free.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# kindest/node for Kubernetes 1.37.0, the default of the pinned kind (0.33.0),
# by the digest of its multi-arch index.
node_image="kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5"
driver="miami.csi.greghaskins.com"
ns="miami-system"
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
for tool in docker kind kubectl; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "kind: $tool not found" >&2
    exit 1
  fi
done

work="$(mktemp -d)"
export KUBECONFIG="$work/kubeconfig"

collect() {
  local art="$1" node pod
  mkdir -p "$art"
  kind export logs --name "$cluster" "$art/kind-logs" >/dev/null 2>&1 || true
  kubectl -n "$ns" describe pods >"$art/describe-pods.txt" 2>&1 || true
  for pod in $(kubectl -n "$ns" get pods -o name 2>/dev/null); do
    kubectl -n "$ns" logs "$pod" --all-containers --prefix --tail=-1 \
      >"$art/logs-${pod#pod/}.txt" 2>&1 || true
  done
  for node in $(kind get nodes --name "$cluster" 2>/dev/null); do
    docker exec "$node" cat /proc/1/mountinfo >"$art/mountinfo-${node}.txt" 2>&1 || true
  done
}

cleanup() {
  status=$?
  trap - EXIT
  if [ "$status" -ne 0 ] && [ -n "${DEPLOY_ARTIFACT_DIR:-}" ]; then
    echo "kind: failed; collecting logs into $DEPLOY_ARTIFACT_DIR" >&2
    collect "$DEPLOY_ARTIFACT_DIR" || true
  fi
  if [ "${KEEP_CLUSTER:-}" != "1" ]; then
    kind delete cluster --name "$cluster" >/dev/null 2>&1 || true
  fi
  rm -rf "$work"
  exit "$status"
}
trap cleanup EXIT

# Start from nothing: a cluster left over from an earlier run is not evidence.
kind delete cluster --name "$cluster" >/dev/null 2>&1 || true

cat >"$work/kind.yaml" <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    image: ${node_image}
  - role: worker
    image: ${node_image}
  - role: worker
    image: ${node_image}
YAML

echo "kind: creating cluster $cluster"
kind create cluster --name "$cluster" --config "$work/kind.yaml" --wait "$timeout"
nodes="$(kubectl get nodes -o name | wc -l)"
echo "kind: $nodes nodes"

echo "kind: loading $image"
kind load docker-image --name "$cluster" "$image"

mkdir "$work/overlay"
hack/e2e-overlay.sh --image "$image" --out "$work/overlay"
kubectl apply -k "$work/overlay"

kubectl -n "$ns" wait --for=condition=Available --timeout="$timeout" deployment/miami-controller
kubectl -n "$ns" wait --for=jsonpath='{.status.desiredNumberScheduled}'="$nodes" --timeout="$timeout" daemonset/miami-node
kubectl -n "$ns" wait --for=jsonpath='{.status.numberReady}'="$nodes" --timeout="$timeout" daemonset/miami-node
desired="$(kubectl -n "$ns" get daemonset miami-node -o jsonpath='{.status.desiredNumberScheduled}')"
ready="$(kubectl -n "$ns" get daemonset miami-node -o jsonpath='{.status.numberReady}')"
echo "kind: DaemonSet miami-node desired=$desired ready=$ready; Deployment miami-controller available"
test "$desired" = "$ready"

for node in $(kubectl get nodes -o name); do
  kubectl wait --for=jsonpath='{.spec.drivers[*].name}'="$driver" --timeout="$timeout" "csinode/${node#node/}"
  echo "kind: CSINode ${node#node/} lists $driver"
done

# The sidecars' leader election takes a Lease each: proof that the controller's
# RBAC covers it.
for sidecar in provisioner attacher; do
  end=$((SECONDS + metrics_deadline))
  until kubectl -n "$ns" get lease -o name | grep -q "$sidecar"; do
    if [ "$SECONDS" -gt "$end" ]; then
      echo "kind: no $sidecar lease within ${metrics_deadline}s" >&2
      exit 1
    fi
  done
  echo "kind: a $sidecar leader election lease exists"
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
    done
    echo "kind: $pod /metrics answered 200 with $(grep -c '^miami_' <<<"$body") miami_ series, for example:"
    grep '^miami_' <<<"$body" | head -3 | sed 's/^/kind:   /'
  done
}
scrape controller 9811
scrape node 9810

echo "kind: deploy check passed"
