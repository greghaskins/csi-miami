# Shared by hack/kind.sh (the deploy check) and hack/e2e-kind.sh (the kind
# end-to-end tests): how the cluster is made, the driver put on it, waited for,
# inspected on failure and deleted. Source it; it does nothing until its
# functions are called, and it sets no shell options.
#
#   kind_init <cluster> [timeout]    sets the names and a scratch directory,
#                                    and exports KUBECONFIG into it
#   kind_require_tools               docker, kind and kubectl are on PATH
#   kind_create                      a fresh cluster of one control plane and
#                                    two workers (any old one of the name is
#                                    deleted first)
#   kind_apply <image> [<nfs-ipv4>]  loads the image into the nodes and applies
#                                    the e2e overlay (hack/e2e-overlay.sh)
#   kind_wait                        waits for the Deployment, the DaemonSet and
#                                    every CSINode
#   kind_collect <dir>               kind's logs, both roles' pod logs and
#                                    descriptions, and each node's mountinfo
#   kind_delete                      deletes the cluster and the scratch dir;
#                                    KEEP_CLUSTER=1 keeps the cluster
#
# The scripts keep their own EXIT traps and call these from them.

# kindest/node for Kubernetes 1.37.0, the default of the pinned kind (0.33.0),
# by the digest of its multi-arch index.
kind_node_image="kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5"
kind_driver="miami.csi.greghaskins.com"
kind_ns="miami-system"

kind_init() {
  kind_cluster="${1:?kind_init needs a cluster name}"
  kind_timeout="${2:-300s}"
  kind_work="$(mktemp -d)"
  export KUBECONFIG="$kind_work/kubeconfig"
}

kind_require_tools() {
  local tool
  for tool in docker kind kubectl; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      echo "kind: $tool not found" >&2
      return 1
    fi
  done
}

kind_create() {
  # Start from nothing: a cluster left over from an earlier run is not evidence.
  kind delete cluster --name "$kind_cluster" >/dev/null 2>&1 || true

  cat >"$kind_work/kind.yaml" <<YAML
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    image: ${kind_node_image}
  - role: worker
    image: ${kind_node_image}
  - role: worker
    image: ${kind_node_image}
YAML

  echo "kind: creating cluster $kind_cluster"
  kind create cluster --name "$kind_cluster" --config "$kind_work/kind.yaml" --wait "$kind_timeout"
  kind_nodes="$(kubectl get nodes -o name | wc -l)"
  echo "kind: $kind_nodes nodes"
}

kind_apply() {
  local image="$1" server="${2:-}"
  echo "kind: loading $image"
  kind load docker-image --name "$kind_cluster" "$image"

  mkdir "$kind_work/overlay"
  if [ -n "$server" ]; then
    hack/e2e-overlay.sh --image "$image" --nfs-server "$server" --out "$kind_work/overlay"
  else
    hack/e2e-overlay.sh --image "$image" --out "$kind_work/overlay"
  fi
  kubectl apply -k "$kind_work/overlay"
}

kind_wait() {
  local desired ready node
  kubectl -n "$kind_ns" wait --for=condition=Available --timeout="$kind_timeout" deployment/miami-controller
  kubectl -n "$kind_ns" wait --for=jsonpath='{.status.desiredNumberScheduled}'="$kind_nodes" --timeout="$kind_timeout" daemonset/miami-node
  kubectl -n "$kind_ns" wait --for=jsonpath='{.status.numberReady}'="$kind_nodes" --timeout="$kind_timeout" daemonset/miami-node
  desired="$(kubectl -n "$kind_ns" get daemonset miami-node -o jsonpath='{.status.desiredNumberScheduled}')"
  ready="$(kubectl -n "$kind_ns" get daemonset miami-node -o jsonpath='{.status.numberReady}')"
  echo "kind: DaemonSet miami-node desired=$desired ready=$ready; Deployment miami-controller available"
  test "$desired" = "$ready"

  for node in $(kubectl get nodes -o name); do
    kubectl wait --for=jsonpath='{.spec.drivers[*].name}'="$kind_driver" --timeout="$kind_timeout" "csinode/${node#node/}"
    echo "kind: CSINode ${node#node/} lists $kind_driver"
  done
}

kind_collect() {
  local art="$1" node pod
  mkdir -p "$art"
  kind export logs --name "$kind_cluster" "$art/kind-logs" >/dev/null 2>&1 || true
  kubectl -n "$kind_ns" describe pods >"$art/describe-pods.txt" 2>&1 || true
  for pod in $(kubectl -n "$kind_ns" get pods -o name 2>/dev/null); do
    kubectl -n "$kind_ns" logs "$pod" --all-containers --prefix --tail=-1 \
      >"$art/logs-${pod#pod/}.txt" 2>&1 || true
  done
  for node in $(kind get nodes --name "$kind_cluster" 2>/dev/null); do
    docker exec "$node" cat /proc/1/mountinfo >"$art/mountinfo-${node}.txt" 2>&1 || true
  done
}

kind_delete() {
  if [ "${KEEP_CLUSTER:-}" != "1" ]; then
    kind delete cluster --name "$kind_cluster" >/dev/null 2>&1 || true
  fi
  rm -rf "$kind_work"
}
