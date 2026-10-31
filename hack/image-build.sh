#!/usr/bin/env bash
# Builds the csi-miami image for one architecture.
#
#   hack/image-build.sh --arch <amd64|arm64> --tag <ref> (--push|--load)
#
# Everything that determines the image bytes and attestations lives here: the
# BuildKit image, provenance, the SBOM generator, the build arguments and the
# platform handling. This file is one of the inputs of hack/content-hash.sh,
# so the command cannot change without the content hash changing. CI's build
# job and `make image` both call it.
#
# The image's version is c-<content hash>, computed here.
#
# rebuild-salt: 1
# Bumping the salt changes the content hash, which forces a rebuild of every
# tag. It is the only way to retire a bad tag without deleting package
# versions.
#
# --push builds natively for the architecture and pushes with provenance and
# an SBOM. --load puts the image in the local Docker daemon, which cannot hold
# attestations, so they are left out; a --load build whose hashed inputs differ
# from the index gets "-dirty" on its tag and version so that it is never
# labelled as the committed hash. --push refuses a dirty tree.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# moby/buildkit v0.34.1
buildkit="moby/buildkit:v0.34.1@sha256:99368f3e46177c6405b9740e4461d91cee004128daeecca78d4ceac50ec1da5c"
# docker/buildkit-syft-scanner 1.12.1
sbom_generator="docker/buildkit-syft-scanner:1.12.1@sha256:873a7458c3c8ac8e32c7b796c10e1600b07b963d2aa000a72a2d97aaaa2d6315"
builder="miami-build"

arch=""
tag=""
mode=""
while [ $# -gt 0 ]; do
  case "$1" in
    --arch) arch="${2:?--arch needs a value}"; shift 2 ;;
    --tag) tag="${2:?--tag needs a value}"; shift 2 ;;
    --push | --load) mode="${1#--}"; shift ;;
    *) echo "image-build: unknown argument: $1" >&2; exit 2 ;;
  esac
done

case "$arch" in
  amd64 | arm64) ;;
  *) echo "image-build: --arch must be amd64 or arm64" >&2; exit 2 ;;
esac
if [ -z "$tag" ] || [ -z "$mode" ]; then
  echo "usage: hack/image-build.sh --arch <amd64|arm64> --tag <ref> (--push|--load)" >&2
  exit 2
fi

hash="$(hack/content-hash.sh)"
if ! [[ "$hash" =~ ^[0-9a-f]{16}$ ]]; then
  echo "image-build: unexpected content hash: $hash" >&2
  exit 1
fi
version="c-${hash}"

mapfile -t paths < <(hack/content-hash.sh --paths)
if [ -n "$(git status --porcelain -- "${paths[@]}")" ]; then
  if [ "$mode" = "push" ]; then
    echo "image-build: build inputs differ from the index; refusing to push" >&2
    exit 1
  fi
  version="${version}-dirty"
  tag="${tag}-dirty"
fi

if ! docker buildx inspect "$builder" > /dev/null 2>&1; then
  docker buildx create --name "$builder" --driver docker-container \
    --driver-opt "image=${buildkit}" > /dev/null
fi

args=(
  --builder "$builder"
  --platform "linux/${arch}"
  --build-arg "VERSION=${version}"
  -t "$tag"
)
if [ "$mode" = "push" ]; then
  args+=(--provenance=true --attest "type=sbom,generator=${sbom_generator}" --push)
else
  args+=(--load)
fi

docker buildx build "${args[@]}" .
