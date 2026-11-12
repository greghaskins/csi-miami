# One image, built natively on each architecture (no emulation). The final
# stage holds the binary and the tools it runs (the list is hack/tools.txt) with
# their shared libraries: no shell, no package manager.

# golang:1.27.4, pinned by digest; keep the tag in step with go.mod and mise.toml.
FROM --platform=$BUILDPLATFORM golang:1.27.4@sha256:5e8afd493a7124a8586e134d9b532731161d4b9becf9dee5a5956b107426f93d AS build
ARG TARGETARCH
ARG VERSION=dev
ENV GOTOOLCHAIN=local
# CPU baselines, pinned so a default change in the toolchain cannot raise them:
# amd64 v1 runs on older x86 without SSE4.2 or AVX, arm64 v8.0 on the
# Raspberry Pi 4. Only the matching variable takes effect for each target.
ENV GOAMD64=v1 GOARM64=v8.0
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /miami ./cmd/miami

# alpine:3.24, pinned by digest. Built for the target architecture, natively:
# the tools are dynamically linked and are copied together with their
# libraries, so they must be the ones this stage can run `ldd` on.
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a AS tools
# resize2fs is in e2fsprogs-extra and lives in /usr/sbin; the other tools are in
# /sbin. Link it there so every tool has one path, /sbin/<name>.
# blkid is its own package. The programs to carry are the paths in
# hack/tools.txt, the same list the smoke test starts from the built image.
#
# Every package that supplies a file to the closure and is not already in the
# base image is pinned to an exact version (name=version), the programs and
# their libraries alike, so a rebuild cannot pick up a newer one unseen. A pin
# fails the build when the repository no longer carries that version; that is
# the prompt to bump. To bump: in a container of the base image above, run
#   apk add --no-cache cryptsetup e2fsprogs e2fsprogs-extra blkid
#   apk list -I | sort
# and copy the versions of the packages below (the base image's own packages
# are not pinned here: the digest fixes them). Then rebuild and run the smoke
# test (hack/smoke.sh), which starts each program from the built image.
RUN apk add --no-cache \
      blkid=2.42.3-r1 \
      cryptsetup=2.8.6-r0 \
      cryptsetup-libs=2.8.6-r0 \
      device-mapper-libs=2.03.35-r3 \
      e2fsprogs=1.47.4-r0 \
      e2fsprogs-extra=1.47.4-r0 \
      e2fsprogs-libs=1.47.4-r0 \
      json-c=0.18-r1 \
      libblkid=2.42.3-r1 \
      libcom_err=1.47.4-r0 \
      libeconf=0.8.3-r0 \
      libuuid=2.42.3-r1 \
      popt=1.19-r4 \
 && ln -s /usr/sbin/resize2fs /sbin/resize2fs
COPY hack/image-closure.sh hack/tools.txt /
RUN /image-closure.sh /closure $(grep -v '^#' /tools.txt | cut -d' ' -f1)

FROM scratch
# Labels only; they add no file to the image. The source label links the
# package to this repository in the registry.
LABEL org.opencontainers.image.source="https://github.com/greghaskins/csi-miami" \
      org.opencontainers.image.licenses="GPL-3.0-only" \
      org.opencontainers.image.description="csi-miami: a Kubernetes CSI driver for directory volumes on NFSv3 and local transports"
COPY --from=tools /closure/ /
COPY --from=build /miami /miami
ENTRYPOINT ["/miami"]
