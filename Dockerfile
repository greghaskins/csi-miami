# One image, built natively on each architecture (no emulation). The final
# stage holds the binary and the tools it runs (cryptsetup, e2fsprogs) with
# their shared libraries: no shell, no package manager.

# golang:1.27.3, pinned by digest; keep the tag in step with go.mod and mise.toml.
FROM --platform=$BUILDPLATFORM golang:1.27.3@sha256:2920bb124f1a2e12a1d39f0cab756ae0068441ee1da7655dec7f5e374944095b AS build
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

# alpine:3.24, pinned by digest (resize2fs ships in e2fsprogs-extra). Built for the target architecture, natively:
# the tools are dynamically linked and are copied together with their
# libraries, so they must be the ones this stage can run `ldd` on.
FROM alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a AS tools
RUN apk add --no-cache cryptsetup e2fsprogs e2fsprogs-extra
COPY hack/image-closure.sh /image-closure.sh
RUN /image-closure.sh /closure /sbin/cryptsetup /sbin/mkfs.ext4 /sbin/e2fsck /sbin/resize2fs

FROM scratch
# Labels only; they add no file to the image. The source label links the
# package to this repository in the registry.
LABEL org.opencontainers.image.source="https://github.com/greghaskins/csi-miami" \
      org.opencontainers.image.licenses="GPL-3.0-only" \
      org.opencontainers.image.description="csi-miami: a Kubernetes CSI driver for directory volumes on NFSv3 and local transports"
COPY --from=tools /closure/ /
COPY --from=build /miami /miami
ENTRYPOINT ["/miami"]
