# One image, built natively on each architecture (no emulation). The final
# stage holds only the binary: no shell, no package manager.

# golang:1.26.10, pinned by digest; keep the tag in step with go.mod and mise.toml.
FROM --platform=$BUILDPLATFORM golang:1.26.10@sha256:670bb5aa5349ae03b8516764718486f4f16399134bdc9acd0c46a23f043ab05c AS build
ARG TARGETARCH
ARG VERSION=dev
ENV GOTOOLCHAIN=local
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /miami ./cmd/miami

FROM scratch
COPY --from=build /miami /miami
ENTRYPOINT ["/miami"]
