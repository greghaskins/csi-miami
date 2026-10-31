# One image, built natively on each architecture (no emulation). The final
# stage holds only the binary: no shell, no package manager.

# golang:1.27.3, pinned by digest; keep the tag in step with go.mod and mise.toml.
FROM --platform=$BUILDPLATFORM golang:1.27.3@sha256:2920bb124f1a2e12a1d39f0cab756ae0068441ee1da7655dec7f5e374944095b AS build
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
