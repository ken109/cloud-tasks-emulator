# syntax=docker/dockerfile:1

# --- build stage -----------------------------------------------------------
# Run the compiler on the native build platform and cross-compile to the
# target platform, so multi-arch builds don't pay the QEMU emulation cost.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -ldflags="-s -w" -o /out/cloud-tasks-emulator .

# --- runtime stage ---------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/cloud-tasks-emulator /cloud-tasks-emulator
EXPOSE 8123
ENTRYPOINT ["/cloud-tasks-emulator"]
CMD ["-host", "0.0.0.0", "-port", "8123"]
