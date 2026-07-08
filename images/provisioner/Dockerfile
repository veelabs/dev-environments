# Temporal worker for dev-environment provisioning.
# Build context: repo root (needs provisioner/).
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY provisioner/go.mod provisioner/go.sum ./
RUN go mod download
COPY provisioner/ ./
RUN CGO_ENABLED=0 go build -trimpath -o /provisioner ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /provisioner /provisioner
USER nonroot
ENTRYPOINT ["/provisioner"]
