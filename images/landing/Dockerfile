# Landing server: "claim a devbox" page + claim API.
# Build context: repo root (needs provisioner/).
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY provisioner/go.mod provisioner/go.sum ./
RUN go mod download
COPY provisioner/ ./
RUN CGO_ENABLED=0 go build -trimpath -o /landing ./cmd/landing

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /landing /landing
USER nonroot
EXPOSE 8080
ENTRYPOINT ["/landing"]
