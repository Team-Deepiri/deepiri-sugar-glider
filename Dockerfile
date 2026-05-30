FROM golang:1.24-alpine AS build
WORKDIR /src
ARG TARGETOS
ARG TARGETARCH

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /out/sidecar ./cmd/sidecar

FROM gcr.io/distroless/static-debian12
WORKDIR /app
COPY --from=build /out/sidecar /app/sidecar
EXPOSE 8081
EXPOSE 50051
ENTRYPOINT ["/app/sidecar"]
