# Synapse Sidecar (Go) in `real-time-gateway`

This sidecar runs next to the realtime gateway service and owns Redis Streams transport concerns (publish, consume, ack, WAL replay, and DLQ scanning).

## Current capabilities

- Env-driven sidecar config (`SIDECAR_*`)
- Redis Streams publish/consume/ack support
- gRPC server from `proto/synapse/v1/sidecar.proto`
- HTTP compatibility endpoints (`/v1/publish`, `/v1/read`, `/v1/ack`) for incremental migration
- `/healthz`, `/readyz`, and `/metrics` HTTP endpoints
- `healthcheck` CLI command for container probes (`/app/sidecar healthcheck`)
- Local WAL append + replay when Redis is unavailable
- Background DLQ scanner for over-retried pending entries

## Runtime configuration

- `SIDECAR_SERVICE_NAME` (default: `real-time-gateway`)
- `SIDECAR_REDIS_URL` (required)
- `SIDECAR_LISTEN_ADDR` (default: `tcp://0.0.0.0:8081`; HTTP probe/compat server)
- `SIDECAR_GRPC_ADDR` (default: `tcp://0.0.0.0:50051`; gRPC server)
- `SIDECAR_PUBLISH_STREAMS` (default: `platform-events`)
- `SIDECAR_CONSUME_STREAMS` (default: empty = allow all streams)
- `SIDECAR_MAX_STREAM_LEN` (default: `10000`)
- `SIDECAR_WAL_DIR` (default: `/data/synapse-wal`)
- `SIDECAR_WAL_REPLAY_BATCH` (default: `100`; set `0` to disable replay)
- `SIDECAR_WAL_REPLAY_INTERVAL_MS` (default: `2000`; set `0` to disable timer loop)
- `SIDECAR_DLQ_MAX_RETRIES` (default: `3`; set `0` to disable DLQ scanner)
- `SIDECAR_DLQ_MIN_IDLE_MS` (default: `30000`)
- `SIDECAR_DLQ_SCAN_INTERVAL_MS` (default: `5000`; set `0` to disable DLQ scanner loop)
- `SIDECAR_READINESS_TIMEOUT_MS` (default: `1500`)

## Proto generation

Generate Go + Python stubs from one proto source:

```bash
cd platform-services/backend/deepiri-realtime-gateway/synapse-sidecar
./scripts/generate_protos.sh
```

This updates:

- `proto/synapse/v1/*.pb.go` (Go stubs)
- `diri-cyrex/app/integrations/streaming/gen/...` (Python stubs)
- `diri-helox/integrations/streaming/gen/...` (Python stubs)

## Local smoke checks

HTTP smoke:

```bash
cd deepiri-platform
make rtg-smoke
```

gRPC smoke:

```bash
cd deepiri-platform
make rtg-grpc-smoke
```

Fast full-chain gate:

```bash
cd deepiri-platform
make rtg-gate
```

Full chaos-inclusive gate:

```bash
cd deepiri-platform
make rtg-gate-full
```

The gRPC smoke command executes `cmd/grpc-smoke` and validates:

1. `Health`
2. `Publish`
3. `Subscribe`
4. `Ack`

## Still to harden

- WAL backpressure/retention policies
- Per-stream retry and DLQ policy overrides
- Extended integration test matrix across all sidecar-attached services
