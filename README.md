# Deepiri Sugar Glider (Go)

This transport service (formerly Synapse Sidecar) runs next to the realtime gateway and owns Redis Streams concerns (publish, consume, ack, WAL replay, and DLQ scanning).
Legacy module/path names remain `synapse-sidecar` for compatibility.

## Current capabilities

- Env-driven Sugar Glider config (`SIDECAR_*` legacy variable namespace)
- Redis Streams publish/consume/ack support
- gRPC server from `proto/synapse/v1/sidecar.proto` (legacy proto path)
- HTTP compatibility endpoints (`/v1/publish`, `/v1/read`, `/v1/ack`) for incremental migration
- `/healthz`, `/readyz`, and `/metrics` HTTP endpoints
- `healthcheck` CLI command for container probes (`/app/sidecar healthcheck`, legacy binary name)
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
- Dispatcher consume tuning:
- `SIDECAR_DISPATCHER_CONSUMER_NAME` (default: `sugar-glider-dispatcher`)
- `SIDECAR_DISPATCHER_READ_COUNT` (default: `100`)
- `SIDECAR_DISPATCHER_BLOCK_MS` (default: `1000`)
- `SIDECAR_DISPATCHER_SUBSCRIBER_BUFFER` (default: `256`)
- `SIDECAR_DISPATCHER_ACK_BATCH_SIZE` (default: `64`)
- `SIDECAR_DISPATCHER_ACK_FLUSH_CONCURRENCY` (default: `2`)
- `SIDECAR_DISPATCHER_ACK_FLUSH_MS` (default: `10`)
- `SIDECAR_DISPATCHER_ACK_QUEUE_SIZE` (default: `4096`)
- `SIDECAR_WAL_DIR` (default: `/data/synapse-wal`)
- WAL filename defaults to `sugar-glider.wal.jsonl` and will reuse legacy `sidecar.wal.jsonl` if present.
- `SIDECAR_WAL_REPLAY_BATCH` (default: `100`; set `0` to disable replay)
- `SIDECAR_WAL_REPLAY_INTERVAL_MS` (default: `2000`; set `0` to disable timer loop)
- `SIDECAR_DLQ_MAX_RETRIES` (default: `3`; set `0` to disable DLQ scanner)
- `SIDECAR_DLQ_MIN_IDLE_MS` (default: `30000`)
- `SIDECAR_DLQ_SCAN_INTERVAL_MS` (default: `5000`; set `0` to disable DLQ scanner loop)
- `SIDECAR_READINESS_TIMEOUT_MS` (default: `1500`)

## Proto generation

Generate Go stubs from one proto source:

```bash
cd deepiri-sugar-glider
./scripts/generate_protos.sh
```

To also generate Python stubs into consumer repos, provide explicit output paths:

```bash
CYREX_PY_GEN_OUT=/abs/path/to/diri-cyrex/app/integrations/streaming/gen \
HELOX_PY_GEN_OUT=/abs/path/to/diri-helox/integrations/streaming/gen \
./scripts/generate_protos.sh
```

This updates:

- `proto/synapse/v1/*.pb.go` (Go stubs)
- Python stubs in Cyrex/Helox only when `CYREX_PY_GEN_OUT` / `HELOX_PY_GEN_OUT` are provided

## Local smoke checks

Unit/integration tests in this repo:

```bash
cd deepiri-sugar-glider
go test ./...
```

Platform integration smoke (from deepiri-platform):

```bash
cd deepiri-platform
make rtg-sugar-grpc-smoke
```

Fast full-chain gate:

```bash
cd deepiri-platform
make rtg-sugar-gate
```

Full chaos-inclusive gate:

```bash
cd deepiri-platform
make rtg-sugar-gate-full
```

Legacy aliases (`rtg-smoke`, `rtg-grpc-smoke`, `rtg-gate`, `rtg-gate-full`) continue to work.

The gRPC smoke command executes `cmd/grpc-smoke` and validates:

1. `Health`
2. `Publish`
3. `Subscribe`
4. `Ack`

## Still to harden

- WAL backpressure/retention policies
- Per-stream retry and DLQ policy overrides
- Extended integration test matrix across all Sugar Glider-attached services
