# Deepiri Sugar Glider (Go)

This transport service (formerly Synapse Sidecar) runs next to the realtime gateway and owns Redis Streams concerns (publish, consume, ack, WAL replay, and DLQ scanning).
Legacy module/path names remain `synapse-sidecar` for compatibility.

## Current capabilities

- Env-driven Sugar Glider config (`SUGAR_GLIDER_*` preferred, `SIDECAR_*` legacy aliases)
- Redis Streams publish/consume/ack support
- gRPC server from `proto/synapse/v1/sidecar.proto` (legacy proto path)
- HTTP compatibility endpoints (`/v1/publish`, `/v1/read`, `/v1/ack`, `/v1/dlq/replay`) for incremental migration
- `/healthz`, `/readyz`, and `/metrics` HTTP endpoints
- `healthcheck` CLI command for container probes (`/app/sidecar healthcheck`, legacy binary name)
- Local WAL append + replay when Redis is unavailable (entry count and optional byte caps)
- Background DLQ scanner for over-retried pending entries
- DLQ replay (HTTP + gRPC) to requeue entries onto target streams
- Dual Prometheus metric prefixes: `synapse_sidecar_*` and `sugar_glider_*`

## Runtime configuration

Preferred keys use the `SUGAR_GLIDER_*` prefix. Matching `SIDECAR_*` keys remain supported aliases.

- `SUGAR_GLIDER_SERVICE_NAME` / `SIDECAR_SERVICE_NAME` (default: `real-time-gateway`)
- `SUGAR_GLIDER_REDIS_URL` / `SIDECAR_REDIS_URL` (required)
- `SUGAR_GLIDER_LISTEN_ADDR` / `SIDECAR_LISTEN_ADDR` (default: `tcp://0.0.0.0:8081`; HTTP probe/compat server)
- `SUGAR_GLIDER_GRPC_ADDR` / `SIDECAR_GRPC_ADDR` (default: `tcp://0.0.0.0:50051`; gRPC server)
- `SUGAR_GLIDER_PUBLISH_STREAMS` / `SIDECAR_PUBLISH_STREAMS` (default: `platform-events`)
- `SUGAR_GLIDER_CONSUME_STREAMS` / `SIDECAR_CONSUME_STREAMS` (default: empty = allow all streams)
- `SUGAR_GLIDER_MAX_STREAM_LEN` / `SIDECAR_MAX_STREAM_LEN` (default: `10000`)
- Dispatcher consume tuning:
- `SUGAR_GLIDER_DISPATCHER_CONSUMER_NAME` / `SIDECAR_DISPATCHER_CONSUMER_NAME` (default: `sugar-glider-dispatcher`)
- `SUGAR_GLIDER_DISPATCHER_READ_COUNT` / `SIDECAR_DISPATCHER_READ_COUNT` (default: `100`)
- `SUGAR_GLIDER_DISPATCHER_BLOCK_MS` / `SIDECAR_DISPATCHER_BLOCK_MS` (default: `1000`)
- `SUGAR_GLIDER_DISPATCHER_SUBSCRIBER_BUFFER` / `SIDECAR_DISPATCHER_SUBSCRIBER_BUFFER` (default: `256`)
- `SUGAR_GLIDER_DISPATCHER_ACK_BATCH_SIZE` / `SIDECAR_DISPATCHER_ACK_BATCH_SIZE` (default: `64`)
- `SUGAR_GLIDER_DISPATCHER_ACK_FLUSH_CONCURRENCY` / `SIDECAR_DISPATCHER_ACK_FLUSH_CONCURRENCY` (default: `2`)
- `SUGAR_GLIDER_DISPATCHER_ACK_FLUSH_MS` / `SIDECAR_DISPATCHER_ACK_FLUSH_MS` (default: `10`)
- `SUGAR_GLIDER_DISPATCHER_ACK_QUEUE_SIZE` / `SIDECAR_DISPATCHER_ACK_QUEUE_SIZE` (default: `4096`)
- `SUGAR_GLIDER_WAL_DIR` / `SIDECAR_WAL_DIR` (default: `/data/synapse-wal`)
- `SUGAR_GLIDER_WAL_MAX_ENTRIES` / `SIDECAR_WAL_MAX_ENTRIES` (default: `0` = unlimited; rejects new WAL writes when full)
- `SUGAR_GLIDER_WAL_MAX_BYTES` / `SIDECAR_WAL_MAX_BYTES` (default: `0` = unlimited disk bytes)
- WAL filename defaults to `sugar-glider.wal.jsonl` and will reuse legacy `sidecar.wal.jsonl` if present.
- `SUGAR_GLIDER_WAL_REPLAY_BATCH` / `SIDECAR_WAL_REPLAY_BATCH` (default: `100`; set `0` to disable replay)
- `SUGAR_GLIDER_WAL_REPLAY_INTERVAL_MS` / `SIDECAR_WAL_REPLAY_INTERVAL_MS` (default: `2000`; set `0` to disable timer loop)
- `SUGAR_GLIDER_DLQ_MAX_RETRIES` / `SIDECAR_DLQ_MAX_RETRIES` (default: `3`; set `0` to disable DLQ scanner)
- `SUGAR_GLIDER_DLQ_MIN_IDLE_MS` / `SIDECAR_DLQ_MIN_IDLE_MS` (default: `30000`)
- `SUGAR_GLIDER_DLQ_SCAN_INTERVAL_MS` / `SIDECAR_DLQ_SCAN_INTERVAL_MS` (default: `5000`; set `0` to disable DLQ scanner loop)
- `SUGAR_GLIDER_DLQ_SCAN_BATCH` / `SIDECAR_DLQ_SCAN_BATCH` (default: `100`; pending entries scanned per DLQ page)
- `SUGAR_GLIDER_DLQ_STREAM_POLICIES` / `SIDECAR_DLQ_STREAM_POLICIES` (optional per-stream overrides: `stream:max_retries:min_idle_ms[:dlq_stream]`, comma-separated)
- `SUGAR_GLIDER_READINESS_TIMEOUT_MS` / `SIDECAR_READINESS_TIMEOUT_MS` (default: `1500`)
- `SUGAR_GLIDER_READY_MAX_WAL_DEPTH` / `SIDECAR_READY_MAX_WAL_DEPTH` (default: `0` = report only; when >0 marks not-ready)
- `SUGAR_GLIDER_READY_MAX_PUBLISH_QUEUE_DEPTH` / `SIDECAR_READY_MAX_PUBLISH_QUEUE_DEPTH` (default: `0` = report only)

### DLQ replay

```bash
curl -sS -X POST http://localhost:8081/v1/dlq/replay \
  -H 'content-type: application/json' \
  -d '{"dlq_stream":"platform-events:dlq","count":100,"delete_from_dlq":true}'
```

gRPC equivalent: `SynapseSidecar/ReplayDLQ`.

## Dispatcher timing observability

Dispatcher micro-timing counters are exposed in:

- `GET /v1/config` under `metrics`
- `GET /metrics` as Prometheus metrics

Key fields:

- `dispatcher_read_samples`
- `dispatcher_read_duration_ms_total`
- `dispatcher_read_duration_ms_max`
- `dispatcher_fanout_samples`
- `dispatcher_fanout_duration_ms_total`
- `dispatcher_fanout_duration_ms_max`
- `dispatcher_ack_flush_calls`
- `dispatcher_ack_flush_chunks`
- `dispatcher_ack_flush_duration_ms_total`
- `dispatcher_ack_flush_duration_ms_max`
- `dispatcher_ack_exec_samples`
- `dispatcher_ack_exec_duration_ms_total`
- `dispatcher_ack_exec_duration_ms_max`
- `dispatcher_ack_queue_depth_peak`
- `dispatcher_ack_input_entries`
- `dispatcher_ack_deduped_entries`
- `dispatcher_ack_duplicate_entries`
- `dispatcher_ack_contiguous_spans`
- `dispatcher_ack_contiguous_saved_entries`

Prometheus names use both `synapse_sidecar_dispatcher_*` (legacy) and `sugar_glider_dispatcher_*` aliases.

The ACK compression counters are measurement-only. Redis `XACK` still receives explicit entry IDs; these fields quantify duplicate IDs removed by the pending map and contiguous stream-ID spans that could justify a future range-style/lower-ACK-pressure experiment.

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

Local bench gate (Docker Redis + Sugar Glider, gRPC smoke, publish load):

```bash
cd deepiri-sugar-glider
./scripts/run_bench_gate.sh
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

- Grafana dashboards for dual metric namespaces
- Extended integration test matrix across all Sugar Glider-attached services
- Production ACL for DLQ replay admin endpoints
