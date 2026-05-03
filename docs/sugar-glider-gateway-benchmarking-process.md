# Sugar Glider Gateway Benchmarking Process

## Executive Summary

This document records the reproducible benchmark process used to evaluate Sugar Glider as the Realtime Gateway transport path.

The current fixed v3 candidate was benchmarked twice against the April 12 baseline and the previous v2 checkpoint. Both fixed v3 runs stayed reliable with `0` lost events, `0` failed ops, and `0%` error rate.

The old blocker was low-concurrency `32KB` traffic. Fixed v3 closes that gap in the local benchmark matrix.

## Benchmark Scope

The benchmark measures the end-to-end path:

```text
HTTP publish -> Sugar Glider -> Redis Streams -> Sugar Glider dispatcher/gRPC -> Realtime Gateway -> Socket.IO delivery
```

It is an end-to-end gateway benchmark, not a raw Redis microbenchmark.

## Baselines And Runs

Original April 12 baseline:

```text
benchmarks/end-to-end/20260412T052514Z
```

Previous proven Sugar Glider checkpoint:

```text
benchmarks/end-to-end/20260423T040104Z-heavy-path-boost-v2
```

Primary fixed v3 run:

```text
benchmarks/end-to-end/20260423T221540Z-heavy-path-boost-v3-fixed
```

Repeat fixed v3 run:

```text
benchmarks/end-to-end/20260423T222329Z-heavy-path-boost-v3-fixed-repeat2
```

Invalid attempts, not used for decision-making:

```text
benchmarks/end-to-end/20260423T215509Z-heavy-path-boost-v3
benchmarks/end-to-end/20260423T220658Z-heavy-path-boost-v3-keepalive-fix
```

The first invalid run exposed gRPC excess-ping rejection. The second invalid run exposed the `json.RawMessage` go-redis marshaling regression. Both were marked invalid and superseded by the fixed v3 runs.

## Fixed Matrix

The benchmark matrix is hard-locked in the harness:

```text
payload_bytes: 1024, 8192, 32768
concurrency: 1, 10, 50
warmup_ops: 500
measure_ops: 5000
repetitions: 3
```

This creates `27` measured cells per full run.

## Acceptance Gates

Reliability gates:

```text
lost_events=0
failed_ops=0
error_rate_pct=0
```

Heavy-path gates:

```text
32768B @ c=50 throughput > 1.13x April 12 baseline
32768B @ c=50 p95 <= 1.05x April 12 baseline
32768B @ c=10 throughput >= 1.00x April 12 baseline
32768B @ c=1 throughput >= 0.90x April 12 baseline
overall throughput near or above 1.35x April 12 baseline
```

Fixed v3 passes these gates in both full runs.

## Reproduce The Benchmark

Start the local stack:

```bash
cd /Users/Kyle/Developer/Deepiri/deepiri-platform-extract-hard
docker compose -f docker-compose.rtg-sugar-glider.local.yml up -d
```

Check health:

```bash
make rtg-health
```

Optional gate check:

```bash
make rtg-gate
```

If this fails only because available memory is below `4GB`, record the failure and proceed only if endpoints and smoke checks pass.

Reset local benchmark state before each run:

```bash
docker exec deepiri-redis-rtg-local redis-cli -a redispassword FLUSHALL
```

Run the full benchmark matrix:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-heavy-path-boost-v3-fixed"
node scripts/dev/sugarglider/e2e_gateway_benchmark.js --out-dir "benchmarks/end-to-end/${RUN_ID}"
```

Run a second matrix for variance:

```bash
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-heavy-path-boost-v3-fixed-repeat2"
docker exec deepiri-redis-rtg-local redis-cli -a redispassword FLUSHALL
node scripts/dev/sugarglider/e2e_gateway_benchmark.js --out-dir "benchmarks/end-to-end/${RUN_ID}"
```

Generate comparison reports against April 12:

```bash
node /Users/Kyle/Developer/Deepiri/planning/plaky/compare-task-14-e2e.js \
  --baseline-summary benchmarks/end-to-end/20260412T052514Z/summary.csv \
  --rerun-summary benchmarks/end-to-end/${RUN_ID}/summary.csv \
  --out-csv benchmarks/end-to-end/${RUN_ID}/comparison_vs_20260412.csv \
  --out-md benchmarks/end-to-end/${RUN_ID}/comparison_vs_20260412.md
```

Generate comparison reports against v2:

```bash
node /Users/Kyle/Developer/Deepiri/planning/plaky/compare-task-14-e2e.js \
  --baseline-summary benchmarks/end-to-end/20260423T040104Z-heavy-path-boost-v2/summary.csv \
  --rerun-summary benchmarks/end-to-end/${RUN_ID}/summary.csv \
  --out-csv benchmarks/end-to-end/${RUN_ID}/comparison_vs_v2.csv \
  --out-md benchmarks/end-to-end/${RUN_ID}/comparison_vs_v2.md
```

## Primary Fixed V3 Result

Run:

```text
20260423T221540Z-heavy-path-boost-v3-fixed
```

Reliability:

```text
lost_events=0
failed_ops=0
error_rate_pct=0
```

Versus April 12 baseline:

```text
average throughput ratio: 2.70x
p95 pass rate at <=1.20x baseline: 9/9
32768B @ c=1: 268.34 ops/s, 1.31x baseline
32768B @ c=10: 592.47 ops/s, 1.77x baseline
32768B @ c=50: 817.05 ops/s, 2.36x baseline
```

Versus v2:

```text
average throughput ratio: 1.93x
p95 pass rate at <=1.20x v2: 9/9
32768B @ c=1: 1.84x v2
32768B @ c=10: 1.88x v2
32768B @ c=50: 2.09x v2
```

## Repeat Fixed V3 Result

Run:

```text
20260423T222329Z-heavy-path-boost-v3-fixed-repeat2
```

Reliability:

```text
lost_events=0
failed_ops=0
error_rate_pct=0
```

Versus April 12 baseline:

```text
average throughput ratio: 2.71x
p95 pass rate at <=1.20x baseline: 9/9
32768B @ c=1: 251.19 ops/s, 1.22x baseline
32768B @ c=10: 649.12 ops/s, 1.94x baseline
32768B @ c=50: 710.06 ops/s, 2.05x baseline
```

Versus v2:

```text
average throughput ratio: 1.95x
p95 pass rate at <=1.20x v2: 9/9
32768B @ c=1: 1.72x v2
32768B @ c=10: 2.05x v2
32768B @ c=50: 1.81x v2
```

## Interpretation

The earlier v2 answer was conservative: use Sugar Glider for the high-concurrency gateway candidate, but keep original/Redis as the safer low-concurrency heavy path.

The fixed v3 answer is stronger. It keeps the clean reliability profile and improves every throughput cell versus both April 12 and v2 in the completed fixed runs. Most importantly, `32768B @ c=1` is no longer below baseline.

The current engineering answer is:

```text
Sugar Glider is viable as the main gateway path candidate for RTG/Synapse transport. The benchmark data now supports moving beyond hybrid-only positioning, while still keeping Redis as the underlying stream substrate and comparison/rollback point.
```

## Reproducibility Checklist

Before benchmark:

```text
record branch and commit
record Docker image rebuilds
record environment flags
record preflight result
record health check result
flush Redis benchmark state
confirm no WAL replay backlog from invalid runs
```

During benchmark:

```text
store raw JSON files
store per-scenario logs
store summary.csv
store report.md
capture RTG and Sugar Glider error logs if a run is invalid
```

After benchmark:

```text
generate comparison_vs_20260412.md
generate comparison_vs_v2.md
run a second full matrix for variance
mark invalid runs with INVALID.md
update latest-run.txt
write decision.md
```

## Boss Meeting Position

Use this concise position:

```text
On April 21 I said Plan #5 core was complete and we were finishing 32KB and low-concurrency tuning. Since then, I fixed the gRPC keepalive issue, fixed the Sugar Glider publish hot-path regression, rebuilt the stack, and reran the full benchmark matrix twice. Both fixed runs had zero lost events, zero failed ops, and zero error rate. The previous blocker was 32KB low-concurrency throughput; fixed v3 now beats the April 12 baseline at 32KB c=1, c=10, and c=50. My recommendation is that Sugar Glider is viable as the main RTG gateway candidate, with Redis retained underneath as the stream substrate and rollback comparison point.
```
