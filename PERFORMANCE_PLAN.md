# Performance Plan

## Initial Targets

- Constant memory profile for large log files.
- Python fallback suitable for local debugging and CI-scale samples.
- Rust core target for multi-GB scans.
- Output Parquet with Zstd compression.

## Metrics

Benchmarks should record:

- Input size in GB.
- Event throughput in events/sec and MB/sec.
- Wall time.
- RSS.
- p50/p95/p99 parse latency when sampled.
- Parse degradation rate.
- Output size by file.

## Datasets

- 1 GB synthetic NDJSON.
- 10 GB synthetic mixed JSON/text.
- Real service sample when available and approved for local analysis.

## Trials

- 1 warmup.
- 3 measured trials.
- Record mean and standard deviation.

## Rust Core Criteria

Enable Rust by default only if it provides a clear improvement on representative datasets:

- Faster file splitting and tokenization.
- Equal template IDs and classifications.
- Lower or comparable RSS.
- No loss in degraded-record accounting.

## Future Optimizations

- `simd-json` for JSON parsing if benchmarks justify it.
- Streaming Arrow record batches instead of holding all events for small/medium runs.
- DataFusion or Polars for large compare workloads.
- Static source-template seeding from `logging.*` calls to reduce template churn.
