# LogsReaper vs drain3

- Date: 2026-05-20T08:38:17.045949+00:00
- Machine: Linux-6.17.0-29-generic-x86_64-with-glibc2.39
- Input: `/tmp/logs-reaper-128mb.ndjson`
- Input bytes: 134217764
- Warmup trials: 1
- Measured trials: 3

## Scope

- `LogsReaper`: Rust hot path via `logs_reaper.rust_engine.scan_file_to_ipc`.
- `drain3`: Python `TemplateMiner.add_log_message()` over `message` after NDJSON `json.loads`.
- This is not a full feature-parity comparison: Drain3 is a template miner, while LogsReaper also parses and aggregates structured output.

## Results

| Engine | MB/s mean | MB/s stdev | Events/s mean | RSS MB mean | Template count mean |
| --- | ---: | ---: | ---: | ---: | ---: |
| LogsReaper | 260.06 | 7.96 | 1394421 | 1340.7 | 2 |
| drain3 | 39.09 | 0.32 | 209619 | 19.9 | 1 |

- Throughput ratio `LogsReaper / drain3`: 6.65x

