# LogsReaper Benchmark Results

## 2026-05-20: LogsReaper vs drain3 on synthetic 128 MiB NDJSON

- Machine: `Linux-6.17.0-29-generic-x86_64-with-glibc2.39`
- Input: `/tmp/logs-reaper-128mb.ndjson`
- Input size: `134217764` bytes
- Warmup: `1`
- Measured trials: `3`
- LogsReaper path: `/usr/bin/python3` -> `logs_reaper.rust_engine.scan_file_to_ipc`
- drain3 path: `/tmp/logsreaper-bench-venv/bin/python` -> `TemplateMiner.add_log_message(message)` after `json.loads`

Results:

- LogsReaper: `260.06 MB/s` mean, `7.96 MB/s` stdev, `1,394,421 events/s` mean, `1340.7 MB RSS` mean, `2` templates.
- drain3: `39.09 MB/s` mean, `0.32 MB/s` stdev, `209,619 events/s` mean, `19.9 MB RSS` mean, `1` template.
- Throughput ratio `LogsReaper / drain3`: `6.65x`

Artifacts:

- JSON: `benchmarks/results/logsreaper-vs-drain3-128mb.json`
- Markdown: `benchmarks/results/logsreaper-vs-drain3-128mb.md`

Caveat: this is a hot-path comparison, not full feature parity. Drain3 is only mining templates from the `message` field, while LogsReaper parses full NDJSON and emits structured Arrow outputs.
