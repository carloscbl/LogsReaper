# LogsReaper Benchmarks

Run benchmarks from `.` after installing dependencies:

```bash
python3 benchmarks/generate_synthetic.py --out /tmp/logs-reaper-1gb.ndjson --target-mb 1024
python3 -m logs_reaper scan --input /tmp/logs-reaper-1gb.ndjson --run-id BENCH_1GB --service synthetic --out out/BENCH_1GB
```

Compare LogsReaper hot path vs `drain3` on the same NDJSON:

```bash
python3 benchmarks/generate_synthetic.py --out /tmp/logs-reaper-256mb.ndjson --target-mb 256
python3 benchmarks/compare_logsreaper_vs_drain3.py   --input /tmp/logs-reaper-256mb.ndjson   --out-json benchmarks/results/logsreaper-vs-drain3.json   --out-md benchmarks/results/logsreaper-vs-drain3.md
```

Notes:

- `compare_logsreaper_vs_drain3.py` uses `python3` for LogsReaper and `/tmp/logsreaper-bench-venv/bin/python` for `drain3`.
- The `drain3` side measures `json.loads` + `TemplateMiner.add_log_message(message)`.
- The `LogsReaper` side measures the Rust hot path via `scan_file_to_ipc`.

Record results in `benchmarks/results.md` with:

- machine details
- Python/Rust path
- input size
- event count
- wall time
- RSS
- output size
- parse degradation count
