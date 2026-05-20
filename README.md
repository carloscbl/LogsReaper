# LogsReaper

LogsReaper is a standalone tool for massive service logs. It parses JSON, NDJSON, and text logs, groups traceback records, mines stable normalized templates, classifies expected and unexpected errors, and emits an intermediate representation optimized for debugging, CI, analytics, and ML.

The tool is self-contained and does not require any changes in the services that produce the logs.

## Performance

LogsReaper does substantially more than a pure template miner on its hot path — it parses NDJSON, normalizes via a regex pipeline, runs a custom Rust Drain over already-normalized templates, aggregates counters, and writes structured Arrow outputs. Even so, it is significantly faster than the reference `drain3` Python implementation on the same input.

Synthetic 128 MiB NDJSON, 686,320 events, 1 warmup + 3 measured trials (`benchmarks/compare_logsreaper_vs_drain3.py`):

| Engine     | Throughput (MB/s) | Events/s    | Mean RSS (MB) | Notes                                       |
|------------|------------------:|------------:|--------------:|---------------------------------------------|
| LogsReaper |            260.06 |   1,394,421 |          1340 | NDJSON parse + normalize + Drain + Arrow out |
| drain3     |             39.09 |     209,619 |            20 | Template mining over `message` after `json.loads` |
| **Ratio**  |        **6.65×**  |    **6.65×**|             — | LogsReaper / drain3                          |

Caveat: this is a hot-path comparison, not full feature parity. `drain3` is only mining templates from the `message` field; LogsReaper covers the whole pipeline end-to-end. The full reports live in `benchmarks/results/`.

## Engine Architecture

LogsReaper is split into two cleanly separated layers:

- **Rust core (`rust/logs_reaper_core/`)** — the hot path. It owns log ingestion, line-continuation detection, JSON/NDJSON/text parsing, template normalization, BLAKE3 hashing and template aggregation. The binary `logs_reaper_hotpath` returns both per-event rows and pre-aggregated template rows for each input file. This is what runs every time you `scan`, and it is the source of truth for `run.json["engine"] == "rust"`.
- **Python management layer (`logs_reaper/`)** — classification rules, baseline diffing, parquet/JSON/markdown outputs, comparison, dataset export and Docker autodiscovery. Python never re-parses or re-aggregates templates when Rust succeeded; it consumes the templates Rust already produced and only applies higher-level concerns.

`run.json` exposes an explicit `engine` field with three possible values:

- `rust` — every input file was processed by the Rust binary.
- `python-fallback` — Rust failed for every input and the pure-Python parser handled them.
- `mixed` — at least one file went through Rust and at least one went through the Python fallback (templates are merged by `template_id`).

If Rust ever falls back per-file (e.g., the binary is missing or crashes on a specific input), Python `logs_reaper.parser` mirrors the Rust parsing/normalization so template IDs stay stable across both paths.

## Quick Start

```bash
python3 -m logs_reaper scan --service my-service
python3 -m logs_reaper scan --input "logs/**/*.log*" --run-id RUN_A --service my-api --lib granian=2.x --out out/RUN_A
python3 -m logs_reaper compare-engines --input out/my-service/captures/latest.log --service my-service
python3 -m logs_reaper compare --left out/RUN_A --right out/RUN_B --out out/diff.md
python3 -m logs_reaper export-dataset --input out/RUN_A --out out/ml-dataset.ndjson
```

Installable entry point after packaging:

```bash
logs-reaper scan --service my-service
logs-reaper scan --input "logs/**/*.log*" --run-id RUN_A --service my-api --lib granian=2.x --out out/RUN_A
```

## Basic Fast Guide

Caso mas simple, servicio levantado en Docker:

```bash
python3 -m logs_reaper scan --service my-service
```

Que hace por defecto:

- Busca el contenedor Docker activo que corresponda al servicio.
- Si encuentra ficheros de log montados y no vacios, los usa.
- Si no, captura `docker logs` del contenedor.
- Guarda resultados en `out/<service>/<run_id>/`.
- Si el fingerprint del snapshot coincide con uno ya escaneado, reutiliza el scan previo y no reprocesa.
- Si el contenedor es el mismo pero los logs cambiaron, marca `same_container_new_logs`.
- Si cambió el contenedor, marca `new_container`.

Comandos basicos:

```bash
python3 -m logs_reaper scan --service my-service
python3 -m logs_reaper scan --service my-service --since 15m
python3 -m logs_reaper compare --left out/my-service/MY_SERVICE_20260514T120000Z --right out/my-service/MY_SERVICE_20260514T121500Z --out out/my-service/diff.md
python3 -m logs_reaper export-dataset --input out/my-service/MY_SERVICE_20260514T121500Z --out out/my-service/dataset.ndjson
```

Donde mirar:

- `out/<service>/<run_id>/summary.json`
- `out/<service>/<run_id>/report.md`
- `out/<service>/service-scan-registry.json`

## Running with Docker (sidecar pattern)

The primary deployment shape of LogsReaper is as a **sidecar** that runs next to your application containers and ingests their logs through the host's docker socket. No code changes are required in the apps you want to observe.

### Building the image

```bash
docker build -t logs-reaper:dev .
```

This produces a multi-stage image (~280 MB) that bundles the Python CLI, the Rust pyo3 hot-path wheel and the docker CLI client.

### Standalone one-shot

```bash
docker run --rm \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  -v "$(pwd)/out:/work/out" \
  -v "$(pwd)/baselines:/work/baselines" \
  logs-reaper:dev collect --services app,worker --duration 60
```

What it needs:

- **`/var/run/docker.sock`** mounted read-only — so the container can call `docker logs -f` on its siblings.
- **`/work/out`** — persistent scan outputs (`run.json`, `events.parquet`, `report.md`, ...).
- **`/work/baselines`** — persistent per-service template baselines used by the diff engine.

### docker-compose (recommended)

The repo ships a ready-to-use [`docker-compose.yml`](./docker-compose.yml) that wires LogsReaper alongside two example services (`app` and `worker`). LogsReaper auto-discovers any sibling whose container name matches `<COMPOSE_PROJECT_NAME>-*-1`:

```bash
COMPOSE_PROJECT_NAME=myapp docker compose up -d
docker compose logs -f logs-reaper          # watch ingestion
docker compose run --rm logs-reaper scan --service app   # ad-hoc scan
```

Drop the `command:` block (or change it to `dashboard`) to expose the Streamlit dashboard on port 8501 instead of (or alongside) the collector.

### Useful subcommands when running inside the container

```bash
# Continuous ingest (foreground); writes one capture file per service:
logs-reaper collect --services all --prefix myapp --duration 600

# Single scan over an already-captured file:
logs-reaper scan --service app --input /work/out/app/captures/latest.log

# Streamlit dashboard (browse runs, deltas, Jira hand-off):
logs-reaper dashboard --registry-root /work/out --host 0.0.0.0 --port 8501

# End-to-end CI pipeline (collect + scan + index + diff + report):
logs-reaper ci-run --services all --duration 300 --out /work/out
```

The `--prefix` flag scopes auto-discovery to a single docker-compose project (it matches `<prefix>-<service>-1`). Leave it empty to match every running container ending in `-1`.

## Service Instances (restart awareness)

A single `docker logs` capture often spans multiple service boots (gunicorn / Granian restarts, container relaunches…). LogsReaper detects those boots and groups events into numbered **instances**, so older runs do not pollute your analysis.

How detection works:

- Python walks events in source order and tags each one with `service_instance_seq` + `service_instance_started_at`.
- Boots are identified from body markers like `Starting gunicorn`, `Starting Granian`, `Granian server starting`, `Booting worker with pid:`, or `Application startup complete`.
- Markers within 30 seconds of the previous boot are coalesced into the same boot event.
- Events before the first detected boot belong to instance `0` (pre-boot history).

By default, `scan` keeps only the most recent instance:

```bash
python3 -m logs_reaper scan --service accounts                # last boot only (default)
python3 -m logs_reaper scan --service accounts --instances all  # everything, no filtering
python3 -m logs_reaper scan --service accounts --instances 2    # last 2 boots
```

When the filter drops events, templates are re-aggregated in Python from the kept events (so the per-template `event_count` stays consistent with what made it into `events.parquet`). When nothing is filtered (`--instances all` or zero detected boots), the Rust-provided templates are reused verbatim — no extra Python work.

### Reverse-scan optimisation

`--instances last` (with a single input file) does not parse the whole capture. Before invoking Rust, Python performs a reverse byte scan from the end of the file in 64 KiB blocks looking for primary boot markers. When it finds the most recent one, it aligns the offset to the start of that line and passes `--start-offset N` to the Rust binary, which `seek()`s straight there.

In a real `accounts` capture this drops parsing work from 16 MiB to 37 KiB (~100× faster wall clock) without changing the templates or event counts of the last instance. `run.json["instances"]` reports `tail_anchor_offset`, `parsed_input_bytes` and `total_input_bytes`, and the report shows the percentage of the file that was skipped before parsing.

`run.json` exposes an `instances` block with `spec`, `filter_active`, `kept_seqs`, `dropped_event_count`, `tail_anchor_offset` and a `detected` list with each boot's start timestamp. The `report.md` includes a *Service Instances* section showing the same information at a glance.

## Code vs Infrastructure focus

Every template and event is tagged with an `issue_kind` of `code`, `infra` or `unknown`:

- **code** — likely a defect in our own services: tracebacks, Python-style exception types (`AttributeError`, `ValueError`, `TypeError`, `AssertionError`…) and asserts.
- **infra** — likely an environment / dependency problem: Kafka `_ALL_BROKERS_DOWN` / `_TRANSPORT`, `Connection refused`, Mongo timeouts, DNS failures, Elasticsearch unreachable, etc.
- **unknown** — anything not matched by the heuristics.

`report.md` is structured so each focus is easy to share with the right audience:

- **Code Issues (engineer focus)** — surfaces only `issue_kind=code` templates so an SRE can raise an alarm with a backend engineer.
- **Infrastructure Issues (ops focus)** — surfaces only `issue_kind=infra` templates plus a *Connectivity timeline* block.

The `--focus` flag controls which sections are rendered:

```bash
python3 -m logs_reaper scan --service my-service --focus both   # default
python3 -m logs_reaper scan --service my-service --focus code   # engineer view only
python3 -m logs_reaper scan --service my-service --focus infra  # ops view only
```

### Connectivity timeline

Built by walking events in time order and pairing "down" markers (`brokers are down`, `_ALL_BROKERS_DOWN`, `_TRANSPORT`, `Connection refused`, `ServerSelectionTimeoutError`, …) with "up" markers (`rejoined group`, `Connected to mongodb`, `partitions assigned`, …). Each detected dependency (kafka, mongo, elasticsearch) gets a row per incident with `down_at`, `up_at`, `duration` and a sample. When `up_at` is missing the dependency never came back during the analysed window — the report marks it `(no recovery)` and the `current state` reads `down`.

## Progress indicator

For large captures (multi-GB), the Rust binary streams `PROGRESS bytes_read=… bytes_total=… events=…` lines to stderr at ~80 ms / 256 KiB intervals. Python renders an in-place progress bar to stderr when stderr is a TTY, and falls back to a quiet 1-line-per-second log otherwise. Per-phase status lines (`Locating last service boot`, `Parsing [1/1] file.log (Rust)`, `Detecting service instances`, `Aggregating templates`, `Classifying templates against rules and baseline`, `Building connectivity timeline`, `Writing outputs to …`) keep the user informed even when Rust is fast enough that the bar barely flashes.

## Outputs

- `events.parquet`: normalized event per physical or logical log record (produced by the Rust core; Python only writes parquet).
- `templates.parquet`: unique normalized template, counters, severity, first/last seen, and classification. Template rows are aggregated inside the Rust core; Python only adds classification metadata on top.
- `errors.parquet`: error-oriented template view with expected/unexpected/noise reason.
- `run.json`: run metadata, input files, library versions, hash algorithm, runtime cardinalities and the `engine` used (`rust`, `python-fallback`, or `mixed`).
- `summary.json`: compact CI/debug summary.
- `report.md`: human-readable report.

Parquet is written with Zstd compression through `pyarrow`.

## Stable Identity

- `template_id = hash(service.name, severity_text, normalized_template, error_kind)`.
- `event_id = hash(run_id, source, offset, raw_hash)`.
- Python uses BLAKE3 when the optional `blake3` package is installed and falls back to BLAKE2b-128 when it is not. The Rust core scaffold uses BLAKE3 as the target fast path.

## Classification

Classification is deterministic and offline:

- `known-noise`: first match in `known_noise` rules.
- `expected`: explicit rule match or template already present in `--baseline`.
- `unexpected`: new error template, exception type, or configured error severity not covered by rules/baseline.
- `observed`: non-error template observed in the run.
- `regression` and `fixed`: assigned by `compare`.

Rules are YAML and default to `configs/default-rules.yaml`.

## Parser Scope

- JSON/NDJSON autodetection.
- Pluggable text format adapters for in-house log layouts.
- Generic timestamp + severity text lines.
- Traceback and continuation lines grouped into the previous event.
- Malformed JSON falls back to text parsing with `parse_status=degraded`.

## Dataset Export

`export-dataset` writes event-level NDJSON without raw log records by default. It includes normalized templates and classification labels. Use `--include-body` only for controlled debugging because bodies can contain PII.
