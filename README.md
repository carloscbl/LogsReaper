# LogsReaper

LogsReaper is a standalone MVP for massive service logs. It parses JSON, NDJSON, and text logs, groups traceback records, mines stable normalized templates, classifies expected and unexpected errors, and emits an intermediate representation optimized for debugging, CI, analytics, and ML.

The tool is self-contained and does not require any changes in the services that produce the logs.

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
