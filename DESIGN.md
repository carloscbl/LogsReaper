# LogsReaper Design

## Goals

LogsReaper is a repo-local, deterministic log analysis tool. Its first version focuses on:

- Parsing massive JSON, NDJSON, and text logs.
- Producing stable template and event IDs across runs.
- Comparing service runs by template, severity, service, version, and library metadata.
- Emitting Parquet IR suitable for ad hoc analysis, CI gates, and ML dataset creation.
- Avoiding online LLM calls in classification.

## Non-Goals for v1

- Replacing service logging libraries.
- Shipping an always-on collector.
- Requiring Rust compilation for normal use.
- Proving semantic correctness for every third-party log format.

## Data Flow

1. Resolve input globs and directories.
2. Read files linearly and group continuation lines into logical records.
3. Parse JSON records first, then known text formats, then generic text fallback.
4. Normalize high-cardinality tokens into placeholders.
5. Compute stable event and template IDs.
6. Aggregate templates and classify them with YAML rules and optional baseline output.
7. Write Parquet, JSON metadata, and Markdown reports.

## Python and Rust Split

The Python package is the control plane:

- CLI.
- Rules and baseline classification.
- Parquet/JSON/report writing.
- Testable fallback parser.

The Rust core scaffold is the intended acceleration plane:

- Parallel file splitting with `rayon` and `memchr`.
- Regex/tokenization with `regex-automata`.
- Stable BLAKE3 hashing.
- Optional Python binding via PyO3/maturin.

The Python path remains authoritative until benchmarks show the Rust core is worth enabling by default.

## Runtime Extraction

LogsReaper extracts runtime hints from structured fields, text, file paths, and rules:

- `service.name`, `service.instance.id`, `microservice`.
- `worker_id`, `threadName`, `thread.name`.
- `processName`, `process.name`, `pid`, `process.pid`.
- `server.kind` for `gunicorn`, `granian`, `kafka`, `mongo`, and `chroot` when present.
- `trace_id`, `span_id`, container IDs, pod names, and container names.

The extraction is intentionally conservative. Unknown fields are preserved as JSON strings in `attributes_json` and `resource_json`.
