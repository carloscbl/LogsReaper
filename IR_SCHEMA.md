# LogsReaper IR Schema

The IR is aligned with the OpenTelemetry Logs Data Model vocabulary where practical: timestamp, severity, body, resource attributes, trace/span IDs, and custom attributes.

## events.parquet

One row per parsed logical log record.

| Column | Type | Description |
| --- | --- | --- |
| `event_id` | string | Stable hash of run id, source, offset, and raw hash. |
| `run_id` | string | User supplied scan id. |
| `source` | string | Input file path. |
| `offset` | int64 | Byte offset of the first physical line. |
| `line_count` | int64 | Physical lines grouped into this event. |
| `timestamp` | string | Log timestamp when present. |
| `observed_timestamp` | string | Scan timestamp. |
| `severity_text` | string | Canonical severity. |
| `severity_number` | int64 | OpenTelemetry-style severity band base. |
| `body` | string | Parsed log body. |
| `normalized_template` | string | Body after token normalization. |
| `template_id` | string | Stable template hash. |
| `error_kind` | string | Exception type, `traceback`, `log_error`, or `none`. |
| `exception_type` | string | Extracted exception type if present. |
| `parse_format` | string | `json` or `text`. |
| `parse_status` | string | `ok` or `degraded`. |
| `classification` | string | Template-derived class copied to event. |
| `service_name` | string | Resource service name. |
| `worker_id` | string | Worker identity when available. |
| `thread_name` | string | Thread identity when available. |
| `process_name` | string | Process name when available. |
| `process_pid` | int64 | Process pid when available. |
| `server_kind` | string | Runtime family hint. |
| `trace_id` | string | Trace id when available. |
| `span_id` | string | Span id when available. |
| `container_id` | string | Container id when available. |
| `k8s_pod_name` | string | Kubernetes pod name when available. |
| `k8s_container_name` | string | Kubernetes container name when available. |
| `attributes_json` | string | Custom attributes serialized as JSON. |
| `resource_json` | string | Resource attributes serialized as JSON. |
| `raw_hash` | string | Hash of the raw logical record. |
| `raw` | string | Raw record only when `--include-raw` is set. |

## templates.parquet

One row per unique template in a run.

| Column | Description |
| --- | --- |
| `template_id` | Stable template hash. |
| `service_name` | Service associated with the template. |
| `severity_text` | Canonical severity. |
| `normalized_template` | Token-normalized body template. |
| `error_kind` | Error kind used in identity. |
| `event_count` | Count of matching events. |
| `first_seen`, `last_seen` | First and last event timestamps. |
| `classification` | `expected`, `unexpected`, `known-noise`, or `observed`. |
| `baseline_match` | Whether the template was found in the baseline. |

## errors.parquet

Error-oriented subset of `templates.parquet` containing error severities, exception kinds, known noise, and unexpected templates.

## run.json and summary.json

`run.json` stores provenance: run id, input globs, file list, service, library versions, hash algorithm, rules, baseline, counts, and runtime cardinalities.

`summary.json` stores compact counts for CI and dashboards.
