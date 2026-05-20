from __future__ import annotations

import glob
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

try:
    import orjson
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    orjson = None

BATCH_SIZE = 8192

# Many event-level string columns are extremely low-cardinality on real captures (service_name,
# severity_text, parse_format etc. are single-digit; body / normalized_template / attributes_json
# are bounded by the number of unique templates, low hundreds to low thousands). Dictionary
# encoding turns these into a small dictionary plus an int32 index per row, cutting in-memory
# size by orders of magnitude on multi-million-row captures. Parquet handles DICTIONARY-encoded
# columns natively; readers that don't pass `read_dictionary` get plain strings back.
_DICT_STRING = pa.dictionary(pa.int32(), pa.string())


EVENT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("run_id", _DICT_STRING),
        ("source", _DICT_STRING),
        ("offset", pa.int64()),
        ("line_count", pa.int64()),
        ("timestamp", pa.string()),
        ("observed_timestamp", _DICT_STRING),
        ("severity_text", _DICT_STRING),
        ("severity_number", pa.int64()),
        ("body", _DICT_STRING),
        ("normalized_template", _DICT_STRING),
        ("template_id", _DICT_STRING),
        ("error_kind", _DICT_STRING),
        ("exception_type", _DICT_STRING),
        ("parse_format", _DICT_STRING),
        ("parse_status", _DICT_STRING),
        ("classification", _DICT_STRING),
        ("classification_reason", _DICT_STRING),
        ("service_name", _DICT_STRING),
        ("service_instance_id", _DICT_STRING),
        ("worker_id", _DICT_STRING),
        ("thread_name", _DICT_STRING),
        ("process_name", _DICT_STRING),
        ("process_pid", pa.int64()),
        ("server_kind", _DICT_STRING),
        ("trace_id", _DICT_STRING),
        ("span_id", _DICT_STRING),
        ("container_id", _DICT_STRING),
        ("k8s_pod_name", _DICT_STRING),
        ("k8s_container_name", _DICT_STRING),
        ("attributes_json", _DICT_STRING),
        ("resource_json", _DICT_STRING),
        ("raw_hash", pa.string()),
        ("raw", _DICT_STRING),
        ("service_instance_seq", pa.int64()),
        ("service_instance_started_at", _DICT_STRING),
        ("issue_kind", _DICT_STRING),
    ]
)

TEMPLATE_SCHEMA = pa.schema(
    [
        ("template_id", pa.string()),
        ("service_name", pa.string()),
        ("severity_text", pa.string()),
        ("severity_number", pa.int64()),
        ("normalized_template", pa.string()),
        ("error_kind", pa.string()),
        ("exception_type", pa.string()),
        ("event_count", pa.int64()),
        ("first_seen", pa.string()),
        ("last_seen", pa.string()),
        ("example_event_id", pa.string()),
        ("parse_status", pa.string()),
        ("classification", pa.string()),
        ("classification_reason", pa.string()),
        ("baseline_match", pa.bool_()),
        ("issue_kind", pa.string()),
    ]
)

ERROR_SCHEMA = pa.schema(
    [
        ("template_id", pa.string()),
        ("service_name", pa.string()),
        ("severity_text", pa.string()),
        ("error_kind", pa.string()),
        ("exception_type", pa.string()),
        ("classification", pa.string()),
        ("reason", pa.string()),
        ("event_count", pa.int64()),
        ("first_seen", pa.string()),
        ("last_seen", pa.string()),
        ("baseline_match", pa.bool_()),
        ("normalized_template", pa.string()),
        ("issue_kind", pa.string()),
    ]
)


def resolve_inputs(patterns: list[str]) -> list[Path]:
    paths: list[Path] = []
    seen: set[Path] = set()
    for pattern in patterns:
        if pattern.endswith(".inputs.json"):
            listed = json.loads(Path(pattern).read_text(encoding="utf-8"))
            if isinstance(listed, list):
                nested_patterns = [str(item) for item in listed]
                for path in resolve_inputs(nested_patterns):
                    resolved = path.resolve()
                    if resolved not in seen:
                        paths.append(path)
                        seen.add(resolved)
                continue
        matches = [Path(item) for item in glob.glob(pattern, recursive=True)]
        if not matches:
            candidate = Path(pattern)
            if candidate.exists():
                matches = [candidate]
        for match in matches:
            if match.is_dir():
                nested = sorted(path for path in match.rglob("*") if path.is_file())
            else:
                nested = [match]
            for path in nested:
                resolved = path.resolve()
                if resolved not in seen:
                    paths.append(path)
                    seen.add(resolved)
    return sorted(paths)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if orjson:
        data = orjson.dumps(payload, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS).decode("utf-8")
    else:
        data = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    path.write_text(data + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_parquet(path: Path, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
    import os as _os
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    writer: pq.ParquetWriter | None = None
    try:
        for start in range(0, len(rows), BATCH_SIZE):
            batch_rows = rows[start : start + BATCH_SIZE]
            table = pa.Table.from_pylist(batch_rows, schema=schema)
            if writer is None:
                writer = pq.ParquetWriter(tmp, schema, compression="zstd", use_dictionary=True)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
    if tmp.exists():
        _os.replace(tmp, path)


def write_parquet_table(path: Path, table: pa.Table, schema: pa.Schema) -> None:
    """Write a pyarrow.Table to Parquet straight, without round-tripping through Python dicts.

    Used by the Arrow-native pipeline: events stay columnar from Rust all the way to disk, which
    is the single biggest speedup on multi-GB captures. Escribimos a `.tmp` y renombramos para
    que readers concurrentes (dashboard) no pillen el fichero a medias.
    """
    import os as _os
    path.parent.mkdir(parents=True, exist_ok=True)
    aligned = _align_table_to_schema(table, schema)
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(aligned, tmp, compression="zstd", use_dictionary=True)
    _os.replace(tmp, path)


def _align_table_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    columns: list[Any] = []
    for field in schema:
        if field.name in table.column_names:
            arr = table.column(field.name)
            if arr.type != field.type:
                arr = arr.cast(field.type, safe=False)
            columns.append(arr)
        else:
            columns.append(pa.nulls(table.num_rows, type=field.type))
    return pa.table(columns, schema=schema)


def read_parquet(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    table = pq.read_table(path)
    return table.to_pylist()
