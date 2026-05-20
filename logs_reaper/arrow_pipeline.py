"""Arrow-native versions of the LogsReaper pipeline stages.

These work directly on `pyarrow.Table` instances and avoid converting events to `list[dict]`
until the very last moment (or not at all when the value is just an aggregate). On large
captures (>=1 GiB) the `Table.to_pylist()` roundtrip costs MORE than the Rust parse itself, so
keeping data columnar end-to-end is the single biggest end-to-end speedup we can get.
"""

from __future__ import annotations

import re
from collections import Counter
from datetime import datetime
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from .issue_kind import classify_issue_kind
from .instances import (
    BOOT_COOLDOWN_SECONDS,
    BOOT_PATTERNS,
    INSTANCES_ALL,
    INSTANCES_LAST,
)
from .timeline import DOWN_PATTERNS, UP_PATTERNS

def _extract_pattern_source(pattern: re.Pattern[Any]) -> str:
    src = pattern.pattern
    if isinstance(src, bytes):
        src = src.decode("utf-8", errors="replace")
    return src


# Combined regex used as a fast vectorised prefilter on the body column. We still apply the
# individual patterns afterwards (to coalesce by cooldown), but on a tiny subset of rows.
_BOOT_PREFILTER = "|".join(_extract_pattern_source(p) for p in BOOT_PATTERNS)

# Combined down/up prefilters for the connectivity timeline. Stored as plain regex strings;
# pyarrow.compute.match_substring_regex accepts the pattern directly.
_DOWN_PREFILTER_BY_SVC = {
    service: "|".join(_extract_pattern_source(pattern) for pattern in patterns)
    for service, patterns in DOWN_PATTERNS.items()
}
_UP_PREFILTER_BY_SVC = {
    service: "|".join(_extract_pattern_source(pattern) for pattern in patterns)
    for service, patterns in UP_PATTERNS.items()
}


def match_substring_regex_smart(col, pattern: str, ignore_case: bool = True):
    """Regex-match across either a plain string column or a dictionary-encoded one.

    PyArrow's `match_substring_regex` kernel has no implementation for dictionary inputs, but
    we use dict-encoding heavily to keep RSS low on large captures. We exploit the fact that
    the dictionary itself is small: run the regex over the (few hundred) unique values once,
    then propagate the boolean result back to per-event positions via `pc.take` against the
    indices. End result: identical semantics, far less work than decoding the full column.
    """
    if isinstance(col, pa.ChunkedArray):
        if pa.types.is_dictionary(col.type):
            chunks = []
            for chunk in col.chunks:
                if chunk.dictionary.null_count > 0:
                    dict_mask = pc.match_substring_regex(
                        pc.fill_null(chunk.dictionary, ""), pattern, ignore_case=ignore_case
                    )
                else:
                    dict_mask = pc.match_substring_regex(
                        chunk.dictionary, pattern, ignore_case=ignore_case
                    )
                chunk_mask = pc.take(dict_mask, chunk.indices)
                chunks.append(chunk_mask)
            return pa.chunked_array(chunks)
        return pc.match_substring_regex(col, pattern, ignore_case=ignore_case)
    # Single (non-chunked) array
    if pa.types.is_dictionary(col.type):
        dict_mask = pc.match_substring_regex(col.dictionary, pattern, ignore_case=ignore_case)
        return pc.take(dict_mask, col.indices)
    return pc.match_substring_regex(col, pattern, ignore_case=ignore_case)


# ---------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------

PYTHON_ADDED_COLUMNS = ("service_instance_seq", "service_instance_started_at", "issue_kind")


def ensure_python_columns(table: pa.Table) -> pa.Table:
    """Ensure the table has the columns Python adds during the pipeline.

    Rust writes the bulk of the schema; Python tops up `service_instance_seq` (int64),
    `service_instance_started_at` (utf8) and `issue_kind` (utf8). When constructing a Table from
    a Python fallback list[dict] these may already be present; when coming from Rust they are not.
    """
    additions: dict[str, pa.Array] = {}
    n = table.num_rows
    dict_string = pa.dictionary(pa.int32(), pa.string())
    if "service_instance_seq" not in table.column_names:
        additions["service_instance_seq"] = pa.nulls(n, type=pa.int64())
    if "service_instance_started_at" not in table.column_names:
        additions["service_instance_started_at"] = pa.nulls(n, type=dict_string)
    if "issue_kind" not in table.column_names:
        additions["issue_kind"] = pa.nulls(n, type=dict_string)
    if not additions:
        return table
    for name, array in additions.items():
        table = table.append_column(name, array)
    return table


def replace_column(table: pa.Table, name: str, array: pa.Array | pa.ChunkedArray) -> pa.Table:
    """Replace a column by name (appending it if missing)."""
    if name in table.column_names:
        idx = table.column_names.index(name)
        return table.set_column(idx, name, array)
    return table.append_column(name, array)


def reorder_table_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Return a table whose columns match `schema` (order + types). Missing columns become null."""
    columns: list[pa.Array | pa.ChunkedArray] = []
    for field in schema:
        if field.name in table.column_names:
            arr = table.column(field.name)
            if arr.type != field.type:
                arr = arr.cast(field.type, safe=False)
            columns.append(arr)
        else:
            columns.append(pa.nulls(table.num_rows, type=field.type))
    return pa.table(columns, schema=schema)


# ---------------------------------------------------------------------------------------
# Instances detection
# ---------------------------------------------------------------------------------------


def annotate_instances_table(table: pa.Table) -> tuple[pa.Table, list[dict[str, Any]]]:
    """Vectorised version of `annotate_instances`.

    Strategy:
      1. Find all rows whose `body` matches a boot marker via `pc.match_substring_regex` —
         this single regex pass replaces the per-event Python loop over millions of rows.
      2. Materialise timestamp/observed_timestamp/event_id ONLY for the tiny set of boot
         indices (via `pc.take`), not for the full table.
      3. Build the `service_instance_seq` column with `np.cumsum` over a boolean mask, and the
         `service_instance_started_at` column with a vectorised `pc.take` on a tiny lookup array.
    """
    n = table.num_rows
    if n == 0:
        table = replace_column(table, "service_instance_seq", pa.array([], type=pa.int64()))
        table = replace_column(table, "service_instance_started_at", pa.array([], type=pa.string()))
        return table, []
    body_col = table.column("body")
    boot_mask_arr = match_substring_regex_smart(body_col, _BOOT_PREFILTER, ignore_case=True)
    boot_mask = np.asarray(boot_mask_arr.combine_chunks().to_numpy(zero_copy_only=False))
    boot_indices = np.where(boot_mask)[0]
    if boot_indices.size == 0:
        table = replace_column(table, "service_instance_seq", pa.array(np.zeros(n, dtype=np.int64)))
        table = replace_column(table, "service_instance_started_at", pa.nulls(n, type=pa.string()))
        return table, []

    boot_idx_pa = pa.array(boot_indices.tolist(), type=pa.int64())
    ts_at_boots = pc.take(table.column("timestamp"), boot_idx_pa).to_pylist()
    obs_at_boots = pc.take(table.column("observed_timestamp"), boot_idx_pa).to_pylist()
    eid_at_boots = pc.take(table.column("event_id"), boot_idx_pa).to_pylist()

    instances: list[dict[str, Any]] = []
    confirmed_idx: list[int] = []
    confirmed_ts: list[str | None] = []
    last_seconds: float | None = None
    for k, idx in enumerate(boot_indices):
        ts = ts_at_boots[k] or obs_at_boots[k]
        seconds = _timestamp_to_seconds(ts)
        if last_seconds is None or seconds is None or (seconds - last_seconds) > BOOT_COOLDOWN_SECONDS:
            confirmed_idx.append(int(idx))
            confirmed_ts.append(ts)
            instances.append(
                {
                    "seq": len(instances) + 1,
                    "started_at": ts,
                    "first_event_id": eid_at_boots[k],
                    "event_count": 0,
                }
            )
        if seconds is not None:
            last_seconds = seconds

    # Build the seq column: each confirmed boot bumps the seq number by 1 from that row onward.
    boot_at = np.zeros(n, dtype=np.int64)
    for idx in confirmed_idx:
        boot_at[idx] = 1
    seq = np.cumsum(boot_at, dtype=np.int64)

    # Build started_at: lookup the seq number into a small list of boot timestamps via
    # pyarrow take. The lookup array has len(instances)+1 entries (index 0 = no boot yet).
    started_lookup = pa.array([None] + confirmed_ts, type=pa.string())
    started_arr = pc.take(started_lookup, pa.array(seq))

    # Event counts per instance: bincount on the seq vector.
    if instances:
        counts = np.bincount(seq, minlength=len(instances) + 1)
        for i, instance in enumerate(instances, start=1):
            instance["event_count"] = int(counts[i])

    table = replace_column(table, "service_instance_seq", pa.array(seq))
    table = replace_column(table, "service_instance_started_at", started_arr)
    return table, instances


def filter_events_table_by_instance(table: pa.Table, keep: set[int] | None) -> tuple[pa.Table, int]:
    if keep is None:
        return table, 0
    keep_array = pa.array(sorted(keep), type=pa.int64())
    mask = pc.is_in(table.column("service_instance_seq"), value_set=keep_array)
    filtered = table.filter(mask)
    dropped = table.num_rows - filtered.num_rows
    return filtered, dropped


# ---------------------------------------------------------------------------------------
# Template lookups onto the events table (classification + issue_kind)
# ---------------------------------------------------------------------------------------


def apply_template_lookup_columns(
    table: pa.Table,
    templates: list[dict[str, Any]],
) -> pa.Table:
    """Apply `classification`, `classification_reason` and `issue_kind` columns by template_id.

    Classification/issue_kind are functions of the template only — events sharing a template_id
    share these three values. We exploit that with a vectorised lookup: `pc.index_in` maps each
    event's template_id to its position in the small (<1k) unique-template array, then
    `pc.take` projects the per-template values back to the per-event arrays. Total cost is
    O(unique_templates) + a single Arrow pass, with no per-event Python materialisation.
    """
    classification_by_tid: dict[str, str] = {}
    reason_by_tid: dict[str, str | None] = {}
    issue_kind_by_tid: dict[str, str] = {}
    for row in templates:
        tid = str(row.get("template_id"))
        classification_by_tid[tid] = row.get("classification") or "unclassified"
        reason_by_tid[tid] = row.get("classification_reason")
        issue_kind_by_tid[tid] = row.get("issue_kind") or "unknown"

    tids_col = table.column("template_id")
    if isinstance(tids_col, pa.ChunkedArray) and pa.types.is_dictionary(tids_col.type):
        # Fast path: walk per-chunk dictionaries directly. Each dictionary is small (~700
        # entries on 1 GiB captures) so building per-dict lookup arrays is cheap, and `take`
        # against the existing int32 indices is O(rows) memory-bandwidth bound.
        class_chunks: list[pa.Array] = []
        reason_chunks: list[pa.Array] = []
        ik_chunks: list[pa.Array] = []
        for chunk in tids_col.chunks:
            dict_values = chunk.dictionary.to_pylist()
            class_per_dict = pa.array(
                [classification_by_tid.get(t, "unclassified") for t in dict_values], type=pa.string()
            )
            reason_per_dict = pa.array(
                [reason_by_tid.get(t) for t in dict_values], type=pa.string()
            )
            ik_per_dict = pa.array(
                [issue_kind_by_tid.get(t, "unknown") for t in dict_values], type=pa.string()
            )
            indices = chunk.indices
            class_chunks.append(pc.take(class_per_dict, indices))
            reason_chunks.append(pc.take(reason_per_dict, indices))
            ik_chunks.append(pc.take(ik_per_dict, indices))
        class_col = pa.chunked_array(class_chunks)
        reason_col = pa.chunked_array(reason_chunks)
        ik_col = pa.chunked_array(ik_chunks)
    else:
        unique_tids_arr = pc.unique(tids_col)
        unique_tids = unique_tids_arr.to_pylist()
        class_per_tid = pa.array(
            [classification_by_tid.get(t, "unclassified") for t in unique_tids], type=pa.string()
        )
        reason_per_tid = pa.array(
            [reason_by_tid.get(t) for t in unique_tids], type=pa.string()
        )
        ik_per_tid = pa.array(
            [issue_kind_by_tid.get(t, "unknown") for t in unique_tids], type=pa.string()
        )
        indices = pc.index_in(tids_col, value_set=unique_tids_arr)
        class_col = pc.take(class_per_tid, indices)
        reason_col = pc.take(reason_per_tid, indices)
        ik_col = pc.take(ik_per_tid, indices)

    table = replace_column(table, "classification", class_col)
    table = replace_column(table, "classification_reason", reason_col)
    table = replace_column(table, "issue_kind", ik_col)
    return table


# ---------------------------------------------------------------------------------------
# Template aggregation from events table (used when --instances filter drops rows)
# ---------------------------------------------------------------------------------------


def _decode_dict_columns_for_aggregation(table: pa.Table) -> pa.Table:
    """Return a table with `template_id`/`timestamp`/`observed_timestamp` decoded to plain
    strings, so pyarrow's `hash_min_max` aggregate kernel can run on them. Other columns are
    left untouched.
    """
    fix = {}
    for col_name in ("template_id", "timestamp", "observed_timestamp"):
        if col_name not in table.column_names:
            continue
        col = table.column(col_name)
        if pa.types.is_dictionary(col.type):
            fix[col_name] = col.cast(pa.string())
    if not fix:
        return table
    result = table
    for name, arr in fix.items():
        idx = result.column_names.index(name)
        result = result.set_column(idx, name, arr)
    return result


def aggregate_templates_from_table(table: pa.Table) -> list[dict[str, Any]]:
    """Re-aggregate template rows from a filtered events table.

    Uses pyarrow group_by for counts + min/max(timestamp); pulls the first-seen row data for
    the static fields (service_name, severity_text, normalized_template, error_kind, …).
    """
    if table.num_rows == 0:
        return []
    # pyarrow's hash_min_max aggregate kernel does not support dictionary inputs; cast the
    # timestamp columns to plain strings only for the aggregation. The cost is bounded
    # (16 bytes per row plus dictionary lookup) and the table itself is not mutated.
    agg_table = _decode_dict_columns_for_aggregation(table)
    grouped = agg_table.group_by("template_id").aggregate(
        [
            ("event_id", "count"),
            ("timestamp", "min"),
            ("timestamp", "max"),
            ("observed_timestamp", "min"),
        ]
    )
    counts_by_tid: dict[str, dict[str, Any]] = {}
    for row in grouped.to_pylist():
        tid = row["template_id"]
        counts_by_tid[tid] = {
            "event_count": int(row["event_id_count"]),
            "first_seen": row.get("timestamp_min") or row.get("observed_timestamp_min"),
            "last_seen": row.get("timestamp_max") or row.get("timestamp_min") or row.get("observed_timestamp_min"),
        }

    # For static fields, find the first occurrence of each template_id and read off its row.
    tids = table.column("template_id").to_pylist()
    seen: dict[str, int] = {}
    for index, tid in enumerate(tids):
        if tid not in seen:
            seen[tid] = index
    columns_needed = [
        "service_name",
        "severity_text",
        "severity_number",
        "normalized_template",
        "error_kind",
        "exception_type",
        "event_id",
        "parse_status",
    ]
    static_pylists = {name: table.column(name).to_pylist() for name in columns_needed}

    rows: list[dict[str, Any]] = []
    for tid, first_idx in seen.items():
        agg = counts_by_tid.get(tid) or {"event_count": 0, "first_seen": None, "last_seen": None}
        rows.append(
            {
                "template_id": tid,
                "service_name": static_pylists["service_name"][first_idx],
                "severity_text": static_pylists["severity_text"][first_idx],
                "severity_number": static_pylists["severity_number"][first_idx],
                "normalized_template": static_pylists["normalized_template"][first_idx],
                "error_kind": static_pylists["error_kind"][first_idx],
                "exception_type": static_pylists["exception_type"][first_idx],
                "event_count": agg["event_count"],
                "first_seen": agg["first_seen"],
                "last_seen": agg["last_seen"],
                "example_event_id": static_pylists["event_id"][first_idx],
                "parse_status": static_pylists["parse_status"][first_idx],
                "classification": "unclassified",
                "classification_reason": None,
                "baseline_match": False,
            }
        )
    rows.sort(key=lambda r: (-int(r["event_count"]), str(r["template_id"])))
    return rows


# ---------------------------------------------------------------------------------------
# Issue kind annotation for templates
# ---------------------------------------------------------------------------------------


def annotate_issue_kind_for_templates(templates: list[dict[str, Any]]) -> None:
    for row in templates:
        row["issue_kind"] = classify_issue_kind(row)


# ---------------------------------------------------------------------------------------
# Connectivity timeline
# ---------------------------------------------------------------------------------------


def build_connectivity_timeline_from_table(table: pa.Table) -> dict[str, Any]:
    """Vectorised connectivity timeline.

    For each service (kafka/mongo/elasticsearch) we run TWO regex passes on the body column
    (down + up) using `pc.match_substring_regex`. The masks already tell us the kind of each
    candidate row, so we dispatch directly without re-running the regex per Python iteration.

    For 1 GiB captures the down mask alone can flag millions of rows; running the patterns a
    second time per row in Python is the dominant cost in the naive version (~12s on 1 GiB).
    Here the inner loop runs only `len(down_indices) + len(up_indices)` cheap dict updates.
    """
    timelines: dict[str, dict[str, Any]] = {}
    for service in DOWN_PATTERNS:
        timelines[service] = {"state": "unknown", "incidents": [], "down_events": 0, "up_events": 0}
    if table.num_rows == 0:
        return timelines

    body_col = table.column("body")
    timestamp_col = table.column("timestamp")
    observed_col = table.column("observed_timestamp")

    for service in DOWN_PATTERNS:
        entry = timelines[service]
        down_pattern = _DOWN_PREFILTER_BY_SVC.get(service)
        up_pattern = _UP_PREFILTER_BY_SVC.get(service)
        down_np = (
            np.asarray(
                match_substring_regex_smart(body_col, down_pattern, ignore_case=True)
                .combine_chunks()
                .to_numpy(zero_copy_only=False)
            )
            if down_pattern
            else np.zeros(table.num_rows, dtype=bool)
        )
        up_np = (
            np.asarray(
                match_substring_regex_smart(body_col, up_pattern, ignore_case=True)
                .combine_chunks()
                .to_numpy(zero_copy_only=False)
            )
            if up_pattern
            else np.zeros(table.num_rows, dtype=bool)
        )
        # Down has priority: if a row matches both, treat it as down (preserves prior semantics).
        up_np = up_np & ~down_np

        down_idx = np.where(down_np)[0]
        up_idx = np.where(up_np)[0]
        if down_idx.size == 0 and up_idx.size == 0:
            continue
        all_idx = np.concatenate([down_idx, up_idx])
        kinds = np.concatenate(
            [np.zeros(down_idx.size, dtype=np.int8), np.ones(up_idx.size, dtype=np.int8)]
        )
        order = np.argsort(all_idx, kind="stable")
        all_idx = all_idx[order]
        kinds = kinds[order]

        entry["down_events"] = int(down_idx.size)
        entry["up_events"] = int(up_idx.size)

        # Identify state-transition positions purely in numpy:
        #   begin_down: kind==0 and previous kind != 0 (or first)
        #   end_down  : kind==1 and previous kind == 0
        prev_kind = np.concatenate([[1], kinds[:-1]])  # treat "before start" as up so initial down begins
        begin_down_positions = np.where((kinds == 0) & (prev_kind != 0))[0]
        end_down_positions = np.where((kinds == 1) & (prev_kind == 0))[0]

        # Fetch body/timestamp ONLY for the handful of transition positions, not for every row.
        transition_positions = np.unique(np.concatenate([begin_down_positions, end_down_positions]))
        if transition_positions.size == 0:
            continue
        trans_global = all_idx[transition_positions]
        trans_idx_pa = pa.array(trans_global.tolist(), type=pa.int64())
        trans_ts = pc.take(timestamp_col, trans_idx_pa).to_pylist()
        trans_obs = pc.take(observed_col, trans_idx_pa).to_pylist()
        # Bodies are only used as the "first_sample" of a down incident. Fetch them only for
        # the begin positions (smaller set still).
        begin_global = all_idx[begin_down_positions]
        begin_idx_pa = pa.array(begin_global.tolist(), type=pa.int64())
        begin_bodies = pc.take(body_col, begin_idx_pa).to_pylist()
        position_to_trans_idx = {int(pos): i for i, pos in enumerate(transition_positions)}

        # Per-incident down_events: count kinds==0 in [begin_i, end_i).
        is_down = (kinds == 0).astype(np.int64)
        cumulative_downs = np.concatenate([[0], np.cumsum(is_down)])

        for inc_idx, begin_pos in enumerate(begin_down_positions):
            begin_pos_int = int(begin_pos)
            ts_idx = position_to_trans_idx[begin_pos_int]
            down_at = trans_ts[ts_idx] or trans_obs[ts_idx]
            # Find matching end_down position (smallest > begin_pos).
            later_ends = end_down_positions[end_down_positions > begin_pos]
            if later_ends.size:
                end_pos = int(later_ends[0])
                end_ts_idx = position_to_trans_idx[end_pos]
                up_at = trans_ts[end_ts_idx] or trans_obs[end_ts_idx]
            else:
                end_pos = int(kinds.size)
                up_at = None
            incident_downs = int(cumulative_downs[end_pos] - cumulative_downs[begin_pos_int])
            entry["incidents"].append(
                {
                    "down_at": down_at,
                    "up_at": up_at,
                    "duration_seconds": _duration_seconds(down_at, up_at) if up_at else None,
                    "down_events": incident_downs,
                    "first_sample": _trim_sample(begin_bodies[inc_idx] or ""),
                }
            )

        # Final state.
        if entry["incidents"]:
            last = entry["incidents"][-1]
            entry["state"] = "up" if last.get("up_at") else "down"
        elif up_idx.size:
            entry["state"] = "up"
    return timelines


# ---------------------------------------------------------------------------------------
# Summary and counts
# ---------------------------------------------------------------------------------------


def severity_counts_from_table(table: pa.Table) -> dict[str, int]:
    return _value_counts(table, "severity_text")


def issue_kind_event_counts_from_table(table: pa.Table) -> dict[str, int]:
    return _value_counts(table, "issue_kind")


def parse_status_counts_from_table(table: pa.Table) -> dict[str, int]:
    return _value_counts(table, "parse_status")


def runtime_counts_from_table(table: pa.Table) -> dict[str, int]:
    """Distinct counts of identifier-like columns, filtering nulls/blanks/'-'."""
    keys = {
        "processes": "process_pid",
        "process_names": "process_name",
        "threads": "thread_name",
        "workers": "worker_id",
        "containers": "container_id",
        "pods": "k8s_pod_name",
        "server_kinds": "server_kind",
        "service_instances": "service_instance_id",
    }
    result: dict[str, int] = {}
    for label, column_name in keys.items():
        if column_name not in table.column_names:
            result[label] = 0
            continue
        col = table.column(column_name)
        col = pc.drop_null(col)
        # Filter out empty-string and '-' sentinel values to match the original semantics.
        if pa.types.is_string(col.type):
            mask = pc.and_(pc.not_equal(col, ""), pc.not_equal(col, "-"))
            col = col.filter(mask)
        result[label] = int(pc.count(pc.unique(col)).as_py())
    return result


def _value_counts(table: pa.Table, column: str) -> dict[str, int]:
    if column not in table.column_names:
        return {}
    counts = pc.value_counts(table.column(column))
    out: dict[str, int] = {}
    for entry in counts.to_pylist():
        out[entry["values"]] = int(entry["counts"])
    return out


# ---------------------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------------------


def _timestamp_to_seconds(value: str | None) -> float | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


def _duration_seconds(start: str | None, end: str | None) -> float | None:
    if not start or not end:
        return None
    s = _timestamp_to_seconds(start)
    e = _timestamp_to_seconds(end)
    if s is None or e is None:
        return None
    return max(e - s, 0.0)


def _trim_sample(body: str, limit: int = 140) -> str:
    body = " ".join(body.split())
    if len(body) > limit:
        return body[: limit - 1] + "…"
    return body


# ---------------------------------------------------------------------------------------
# Legacy fallback bridge
# ---------------------------------------------------------------------------------------


def events_list_to_table(events: list[dict[str, Any]], schema_with_python_columns: pa.Schema) -> pa.Table:
    """Convert the Python-fallback parser output (list[dict]) into a pyarrow.Table that matches
    the canonical EVENT_SCHEMA. Used only when Rust falls back per-file.
    """
    if not events:
        return pa.table({field.name: pa.array([], type=field.type) for field in schema_with_python_columns},
                        schema=schema_with_python_columns)
    return pa.Table.from_pylist(events, schema=schema_with_python_columns)
