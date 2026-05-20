from __future__ import annotations

import shlex
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

import pyarrow as pa

from . import __version__
from .arrow_pipeline import (
    aggregate_templates_from_table,
    annotate_instances_table,
    annotate_issue_kind_for_templates,
    apply_template_lookup_columns,
    build_connectivity_timeline_from_table,
    ensure_python_columns,
    filter_events_table_by_instance,
    issue_kind_event_counts_from_table,
    parse_status_counts_from_table,
    reorder_table_to_schema,
    runtime_counts_from_table,
    severity_counts_from_table,
)
from .classify import build_error_rows, classify_templates, load_baseline_templates
from .hashing import HASH_ALGORITHM
from .instances import (
    INSTANCES_ALL,
    INSTANCES_LAST,
    locate_last_boot_offset,
    select_instance_seqs,
)
from .io import EVENT_SCHEMA, ERROR_SCHEMA, TEMPLATE_SCHEMA, resolve_inputs, write_json, write_parquet, write_parquet_table
from .models import RunMetadata, Summary
from .progress import ProgressReporter
from .reports import render_scan_report
from .rust_engine import read_events_ipc, read_templates_ipc, scan_file_to_ipc
from .rules import load_rules


def scan(
    *,
    input_patterns: list[str],
    run_id: str,
    out_dir: str | Path,
    service_name: str | None = None,
    lib_versions: dict[str, str] | None = None,
    rules_path: str | Path | None = None,
    baseline_dir: str | Path | None = None,
    include_raw: bool = False,
    autodiscovery: dict[str, Any] | None = None,
    invocation_command: str | None = None,
    instances: str = INSTANCES_LAST,
    focus: str = "both",
    progress: ProgressReporter | None = None,
) -> dict[str, Any]:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    started_at = perf_counter()
    reporter = progress or ProgressReporter()

    paths = resolve_inputs(input_patterns)
    if not paths:
        raise FileNotFoundError(f"No input files matched: {', '.join(input_patterns)}")
    total_input_bytes = sum(path.stat().st_size for path in paths)

    created_at = datetime.now(timezone.utc).isoformat()
    # Rust streams each input file directly into a per-file Arrow IPC pair under _rust/.
    # The on-disk format is the raw Arrow buffer layout, so Python re-opens these via
    # `pa.memory_map` + `pa.ipc.open_file` and gets a zero-copy `pa.Table`. No compression
    # round-trip, no parquet decoding pass.
    rust_tmp_dir = out_path / "_rust"
    rust_tmp_dir.mkdir(parents=True, exist_ok=True)
    events_ipc_paths: list[Path] = []
    templates_ipc_paths: list[Path] = []
    rust_summaries: list[dict[str, Any]] = []
    tail_anchor_offset: int | None = None
    use_tail_anchor = (instances or "").strip().lower() == INSTANCES_LAST and len(paths) == 1
    if use_tail_anchor:
        reporter.phase("Locating last service boot (reverse-scan)")
        tail_anchor_offset = locate_last_boot_offset(paths[0])
    for index, path in enumerate(paths, start=1):
        start_offset = tail_anchor_offset if use_tail_anchor and tail_anchor_offset else 0
        reporter.phase(f"Parsing [{index}/{len(paths)}] {path.name} (Rust)")

        def _on_progress(bytes_read: int, bytes_total: int, events: int) -> None:
            reporter.update(bytes_read=bytes_read, bytes_total=bytes_total, events=events, prefix=path.name)

        events_ipc = rust_tmp_dir / f"events_{index:04d}.arrow"
        templates_ipc = rust_tmp_dir / f"templates_{index:04d}.arrow"
        summary_rust = scan_file_to_ipc(
            input_path=path,
            events_out=events_ipc,
            templates_out=templates_ipc,
            service_name=service_name or "-",
            run_id=run_id,
            observed_timestamp=created_at,
            include_raw=include_raw,
            start_offset=start_offset,
            progress_callback=_on_progress if reporter.enabled else None,
        )
        events_ipc_paths.append(events_ipc)
        templates_ipc_paths.append(templates_ipc)
        rust_summaries.append(summary_rust)

    engine_used = "rust"
    reporter.finish()

    # Materialise events back from disk — mmapped Arrow IPC, so it's zero-copy to the same
    # buffers Rust wrote. For multi-file scans we stitch per-file tables; pyarrow's
    # `concat_tables` is also zero-copy when the schemas match.
    if events_ipc_paths:
        if len(events_ipc_paths) == 1:
            events_table = read_events_ipc(events_ipc_paths[0])
        else:
            events_table = pa.concat_tables(
                [read_events_ipc(p) for p in events_ipc_paths], promote_options="default"
            )
    else:
        events_table = _empty_events_table()
    events_table = ensure_python_columns(events_table)

    reporter.phase("Detecting service instances")
    events_table, detected_instances = annotate_instances_table(events_table)

    keep_seqs = select_instance_seqs(detected_instances, instances)
    instance_filter_active = keep_seqs is not None
    events_table, dropped_event_count = filter_events_table_by_instance(events_table, keep_seqs)

    reporter.phase("Aggregating templates")
    if instance_filter_active:
        # When the working set has been filtered by instance, re-derive the canonical template
        # rows from the events table so they stay consistent with the rows we keep.
        templates = aggregate_templates_from_table(events_table)
    else:
        template_payloads = [read_templates_ipc(p) for p in templates_ipc_paths]
        templates = _merge_template_payloads(template_payloads)

    reporter.phase("Classifying templates against rules and baseline")
    rules = load_rules(rules_path)
    baseline = load_baseline_templates(baseline_dir)
    classify_templates(templates, rules=rules, baseline=baseline)
    annotate_issue_kind_for_templates(templates)

    reporter.phase("Mining templates with Drain")
    # Phase 2: the Rust normalize_message + entropy_mask already collapses
    # well-known variable shapes. Drain runs OVER the resulting templates
    # (cardinality O(thousands) per service, not events) and merges any
    # variants whose only difference is a Drain-learned wildcard position.
    #
    # Path-based handoff: when there's exactly one events.arrow on disk and
    # we haven't already mutated the table (no instance filter applied), we
    # let Rust rewrite the events.arrow in-place. The Python events_table
    # is then reloaded from the rewritten file so downstream phases see the
    # remapped template_id column. For multi-file or instance-filtered
    # scans we keep the in-memory path (no rewrite, only the templates
    # list is merged) — Python pyarrow handles the dict remap via
    # apply_template_lookup_columns anyway.
    drain_events_in: Path | None = None
    drain_events_out: Path | None = None
    if (
        not instance_filter_active
        and len(events_ipc_paths) == 1
        and events_ipc_paths[0].exists()
    ):
        drain_events_in = events_ipc_paths[0]
        drain_events_out = rust_tmp_dir / "events_drained.arrow"

    templates, rewritten_events_path = _apply_drain_to_templates(
        templates=templates,
        events_ipc_path=drain_events_in,
        events_ipc_out=drain_events_out,
        service_name=service_name,
        out_dir=out_path,
    )

    if rewritten_events_path is not None and rewritten_events_path.exists():
        # Drop the old in-memory table and remap straight from the Rust
        # output — zero-copy mmap, no per-event pyarrow rebuild.
        events_table = read_events_ipc(rewritten_events_path)
        events_table = ensure_python_columns(events_table)

    events_table = apply_template_lookup_columns(events_table, templates)

    reporter.phase("Building connectivity timeline")
    connectivity_timeline = build_connectivity_timeline_from_table(events_table)
    errors = build_error_rows(templates)
    template_issue_kind = {str(row["template_id"]): row.get("issue_kind") for row in templates}
    for error_row in errors:
        error_row["issue_kind"] = template_issue_kind.get(str(error_row.get("template_id")))

    if rust_summaries:
        rust_seconds = sum(float(item.get("scan_duration_seconds") or 0.0) for item in rust_summaries)
        duration_seconds = max(rust_seconds, 1e-9)
    else:
        duration_seconds = max(perf_counter() - started_at, 1e-9)
    parsed_bytes = sum(int(item.get("input_bytes") or 0) for item in rust_summaries)
    if not rust_summaries:
        parsed_bytes = total_input_bytes
    input_bytes = parsed_bytes
    input_gigabytes = input_bytes / (1024**3)
    throughput_gb_per_second = input_gigabytes / duration_seconds
    events_per_second = events_table.num_rows / duration_seconds

    summary = Summary(
        **_build_summary(
            events_table,
            templates,
            errors,
            input_bytes=input_bytes,
            input_gigabytes=input_gigabytes,
            duration_seconds=duration_seconds,
            throughput_gb_per_second=throughput_gb_per_second,
            events_per_second=events_per_second,
        )
    ).model_dump(mode="json")
    instance_metadata = _build_instance_metadata(
        detected=detected_instances,
        kept_seqs=keep_seqs,
        spec=instances,
        dropped_events=dropped_event_count,
        tail_anchor_offset=tail_anchor_offset,
        total_input_bytes=total_input_bytes,
        parsed_input_bytes=parsed_bytes,
    )
    run_metadata_payload = {
        "tool": "LogsReaper",
        "tool_version": __version__,
        "run_id": run_id,
        "created_at": created_at,
        "service_name": service_name,
        "invocation_command": invocation_command or _resolve_invocation_command(),
        "input_globs": input_patterns,
        "input_files": [str(path) for path in paths],
        "file_count": len(paths),
        "instances": instance_metadata,
        "focus": focus,
        "connectivity_timeline": connectivity_timeline,
        "event_count": events_table.num_rows,
        "template_count": len(templates),
        "error_count": len(errors),
        "lib_versions": lib_versions or {},
        "rules_path": str(rules_path) if rules_path else None,
        "baseline_dir": str(baseline_dir) if baseline_dir else None,
        "hash_algorithm": HASH_ALGORITHM,
        "runtime_counts": runtime_counts_from_table(events_table),
        "parse_status": summary["parse_status"],
        "autodiscovery": autodiscovery,
        "engine": engine_used,
        "scan_duration_seconds": duration_seconds,
        "input_bytes": input_bytes,
        "input_gigabytes": input_gigabytes,
        "throughput_gb_per_second": throughput_gb_per_second,
        "events_per_second": events_per_second,
    }
    run_metadata = RunMetadata(**run_metadata_payload).model_dump(mode="json")

    reporter.phase(f"Writing outputs to {out_path}")
    events_table_for_write = reorder_table_to_schema(events_table, EVENT_SCHEMA)
    write_parquet_table(out_path / "events.parquet", events_table_for_write, EVENT_SCHEMA)
    write_parquet(out_path / "templates.parquet", templates, TEMPLATE_SCHEMA)
    write_parquet(out_path / "errors.parquet", errors, ERROR_SCHEMA)
    write_json(out_path / "run.json", run_metadata)
    write_json(out_path / "summary.json", summary)
    (out_path / "report.md").write_text(
        render_scan_report(run_metadata, summary, templates, errors),
        encoding="utf-8",
    )
    # Drop the per-file parquets that Rust used as the streaming-IPC boundary now that the
    # consolidated outputs are in place.
    shutil.rmtree(rust_tmp_dir, ignore_errors=True)
    reporter.finish()
    return {"run": run_metadata, "summary": summary, "out_dir": str(out_path)}


def _build_instance_metadata(
    *,
    detected: list[dict[str, Any]],
    kept_seqs: set[int] | None,
    spec: str,
    dropped_events: int,
    tail_anchor_offset: int | None,
    total_input_bytes: int,
    parsed_input_bytes: int,
) -> dict[str, Any]:
    return {
        "spec": spec,
        "filter_active": kept_seqs is not None,
        "kept_seqs": sorted(kept_seqs) if kept_seqs is not None else None,
        "dropped_event_count": dropped_events,
        "detected_count": len(detected),
        "detected": detected,
        "tail_anchor_offset": tail_anchor_offset,
        "total_input_bytes": total_input_bytes,
        "parsed_input_bytes": parsed_input_bytes,
    }


def _resolve_invocation_command() -> str:
    argv = list(sys.argv)
    if not argv:
        return ""
    entry = Path(argv[0])
    if entry.name == "__main__.py" and entry.parent.name == "logs_reaper":
        argv = ["python3", "-m", "logs_reaper", *argv[1:]]
    return shlex.join(argv)


def _merge_template_payloads(payloads: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for rows in payloads:
        for row in rows:
            _merge_template_row(by_id, row)
    return sorted(by_id.values(), key=lambda item: (-int(item["event_count"]), str(item["template_id"])))


def _merge_template_row(by_id: dict[str, dict[str, Any]], row: dict[str, Any]) -> None:
    template_id = str(row.get("template_id"))
    if not template_id:
        return
    existing = by_id.get(template_id)
    incoming = {
        "template_id": template_id,
        "service_name": row.get("service_name"),
        "severity_text": row.get("severity_text"),
        "severity_number": row.get("severity_number"),
        "normalized_template": row.get("normalized_template"),
        "error_kind": row.get("error_kind"),
        "exception_type": row.get("exception_type"),
        "event_count": int(row.get("event_count") or 0),
        "first_seen": row.get("first_seen"),
        "last_seen": row.get("last_seen"),
        "example_event_id": row.get("example_event_id"),
        "parse_status": row.get("parse_status") or "ok",
        "classification": row.get("classification") or "unclassified",
        "classification_reason": row.get("classification_reason"),
        "baseline_match": bool(row.get("baseline_match") or False),
    }
    if existing is None:
        by_id[template_id] = incoming
        return
    existing["event_count"] += incoming["event_count"]
    existing["first_seen"] = _min_nonempty(existing.get("first_seen"), incoming["first_seen"])
    existing["last_seen"] = _max_nonempty(existing.get("last_seen"), incoming["last_seen"])
    if existing.get("parse_status") == "ok" and incoming["parse_status"] != "ok":
        existing["parse_status"] = incoming["parse_status"]


def _empty_events_table() -> pa.Table:
    return pa.table({field.name: pa.array([], type=field.type) for field in EVENT_SCHEMA}, schema=EVENT_SCHEMA)


def _build_summary(
    events_table: pa.Table,
    templates: list[dict[str, Any]],
    errors: list[dict[str, Any]],
    *,
    input_bytes: int,
    input_gigabytes: float,
    duration_seconds: float,
    throughput_gb_per_second: float,
    events_per_second: float,
) -> dict[str, Any]:
    from collections import Counter

    return {
        "event_count": events_table.num_rows,
        "template_count": len(templates),
        "error_count": len(errors),
        "severity_counts": severity_counts_from_table(events_table),
        "classification_counts": dict(Counter(row.get("classification") for row in templates)),
        "issue_kind_counts": dict(Counter(row.get("issue_kind") for row in templates)),
        "issue_kind_event_counts": issue_kind_event_counts_from_table(events_table),
        "parse_status": parse_status_counts_from_table(events_table),
        "scan_duration_seconds": duration_seconds,
        "input_bytes": input_bytes,
        "input_gigabytes": input_gigabytes,
        "throughput_gb_per_second": throughput_gb_per_second,
        "events_per_second": events_per_second,
        "top_templates": [
            {
                "template_id": row["template_id"],
                "count": row["event_count"],
                "severity": row["severity_text"],
                "classification": row["classification"],
                "template": row["normalized_template"],
            }
            for row in templates[:20]
        ],
    }


def _apply_drain_to_templates(
    *,
    templates: list[dict[str, Any]],
    events_ipc_path: Path | None,
    events_ipc_out: Path | None,
    service_name: str | None,
    out_dir: Path,
) -> tuple[list[dict[str, Any]], Path | None]:
    """Single-call Drain phase: all the work runs in Rust.

    Templates cross the FFI as a list of 3-tuples (id, normalized,
    event_count) because Python owns rich metadata (classification,
    issue_kind, ...) that the templates IPC schema doesn't carry. The
    events IPC is rewritten Rust-side — Python never iterates events.

    Returns the merged templates list and the path of the rewritten events
    IPC (None when nothing was rewritten, in which case callers keep the
    input path).
    """
    if not templates or len(templates) == 1:
        return templates, events_ipc_out if events_ipc_out and events_ipc_out.exists() else None

    try:
        from logs_reaper_core import apply_drain_phase_py
    except ImportError:
        # Rust extension not available — degrade gracefully.
        return templates, None

    service_slug = service_name or "default"
    drain_dir = out_dir.parent / "drain"
    drain_dir.mkdir(parents=True, exist_ok=True)
    drain_path = drain_dir / f"{service_slug}.json"

    drain_input = [
        (
            str(row.get("template_id") or ""),
            str(row.get("normalized_template") or ""),
            int(row.get("event_count") or 0),
        )
        for row in templates
    ]

    summary, remap, canonical_rows = apply_drain_phase_py(
        templates=drain_input,
        drain_state=str(drain_path),
        events_ipc=str(events_ipc_path) if events_ipc_path else None,
        events_out=str(events_ipc_out) if events_ipc_out else None,
    )

    # No actual merges → templates list unchanged. The events IPC is also
    # left untouched (Rust short-circuits the rewrite when remap is identity).
    actually_merged = any(old != new for old, new in dict(remap).items())
    if not actually_merged:
        rewritten = events_ipc_out if summary.get("events_rewritten") else None
        return templates, rewritten

    # Apply the remap + drain template to the in-memory templates list,
    # preserving every Python-side metadata field (classification,
    # issue_kind, instance_count, ...). Templates that map to themselves
    # remain as-is; non-canonical members get merged into their canonical
    # row's event_count / first_seen / last_seen.
    canonical_by_id = {row["template_id"]: row for row in canonical_rows}
    templates_by_id = {str(t.get("template_id")): t for t in templates}
    merged_templates: list[dict[str, Any]] = []
    for canonical_id, canonical in canonical_by_id.items():
        base = dict(templates_by_id.get(canonical_id) or {})
        base["template_id"] = canonical_id
        base["normalized_template"] = canonical["drain_template"]
        base["event_count"] = canonical["event_count"]
        members = canonical["member_template_ids"]
        if len(members) > 1:
            instance_total = 0
            firsts: list[Any] = []
            lasts: list[Any] = []
            for member_id in members:
                src = templates_by_id.get(member_id) or {}
                instance_total += int(src.get("instance_count") or 0)
                if src.get("first_seen"):
                    firsts.append(src["first_seen"])
                if src.get("last_seen"):
                    lasts.append(src["last_seen"])
            if instance_total:
                base["instance_count"] = instance_total
            if firsts:
                base["first_seen"] = min(firsts)
            if lasts:
                base["last_seen"] = max(lasts)
            base["drain_merged_from"] = list(members)
        merged_templates.append(base)

    rewritten = events_ipc_out if summary.get("events_rewritten") else None
    return merged_templates, rewritten


def _min_nonempty(left: str | None, right: str | None) -> str | None:
    values = [value for value in (left, right) if value]
    return min(values) if values else None


def _max_nonempty(left: str | None, right: str | None) -> str | None:
    values = [value for value in (left, right) if value]
    return max(values) if values else None


def _write_events_snapshot(table: pa.Table, path: Path) -> None:
    """Persist the consolidated events table as Arrow IPC stream (mmap-friendly).

    Atomic via tmp + os.replace so a crash mid-write never leaves a
    half-written snapshot that the next materialize would treat as a
    valid fragment.
    """
    import os as _os
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".arrow.tmp")
    with pa.OSFile(str(tmp), "wb") as sink:
        with pa.ipc.new_stream(sink, table.schema) as writer:
            for batch in table.to_batches():
                writer.write_batch(batch)
    _os.replace(tmp, path)


def scan_from_ipc_fragments(
    *,
    events_ipc_paths: list[Path],
    templates_ipc_paths: list[Path],
    run_id: str,
    out_dir: Path,
    service_name: str,
    rules_path: str | Path | None = None,
    baseline_dir: str | Path | None = None,
    autodiscovery: dict[str, Any] | None = None,
    lib_versions: dict[str, str] | None = None,
    events_snapshot_out: Path | None = None,
) -> dict[str, Any]:
    """Materialise a consolidated run from pre-computed Rust IPC fragments.

    This is the live-mode counterpart to `scan()`: the Rust scan phase
    has already been run per-tick by `incremental.delta_scan_tick` and
    produced N small `_rust/events_NNNN.arrow` + `_rust/templates_NNNN.arrow`
    fragments. Here we just stitch them, run the same Drain + classify +
    parquet pipeline `scan()` runs, and write the canonical `events.parquet`
    / `templates.parquet` / `summary.json` the dashboard reads.

    Empty fragment list → still writes empty parquet artifacts so the
    dashboard's reader can mmap them without special-casing absence.
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    started_at = perf_counter()
    created_at = datetime.now(timezone.utc).isoformat()

    if events_ipc_paths:
        if len(events_ipc_paths) == 1:
            events_table = read_events_ipc(events_ipc_paths[0])
        else:
            events_table = pa.concat_tables(
                [read_events_ipc(p) for p in events_ipc_paths], promote_options="default"
            )
    else:
        events_table = _empty_events_table()
    events_table = ensure_python_columns(events_table)

    # Live mode keeps every instance — we're a continuous tail.
    events_table, detected_instances = annotate_instances_table(events_table)

    if templates_ipc_paths:
        template_payloads = [read_templates_ipc(p) for p in templates_ipc_paths]
        templates = _merge_template_payloads(template_payloads)
    else:
        templates = aggregate_templates_from_table(events_table)

    rules = load_rules(rules_path)
    baseline = load_baseline_templates(baseline_dir)
    classify_templates(templates, rules=rules, baseline=baseline)
    annotate_issue_kind_for_templates(templates)

    # Drain phase: write a single consolidated events.arrow (in a temp
    # location under _rust/) so the Rust drain_phase can rewrite the
    # dictionary in one shot. The Python events_table is then reloaded
    # from the rewritten file so downstream sees the remapped template_id.
    rust_tmp_dir = out_path / "_rust"
    rust_tmp_dir.mkdir(parents=True, exist_ok=True)
    drain_events_in: Path | None = None
    drain_events_out: Path | None = None
    if events_table.num_rows > 0 and len(templates) > 1:
        drain_events_in = rust_tmp_dir / "events_merged.arrow"
        drain_events_out = rust_tmp_dir / "events_drained.arrow"
        with pa.OSFile(str(drain_events_in), "wb") as sink:
            with pa.ipc.new_stream(sink, events_table.schema) as writer:
                for batch in events_table.to_batches():
                    writer.write_batch(batch)

    templates, rewritten_events_path = _apply_drain_to_templates(
        templates=templates,
        events_ipc_path=drain_events_in,
        events_ipc_out=drain_events_out,
        service_name=service_name,
        out_dir=out_path,
    )
    if rewritten_events_path is not None and rewritten_events_path.exists():
        events_table = read_events_ipc(rewritten_events_path)
        events_table = ensure_python_columns(events_table)

    # Snapshot persisted BEFORE apply_template_lookup_columns so the schema
    # stays identical to fresh Rust fragments — that's the contract that
    # lets the next materialize concatenate this snapshot with new fragments
    # without a costly schema-promotion pass.
    if events_snapshot_out is not None and events_table.num_rows > 0:
        _write_events_snapshot(events_table, Path(events_snapshot_out))

    events_table = apply_template_lookup_columns(events_table, templates)
    connectivity_timeline = build_connectivity_timeline_from_table(events_table)
    errors = build_error_rows(templates)
    template_issue_kind = {str(row["template_id"]): row.get("issue_kind") for row in templates}
    for error_row in errors:
        error_row["issue_kind"] = template_issue_kind.get(str(error_row.get("template_id")))

    duration_seconds = max(perf_counter() - started_at, 1e-9)
    input_bytes = sum(int(path.stat().st_size) for path in events_ipc_paths if path.exists())
    input_gigabytes = input_bytes / (1024**3)
    throughput_gb_per_second = input_gigabytes / duration_seconds
    events_per_second = events_table.num_rows / duration_seconds

    summary = Summary(
        **_build_summary(
            events_table,
            templates,
            errors,
            input_bytes=input_bytes,
            input_gigabytes=input_gigabytes,
            duration_seconds=duration_seconds,
            throughput_gb_per_second=throughput_gb_per_second,
            events_per_second=events_per_second,
        )
    ).model_dump(mode="json")

    instance_metadata = _build_instance_metadata(
        detected=detected_instances,
        kept_seqs=None,
        spec="all",
        dropped_events=0,
        tail_anchor_offset=None,
        total_input_bytes=input_bytes,
        parsed_input_bytes=input_bytes,
    )
    run_metadata_payload = {
        "tool": "LogsReaper",
        "tool_version": __version__,
        "run_id": run_id,
        "created_at": created_at,
        "service_name": service_name,
        "invocation_command": _resolve_invocation_command(),
        "input_globs": [str(p) for p in events_ipc_paths],
        "input_files": [str(p) for p in events_ipc_paths],
        "file_count": len(events_ipc_paths),
        "instances": instance_metadata,
        "focus": "both",
        "connectivity_timeline": connectivity_timeline,
        "event_count": events_table.num_rows,
        "template_count": len(templates),
        "error_count": len(errors),
        "lib_versions": lib_versions or {},
        "rules_path": str(rules_path) if rules_path else None,
        "baseline_dir": str(baseline_dir) if baseline_dir else None,
        "hash_algorithm": HASH_ALGORITHM,
        "runtime_counts": runtime_counts_from_table(events_table),
        "parse_status": summary["parse_status"],
        "autodiscovery": autodiscovery,
        "engine": "rust",
        "scan_duration_seconds": duration_seconds,
        "input_bytes": input_bytes,
        "input_gigabytes": input_gigabytes,
        "throughput_gb_per_second": throughput_gb_per_second,
        "events_per_second": events_per_second,
    }
    run_metadata = RunMetadata(**run_metadata_payload).model_dump(mode="json")

    events_table_for_write = reorder_table_to_schema(events_table, EVENT_SCHEMA)
    write_parquet_table(out_path / "events.parquet", events_table_for_write, EVENT_SCHEMA)
    write_parquet(out_path / "templates.parquet", templates, TEMPLATE_SCHEMA)
    write_parquet(out_path / "errors.parquet", errors, ERROR_SCHEMA)
    write_json(out_path / "run.json", run_metadata)
    write_json(out_path / "summary.json", summary)
    (out_path / "report.md").write_text(
        render_scan_report(run_metadata, summary, templates, errors),
        encoding="utf-8",
    )
    # Clean up the temp drain artifacts; the fragments themselves are
    # deleted by the caller (incremental.materialize) after success.
    for tmp in (drain_events_in, drain_events_out):
        if tmp is not None and tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass
    return {"run": run_metadata, "summary": summary, "out_dir": str(out_path)}
