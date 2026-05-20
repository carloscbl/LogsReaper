"""Cross-run catalog: registry, template registry and statistical baseline.

The `index` command scans an `out/` tree (one folder per scan) and refreshes three
Parquet files under a shared `runs/` directory:

* `registry.parquet` — one row per scan: run_id, service, scenario, status,
  event_count, throughput, etc.
* `template_registry.parquet` — one row per (service, template_id) globally.
* `baseline.parquet` — per (service, scenario, template_id) statistical baseline
  computed from runs marked green by the heuristic in `classify_status`.

State is kept in `runs/index_state.json` so re-indexing is incremental — only
run.json files that are new or whose mtime changed are reprocessed.
"""
from __future__ import annotations

import json
import math
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from .io import read_json, read_parquet, write_json


def _atomic_write_parquet(table: pa.Table, path: Path, **kwargs) -> None:
    """Escribe parquet a `<path>.tmp` y hace os.replace al destino final.

    Imprescindible porque el dashboard (Streamlit) re-lee los parquet en cada
    refresh: si `pq.write_table` los está escribiendo en directo, los reads
    concurrentes pillan ficheros parciales y fallan con thrift size errors.
    `os.replace` es atómico a nivel de filesystem en POSIX.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, tmp, **kwargs)
    os.replace(tmp, path)


REGISTRY_VERSION = "v1"
BASELINE_VERSION = "v1"
MIN_GREEN_RUNS_FOR_BASELINE = 2


RUN_REGISTRY_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("service_name", pa.string()),
        ("scenario", pa.string()),
        ("created_at", pa.string()),
        ("status", pa.string()),
        ("event_count", pa.int64()),
        ("template_count", pa.int64()),
        ("error_count", pa.int64()),
        ("code_event_count", pa.int64()),
        ("infra_event_count", pa.int64()),
        ("connectivity_incident_count", pa.int64()),
        ("connectivity_total_downtime_s", pa.float64()),
        ("scan_duration_seconds", pa.float64()),
        ("events_per_second", pa.float64()),
        ("throughput_gb_per_second", pa.float64()),
        ("input_bytes", pa.int64()),
        ("engine", pa.string()),
        ("git_branch", pa.string()),
        ("git_commit", pa.string()),
        ("image_fingerprint", pa.string()),
        ("autodiscovery_status", pa.string()),
        ("run_dir", pa.string()),
        ("run_json_mtime_ns", pa.int64()),
        ("indexed_at", pa.string()),
    ]
)

TEMPLATE_REGISTRY_SCHEMA = pa.schema(
    [
        ("service_name", pa.string()),
        ("template_id", pa.string()),
        ("normalized_template", pa.string()),
        ("severity_text", pa.string()),
        ("severity_number", pa.int64()),
        ("issue_kind", pa.string()),
        ("error_kind", pa.string()),
        ("exception_type", pa.string()),
        ("first_seen_run_id", pa.string()),
        ("last_seen_run_id", pa.string()),
        ("first_seen_at", pa.string()),
        ("last_seen_at", pa.string()),
        ("runs_seen_count", pa.int64()),
        ("green_runs_seen_count", pa.int64()),
        ("total_event_count", pa.int64()),
    ]
)

BASELINE_SCHEMA = pa.schema(
    [
        ("service_name", pa.string()),
        ("scenario", pa.string()),
        ("template_id", pa.string()),
        ("normalized_template", pa.string()),
        ("severity_text", pa.string()),
        ("issue_kind", pa.string()),
        ("mean_count", pa.float64()),
        ("std_count", pa.float64()),
        ("p50_count", pa.float64()),
        ("p95_count", pa.float64()),
        ("p99_count", pa.float64()),
        ("min_count", pa.int64()),
        ("max_count", pa.int64()),
        ("runs_in_baseline", pa.int64()),
        ("baseline_version", pa.string()),
        ("last_updated", pa.string()),
    ]
)


# Default scenario derivation. Recognises a few prefixes:
# E2E_<SCENARIO>_<...> → scenario = SCENARIO.lower()
# BENCH_* / BENCH-* → bench
# Anything else → default
_E2E_RE = re.compile(r"^E2E_([A-Za-z0-9]+)", re.IGNORECASE)
_BENCH_RE = re.compile(r"^BENCH[_\-]", re.IGNORECASE)


def derive_scenario(run_id: str | None, override_regex: re.Pattern[str] | None = None) -> str:
    if not run_id:
        return "default"
    if override_regex is not None:
        match = override_regex.search(run_id)
        if match:
            group = match.groupdict().get("scenario") if match.groupdict() else None
            if group is None and match.groups():
                group = match.group(1)
            if group:
                return group.lower()
    match = _E2E_RE.match(run_id)
    if match:
        return match.group(1).lower()
    if _BENCH_RE.match(run_id):
        return "bench"
    return "default"


def classify_status(run: dict[str, Any]) -> tuple[str, dict[str, int | float]]:
    """Return (status, derived_counts) for a run.json payload.

    Heuristic for green: no code-classified events, no error templates and no
    connectivity incidents. Runs with zero events are treated as unknown rather
    than green so they cannot pollute the baseline.

    Real scan output stores ``issue_kind_event_counts`` on summary.json and
    only the runtime-entity ``runtime_counts`` on run.json; the synthetic
    tests pre-flatten the issue kinds into ``runtime_counts``. We accept both.
    """
    event_count = int(run.get("event_count") or 0)
    error_count = int(run.get("error_count") or 0)
    runtime_counts = run.get("runtime_counts") or {}
    issue_kinds = run.get("issue_kind_event_counts") or runtime_counts
    code_count = int(issue_kinds.get("code") or runtime_counts.get("code") or 0)
    infra_count = int(issue_kinds.get("infra") or runtime_counts.get("infra") or 0)
    timeline = run.get("connectivity_timeline") or {}
    incident_count = 0
    total_downtime = 0.0
    for dep_payload in timeline.values():
        if not isinstance(dep_payload, dict):
            continue
        for incident in dep_payload.get("incidents") or []:
            incident_count += 1
            duration = incident.get("duration_seconds")
            if isinstance(duration, (int, float)):
                total_downtime += float(duration)
    derived = {
        "code_event_count": code_count,
        "infra_event_count": infra_count,
        "connectivity_incident_count": incident_count,
        "connectivity_total_downtime_s": total_downtime,
    }
    if event_count == 0:
        return "unknown", derived
    # Green = the runtime behaved as expected. ERROR-severity log lines emitted
    # at boot (e.g. mongo "equivalent index already exists") are noise, not test
    # failures — what disqualifies a run is the presence of issue_kind=code
    # events or active connectivity incidents.
    if code_count == 0 and incident_count == 0:
        return "green", derived
    return "red", derived


def find_run_jsons(root: Path) -> list[Path]:
    """Find all run.json files under root, sorted deterministically."""
    return sorted(path for path in root.rglob("run.json") if path.is_file())


def _load_state(out_dir: Path) -> dict[str, Any]:
    state_path = out_dir / "index_state.json"
    if state_path.exists():
        payload = read_json(state_path)
        if isinstance(payload, dict):
            payload.setdefault("runs_seen", {})
            return payload
    return {"registry_version": REGISTRY_VERSION, "runs_seen": {}}


def _save_state(out_dir: Path, state: dict[str, Any]) -> None:
    write_json(out_dir / "index_state.json", state)


def _load_existing_runs(out_dir: Path) -> dict[str, dict[str, Any]]:
    path = out_dir / "registry.parquet"
    if not path.exists():
        return {}
    table = pq.read_table(path)
    return {str(row["run_id"]): row for row in table.to_pylist()}


def build_registry(
    runs_root: str | Path,
    out_dir: str | Path,
    *,
    rebuild: bool = False,
    scenario_regex: str | None = None,
    min_green_runs: int = MIN_GREEN_RUNS_FOR_BASELINE,
) -> dict[str, Any]:
    """Refresh registry/template_registry/baseline parquet under `out_dir`.

    Idempotent. When `rebuild=True` ignores any prior state and reprocesses
    every run.json found under `runs_root`.
    """
    runs_root_path = Path(runs_root).resolve()
    out_dir_path = Path(out_dir).resolve()
    out_dir_path.mkdir(parents=True, exist_ok=True)

    scenario_pattern = re.compile(scenario_regex) if scenario_regex else None
    state = {"registry_version": REGISTRY_VERSION, "runs_seen": {}} if rebuild else _load_state(out_dir_path)
    existing_runs = {} if rebuild else _load_existing_runs(out_dir_path)
    runs_seen: dict[str, dict[str, Any]] = state.get("runs_seen", {})

    indexed_at = datetime.now(timezone.utc).isoformat()
    candidates = find_run_jsons(runs_root_path)
    new_or_changed: list[tuple[Path, dict[str, Any]]] = []
    skipped = 0
    for run_json in candidates:
        run_id = _read_run_id(run_json)
        if run_id is None:
            continue
        mtime_ns = run_json.stat().st_mtime_ns
        seen = runs_seen.get(run_id)
        if not rebuild and seen and int(seen.get("run_json_mtime_ns") or 0) == mtime_ns and run_id in existing_runs:
            skipped += 1
            continue
        payload = read_json(run_json)
        # If summary.json is present, fold issue_kind_event_counts into the
        # payload so classify_status sees the real code/infra counts produced
        # by the scan pipeline.
        summary_path = run_json.parent / "summary.json"
        if summary_path.exists():
            summary = read_json(summary_path)
            if isinstance(summary, dict) and summary.get("issue_kind_event_counts"):
                payload["issue_kind_event_counts"] = summary["issue_kind_event_counts"]
        new_or_changed.append((run_json, payload))
        runs_seen[run_id] = {
            "run_dir": str(run_json.parent),
            "run_json_mtime_ns": mtime_ns,
            "indexed_at": indexed_at,
        }

    # Build the new rows for changed runs.
    new_run_rows: dict[str, dict[str, Any]] = {}
    for run_json, payload in new_or_changed:
        row = _run_row_from_payload(payload, run_json, indexed_at, scenario_pattern)
        if row is not None:
            new_run_rows[row["run_id"]] = row

    # Merge with existing rows (untouched ones survive unchanged).
    merged: dict[str, dict[str, Any]] = {}
    for run_id, row in existing_runs.items():
        if run_id in new_run_rows:
            continue
        merged[run_id] = row
    merged.update(new_run_rows)

    registry_table = _runs_dict_to_table(merged.values())
    _atomic_write_parquet(registry_table, out_dir_path / "registry.parquet", compression="zstd", use_dictionary=True)

    template_registry_path = out_dir_path / "template_registry.parquet"
    baseline_path = out_dir_path / "baseline.parquet"
    # If nothing changed and prior artefacts exist, skip the expensive walk over
    # every per-run templates.parquet. This is what keeps incremental runs cheap.
    must_rebuild_aggregates = (
        rebuild
        or bool(new_run_rows)
        or not template_registry_path.exists()
        or not baseline_path.exists()
    )
    if must_rebuild_aggregates:
        template_registry = _build_template_registry(merged.values())
        _atomic_write_parquet(template_registry, template_registry_path, compression="zstd", use_dictionary=True)
        baseline_table = _build_baseline(merged.values(), indexed_at, min_green_runs)
        _atomic_write_parquet(baseline_table, baseline_path, compression="zstd", use_dictionary=True)
    else:
        template_registry = pq.read_table(template_registry_path)
        baseline_table = pq.read_table(baseline_path)

    state["runs_seen"] = runs_seen
    state["registry_version"] = REGISTRY_VERSION
    state["last_indexed_at"] = indexed_at
    _save_state(out_dir_path, state)

    summary = {
        "out_dir": str(out_dir_path),
        "runs_total": len(merged),
        "runs_new_or_changed": len(new_run_rows),
        "runs_skipped_unchanged": skipped,
        "templates_total": template_registry.num_rows,
        "baseline_rows": baseline_table.num_rows,
        "baseline_version": BASELINE_VERSION,
        "indexed_at": indexed_at,
    }
    return summary


def _read_run_id(run_json: Path) -> str | None:
    try:
        with run_json.open("rb") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    run_id = payload.get("run_id")
    return str(run_id) if run_id else None


def _run_row_from_payload(
    payload: dict[str, Any],
    run_json: Path,
    indexed_at: str,
    scenario_pattern: re.Pattern[str] | None,
) -> dict[str, Any] | None:
    run_id = payload.get("run_id")
    if not run_id:
        return None
    status, derived = classify_status(payload)
    autodiscovery = payload.get("autodiscovery") or {}
    image_fingerprint = autodiscovery.get("fingerprint") if isinstance(autodiscovery, dict) else None
    autodiscovery_status = autodiscovery.get("status") if isinstance(autodiscovery, dict) else None
    return {
        "run_id": str(run_id),
        "service_name": payload.get("service_name"),
        "scenario": derive_scenario(str(run_id), scenario_pattern),
        "created_at": payload.get("created_at"),
        "status": status,
        "event_count": int(payload.get("event_count") or 0),
        "template_count": int(payload.get("template_count") or 0),
        "error_count": int(payload.get("error_count") or 0),
        "code_event_count": int(derived.get("code_event_count") or 0),
        "infra_event_count": int(derived.get("infra_event_count") or 0),
        "connectivity_incident_count": int(derived.get("connectivity_incident_count") or 0),
        "connectivity_total_downtime_s": float(derived.get("connectivity_total_downtime_s") or 0.0),
        "scan_duration_seconds": float(payload.get("scan_duration_seconds") or 0.0),
        "events_per_second": float(payload.get("events_per_second") or 0.0),
        "throughput_gb_per_second": float(payload.get("throughput_gb_per_second") or 0.0),
        "input_bytes": int(payload.get("input_bytes") or 0),
        "engine": payload.get("engine"),
        "git_branch": payload.get("git_branch"),
        "git_commit": payload.get("git_commit"),
        "image_fingerprint": image_fingerprint,
        "autodiscovery_status": autodiscovery_status,
        "run_dir": str(run_json.parent),
        "run_json_mtime_ns": int(run_json.stat().st_mtime_ns),
        "indexed_at": indexed_at,
    }


def _runs_dict_to_table(rows: Iterable[dict[str, Any]]) -> pa.Table:
    rows_list = sorted(rows, key=lambda row: (row.get("service_name") or "", row.get("created_at") or "", row.get("run_id") or ""))
    if not rows_list:
        return pa.table({field.name: pa.array([], type=field.type) for field in RUN_REGISTRY_SCHEMA}, schema=RUN_REGISTRY_SCHEMA)
    columns: dict[str, list[Any]] = {field.name: [] for field in RUN_REGISTRY_SCHEMA}
    for row in rows_list:
        for field in RUN_REGISTRY_SCHEMA:
            columns[field.name].append(row.get(field.name))
    return pa.table(columns, schema=RUN_REGISTRY_SCHEMA)


def _build_template_registry(runs: Iterable[dict[str, Any]]) -> pa.Table:
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for run in runs:
        run_dir = run.get("run_dir")
        if not run_dir:
            continue
        templates_path = Path(run_dir) / "templates.parquet"
        templates = read_parquet(templates_path)
        if not templates:
            continue
        run_id = str(run.get("run_id") or "")
        service = str(run.get("service_name") or "")
        created_at = run.get("created_at") or ""
        is_green = run.get("status") == "green"
        for tpl in templates:
            template_id = str(tpl.get("template_id") or "")
            if not template_id:
                continue
            key = (service, template_id)
            entry = by_key.get(key)
            count = int(tpl.get("event_count") or 0)
            severity_text = tpl.get("severity_text") or ""
            severity_number = int(tpl.get("severity_number") or 0)
            normalized = tpl.get("normalized_template") or ""
            issue_kind = tpl.get("issue_kind")
            error_kind = tpl.get("error_kind")
            exception_type = tpl.get("exception_type")
            if entry is None:
                by_key[key] = {
                    "service_name": service,
                    "template_id": template_id,
                    "normalized_template": normalized,
                    "severity_text": severity_text,
                    "severity_number": severity_number,
                    "issue_kind": issue_kind,
                    "error_kind": error_kind,
                    "exception_type": exception_type,
                    "first_seen_run_id": run_id,
                    "last_seen_run_id": run_id,
                    "first_seen_at": created_at,
                    "last_seen_at": created_at,
                    "runs_seen_count": 1,
                    "green_runs_seen_count": 1 if is_green else 0,
                    "total_event_count": count,
                }
                continue
            entry["runs_seen_count"] += 1
            entry["total_event_count"] += count
            if is_green:
                entry["green_runs_seen_count"] += 1
            if not entry.get("first_seen_at") or (created_at and created_at < entry["first_seen_at"]):
                entry["first_seen_at"] = created_at
                entry["first_seen_run_id"] = run_id
            if not entry.get("last_seen_at") or (created_at and created_at > entry["last_seen_at"]):
                entry["last_seen_at"] = created_at
                entry["last_seen_run_id"] = run_id
    rows = sorted(by_key.values(), key=lambda row: (row["service_name"], -row["total_event_count"], row["template_id"]))
    if not rows:
        return pa.table(
            {field.name: pa.array([], type=field.type) for field in TEMPLATE_REGISTRY_SCHEMA},
            schema=TEMPLATE_REGISTRY_SCHEMA,
        )
    columns: dict[str, list[Any]] = {field.name: [] for field in TEMPLATE_REGISTRY_SCHEMA}
    for row in rows:
        for field in TEMPLATE_REGISTRY_SCHEMA:
            columns[field.name].append(row.get(field.name))
    return pa.table(columns, schema=TEMPLATE_REGISTRY_SCHEMA)


def _build_baseline(
    runs: Iterable[dict[str, Any]],
    indexed_at: str,
    min_green_runs: int,
) -> pa.Table:
    # Collect per-(service, scenario, template_id) the event_count from each green run.
    samples: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "counts": [],
            "normalized_template": "",
            "severity_text": "",
            "issue_kind": None,
        }
    )
    green_runs_per_cohort: dict[tuple[str, str], int] = defaultdict(int)

    for run in runs:
        if run.get("status") != "green":
            continue
        service = str(run.get("service_name") or "")
        scenario = str(run.get("scenario") or "default")
        green_runs_per_cohort[(service, scenario)] += 1
        templates_path = Path(run.get("run_dir") or "") / "templates.parquet"
        templates = read_parquet(templates_path)
        if not templates:
            continue
        seen_in_run: set[str] = set()
        for tpl in templates:
            template_id = str(tpl.get("template_id") or "")
            if not template_id:
                continue
            seen_in_run.add(template_id)
            entry = samples[(service, scenario, template_id)]
            entry["counts"].append(int(tpl.get("event_count") or 0))
            entry["normalized_template"] = tpl.get("normalized_template") or entry["normalized_template"]
            entry["severity_text"] = tpl.get("severity_text") or entry["severity_text"]
            entry["issue_kind"] = tpl.get("issue_kind") or entry["issue_kind"]
        # Templates that exist in baseline but were ABSENT from this green run still
        # contribute a zero so we don't over-estimate the expected count.
        # We add the zeros after the loop because we don't know the universe yet
        # — handled by post-processing below.

    # Post-process: pad with zeros for templates absent from individual green runs.
    cohort_template_ids: dict[tuple[str, str], set[str]] = defaultdict(set)
    for service, scenario, template_id in samples:
        cohort_template_ids[(service, scenario)].add(template_id)
    cohort_runs: dict[tuple[str, str], list[str]] = defaultdict(list)
    for run in runs:
        if run.get("status") != "green":
            continue
        service = str(run.get("service_name") or "")
        scenario = str(run.get("scenario") or "default")
        cohort_runs[(service, scenario)].append(str(run.get("run_id") or ""))
        templates_path = Path(run.get("run_dir") or "") / "templates.parquet"
        templates = read_parquet(templates_path)
        present_ids = {str(tpl.get("template_id") or "") for tpl in templates}
        for template_id in cohort_template_ids[(service, scenario)]:
            if template_id in present_ids:
                continue
            entry = samples.get((service, scenario, template_id))
            if entry is not None:
                entry["counts"].append(0)

    rows: list[dict[str, Any]] = []
    for (service, scenario, template_id), entry in samples.items():
        runs_in_cohort = green_runs_per_cohort.get((service, scenario), 0)
        if runs_in_cohort < min_green_runs:
            continue
        counts = entry["counts"]
        if not counts:
            continue
        mean = sum(counts) / len(counts)
        variance = sum((c - mean) ** 2 for c in counts) / max(len(counts) - 1, 1) if len(counts) > 1 else 0.0
        std = math.sqrt(variance)
        rows.append(
            {
                "service_name": service,
                "scenario": scenario,
                "template_id": template_id,
                "normalized_template": entry["normalized_template"],
                "severity_text": entry["severity_text"],
                "issue_kind": entry["issue_kind"],
                "mean_count": mean,
                "std_count": std,
                "p50_count": _percentile(counts, 0.50),
                "p95_count": _percentile(counts, 0.95),
                "p99_count": _percentile(counts, 0.99),
                "min_count": min(counts),
                "max_count": max(counts),
                "runs_in_baseline": len(counts),
                "baseline_version": BASELINE_VERSION,
                "last_updated": indexed_at,
            }
        )
    rows.sort(key=lambda row: (row["service_name"], row["scenario"], -row["mean_count"], row["template_id"]))
    if not rows:
        return pa.table(
            {field.name: pa.array([], type=field.type) for field in BASELINE_SCHEMA},
            schema=BASELINE_SCHEMA,
        )
    columns: dict[str, list[Any]] = {field.name: [] for field in BASELINE_SCHEMA}
    for row in rows:
        for field in BASELINE_SCHEMA:
            columns[field.name].append(row.get(field.name))
    return pa.table(columns, schema=BASELINE_SCHEMA)


def _percentile(values: list[int], q: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    pos = q * (len(sorted_vals) - 1)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return float(sorted_vals[lo])
    frac = pos - lo
    return float(sorted_vals[lo]) * (1 - frac) + float(sorted_vals[hi]) * frac


def load_registry(out_dir: str | Path) -> pa.Table:
    return pq.read_table(Path(out_dir) / "registry.parquet")


def load_template_registry(out_dir: str | Path) -> pa.Table:
    return pq.read_table(Path(out_dir) / "template_registry.parquet")


def load_baseline(out_dir: str | Path) -> pa.Table:
    return pq.read_table(Path(out_dir) / "baseline.parquet")
