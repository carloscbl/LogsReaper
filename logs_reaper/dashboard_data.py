"""Pure-data layer for the dashboard.

Everything visual in ``dashboard.py`` reads only from these functions, so the
heavy lifting (sort, pivot, z-score, novelty index) is testable without a
Streamlit/Plotly runtime.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .io import read_json, read_parquet


def list_services(registry_table: pa.Table) -> list[str]:
    if registry_table.num_rows == 0:
        return []
    return sorted({str(row["service_name"]) for row in registry_table.to_pylist() if row.get("service_name")})


def list_scenarios(registry_table: pa.Table, service: str) -> list[str]:
    if registry_table.num_rows == 0:
        return []
    return sorted(
        {
            str(row["scenario"])
            for row in registry_table.to_pylist()
            if row.get("service_name") == service and row.get("scenario")
        }
    )


def filter_runs(registry_table: pa.Table, service: str | None, scenario: str | None) -> list[dict[str, Any]]:
    rows = registry_table.to_pylist()
    if service is not None:
        rows = [row for row in rows if row.get("service_name") == service]
    if scenario is not None:
        rows = [row for row in rows if row.get("scenario") == scenario]
    rows.sort(key=lambda row: (row.get("created_at") or "", row.get("run_id") or ""))
    return rows


def heatmap_matrix(
    runs: list[dict[str, Any]],
    *,
    baseline_for_cohort: dict[str, dict[str, Any]],
    top_n: int = 50,
) -> dict[str, Any]:
    """Return the data needed to render a template × run heatmap.

    The cell value is the z-score of ``observed_count`` against the baseline
    mean/std. When the template is absent from the baseline, the cell is None
    so the dashboard can colour it as "novel".
    """
    template_counts: dict[str, dict[str, int]] = {}
    template_totals: dict[str, int] = {}
    for run in runs:
        run_id = str(run.get("run_id") or "")
        run_dir = Path(run.get("run_dir") or "")
        templates = read_parquet(run_dir / "templates.parquet") if run_dir.exists() else []
        for tpl in templates:
            template_id = str(tpl.get("template_id") or "")
            if not template_id:
                continue
            count = int(tpl.get("event_count") or 0)
            template_counts.setdefault(template_id, {})[run_id] = count
            template_totals[template_id] = template_totals.get(template_id, 0) + count

    top_template_ids = [
        template_id
        for template_id, _ in sorted(template_totals.items(), key=lambda kv: (-kv[1], kv[0]))[:top_n]
    ]
    run_ids = [str(run.get("run_id") or "") for run in runs]

    z_rows: list[list[float | None]] = []
    raw_rows: list[list[int]] = []
    is_novel_rows: list[list[bool]] = []
    for template_id in top_template_ids:
        z_row: list[float | None] = []
        raw_row: list[int] = []
        novel_row: list[bool] = []
        base = baseline_for_cohort.get(template_id)
        for run_id in run_ids:
            count = template_counts.get(template_id, {}).get(run_id, 0)
            raw_row.append(count)
            if base is None:
                z_row.append(None)
                novel_row.append(True)
            else:
                mean = float(base.get("mean_count") or 0.0)
                std = float(base.get("std_count") or 0.0)
                if std < 1e-9:
                    z_row.append(0.0 if count == mean else (math.inf if count > mean else -math.inf))
                else:
                    z_row.append((count - mean) / std)
                novel_row.append(False)
        z_rows.append(z_row)
        raw_rows.append(raw_row)
        is_novel_rows.append(novel_row)

    return {
        "template_ids": top_template_ids,
        "run_ids": run_ids,
        "z_scores": z_rows,
        "raw_counts": raw_rows,
        "is_novel": is_novel_rows,
    }


def novelty_curve(
    runs: list[dict[str, Any]],
    *,
    window: int = 5,
) -> dict[str, Any]:
    """For each run, fraction of templates not seen in the previous ``window`` runs."""
    run_template_sets: list[tuple[str, set[str], str | None]] = []
    for run in runs:
        run_id = str(run.get("run_id") or "")
        created_at = run.get("created_at")
        templates_path = Path(run.get("run_dir") or "") / "templates.parquet"
        templates = read_parquet(templates_path) if templates_path.exists() else []
        ids = {str(tpl.get("template_id") or "") for tpl in templates}
        ids.discard("")
        run_template_sets.append((run_id, ids, created_at))

    novelties: list[dict[str, Any]] = []
    for idx, (run_id, ids, created_at) in enumerate(run_template_sets):
        window_start = max(0, idx - window)
        prior: set[str] = set()
        for j in range(window_start, idx):
            prior |= run_template_sets[j][1]
        novel = ids - prior
        fraction = len(novel) / len(ids) if ids else 0.0
        novelties.append(
            {
                "run_id": run_id,
                "created_at": created_at,
                "templates_in_run": len(ids),
                "novel_count": len(novel),
                "novelty_fraction": fraction,
            }
        )
    return {"window": window, "rows": novelties}


def connectivity_gantt(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten every connectivity incident across runs for a Gantt-style chart."""
    out: list[dict[str, Any]] = []
    for run in runs:
        run_dir = Path(run.get("run_dir") or "")
        if not run_dir.exists():
            continue
        meta_path = run_dir / "run.json"
        if not meta_path.exists():
            continue
        meta = read_json(meta_path)
        timeline = meta.get("connectivity_timeline") or {}
        for dep, payload in timeline.items():
            if not isinstance(payload, dict):
                continue
            for incident in payload.get("incidents") or []:
                out.append(
                    {
                        "run_id": str(meta.get("run_id") or ""),
                        "service_name": str(meta.get("service_name") or ""),
                        "dependency": dep,
                        "down_at": incident.get("down_at"),
                        "up_at": incident.get("up_at"),
                        "duration_seconds": incident.get("duration_seconds"),
                    }
                )
    return out


def regression_burndown(
    runs: list[dict[str, Any]],
    *,
    baseline_for_cohort: dict[str, dict[str, Any]],
    z_threshold: float = 3.0,
    min_observed_count: int = 5,
) -> list[dict[str, Any]]:
    """For each run produce (new_count, fixed_count, net) versus the previous run.

    A template is "new regression" when its z-score > threshold and observed >
    p95 and was not flagged in the previous run. A template is "fixed" when it
    was flagged in the previous run and is no longer flagged in this run.
    """
    prior_flagged: set[str] = set()
    rows: list[dict[str, Any]] = []
    for run in runs:
        run_id = str(run.get("run_id") or "")
        run_dir = Path(run.get("run_dir") or "")
        templates = read_parquet(run_dir / "templates.parquet") if run_dir.exists() else []
        current_flagged: set[str] = set()
        for tpl in templates:
            template_id = str(tpl.get("template_id") or "")
            if not template_id:
                continue
            count = int(tpl.get("event_count") or 0)
            base = baseline_for_cohort.get(template_id)
            if base is None:
                if count >= min_observed_count:
                    current_flagged.add(template_id)
                continue
            mean = float(base.get("mean_count") or 0.0)
            std = float(base.get("std_count") or 0.0)
            p95 = float(base.get("p95_count") or 0.0)
            if count < min_observed_count or count <= p95:
                continue
            z = (count - mean) / std if std > 1e-9 else (math.inf if count > mean else 0.0)
            if z > z_threshold:
                current_flagged.add(template_id)
        new_regressions = current_flagged - prior_flagged
        fixed = prior_flagged - current_flagged
        rows.append(
            {
                "run_id": run_id,
                "created_at": run.get("created_at"),
                "current_flagged_count": len(current_flagged),
                "new_regressions": len(new_regressions),
                "fixed_regressions": len(fixed),
                "net": len(new_regressions) - len(fixed),
            }
        )
        prior_flagged = current_flagged
    return rows


def survival_post_boot(run_dir: Path) -> list[dict[str, Any]]:
    """For each service_instance_seq, time-to-first-issue_kind=code event."""
    events_path = run_dir / "events.parquet"
    if not events_path.exists():
        return []
    table = pq.read_table(events_path, columns=[
        "service_instance_seq",
        "service_instance_started_at",
        "timestamp",
        "issue_kind",
    ])
    rows = table.to_pylist()
    by_seq: dict[int, dict[str, Any]] = {}
    for row in rows:
        seq = row.get("service_instance_seq")
        if seq is None:
            continue
        seq = int(seq)
        started = row.get("service_instance_started_at")
        ts = row.get("timestamp")
        entry = by_seq.setdefault(seq, {"started_at": started, "first_code_ts": None})
        if entry["started_at"] is None and started:
            entry["started_at"] = started
        if row.get("issue_kind") == "code" and entry["first_code_ts"] is None:
            entry["first_code_ts"] = ts
    out: list[dict[str, Any]] = []
    for seq, entry in sorted(by_seq.items()):
        out.append(
            {
                "service_instance_seq": seq,
                "started_at": entry["started_at"],
                "first_code_ts": entry["first_code_ts"],
                "survived": entry["first_code_ts"] is None,
            }
        )
    return out
