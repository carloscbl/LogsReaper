"""Diff engine: compare a fresh scan against the statistical baseline.

Outputs four classes of findings:

* ``new_templates``: template_id present in the run, absent in the baseline.
* ``missing_templates``: present in the baseline, absent in the run.
* ``regressed_templates``: present in both, observed_count exceeds the
  expected band (count > p95 AND z-score > threshold).
* ``severity_shifted``: severity_text differs run vs. baseline.

Plus ``connectivity_regressions`` (incidents that exceed historical MTTR).

The engine is data-only. It returns a dict-of-lists; persistence (parquet, json,
markdown) is done by the CLI wrapper.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .io import read_json, read_parquet
from .lineage import annotate_diff_with_lineage
from .overrides import apply_to_diff, load_overrides
from .registry import derive_scenario


def load_baseline_for(
    baseline_table: pa.Table,
    service: str,
    scenario: str,
) -> dict[str, dict[str, Any]]:
    """Return baseline rows for the (service, scenario) cohort keyed by template_id."""
    if baseline_table.num_rows == 0:
        return {}
    rows = baseline_table.to_pylist()
    return {
        row["template_id"]: row
        for row in rows
        if row.get("service_name") == service and row.get("scenario") == scenario
    }


def compute_diff(
    *,
    run_dir: Path,
    baseline_path: Path,
    z_threshold: float = 3.0,
    min_observed_count: int = 5,
    scenario_override: str | None = None,
    lineage_min_similarity: float = 0.5,
    lineage_enabled: bool = True,
    overrides_dir: Path | None = None,
) -> dict[str, Any]:
    """Compare the templates of one scan against the baseline parquet.

    The baseline cohort is inferred from the run's ``service_name`` and a scenario
    derived from ``run_id`` (or the explicit override).
    """
    run_meta = read_json(run_dir / "run.json")
    service = str(run_meta.get("service_name") or "")
    run_id = str(run_meta.get("run_id") or "")
    scenario = scenario_override or derive_scenario(run_id)
    templates = read_parquet(run_dir / "templates.parquet")
    observed = {str(row["template_id"]): row for row in templates}

    if not baseline_path.exists():
        baseline_for_cohort: dict[str, dict[str, Any]] = {}
        baseline_status = "missing"
    else:
        baseline_table = pq.read_table(baseline_path)
        baseline_for_cohort = load_baseline_for(baseline_table, service, scenario)
        baseline_status = "present" if baseline_for_cohort else "empty_cohort"

    new_templates: list[dict[str, Any]] = []
    missing_templates: list[dict[str, Any]] = []
    regressed_templates: list[dict[str, Any]] = []
    severity_shifted: list[dict[str, Any]] = []

    observed_ids = set(observed)
    baseline_ids = set(baseline_for_cohort)

    for template_id in sorted(observed_ids - baseline_ids):
        row = observed[template_id]
        new_templates.append(
            {
                "template_id": template_id,
                "service_name": service,
                "scenario": scenario,
                "normalized_template": row.get("normalized_template"),
                "severity_text": row.get("severity_text"),
                "observed_count": int(row.get("event_count") or 0),
                "issue_kind": row.get("issue_kind"),
            }
        )

    for template_id in sorted(baseline_ids - observed_ids):
        base = baseline_for_cohort[template_id]
        missing_templates.append(
            {
                "template_id": template_id,
                "service_name": service,
                "scenario": scenario,
                "normalized_template": base.get("normalized_template"),
                "severity_text": base.get("severity_text"),
                "baseline_mean": float(base.get("mean_count") or 0.0),
                "baseline_p95": float(base.get("p95_count") or 0.0),
                "runs_in_baseline": int(base.get("runs_in_baseline") or 0),
            }
        )

    for template_id in sorted(observed_ids & baseline_ids):
        observed_row = observed[template_id]
        baseline_row = baseline_for_cohort[template_id]
        observed_count = int(observed_row.get("event_count") or 0)
        mean = float(baseline_row.get("mean_count") or 0.0)
        std = float(baseline_row.get("std_count") or 0.0)
        p95 = float(baseline_row.get("p95_count") or 0.0)
        delta_factor = observed_count / mean if mean > 0 else math.inf if observed_count > 0 else 1.0
        z_score = (observed_count - mean) / std if std > 1e-9 else (math.inf if observed_count > mean else 0.0)
        is_regression = (
            observed_count >= min_observed_count
            and observed_count > p95
            and z_score > z_threshold
        )
        if is_regression:
            regressed_templates.append(
                {
                    "template_id": template_id,
                    "service_name": service,
                    "scenario": scenario,
                    "normalized_template": observed_row.get("normalized_template"),
                    "severity_text": observed_row.get("severity_text"),
                    "observed_count": observed_count,
                    "baseline_mean": mean,
                    "baseline_std": std,
                    "baseline_p95": p95,
                    "delta_factor": delta_factor,
                    "z_score": z_score,
                    "runs_in_baseline": int(baseline_row.get("runs_in_baseline") or 0),
                }
            )
        run_severity = str(observed_row.get("severity_text") or "")
        base_severity = str(baseline_row.get("severity_text") or "")
        if run_severity and base_severity and run_severity != base_severity:
            severity_shifted.append(
                {
                    "template_id": template_id,
                    "service_name": service,
                    "scenario": scenario,
                    "normalized_template": observed_row.get("normalized_template"),
                    "previous_severity": base_severity,
                    "current_severity": run_severity,
                    "observed_count": observed_count,
                }
            )

    connectivity_regressions = _connectivity_regressions(run_meta)

    # Headline finding: high-impact code errors (issue_kind=code AND severity in
    # {ERROR,CRITICAL,FATAL}) regardless of whether the template existed in the
    # baseline. A 25-event traceback is what an engineer needs to see first.
    error_severities = {"ERROR", "CRITICAL", "FATAL"}
    code_errors: list[dict[str, Any]] = []
    for template_id, row in observed.items():
        if str(row.get("issue_kind") or "") != "code":
            continue
        if str(row.get("severity_text") or "").upper() not in error_severities:
            continue
        count = int(row.get("event_count") or 0)
        if count <= 0:
            continue
        base = baseline_for_cohort.get(template_id)
        was_in_baseline = base is not None
        code_errors.append(
            {
                "template_id": template_id,
                "service_name": service,
                "scenario": scenario,
                "normalized_template": row.get("normalized_template"),
                "severity_text": row.get("severity_text"),
                "exception_type": row.get("exception_type"),
                "error_kind": row.get("error_kind"),
                "observed_count": count,
                "is_new": not was_in_baseline,
                "baseline_mean": float(base.get("mean_count") or 0.0) if base else None,
                "first_seen_at": row.get("first_seen"),
                "last_seen_at": row.get("last_seen"),
                "example_event_id": row.get("example_event_id"),
            }
        )
    code_errors.sort(key=lambda r: (-r["observed_count"], r["template_id"]))

    diff = {
        "run_id": run_id,
        "service_name": service,
        "scenario": scenario,
        "baseline_status": baseline_status,
        "baseline_cohort_size": len(baseline_for_cohort),
        "new_templates": new_templates,
        "missing_templates": missing_templates,
        "regressed_templates": sorted(regressed_templates, key=lambda r: (-r["delta_factor"], r["template_id"])),
        "severity_shifted": severity_shifted,
        "connectivity_regressions": connectivity_regressions,
        "code_errors": code_errors,
        "summary_counts": {
            "new": len(new_templates),
            "missing": len(missing_templates),
            "regressed": len(regressed_templates),
            "severity_shifted": len(severity_shifted),
            "connectivity_regressions": len(connectivity_regressions),
            "code_errors": len(code_errors),
            "code_error_events": sum(r["observed_count"] for r in code_errors),
        },
    }
    if lineage_enabled and baseline_for_cohort:
        annotate_diff_with_lineage(
            diff, baseline_for_cohort, min_similarity=lineage_min_similarity
        )
        evolved = sum(1 for entry in diff["new_templates"] if entry.get("kind") == "evolved_template")
        diff["summary_counts"]["evolved"] = evolved
    else:
        diff["summary_counts"]["evolved"] = 0

    # Overrides manuales (pinned/banned) — aplicados al final para que su
    # efecto pise tanto baseline estadístico como lineage.
    overrides_root = overrides_dir or baseline_path.parent
    overrides_data = load_overrides(overrides_root)
    apply_to_diff(diff, overrides_data)
    return diff


def _connectivity_regressions(run_meta: dict[str, Any]) -> list[dict[str, Any]]:
    timeline = run_meta.get("connectivity_timeline") or {}
    out: list[dict[str, Any]] = []
    for dep, payload in timeline.items():
        if not isinstance(payload, dict):
            continue
        incidents = payload.get("incidents") or []
        for incident in incidents:
            out.append(
                {
                    "dependency": dep,
                    "down_at": incident.get("down_at"),
                    "up_at": incident.get("up_at"),
                    "duration_seconds": incident.get("duration_seconds"),
                }
            )
    return out


def diff_to_table(diff: dict[str, Any]) -> pa.Table:
    """Flatten the diff dict to a single tall parquet-friendly table."""
    rows: list[dict[str, Any]] = []
    for entry in diff.get("new_templates", []):
        kind = entry.get("kind") or "new_template"
        rows.append({**entry, "kind": kind})
    for entry in diff.get("missing_templates", []):
        rows.append({"kind": "missing_template", **entry})
    for entry in diff.get("regressed_templates", []):
        rows.append({"kind": "regressed_template", **entry})
    for entry in diff.get("severity_shifted", []):
        rows.append({"kind": "severity_shifted", **entry})
    for entry in diff.get("connectivity_regressions", []):
        rows.append({"kind": "connectivity_regression", **entry})
    for entry in diff.get("code_errors", []):
        rows.append({"kind": "code_error", **entry})
    for entry in diff.get("policy_violations", []):
        rows.append({"kind": "policy_violation", **entry})
    for entry in diff.get("pinned_missing", []):
        rows.append({"kind": "pinned_missing", **entry})
    if not rows:
        return pa.table({"kind": pa.array([], type=pa.string())})
    return pa.Table.from_pylist(rows)
