from __future__ import annotations

from pathlib import Path
from typing import Any

from .io import read_json, read_parquet, write_json
from .reports import render_compare_report

ERROR_CLASSES = {"unexpected", "regression"}
ERROR_SEVERITIES = {"ERROR", "CRITICAL", "FATAL"}


def compare_runs(
    *,
    left_dir: str | Path,
    right_dir: str | Path,
    out: str | Path,
    frequency_ratio: float = 2.0,
    min_count: int = 5,
) -> dict[str, Any]:
    left_path = Path(left_dir)
    right_path = Path(right_dir)
    left_run = _read_run(left_path)
    right_run = _read_run(right_path)
    left_templates = {row["template_id"]: row for row in read_parquet(left_path / "templates.parquet")}
    right_templates = {row["template_id"]: row for row in read_parquet(right_path / "templates.parquet")}

    left_ids = set(left_templates)
    right_ids = set(right_templates)
    new_ids = right_ids - left_ids
    fixed_ids = left_ids - right_ids

    regressions = []
    for template_id in sorted(new_ids):
        row = dict(right_templates[template_id])
        if _is_error_template(row):
            row["classification"] = "regression"
            row["reason"] = "new error template in right run"
            regressions.append(row)

    fixed_errors = []
    for template_id in sorted(fixed_ids):
        row = dict(left_templates[template_id])
        if _is_error_template(row):
            row["classification"] = "fixed"
            row["reason"] = "error template disappeared in right run"
            fixed_errors.append(row)

    frequency_increases = []
    for template_id in sorted(left_ids & right_ids):
        left_row = left_templates[template_id]
        right_row = right_templates[template_id]
        left_count = int(left_row.get("event_count") or 0)
        right_count = int(right_row.get("event_count") or 0)
        if right_count >= min_count and right_count >= max(left_count * frequency_ratio, left_count + min_count):
            payload = dict(right_row)
            payload["left_count"] = left_count
            payload["right_count"] = right_count
            frequency_increases.append(payload)

    payload = {
        "left_run_id": left_run.get("run_id", str(left_path)),
        "right_run_id": right_run.get("run_id", str(right_path)),
        "left_dir": str(left_path),
        "right_dir": str(right_path),
        "new_template_count": len(new_ids),
        "fixed_template_count": len(fixed_ids),
        "regression_count": len(regressions),
        "fixed_error_count": len(fixed_errors),
        "frequency_increase_count": len(frequency_increases),
        "regressions": regressions,
        "fixed_errors": fixed_errors,
        "frequency_increases": frequency_increases,
        "left_lib_versions": left_run.get("lib_versions", {}),
        "right_lib_versions": right_run.get("lib_versions", {}),
    }

    out_path = Path(out)
    if out_path.suffix.lower() == ".md":
        out_path.parent.mkdir(parents=True, exist_ok=True)
        md_path = out_path
        json_path = out_path.with_suffix(".json")
    else:
        out_path.mkdir(parents=True, exist_ok=True)
        md_path = out_path / "diff.md"
        json_path = out_path / "diff.json"
    md_path.write_text(render_compare_report(payload), encoding="utf-8")
    write_json(json_path, _json_safe(payload))
    return payload


def _read_run(path: Path) -> dict[str, Any]:
    run_path = path / "run.json"
    return read_json(run_path) if run_path.exists() else {}


def _is_error_template(row: dict[str, Any]) -> bool:
    severity = str(row.get("severity_text") or "").upper()
    return (
        severity in ERROR_SEVERITIES
        or row.get("error_kind") not in (None, "", "none")
        or row.get("classification") in ERROR_CLASSES
    )


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value
