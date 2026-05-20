from __future__ import annotations

from pathlib import Path
from typing import Any

from .io import read_parquet
from .rules import matching_rule, rule_reason

ERROR_SEVERITIES = {"ERROR", "CRITICAL", "FATAL"}


def load_baseline_templates(baseline_dir: str | Path | None) -> dict[str, dict[str, Any]]:
    if not baseline_dir:
        return {}
    path = Path(baseline_dir) / "templates.parquet"
    rows = read_parquet(path)
    return {str(row["template_id"]): row for row in rows if row.get("template_id")}


def classify_templates(
    templates: list[dict[str, Any]],
    *,
    rules: dict[str, Any],
    baseline: dict[str, dict[str, Any]],
) -> None:
    known_noise_rules = rules.get("known_noise") or []
    expected_rules = rules.get("expected") or []
    unexpected_cfg = rules.get("unexpected") or {}
    unexpected_severities = {
        str(item).upper() for item in unexpected_cfg.get("new_error_severities", ["ERROR", "CRITICAL", "FATAL"])
    }

    for row in templates:
        template_id = str(row.get("template_id"))
        row["baseline_match"] = template_id in baseline
        noise_rule = matching_rule(row, known_noise_rules)
        if noise_rule:
            row["classification"] = "known-noise"
            row["classification_reason"] = rule_reason(noise_rule, "matched known-noise rule")
            continue

        expected_rule = matching_rule(row, expected_rules)
        if expected_rule:
            row["classification"] = "expected"
            row["classification_reason"] = rule_reason(expected_rule, "matched expected rule")
            continue

        if row["baseline_match"]:
            row["classification"] = "expected"
            row["classification_reason"] = "template_id exists in baseline"
            continue

        severity = str(row.get("severity_text") or "").upper()
        if severity in unexpected_severities or row.get("error_kind") not in (None, "", "none"):
            row["classification"] = "unexpected"
            row["classification_reason"] = "new error template not found in baseline or expected rules"
        else:
            row["classification"] = "observed"
            row["classification_reason"] = "non-error template observed in this run"


def classify_events_from_templates(events: list[dict[str, Any]], templates: list[dict[str, Any]]) -> None:
    by_id = {row["template_id"]: row for row in templates}
    for event in events:
        template = by_id.get(event["template_id"])
        if template:
            event["classification"] = template.get("classification")
            event["classification_reason"] = template.get("classification_reason")


def build_error_rows(templates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in templates:
        severity = str(row.get("severity_text") or "").upper()
        classification = row.get("classification")
        is_error = severity in ERROR_SEVERITIES or row.get("error_kind") not in (None, "", "none")
        if not is_error and classification not in {"unexpected", "known-noise"}:
            continue
        rows.append(
            {
                "template_id": row.get("template_id"),
                "service_name": row.get("service_name"),
                "severity_text": row.get("severity_text"),
                "error_kind": row.get("error_kind"),
                "exception_type": row.get("exception_type"),
                "classification": classification,
                "reason": row.get("classification_reason"),
                "event_count": row.get("event_count"),
                "first_seen": row.get("first_seen"),
                "last_seen": row.get("last_seen"),
                "baseline_match": row.get("baseline_match"),
                "normalized_template": row.get("normalized_template"),
            }
        )
    return rows
