from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

DEFAULT_RULES_PATH = Path(__file__).resolve().parents[1] / "configs" / "default-rules.yaml"


def load_rules(path: str | Path | None = None) -> dict[str, Any]:
    rules_path = Path(path) if path else DEFAULT_RULES_PATH
    if not rules_path.exists():
        return {}
    payload = yaml.safe_load(rules_path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def matching_rule(row: dict[str, Any], rules: list[dict[str, Any]]) -> dict[str, Any] | None:
    for rule in rules:
        if _matches(row, rule):
            return rule
    return None


def rule_reason(rule: dict[str, Any], default: str) -> str:
    return str(rule.get("reason") or rule.get("id") or default)


def _matches(row: dict[str, Any], rule: dict[str, Any]) -> bool:
    if not isinstance(rule, dict):
        return False
    for key in ("template_id", "service_name", "error_kind", "exception_type"):
        expected = rule.get(key)
        if expected is not None and str(row.get(key)) != str(expected):
            return False

    severity = rule.get("severity")
    if severity is not None:
        values = {item.upper() for item in _as_list(severity)}
        if str(row.get("severity_text") or "").upper() not in values:
            return False

    contains = rule.get("template_contains")
    if contains and str(contains) not in str(row.get("normalized_template") or ""):
        return False

    body_contains = rule.get("body_contains")
    if body_contains and str(body_contains) not in str(row.get("body") or ""):
        return False

    regex = rule.get("template_regex")
    if regex and not re.search(str(regex), str(row.get("normalized_template") or "")):
        return False

    body_regex = rule.get("body_regex")
    if body_regex and not re.search(str(body_regex), str(row.get("body") or "")):
        return False

    parse_status = rule.get("parse_status")
    if parse_status and str(row.get("parse_status")) != str(parse_status):
        return False

    return True


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    return [value]
