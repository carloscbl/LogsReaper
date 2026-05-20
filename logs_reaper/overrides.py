"""Baseline overrides — manual annotations on top of the statistical baseline.

Two decisions per (service, scenario, template_id):

* ``pinned``: este template SIEMPRE forma parte del baseline esperado, aunque
  las estadísticas digan lo contrario. Si aparece en un run, NO es anomalía;
  si NO aparece, se reporta en ``missing_templates`` con flag ``pinned``.

* ``banned``: este template NO debe aparecer; si aparece (incluso 1 evento),
  se reporta como ``policy_violation`` — independiente del baseline estadístico.

Persistencia: ``<registry_dir>/baseline_overrides.json``. Escritura atómica
(via ``.tmp`` + ``replace``) para que un dashboard concurrente nunca lea un
fichero parcial.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

OVERRIDES_FILENAME = "baseline_overrides.json"
DECISIONS = ("pinned", "banned")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _key(service: str, scenario: str, template_id: str) -> str:
    return f"{service}::{scenario}::{template_id}"


def load_overrides(registry_dir: Path) -> dict[str, Any]:
    path = registry_dir / OVERRIDES_FILENAME
    if not path.exists():
        return {"version": 1, "updated_at": None, "overrides": {}}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {"version": 1, "updated_at": None, "overrides": {}}


def save_overrides(registry_dir: Path, data: dict[str, Any]) -> Path:
    registry_dir.mkdir(parents=True, exist_ok=True)
    data["updated_at"] = _now_iso()
    target = registry_dir / OVERRIDES_FILENAME
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    os.replace(tmp, target)
    return target


def set_override(
    data: dict[str, Any],
    *,
    service: str,
    scenario: str,
    template_id: str,
    decision: str | None,
    reason: str = "",
    set_by: str = "",
) -> dict[str, Any]:
    """Mutate the overrides dict in-place. ``decision=None`` clears the entry."""
    overrides = data.setdefault("overrides", {})
    key = _key(service, scenario, template_id)
    if decision is None:
        overrides.pop(key, None)
        return data
    if decision not in DECISIONS:
        raise ValueError(f"decision must be one of {DECISIONS}, got {decision!r}")
    overrides[key] = {
        "service": service,
        "scenario": scenario,
        "template_id": template_id,
        "decision": decision,
        "reason": reason,
        "set_by": set_by,
        "set_at": _now_iso(),
    }
    return data


def get_decision(
    data: dict[str, Any],
    *,
    service: str,
    scenario: str,
    template_id: str,
) -> str | None:
    entry = (data.get("overrides") or {}).get(_key(service, scenario, template_id))
    return entry.get("decision") if entry else None


def list_overrides_for(
    data: dict[str, Any], *, service: str, scenario: str
) -> list[dict[str, Any]]:
    prefix = f"{service}::{scenario}::"
    return [
        entry
        for k, entry in (data.get("overrides") or {}).items()
        if k.startswith(prefix)
    ]


def apply_to_diff(
    diff: dict[str, Any], overrides_data: dict[str, Any]
) -> dict[str, Any]:
    """Adjust an in-place diff dict according to the overrides.

    Rules:
      pinned + appears in new_templates -> remove from new_templates
      pinned + NOT in observed         -> add to missing_templates (with pinned=True)
      banned + appears                 -> add to new ``policy_violations``
                                           (and remove from new_templates if there)

    Returns the (mutated) diff for convenience.
    """
    overrides = overrides_data.get("overrides") or {}
    if not overrides:
        diff.setdefault("policy_violations", [])
        diff.setdefault("pinned_missing", [])
        diff.setdefault("policy_violations_count", 0)
        return diff

    service = diff.get("service_name") or ""
    scenario = diff.get("scenario") or ""

    cohort = {
        k.split("::", 2)[2]: entry
        for k, entry in overrides.items()
        if k.startswith(f"{service}::{scenario}::")
    }

    observed_ids: set[str] = set()
    for entry in diff.get("new_templates", []):
        observed_ids.add(str(entry.get("template_id")))
    # Run-side templates that didn't fall in new (i.e. already-in-baseline) come
    # via the regressed/severity_shifted/code_errors slots. Aggregate all of them.
    for src in ("regressed_templates", "severity_shifted", "code_errors"):
        for entry in diff.get(src, []):
            observed_ids.add(str(entry.get("template_id")))

    new_templates = list(diff.get("new_templates", []))
    missing_templates = list(diff.get("missing_templates", []))
    policy_violations: list[dict[str, Any]] = []
    pinned_missing: list[dict[str, Any]] = []

    # Index helpers
    def _find_observed_payload(template_id: str) -> dict[str, Any] | None:
        for src in ("new_templates", "regressed_templates", "code_errors", "severity_shifted"):
            for entry in diff.get(src, []):
                if str(entry.get("template_id")) == template_id:
                    return entry
        return None

    for template_id, override in cohort.items():
        decision = override.get("decision")
        if decision == "pinned":
            new_templates = [e for e in new_templates if str(e.get("template_id")) != template_id]
            if template_id not in observed_ids:
                pinned_missing.append({
                    "template_id": template_id,
                    "service_name": service,
                    "scenario": scenario,
                    "reason": override.get("reason"),
                    "set_by": override.get("set_by"),
                })
        elif decision == "banned":
            if template_id in observed_ids:
                src = _find_observed_payload(template_id) or {}
                policy_violations.append({
                    "template_id": template_id,
                    "service_name": service,
                    "scenario": scenario,
                    "normalized_template": src.get("normalized_template"),
                    "severity_text": src.get("severity_text"),
                    "observed_count": int(src.get("observed_count") or src.get("event_count") or 0),
                    "exception_type": src.get("exception_type"),
                    "reason": override.get("reason"),
                    "set_by": override.get("set_by"),
                    "example_event_id": src.get("example_event_id"),
                })
                # Banned se enseña en su propia sección; quitarlo de "new" para
                # no duplicar señal.
                new_templates = [
                    e for e in new_templates if str(e.get("template_id")) != template_id
                ]

    diff["new_templates"] = new_templates
    diff["missing_templates"] = missing_templates
    diff["policy_violations"] = policy_violations
    diff["pinned_missing"] = pinned_missing
    diff["policy_violations_count"] = len(policy_violations)

    counts = diff.setdefault("summary_counts", {})
    counts["policy_violations"] = len(policy_violations)
    counts["pinned_missing"] = len(pinned_missing)
    counts["new"] = len(new_templates)
    return diff
