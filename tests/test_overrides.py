"""Tests for the manual baseline overrides layer."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from logs_reaper.overrides import (
    apply_to_diff,
    get_decision,
    list_overrides_for,
    load_overrides,
    save_overrides,
    set_override,
)


@pytest.fixture()
def fresh_registry(tmp_path: Path) -> Path:
    return tmp_path / "runs"


def test_load_empty_returns_skeleton(fresh_registry: Path) -> None:
    data = load_overrides(fresh_registry)
    assert data["version"] == 1
    assert data["overrides"] == {}


def test_save_then_load_round_trip(fresh_registry: Path) -> None:
    data = load_overrides(fresh_registry)
    set_override(
        data,
        service="accounts",
        scenario="provision",
        template_id="abc123",
        decision="pinned",
        reason="boot-time mongo notice",
        set_by="cbernal",
    )
    save_overrides(fresh_registry, data)
    reloaded = load_overrides(fresh_registry)
    assert get_decision(reloaded, service="accounts", scenario="provision", template_id="abc123") == "pinned"
    entries = list_overrides_for(reloaded, service="accounts", scenario="provision")
    assert len(entries) == 1
    assert entries[0]["reason"] == "boot-time mongo notice"


def test_clear_override_removes_entry(fresh_registry: Path) -> None:
    data = load_overrides(fresh_registry)
    set_override(data, service="svc", scenario="s", template_id="tid", decision="banned")
    set_override(data, service="svc", scenario="s", template_id="tid", decision=None)
    assert get_decision(data, service="svc", scenario="s", template_id="tid") is None


def test_set_override_rejects_unknown_decision(fresh_registry: Path) -> None:
    data = load_overrides(fresh_registry)
    with pytest.raises(ValueError):
        set_override(data, service="svc", scenario="s", template_id="tid", decision="ignored")


def test_apply_pinned_removes_new_template() -> None:
    diff = {
        "service_name": "accounts",
        "scenario": "provision",
        "new_templates": [
            {"template_id": "abc", "normalized_template": "foo", "severity_text": "INFO"},
            {"template_id": "zzz", "normalized_template": "bar", "severity_text": "INFO"},
        ],
        "missing_templates": [],
        "regressed_templates": [],
        "severity_shifted": [],
        "code_errors": [],
        "summary_counts": {"new": 2, "regressed": 0},
    }
    overrides = {"overrides": {
        "accounts::provision::abc": {"decision": "pinned", "reason": ""}
    }}
    apply_to_diff(diff, overrides)
    ids = [e["template_id"] for e in diff["new_templates"]]
    assert "abc" not in ids and "zzz" in ids
    assert diff["summary_counts"]["new"] == 1


def test_apply_pinned_missing_when_absent_in_run() -> None:
    diff = {
        "service_name": "accounts",
        "scenario": "provision",
        "new_templates": [],
        "missing_templates": [],
        "regressed_templates": [],
        "severity_shifted": [],
        "code_errors": [],
        "summary_counts": {"new": 0},
    }
    overrides = {"overrides": {
        "accounts::provision::deadbeef": {"decision": "pinned", "reason": "must show up"}
    }}
    apply_to_diff(diff, overrides)
    assert len(diff["pinned_missing"]) == 1
    assert diff["pinned_missing"][0]["template_id"] == "deadbeef"
    assert diff["summary_counts"]["pinned_missing"] == 1


def test_apply_banned_promotes_to_policy_violation() -> None:
    diff = {
        "service_name": "accounts",
        "scenario": "provision",
        "new_templates": [
            {"template_id": "bad1", "normalized_template": "do not do this",
             "severity_text": "ERROR", "observed_count": 7},
        ],
        "missing_templates": [],
        "regressed_templates": [],
        "severity_shifted": [],
        "code_errors": [],
        "summary_counts": {"new": 1},
    }
    overrides = {"overrides": {
        "accounts::provision::bad1": {"decision": "banned", "reason": "deprecated path"}
    }}
    apply_to_diff(diff, overrides)
    assert len(diff["policy_violations"]) == 1
    pv = diff["policy_violations"][0]
    assert pv["template_id"] == "bad1"
    assert pv["observed_count"] == 7
    assert pv["reason"] == "deprecated path"
    # banned removed from new_templates so we don't show duplicate signal:
    assert diff["new_templates"] == []
    assert diff["summary_counts"]["policy_violations"] == 1


def test_apply_banned_silent_when_absent() -> None:
    diff = {
        "service_name": "accounts",
        "scenario": "provision",
        "new_templates": [],
        "missing_templates": [],
        "regressed_templates": [],
        "severity_shifted": [],
        "code_errors": [],
        "summary_counts": {"new": 0},
    }
    overrides = {"overrides": {
        "accounts::provision::quiet": {"decision": "banned", "reason": "should not appear"}
    }}
    apply_to_diff(diff, overrides)
    assert diff["policy_violations"] == []
    assert diff["summary_counts"]["policy_violations"] == 0


def test_overrides_for_other_cohort_are_ignored() -> None:
    diff = {
        "service_name": "accounts",
        "scenario": "provision",
        "new_templates": [
            {"template_id": "shared", "normalized_template": "x", "severity_text": "INFO"},
        ],
        "missing_templates": [], "regressed_templates": [], "severity_shifted": [],
        "code_errors": [],
        "summary_counts": {"new": 1},
    }
    overrides = {"overrides": {
        # mismo template_id pero distinto service:
        "gateway-isp::provision::shared": {"decision": "banned", "reason": ""},
    }}
    apply_to_diff(diff, overrides)
    assert len(diff["new_templates"]) == 1
    assert diff["policy_violations"] == []


def test_save_overrides_writes_atomic_file(fresh_registry: Path, tmp_path: Path) -> None:
    data = load_overrides(fresh_registry)
    set_override(data, service="s", scenario="x", template_id="t", decision="pinned")
    path = save_overrides(fresh_registry, data)
    assert path.exists()
    raw = json.loads(path.read_text())
    assert raw["overrides"]
    assert raw["updated_at"]
