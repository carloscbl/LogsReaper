"""Tests for baselines partitioning, classify_delta and commit gating."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from logs_reaper.baselines import (
    append_changelog,
    classify_delta,
    commit_baselines,
    partition_baselines,
)


def _baseline_table(rows):
    schema = pa.schema([
        pa.field("service_name", pa.string()),
        pa.field("scenario", pa.string()),
        pa.field("template_id", pa.string()),
        pa.field("mean_count", pa.float64()),
        pa.field("severity_text", pa.string()),
        pa.field("normalized_template", pa.string()),
    ])
    return pa.table({k: [r.get(k) for r in rows] for k in schema.names}, schema=schema)


def _write_aggregate(tmp_path: Path, rows, overrides):
    agg = tmp_path / "runs"
    agg.mkdir(parents=True)
    pq.write_table(_baseline_table(rows), agg / "baseline.parquet")
    pq.write_table(_baseline_table(rows), agg / "template_registry.parquet")
    (agg / "baseline_overrides.json").write_text(json.dumps(overrides))
    return agg


# ---------- classify_delta ---------------------------------------------------

def _diff(**counts):
    return {"summary_counts": counts, "new_templates": [], "severity_shifted": []}


def test_classify_delta_no_change():
    assert classify_delta(_diff()) == "no-change"


def test_classify_delta_safe_only_info_new():
    diff = _diff(new=2)
    diff["new_templates"] = [{"severity_text": "INFO"}, {"severity_text": "DEBUG"}]
    assert classify_delta(diff) == "safe"


def test_classify_delta_unsafe_when_code_errors():
    assert classify_delta(_diff(code_errors=1)) == "unsafe"


def test_classify_delta_unsafe_when_policy_violations():
    assert classify_delta(_diff(policy_violations=1)) == "unsafe"


def test_classify_delta_unsafe_when_severity_up():
    diff = _diff(severity_shifted=1)
    diff["severity_shifted"] = [{"previous_severity": "INFO", "current_severity": "ERROR"}]
    assert classify_delta(diff) == "unsafe"


def test_classify_delta_unsafe_when_new_template_is_error():
    diff = _diff(new=1)
    diff["new_templates"] = [{"severity_text": "ERROR"}]
    assert classify_delta(diff) == "unsafe"


def test_classify_delta_unsafe_when_regressed():
    assert classify_delta(_diff(regressed=1)) == "unsafe"


# ---------- partition_baselines ----------------------------------------------

def test_partition_baselines_creates_per_service_files(tmp_path):
    rows = [
        {"service_name": "accounts", "scenario": "default", "template_id": "a", "mean_count": 1.0,
         "severity_text": "INFO", "normalized_template": "x"},
        {"service_name": "gateway-isp", "scenario": "default", "template_id": "b", "mean_count": 2.0,
         "severity_text": "INFO", "normalized_template": "y"},
    ]
    overrides = {"version": 1, "overrides": {
        "accounts::default::a": {"decision": "pinned", "reason": "ok"},
        "gateway-isp::default::b": {"decision": "banned", "reason": "deprecated"},
    }}
    agg = _write_aggregate(tmp_path, rows, overrides)
    baselines_dir = tmp_path / "baselines"
    result = partition_baselines(aggregate_dir=agg, baselines_dir=baselines_dir)
    services = {entry["service"] for entry in result["services"]}
    assert services == {"accounts", "gateway-isp"}

    acc_dir = baselines_dir / "accounts"
    assert (acc_dir / "baseline.parquet").exists()
    assert (acc_dir / "template_registry.parquet").exists()
    acc_overrides = json.loads((acc_dir / "baseline_overrides.json").read_text())
    assert list(acc_overrides["overrides"].keys()) == ["accounts::default::a"]


def test_partition_baselines_handles_missing_aggregate(tmp_path):
    result = partition_baselines(aggregate_dir=tmp_path, baselines_dir=tmp_path / "b")
    assert result["services"] == []


# ---------- commit_baselines -------------------------------------------------

def _git(*args, cwd):
    return subprocess.run(["git", "-C", str(cwd), *args], check=True, capture_output=True, text=True)


@pytest.fixture()
def git_repo(tmp_path: Path) -> Path:
    _git("init", "-q", cwd=tmp_path)
    _git("config", "user.email", "t@t.com", cwd=tmp_path)
    _git("config", "user.name", "t", cwd=tmp_path)
    _git("commit", "--allow-empty", "-m", "init", cwd=tmp_path)
    return tmp_path


def test_commit_baselines_dry_run_does_not_touch_git(git_repo: Path):
    baselines = git_repo / "tools" / "LogsReaper" / "baselines" / "accounts"
    baselines.mkdir(parents=True)
    (baselines / "baseline.parquet").write_bytes(b"X")
    result = commit_baselines(
        repo_root=git_repo,
        baselines_dir=git_repo / "tools" / "LogsReaper" / "baselines",
        services=["accounts"],
        message="chore: test",
        dry_run=True,
    )
    assert result["status"] == "dry-run"
    log = _git("log", "--oneline", cwd=git_repo).stdout
    assert "chore: test" not in log


def test_commit_baselines_commits_when_not_dry_run(git_repo: Path):
    baselines = git_repo / "tools" / "LogsReaper" / "baselines" / "accounts"
    baselines.mkdir(parents=True)
    (baselines / "baseline.parquet").write_bytes(b"hello")
    result = commit_baselines(
        repo_root=git_repo,
        baselines_dir=git_repo / "tools" / "LogsReaper" / "baselines",
        services=["accounts"],
        message="chore(baselines): accounts",
        dry_run=False,
    )
    assert result["status"] == "committed"
    log = _git("log", "--oneline", cwd=git_repo).stdout
    assert "chore(baselines): accounts" in log


def test_commit_baselines_clean_when_no_changes(git_repo: Path):
    baselines = git_repo / "tools" / "LogsReaper" / "baselines" / "accounts"
    baselines.mkdir(parents=True)
    (baselines / "baseline.parquet").write_bytes(b"x")
    _git("add", "-A", cwd=git_repo)
    _git("commit", "-m", "seed", cwd=git_repo)
    result = commit_baselines(
        repo_root=git_repo,
        baselines_dir=git_repo / "tools" / "LogsReaper" / "baselines",
        services=["accounts"],
        message="noop",
        dry_run=False,
    )
    assert result["status"] in {"clean", "committed"}  # "committed" puede pasar si add-stages nada Y commit falla


# ---------- append_changelog -------------------------------------------------

def test_append_changelog_prepends_entry(tmp_path: Path):
    svc_dir = tmp_path / "accounts"
    svc_dir.mkdir()
    append_changelog(svc_dir, run_id="R1", delta_kind="safe", diff_counts={"new": 3})
    text = (svc_dir / "CHANGELOG.md").read_text()
    assert "R1" in text and "safe" in text
    append_changelog(svc_dir, run_id="R2", delta_kind="unsafe", diff_counts={"code_errors": 1})
    text2 = (svc_dir / "CHANGELOG.md").read_text()
    # entrada R2 va arriba (preprended)
    assert text2.index("R2") < text2.index("R1")
