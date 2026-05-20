"""Tests for template lineage + scenario-aware diff (Fase 3)."""
from __future__ import annotations

import sys
from pathlib import Path
from time import perf_counter

import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from logs_reaper.diff_engine import compute_diff, load_baseline_for
from logs_reaper.io import TEMPLATE_SCHEMA, write_json, write_parquet
from logs_reaper.lineage import (
    annotate_diff_with_lineage,
    find_predecessor,
    jaccard,
    template_shingles,
    tokenize,
)
from logs_reaper.registry import build_registry


def _baseline_template(template_id: str, count: int, normalized: str, severity: str = "ERROR") -> dict[str, object]:
    return {
        "template_id": template_id,
        "service_name": "accounts",
        "severity_text": severity,
        "severity_number": 17,
        "normalized_template": normalized,
        "error_kind": None,
        "exception_type": None,
        "event_count": count,
        "first_seen": "2026-05-14T10:00:00Z",
        "last_seen": "2026-05-14T10:05:00Z",
        "example_event_id": f"evt-{template_id}",
        "parse_status": "ok",
        "classification": "expected",
        "classification_reason": None,
        "baseline_match": True,
        "issue_kind": "code",
    }


def _make_run(
    runs_root: Path,
    *,
    run_id: str,
    service: str,
    created_at: str,
    templates: list[dict[str, object]],
    error_count: int = 0,
) -> Path:
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(run_dir / "templates.parquet", templates, TEMPLATE_SCHEMA)
    runtime_counts = {"code": error_count, "infra": 0} if error_count else {"code": 0, "infra": 0}
    write_json(
        run_dir / "run.json",
        {
            "tool": "LogsReaper",
            "tool_version": "0.1.0",
            "run_id": run_id,
            "created_at": created_at,
            "service_name": service,
            "input_globs": [],
            "input_files": [],
            "file_count": 0,
            "event_count": sum(int(t.get("event_count") or 0) for t in templates),
            "template_count": len(templates),
            "error_count": error_count,
            "hash_algorithm": "blake3",
            "runtime_counts": runtime_counts,
            "parse_status": {"ok": 100},
            "engine": "rust",
            "scan_duration_seconds": 1.0,
            "events_per_second": 1.0,
            "throughput_gb_per_second": 0.0001,
            "input_bytes": 1024,
            "input_gigabytes": 1e-6,
            "connectivity_timeline": {},
        },
    )
    return run_dir


def test_tokenize_and_shingles_basic() -> None:
    tokens = tokenize("Connection lost to kafka broker")
    assert "connection" in tokens
    assert "kafka" in tokens
    sh = template_shingles("Connection lost to kafka broker")
    # Bare tokens are included
    assert "kafka" in sh
    # 2-grams included
    assert "connection lost" in sh


def test_jaccard_distinguishes_close_and_far() -> None:
    a = template_shingles("Connection lost to kafka broker")
    b = template_shingles("Connection lost to kafka broker on host node-7")
    c = template_shingles("Mongo write timed out after 30s")
    assert jaccard(a, b) > 0.5
    assert jaccard(a, c) < 0.2
    assert jaccard(a, a) == 1.0


def test_find_predecessor_above_threshold_only() -> None:
    baseline = [
        {"template_id": "T_KAFKA_LOST", "normalized_template": "Connection lost to kafka broker", "severity_text": "ERROR"},
        {"template_id": "T_MONGO_TIMEOUT", "normalized_template": "Mongo write timed out after duration", "severity_text": "ERROR"},
    ]
    candidate = template_shingles("Connection lost to kafka broker on host")
    res = find_predecessor(
        candidate_template_id="NEW_KAFKA",
        candidate_shingles=candidate,
        baseline_templates=baseline,
        min_similarity=0.5,
        candidate_severity="ERROR",
    )
    assert res is not None
    assert res["predecessor_template_id"] == "T_KAFKA_LOST"

    # Distant template — no predecessor.
    distant = template_shingles("Quota exceeded for user")
    res2 = find_predecessor(
        candidate_template_id="NEW_X",
        candidate_shingles=distant,
        baseline_templates=baseline,
        min_similarity=0.5,
        candidate_severity="ERROR",
    )
    assert res2 is None


def test_find_predecessor_respects_severity_filter() -> None:
    baseline = [
        {"template_id": "T1", "normalized_template": "Connection lost to kafka broker", "severity_text": "INFO"},
    ]
    candidate = template_shingles("Connection lost to kafka broker on host")
    res = find_predecessor(
        candidate_template_id="NEW",
        candidate_shingles=candidate,
        baseline_templates=baseline,
        min_similarity=0.5,
        candidate_severity="ERROR",
    )
    assert res is None  # severity mismatch


def test_compute_diff_with_lineage_links_evolved_template(tmp_path: Path) -> None:
    runs_root = tmp_path / "out"
    registry_out = tmp_path / "runs"
    # Three green TRAITS runs establish a baseline with a stable error template.
    for i in range(3):
        _make_run(
            runs_root,
            run_id=f"E2E_TRAITS_GREEN_{i:03d}",
            service="accounts",
            created_at=f"2026-05-14T0{i}:00:00Z",
            templates=[
                _baseline_template(
                    "TPL_KAFKA",
                    20 + i,
                    "Connection lost to kafka broker",
                )
            ],
        )
    build_registry(runs_root, registry_out, min_green_runs=2)

    # Candidate run has a slightly mutated template (same severity, extra words).
    candidate_run = _make_run(
        runs_root,
        run_id="E2E_TRAITS_CAND_LIN",
        service="accounts",
        created_at="2026-05-14T05:00:00Z",
        templates=[
            _baseline_template(
                "TPL_KAFKA_EVOLVED",
                15,
                "Connection lost to kafka broker on host node-7",
            )
        ],
        error_count=1,
    )
    diff = compute_diff(run_dir=candidate_run, baseline_path=registry_out / "baseline.parquet")
    # Without lineage this would surface as a brand-new template; with lineage on,
    # we expect it tagged as evolved with the right predecessor.
    new = diff["new_templates"]
    assert len(new) == 1
    entry = new[0]
    assert entry.get("kind") == "evolved_template", entry
    assert entry["predecessor_template_id"] == "TPL_KAFKA"
    assert entry["similarity_score"] > 0.5
    assert diff["summary_counts"]["evolved"] == 1


def test_compute_diff_no_lineage_when_dissimilar(tmp_path: Path) -> None:
    runs_root = tmp_path / "out"
    registry_out = tmp_path / "runs"
    for i in range(3):
        _make_run(
            runs_root,
            run_id=f"E2E_TRAITS_GREEN_{i:03d}",
            service="accounts",
            created_at=f"2026-05-14T0{i}:00:00Z",
            templates=[
                _baseline_template(
                    "TPL_KAFKA",
                    20 + i,
                    "Connection lost to kafka broker",
                )
            ],
        )
    build_registry(runs_root, registry_out, min_green_runs=2)
    candidate_run = _make_run(
        runs_root,
        run_id="E2E_TRAITS_CAND_NEW",
        service="accounts",
        created_at="2026-05-14T05:00:00Z",
        templates=[
            _baseline_template(
                "BRAND_NEW",
                15,
                "Quota exceeded for user during traits provisioning",
            )
        ],
        error_count=1,
    )
    diff = compute_diff(run_dir=candidate_run, baseline_path=registry_out / "baseline.parquet")
    entry = diff["new_templates"][0]
    assert entry.get("kind") in (None, "new_template")
    assert "predecessor_template_id" not in entry
    assert diff["summary_counts"]["evolved"] == 0


def test_scenarios_isolated_across_cohorts(tmp_path: Path) -> None:
    """A template that exists in scenario A baseline must NOT count as
    predecessor for a candidate in scenario B."""
    runs_root = tmp_path / "out"
    registry_out = tmp_path / "runs"
    # Scenario QUOTAS baseline.
    for i in range(3):
        _make_run(
            runs_root,
            run_id=f"E2E_QUOTAS_{i:03d}",
            service="accounts",
            created_at=f"2026-05-14T0{i}:00:00Z",
            templates=[
                _baseline_template("TPL_KAFKA_Q", 20, "Connection lost to kafka broker"),
            ],
        )
    build_registry(runs_root, registry_out, min_green_runs=2)
    # Candidate under TRAITS scenario — the QUOTAS baseline should not be visible.
    candidate_run = _make_run(
        runs_root,
        run_id="E2E_TRAITS_CAND_ISO",
        service="accounts",
        created_at="2026-05-14T05:00:00Z",
        templates=[
            _baseline_template("TPL_NEW", 15, "Connection lost to kafka broker on host node-7"),
        ],
        error_count=1,
    )
    diff = compute_diff(run_dir=candidate_run, baseline_path=registry_out / "baseline.parquet")
    assert diff["scenario"] == "traits"
    assert diff["baseline_status"] == "empty_cohort"
    entry = diff["new_templates"][0]
    # No predecessor because TRAITS cohort is empty — lineage requires baseline.
    assert "predecessor_template_id" not in entry


def test_lineage_performance(tmp_path: Path) -> None:
    """Annotating lineage over 200 candidates × 5000 baseline templates must be fast.

    Targets the worst-realistic case: one full E2E run with ~200 new
    templates checked against a large baseline. Required < 1.5s.
    """
    baseline = {
        f"T{i}": {
            "template_id": f"T{i}",
            "normalized_template": f"baseline event {i} variant token foo bar baz qux",
            "severity_text": "ERROR",
        }
        for i in range(5000)
    }
    diff = {
        "new_templates": [
            {
                "template_id": f"N{i}",
                "normalized_template": f"baseline event {i} variant token foo bar baz qux extra",
                "severity_text": "ERROR",
            }
            for i in range(200)
        ]
    }
    started = perf_counter()
    annotate_diff_with_lineage(diff, baseline, min_similarity=0.85)
    elapsed = perf_counter() - started
    assert elapsed < 1.5, f"lineage annotation too slow: {elapsed:.2f}s"
    # Each candidate maps to its exact predecessor.
    by_new = {e["template_id"]: e for e in diff["new_templates"]}
    for i in range(200):
        assert by_new[f"N{i}"].get("predecessor_template_id") == f"T{i}", by_new[f"N{i}"]
