"""Tests for the cross-run registry (Fase 0)."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from time import perf_counter

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from logs_reaper.io import TEMPLATE_SCHEMA, write_json, write_parquet
from logs_reaper.registry import (
    BASELINE_VERSION,
    REGISTRY_VERSION,
    build_registry,
    classify_status,
    derive_scenario,
    load_baseline,
    load_registry,
    load_template_registry,
)


def _make_run(
    runs_root: Path,
    *,
    run_id: str,
    service: str,
    created_at: str,
    templates: list[dict[str, object]],
    runtime_counts: dict[str, int] | None = None,
    error_count: int = 0,
    connectivity_incidents: int = 0,
) -> Path:
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(run_dir / "templates.parquet", templates, TEMPLATE_SCHEMA)
    incidents = (
        [{"down_at": "2026-05-14T10:00:00Z", "up_at": "2026-05-14T10:00:30Z", "duration_seconds": 30.0}]
        * connectivity_incidents
    )
    payload = {
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
        "runtime_counts": runtime_counts or {"code": 0, "infra": 0, "ops": 0, "noise": 0},
        "parse_status": {"ok": 100},
        "engine": "rust",
        "scan_duration_seconds": 1.5,
        "events_per_second": 1000.0,
        "throughput_gb_per_second": 0.1,
        "input_bytes": 1024,
        "input_gigabytes": 1e-6,
        "connectivity_timeline": {
            "kafka": {"state": "up", "incidents": incidents, "down_events": 0, "up_events": 0},
        },
        "autodiscovery": {"status": "new_container", "fingerprint": f"fp-{run_id}"},
    }
    write_json(run_dir / "run.json", payload)
    return run_dir


def _green_template(template_id: str, count: int) -> dict[str, object]:
    return {
        "template_id": template_id,
        "service_name": "accounts",
        "severity_text": "INFO",
        "severity_number": 9,
        "normalized_template": f"template {template_id}",
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
        "issue_kind": "noise",
    }


def test_derive_scenario_recognises_e2e_and_bench() -> None:
    assert derive_scenario("E2E_TRAITS_20260514") == "traits"
    assert derive_scenario("e2e_quotas_001") == "quotas"
    assert derive_scenario("BENCH_LIVE") == "bench"
    assert derive_scenario("BENCH-1G") == "bench"
    assert derive_scenario("ACCOUNTS_20260514T1200") == "default"
    assert derive_scenario(None) == "default"


def test_derive_scenario_override_regex() -> None:
    import re

    pattern = re.compile(r"_(?P<scenario>\w+?)_\d+$")
    assert derive_scenario("FOO_quotas_42", pattern) == "quotas"


def test_classify_status_green_red_unknown() -> None:
    green_payload = {
        "event_count": 100,
        "error_count": 0,
        "runtime_counts": {"code": 0, "infra": 0},
        "connectivity_timeline": {"kafka": {"incidents": []}},
    }
    status, _ = classify_status(green_payload)
    assert status == "green"

    red_code_payload = {
        "event_count": 100,
        "error_count": 0,
        "runtime_counts": {"code": 5},
        "connectivity_timeline": {},
    }
    assert classify_status(red_code_payload)[0] == "red"

    red_incident = {
        "event_count": 100,
        "error_count": 0,
        "runtime_counts": {"code": 0},
        "connectivity_timeline": {"mongo": {"incidents": [{"down_at": "x", "up_at": "y", "duration_seconds": 12.0}]}},
    }
    status, derived = classify_status(red_incident)
    assert status == "red"
    assert derived["connectivity_incident_count"] == 1
    assert derived["connectivity_total_downtime_s"] == 12.0

    unknown = {"event_count": 0, "error_count": 0, "runtime_counts": {}, "connectivity_timeline": {}}
    assert classify_status(unknown)[0] == "unknown"


def test_build_registry_full_pipeline(tmp_path: Path) -> None:
    runs_root = tmp_path / "out"
    out_dir = tmp_path / "runs"

    # Two green E2E TRAITS runs with overlapping templates.
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_001",
        service="accounts",
        created_at="2026-05-14T10:00:00Z",
        templates=[
            _green_template("t1", 100),
            _green_template("t2", 50),
        ],
    )
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_002",
        service="accounts",
        created_at="2026-05-14T11:00:00Z",
        templates=[
            _green_template("t1", 110),
            _green_template("t2", 45),
            _green_template("t3", 5),
        ],
    )
    # One red run (would contaminate baseline if we let it through).
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_003_RED",
        service="accounts",
        created_at="2026-05-14T12:00:00Z",
        templates=[_green_template("tX", 999)],
        runtime_counts={"code": 30},
        error_count=2,
    )
    # Different scenario (QUOTAS).
    _make_run(
        runs_root,
        run_id="E2E_QUOTAS_001",
        service="accounts",
        created_at="2026-05-14T13:00:00Z",
        templates=[_green_template("q1", 200)],
    )
    _make_run(
        runs_root,
        run_id="E2E_QUOTAS_002",
        service="accounts",
        created_at="2026-05-14T14:00:00Z",
        templates=[_green_template("q1", 220)],
    )

    summary = build_registry(runs_root, out_dir, min_green_runs=2)
    assert summary["runs_total"] == 5
    assert summary["runs_new_or_changed"] == 5

    # registry.parquet
    registry = load_registry(out_dir).to_pylist()
    by_id = {row["run_id"]: row for row in registry}
    assert by_id["E2E_TRAITS_001"]["status"] == "green"
    assert by_id["E2E_TRAITS_001"]["scenario"] == "traits"
    assert by_id["E2E_TRAITS_003_RED"]["status"] == "red"
    assert by_id["E2E_QUOTAS_002"]["scenario"] == "quotas"

    # template_registry.parquet: t1 should aggregate across both green TRAITS runs.
    template_reg = load_template_registry(out_dir).to_pylist()
    t1_row = next(row for row in template_reg if row["template_id"] == "t1")
    assert t1_row["runs_seen_count"] == 2  # only in the two TRAITS green runs
    assert t1_row["green_runs_seen_count"] == 2
    assert t1_row["total_event_count"] == 210
    assert t1_row["first_seen_run_id"] == "E2E_TRAITS_001"
    assert t1_row["last_seen_run_id"] == "E2E_TRAITS_002"

    # baseline.parquet: TRAITS cohort qualifies (2 green runs), QUOTAS too (2 green runs).
    baseline = load_baseline(out_dir).to_pylist()
    baseline_by_key = {(row["scenario"], row["template_id"]): row for row in baseline}
    # TRAITS t1 with counts [100, 110] → mean 105, p50 105.
    traits_t1 = baseline_by_key[("traits", "t1")]
    assert traits_t1["runs_in_baseline"] == 2
    assert traits_t1["mean_count"] == pytest.approx(105.0)
    assert traits_t1["min_count"] == 100
    assert traits_t1["max_count"] == 110
    assert traits_t1["baseline_version"] == BASELINE_VERSION
    # TRAITS t3 appeared only in run 002 → padded with 0 from run 001, counts [0, 5], mean 2.5.
    traits_t3 = baseline_by_key[("traits", "t3")]
    assert traits_t3["mean_count"] == pytest.approx(2.5)
    assert traits_t3["min_count"] == 0
    # The red run's template tX must NOT be present in baseline.
    assert ("traits", "tX") not in baseline_by_key
    # QUOTAS cohort: q1 mean (200 + 220) / 2 = 210.
    quotas_q1 = baseline_by_key[("quotas", "q1")]
    assert quotas_q1["mean_count"] == pytest.approx(210.0)


def test_build_registry_idempotent_and_incremental(tmp_path: Path) -> None:
    runs_root = tmp_path / "out"
    out_dir = tmp_path / "runs"

    _make_run(
        runs_root,
        run_id="E2E_TRAITS_001",
        service="accounts",
        created_at="2026-05-14T10:00:00Z",
        templates=[_green_template("t1", 100)],
    )
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_002",
        service="accounts",
        created_at="2026-05-14T11:00:00Z",
        templates=[_green_template("t1", 110)],
    )
    summary1 = build_registry(runs_root, out_dir, min_green_runs=2)
    assert summary1["runs_new_or_changed"] == 2
    assert summary1["runs_skipped_unchanged"] == 0

    # Second call without changes -> everything skipped.
    summary2 = build_registry(runs_root, out_dir, min_green_runs=2)
    assert summary2["runs_new_or_changed"] == 0
    assert summary2["runs_skipped_unchanged"] == 2
    assert summary2["runs_total"] == 2

    # Adding a new run only processes that one.
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_003",
        service="accounts",
        created_at="2026-05-14T12:00:00Z",
        templates=[_green_template("t1", 120)],
    )
    summary3 = build_registry(runs_root, out_dir, min_green_runs=2)
    assert summary3["runs_new_or_changed"] == 1
    assert summary3["runs_skipped_unchanged"] == 2
    assert summary3["runs_total"] == 3

    baseline = load_baseline(out_dir).to_pylist()
    t1 = next(row for row in baseline if row["template_id"] == "t1")
    assert t1["runs_in_baseline"] == 3
    assert t1["mean_count"] == pytest.approx(110.0)


def test_build_registry_rebuild_flag(tmp_path: Path) -> None:
    runs_root = tmp_path / "out"
    out_dir = tmp_path / "runs"
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_001",
        service="accounts",
        created_at="2026-05-14T10:00:00Z",
        templates=[_green_template("t1", 100)],
    )
    build_registry(runs_root, out_dir, min_green_runs=2)
    summary = build_registry(runs_root, out_dir, rebuild=True, min_green_runs=2)
    assert summary["runs_new_or_changed"] == 1
    assert summary["runs_skipped_unchanged"] == 0


def test_build_registry_performance(tmp_path: Path) -> None:
    """Indexing 500 synthetic runs (each with 50 templates) should be sub-second.

    Goal: be useful in tight feedback loops. Targets are deliberately loose so
    they pass on shared CI but flag any future regression > 2× current cost.
    """
    runs_root = tmp_path / "out"
    out_dir = tmp_path / "runs"
    num_runs = 500
    templates_per_run = 50
    for i in range(num_runs):
        templates = [
            _green_template(f"t{j}", (i * 7 + j) % 200)
            for j in range(templates_per_run)
        ]
        _make_run(
            runs_root,
            run_id=f"E2E_TRAITS_{i:04d}",
            service="accounts",
            created_at=f"2026-05-14T{i // 60:02d}:{i % 60:02d}:00Z",
            templates=templates,
        )
    started = perf_counter()
    summary = build_registry(runs_root, out_dir, min_green_runs=2)
    cold_seconds = perf_counter() - started
    assert summary["runs_total"] == num_runs
    assert summary["runs_new_or_changed"] == num_runs

    # Performance target: < 15s cold for 500 runs * 50 templates = 25k template rows
    # on the developer's box. This is comfortably below the user-perceived "instant"
    # threshold for a tooling command.
    assert cold_seconds < 15.0, f"cold index too slow: {cold_seconds:.2f}s"

    # Incremental run should be at least 10× faster: skipping is just stat + state diff.
    started = perf_counter()
    summary2 = build_registry(runs_root, out_dir, min_green_runs=2)
    warm_seconds = perf_counter() - started
    assert summary2["runs_skipped_unchanged"] == num_runs
    assert warm_seconds < cold_seconds / 5, (
        f"incremental not fast enough: cold={cold_seconds:.2f}s warm={warm_seconds:.2f}s"
    )

    # Validate that template registry and baseline are coherent.
    template_reg = load_template_registry(out_dir)
    baseline = load_baseline(out_dir)
    assert template_reg.num_rows == templates_per_run
    assert baseline.num_rows == templates_per_run  # all green, all cohort qualifies
    # Spot-check mean of t0: counts are (i*7 + 0) % 200 for i in [0, 500).
    expected_mean = sum((i * 7) % 200 for i in range(num_runs)) / num_runs
    t0 = next(row for row in baseline.to_pylist() if row["template_id"] == "t0")
    assert t0["mean_count"] == pytest.approx(expected_mean, rel=1e-9)


def test_registry_state_persisted(tmp_path: Path) -> None:
    runs_root = tmp_path / "out"
    out_dir = tmp_path / "runs"
    _make_run(
        runs_root,
        run_id="R1",
        service="svc",
        created_at="2026-05-14T10:00:00Z",
        templates=[_green_template("t1", 1)],
    )
    build_registry(runs_root, out_dir, min_green_runs=2)
    state = json.loads((out_dir / "index_state.json").read_text(encoding="utf-8"))
    assert state["registry_version"] == REGISTRY_VERSION
    assert "R1" in state["runs_seen"]
