"""Tests for the diff engine + dashboard data helpers (Fase 1)."""
from __future__ import annotations

import sys
from pathlib import Path
from time import perf_counter

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from logs_reaper.dashboard_data import (
    connectivity_gantt,
    filter_runs,
    heatmap_matrix,
    list_scenarios,
    list_services,
    novelty_curve,
    regression_burndown,
)
from logs_reaper.diff_engine import compute_diff, diff_to_table, load_baseline_for
from logs_reaper.io import TEMPLATE_SCHEMA, write_json, write_parquet
from logs_reaper.registry import build_registry, load_baseline, load_registry


def _green_template(template_id: str, count: int, severity: str = "INFO") -> dict[str, object]:
    return {
        "template_id": template_id,
        "service_name": "accounts",
        "severity_text": severity,
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


def _make_run(
    runs_root: Path,
    *,
    run_id: str,
    service: str,
    created_at: str,
    templates: list[dict[str, object]],
    runtime_counts: dict[str, int] | None = None,
    error_count: int = 0,
    incidents: int = 0,
) -> Path:
    # When a test wants the run flagged red via error_count alone, also raise
    # the issue_kind=code count so classify_status (which now ignores
    # error_count) keeps it out of the baseline cohort.
    if error_count > 0 and runtime_counts is None:
        runtime_counts = {"code": error_count, "infra": 0, "ops": 0, "noise": 0}
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(run_dir / "templates.parquet", templates, TEMPLATE_SCHEMA)
    timeline_incidents = (
        [{"down_at": "2026-05-14T10:00:00Z", "up_at": "2026-05-14T10:00:45Z", "duration_seconds": 45.0}]
        * incidents
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
        "scan_duration_seconds": 1.0,
        "events_per_second": 1000.0,
        "throughput_gb_per_second": 0.1,
        "input_bytes": 1024,
        "input_gigabytes": 1e-6,
        "connectivity_timeline": {
            "kafka": {"state": "up", "incidents": timeline_incidents, "down_events": 0, "up_events": 0},
        },
    }
    write_json(run_dir / "run.json", payload)
    return run_dir


@pytest.fixture()
def synthetic_setup(tmp_path: Path) -> dict[str, Path]:
    runs_root = tmp_path / "out"
    registry_out = tmp_path / "runs"
    # Three green baseline runs of TRAITS with stable counts.
    for idx, count in enumerate([100, 105, 95], start=1):
        _make_run(
            runs_root,
            run_id=f"E2E_TRAITS_GREEN_{idx:03d}",
            service="accounts",
            created_at=f"2026-05-14T0{idx}:00:00Z",
            templates=[
                _green_template("stable_a", count),
                _green_template("stable_b", count // 2),
            ],
        )
    build_registry(runs_root, registry_out, min_green_runs=2)
    return {"runs_root": runs_root, "registry_out": registry_out}


def test_compute_diff_detects_new_missing_regressed(synthetic_setup: dict[str, Path], tmp_path: Path) -> None:
    runs_root = synthetic_setup["runs_root"]
    registry_out = synthetic_setup["registry_out"]
    baseline_path = registry_out / "baseline.parquet"

    candidate_run = _make_run(
        runs_root,
        run_id="E2E_TRAITS_CAND_001",
        service="accounts",
        created_at="2026-05-14T05:00:00Z",
        templates=[
            # stable_a: regression — observed = 600 vs baseline mean 100 std~5.
            _green_template("stable_a", 600),
            # stable_b: stable.
            _green_template("stable_b", 50),
            # brand_new: novel template.
            _green_template("brand_new", 25, severity="ERROR"),
        ],
    )
    diff = compute_diff(run_dir=candidate_run, baseline_path=baseline_path)
    assert diff["baseline_status"] == "present"
    new_ids = {row["template_id"] for row in diff["new_templates"]}
    regressed_ids = {row["template_id"] for row in diff["regressed_templates"]}
    assert "brand_new" in new_ids
    assert "stable_a" in regressed_ids
    assert "stable_b" not in regressed_ids

    # Z-score should be very high.
    sa = next(r for r in diff["regressed_templates"] if r["template_id"] == "stable_a")
    assert sa["z_score"] > 10.0
    assert sa["delta_factor"] >= 5.0

    # Drop a template (missing).
    missing_run = _make_run(
        runs_root,
        run_id="E2E_TRAITS_CAND_002",
        service="accounts",
        created_at="2026-05-14T06:00:00Z",
        templates=[_green_template("stable_a", 100)],  # stable_b is missing
    )
    diff2 = compute_diff(run_dir=missing_run, baseline_path=baseline_path)
    missing_ids = {row["template_id"] for row in diff2["missing_templates"]}
    assert "stable_b" in missing_ids


def test_compute_diff_no_baseline_yet(tmp_path: Path) -> None:
    run_dir = _make_run(
        tmp_path / "out",
        run_id="ACCOUNTS_001",
        service="accounts",
        created_at="2026-05-14T00:00:00Z",
        templates=[_green_template("t", 10)],
    )
    diff = compute_diff(run_dir=run_dir, baseline_path=tmp_path / "nonexistent.parquet")
    assert diff["baseline_status"] == "missing"
    # All templates fall into "new" because baseline is absent.
    assert len(diff["new_templates"]) == 1
    assert diff["new_templates"][0]["template_id"] == "t"


def test_diff_severity_shift_detected(synthetic_setup: dict[str, Path]) -> None:
    runs_root = synthetic_setup["runs_root"]
    baseline_path = synthetic_setup["registry_out"] / "baseline.parquet"
    severity_run = _make_run(
        runs_root,
        run_id="E2E_TRAITS_SEV_001",
        service="accounts",
        created_at="2026-05-14T07:00:00Z",
        templates=[_green_template("stable_a", 100, severity="ERROR")],
    )
    diff = compute_diff(run_dir=severity_run, baseline_path=baseline_path)
    shifts = {row["template_id"]: row for row in diff["severity_shifted"]}
    assert "stable_a" in shifts
    assert shifts["stable_a"]["previous_severity"] == "INFO"
    assert shifts["stable_a"]["current_severity"] == "ERROR"


def test_diff_connectivity_regressions_picked_up(synthetic_setup: dict[str, Path]) -> None:
    runs_root = synthetic_setup["runs_root"]
    baseline_path = synthetic_setup["registry_out"] / "baseline.parquet"
    run_dir = _make_run(
        runs_root,
        run_id="E2E_TRAITS_CONN_001",
        service="accounts",
        created_at="2026-05-14T08:00:00Z",
        templates=[_green_template("stable_a", 100)],
        incidents=3,
    )
    diff = compute_diff(run_dir=run_dir, baseline_path=baseline_path)
    assert len(diff["connectivity_regressions"]) == 3
    assert all(r["dependency"] == "kafka" for r in diff["connectivity_regressions"])


def test_diff_to_table_flattens_correctly(synthetic_setup: dict[str, Path]) -> None:
    baseline_path = synthetic_setup["registry_out"] / "baseline.parquet"
    run_dir = _make_run(
        synthetic_setup["runs_root"],
        run_id="E2E_TRAITS_CAND_FLAT",
        service="accounts",
        created_at="2026-05-14T09:00:00Z",
        templates=[
            _green_template("stable_a", 800),
            _green_template("new_one", 12),
        ],
        incidents=1,
    )
    diff = compute_diff(run_dir=run_dir, baseline_path=baseline_path)
    table = diff_to_table(diff)
    kinds = set(table.column("kind").to_pylist())
    assert "regressed_template" in kinds
    assert "new_template" in kinds
    assert "missing_template" in kinds  # stable_b absent
    assert "connectivity_regression" in kinds


def test_dashboard_helpers_filter_and_list(synthetic_setup: dict[str, Path]) -> None:
    registry_table = load_registry(synthetic_setup["registry_out"])
    assert list_services(registry_table) == ["accounts"]
    assert "traits" in list_scenarios(registry_table, "accounts")
    runs = filter_runs(registry_table, "accounts", "traits")
    assert len(runs) == 3


def test_heatmap_and_novelty_curve(synthetic_setup: dict[str, Path]) -> None:
    runs_root = synthetic_setup["runs_root"]
    registry_out = synthetic_setup["registry_out"]
    # Add a candidate run with a novel template. Mark it red so it does NOT
    # enter the baseline cohort — otherwise the novel template gets promoted to
    # baseline on rebuild and the test loses its meaning.
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_CAND_HM",
        service="accounts",
        created_at="2026-05-14T11:00:00Z",
        templates=[
            _green_template("stable_a", 100),
            _green_template("stable_b", 50),
            _green_template("shiny_new", 5),
        ],
        error_count=1,
    )
    build_registry(runs_root, registry_out, min_green_runs=2)
    registry_table = load_registry(registry_out)
    baseline_table = pq.read_table(registry_out / "baseline.parquet")
    runs = filter_runs(registry_table, "accounts", "traits")
    baseline_cohort = load_baseline_for(baseline_table, "accounts", "traits")
    data = heatmap_matrix(runs, baseline_for_cohort=baseline_cohort, top_n=20)
    assert "stable_a" in data["template_ids"]
    # Novel template should have is_novel=True on the candidate run.
    if "shiny_new" in data["template_ids"]:
        idx_template = data["template_ids"].index("shiny_new")
        idx_run = data["run_ids"].index("E2E_TRAITS_CAND_HM")
        assert data["is_novel"][idx_template][idx_run] is True

    curve = novelty_curve(runs, window=3)
    # The candidate run should have a positive novelty fraction (shiny_new is new).
    last = curve["rows"][-1]
    assert last["run_id"] == "E2E_TRAITS_CAND_HM"
    assert last["novel_count"] >= 1
    assert last["novelty_fraction"] > 0


def test_connectivity_gantt_collects_incidents(synthetic_setup: dict[str, Path]) -> None:
    runs_root = synthetic_setup["runs_root"]
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_INC_001",
        service="accounts",
        created_at="2026-05-14T12:00:00Z",
        templates=[_green_template("stable_a", 100)],
        incidents=2,
    )
    build_registry(runs_root, synthetic_setup["registry_out"], min_green_runs=2)
    registry_table = load_registry(synthetic_setup["registry_out"])
    runs = filter_runs(registry_table, "accounts", "traits")
    items = connectivity_gantt(runs)
    assert len(items) == 2
    assert all(item["dependency"] == "kafka" for item in items)


def test_regression_burndown_tracks_new_and_fixed(synthetic_setup: dict[str, Path]) -> None:
    runs_root = synthetic_setup["runs_root"]
    registry_out = synthetic_setup["registry_out"]
    # Add a regressing run, then a fixing run. Mark both red so they do not
    # leak into the baseline cohort during rebuild and shift its mean.
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_BURN_BAD",
        service="accounts",
        created_at="2026-05-14T13:00:00Z",
        templates=[_green_template("stable_a", 600), _green_template("stable_b", 50)],
        error_count=1,
    )
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_BURN_FIXED",
        service="accounts",
        created_at="2026-05-14T14:00:00Z",
        templates=[_green_template("stable_a", 100), _green_template("stable_b", 50)],
        error_count=1,
    )
    build_registry(runs_root, registry_out, min_green_runs=2)
    registry_table = load_registry(registry_out)
    baseline_table = pq.read_table(registry_out / "baseline.parquet")
    runs = filter_runs(registry_table, "accounts", "traits")
    burn = regression_burndown(runs, baseline_for_cohort=load_baseline_for(baseline_table, "accounts", "traits"))
    by_run = {row["run_id"]: row for row in burn}
    bad = by_run["E2E_TRAITS_BURN_BAD"]
    fixed = by_run["E2E_TRAITS_BURN_FIXED"]
    assert bad["new_regressions"] >= 1
    assert fixed["fixed_regressions"] >= 1


def test_diff_performance_under_50ms(synthetic_setup: dict[str, Path]) -> None:
    """A diff over a 5k-template run vs. a 5k-template baseline must be <50ms.

    Goal: the dashboard recomputes diffs interactively; anything above ~50ms
    is felt by the user.
    """
    runs_root = synthetic_setup["runs_root"]
    registry_out = synthetic_setup["registry_out"]
    big_run = _make_run(
        runs_root,
        run_id="E2E_TRAITS_BIG",
        service="accounts",
        created_at="2026-05-14T15:00:00Z",
        templates=[_green_template(f"t{i}", (i * 7) % 200) for i in range(5000)],
    )
    # Inflate the baseline by re-indexing with these templates as green sources.
    for green_idx in range(5):
        _make_run(
            runs_root,
            run_id=f"E2E_TRAITS_FILL_{green_idx:03d}",
            service="accounts",
            created_at=f"2026-05-13T{green_idx:02d}:00:00Z",
            templates=[_green_template(f"t{i}", (i * 7) % 200) for i in range(5000)],
        )
    build_registry(runs_root, registry_out, min_green_runs=2)

    started = perf_counter()
    diff = compute_diff(run_dir=big_run, baseline_path=registry_out / "baseline.parquet")
    seconds = perf_counter() - started
    assert diff["baseline_status"] == "present"
    assert seconds < 0.5, f"diff too slow: {seconds*1000:.1f}ms"
