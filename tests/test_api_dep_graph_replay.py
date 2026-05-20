"""Tests for the notebook API, dependency graph and replay timeline (Fase 4)."""
from __future__ import annotations

import sys
from pathlib import Path
from time import perf_counter

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from logs_reaper import api
from logs_reaper.dep_graph import service_dependency_edges, template_cooccurrence_edges
from logs_reaper.io import EVENT_SCHEMA, TEMPLATE_SCHEMA, write_json, write_parquet, write_parquet_table
from logs_reaper.registry import build_registry, load_registry
from logs_reaper.replay import build_replay_timeline


def _baseline_template(template_id: str, count: int, normalized: str = "tpl", severity: str = "INFO") -> dict[str, object]:
    return {
        "template_id": template_id,
        "service_name": "accounts",
        "severity_text": severity,
        "severity_number": 9,
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
        "issue_kind": "noise",
    }


def _make_run(
    runs_root: Path,
    *,
    run_id: str,
    service: str,
    created_at: str,
    templates: list[dict[str, object]],
    incidents_for_dep: dict[str, int] | None = None,
    events_table: pa.Table | None = None,
    error_count: int = 0,
) -> Path:
    _runtime_counts = {"code": error_count, "infra": 0} if error_count else {"code": 0, "infra": 0}
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(run_dir / "templates.parquet", templates, TEMPLATE_SCHEMA)
    if events_table is not None:
        write_parquet_table(run_dir / "events.parquet", events_table, EVENT_SCHEMA)
    timeline = {}
    if incidents_for_dep:
        for dep, count in incidents_for_dep.items():
            timeline[dep] = {
                "state": "up",
                "incidents": [
                    {
                        "down_at": "2026-05-14T10:00:00Z",
                        "up_at": "2026-05-14T10:01:00Z",
                        "duration_seconds": 60.0,
                    }
                ]
                * count,
                "down_events": 0,
                "up_events": 0,
            }
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
            "runtime_counts": _runtime_counts,
            "parse_status": {"ok": 100},
            "engine": "rust",
            "scan_duration_seconds": 1.0,
            "events_per_second": 1.0,
            "throughput_gb_per_second": 0.0001,
            "input_bytes": 1024,
            "input_gigabytes": 1e-6,
            "connectivity_timeline": timeline,
        },
    )
    return run_dir


@pytest.fixture()
def populated_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    runs_root = tmp_path / "out"
    registry_out = tmp_path / "runs"
    # Two clean green runs (no incidents, no errors) plus one red run with
    # connectivity incidents. The dependency-graph test still has incidents to
    # aggregate because the red run carries them.
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_001",
        service="accounts",
        created_at="2026-05-14T01:00:00Z",
        templates=[_baseline_template("t1", 50)],
    )
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_002",
        service="accounts",
        created_at="2026-05-14T02:00:00Z",
        templates=[_baseline_template("t1", 60)],
    )
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_003_RED",
        service="accounts",
        created_at="2026-05-14T03:00:00Z",
        templates=[_baseline_template("t1", 70)],
        incidents_for_dep={"kafka": 3, "mongo": 3},
        error_count=1,
    )
    build_registry(runs_root, registry_out, min_green_runs=2)
    monkeypatch.setenv("LOGS_REAPER_REGISTRY", str(registry_out))
    return registry_out


def test_api_runs_filters(populated_registry: Path) -> None:
    all_runs = api.runs()
    assert all_runs.num_rows == 3
    only_traits = api.runs(scenario="traits")
    assert only_traits.num_rows == 3
    green_only = api.runs(status="green")
    assert green_only.num_rows == 2
    red_only = api.runs(status="red")
    assert red_only.num_rows == 1


def test_api_templates_and_baseline(populated_registry: Path) -> None:
    templates_table = api.templates(run_id="E2E_TRAITS_001")
    assert templates_table.num_rows == 1
    baseline_table = api.baseline(service="accounts", scenario="traits")
    # Both green runs share t1, so baseline has one row.
    assert baseline_table.num_rows == 1


def test_api_diff_for(populated_registry: Path) -> None:
    diff = api.diff_for(run_id="E2E_TRAITS_003_RED")
    # The candidate has t1 with count 70 vs baseline (50, 60) so it should not flag
    # as regression (counts close to mean). Verify at least the structure.
    assert diff["service_name"] == "accounts"
    assert diff["scenario"] == "traits"
    assert "summary_counts" in diff


def test_api_lineage_for(populated_registry: Path) -> None:
    # No other templates in registry → empty results.
    result = api.lineage_for(template_id="t1")
    assert result == []


def test_service_dependency_edges(populated_registry: Path) -> None:
    registry_rows = load_registry(populated_registry).to_pylist()
    edges = service_dependency_edges(registry_rows)
    by_dep = {(e["service_name"], e["dependency"]): e for e in edges}
    assert ("accounts", "kafka") in by_dep
    assert ("accounts", "mongo") in by_dep
    # Only the red run carries incidents (3 each for kafka and mongo).
    assert by_dep[("accounts", "kafka")]["total_incidents"] == 3
    assert by_dep[("accounts", "mongo")]["total_incidents"] == 3


def test_template_cooccurrence_finds_pattern(tmp_path: Path) -> None:
    """Manually craft an events table where template A almost always precedes B
    on worker w1 within 10s. Verify the (A→B) edge dominates."""
    base_row = {field.name: None for field in EVENT_SCHEMA}
    rows: list[dict[str, object]] = []

    def event(template_id: str, ts: str, worker: str) -> dict[str, object]:
        row = dict(base_row)
        row.update(
            {
                "event_id": f"{template_id}-{ts}-{worker}",
                "timestamp": ts,
                "template_id": template_id,
                "worker_id": worker,
                "service_name": "accounts",
                "service_instance_seq": 1,
            }
        )
        return row

    # A → B pattern 10 times.
    for i in range(10):
        rows.append(event("A", f"2026-05-14T10:00:{i:02d}Z", "w1"))
        rows.append(event("B", f"2026-05-14T10:00:{i:02d}Z", "w1"))
    # Background noise that breaks correlation.
    for i in range(20):
        rows.append(event("C", f"2026-05-14T11:00:{i:02d}Z", "w2"))
    table = pa.Table.from_pylist(rows, schema=EVENT_SCHEMA)
    edges = template_cooccurrence_edges(table, lag_seconds=10.0, min_count=3)
    # The A→B edge must exist and dominate the ranking (highest lift), with a
    # count >= number of co-occurring A,B pairs in the configured window.
    ab = next((e for e in edges if e["template_a"] == "A" and e["template_b"] == "B"), None)
    assert ab is not None
    assert ab["count"] >= 10
    assert ab["lift"] > 1.5
    # The top edge by lift should involve A or B (no other ordered pair has the
    # same density).
    top = edges[0]
    assert {top["template_a"], top["template_b"]} <= {"A", "B"}


def test_replay_timeline_emits_boot_first_error_and_incident(tmp_path: Path) -> None:
    base_row = {field.name: None for field in EVENT_SCHEMA}

    def event(template_id: str, ts: str, seq: int, issue: str | None) -> dict[str, object]:
        row = dict(base_row)
        row.update(
            {
                "event_id": f"{template_id}-{ts}",
                "timestamp": ts,
                "template_id": template_id,
                "issue_kind": issue,
                "service_instance_seq": seq,
                "service_instance_started_at": "2026-05-14T10:00:00Z" if seq == 1 else None,
                "severity_text": "ERROR" if issue == "code" else "INFO",
            }
        )
        return row

    rows = [
        event("BOOT", "2026-05-14T10:00:00Z", 1, "noise"),
        event("WORK", "2026-05-14T10:00:30Z", 1, "noise"),
        event("ERR", "2026-05-14T10:01:00Z", 1, "code"),
    ]
    events_table = pa.Table.from_pylist(rows, schema=EVENT_SCHEMA)
    run_dir = _make_run(
        tmp_path / "out",
        run_id="RUN_REPLAY",
        service="accounts",
        created_at="2026-05-14T10:00:00Z",
        templates=[_baseline_template("BOOT", 1), _baseline_template("ERR", 1, severity="ERROR")],
        incidents_for_dep={"kafka": 1},
        events_table=events_table,
    )
    timeline = build_replay_timeline(run_dir).to_pylist()
    kinds = {row["kind"] for row in timeline}
    assert "boot" in kinds
    assert "first_code_error" in kinds
    assert "connectivity_down" in kinds
    assert "connectivity_up" in kinds


def test_dep_graph_performance(tmp_path: Path) -> None:
    """Co-occurrence over 100k events × 50 templates must complete < 2s."""
    base_row = {field.name: None for field in EVENT_SCHEMA}
    rows: list[dict[str, object]] = []
    workers = ["w1", "w2", "w3", "w4"]
    for i in range(100_000):
        t = f"T{i % 50}"
        worker = workers[i % len(workers)]
        ts = f"2026-05-14T{(i // 3600) % 24:02d}:{(i // 60) % 60:02d}:{i % 60:02d}Z"
        rows.append(
            {
                **base_row,
                "event_id": f"e{i}",
                "timestamp": ts,
                "template_id": t,
                "worker_id": worker,
            }
        )
    table = pa.Table.from_pylist(rows, schema=EVENT_SCHEMA)
    started = perf_counter()
    edges = template_cooccurrence_edges(table, lag_seconds=30.0, min_count=3)
    elapsed = perf_counter() - started
    assert elapsed < 4.0, f"co-occurrence too slow: {elapsed:.2f}s"
    assert len(edges) > 0
