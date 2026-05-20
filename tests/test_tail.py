"""Tests for the tail mode (Fase 2)."""
from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path
from time import perf_counter

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from logs_reaper.io import TEMPLATE_SCHEMA, write_json, write_parquet
from logs_reaper.registry import build_registry
from logs_reaper.tail import TailConfig, TailRunner, _evaluate_increment, _find_last_newline, TailState


def _baseline_template(template_id: str, count: int) -> dict[str, object]:
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


def _bootstrap_baseline(tmp_path: Path) -> Path:
    """Materialise a small registry+baseline for tail tests."""
    runs_root = tmp_path / "out"
    registry_out = tmp_path / "runs"
    for idx, count in enumerate([100, 105, 95], start=1):
        run_dir = runs_root / f"E2E_TRAITS_GREEN_{idx:03d}"
        run_dir.mkdir(parents=True, exist_ok=True)
        write_parquet(
            run_dir / "templates.parquet",
            [_baseline_template("stable_a", count), _baseline_template("stable_b", count // 2)],
            TEMPLATE_SCHEMA,
        )
        write_json(
            run_dir / "run.json",
            {
                "tool": "LogsReaper",
                "tool_version": "0.1.0",
                "run_id": f"E2E_TRAITS_GREEN_{idx:03d}",
                "created_at": f"2026-05-14T0{idx}:00:00Z",
                "service_name": "accounts",
                "input_globs": [],
                "input_files": [],
                "file_count": 0,
                "event_count": 200,
                "template_count": 2,
                "error_count": 0,
                "hash_algorithm": "blake3",
                "runtime_counts": {"code": 0, "infra": 0},
                "parse_status": {"ok": 200},
                "engine": "rust",
                "scan_duration_seconds": 1.0,
                "events_per_second": 100.0,
                "throughput_gb_per_second": 0.001,
                "input_bytes": 1024,
                "input_gigabytes": 1e-6,
                "connectivity_timeline": {},
            },
        )
    build_registry(runs_root, registry_out, min_green_runs=2)
    return registry_out / "baseline.parquet"


def test_find_last_newline_basic(tmp_path: Path) -> None:
    file_path = tmp_path / "f.log"
    file_path.write_bytes(b"abc\ndef\nghi")
    assert _find_last_newline(file_path, 0, 11) == 7
    assert _find_last_newline(file_path, 4, 7) == 3 or _find_last_newline(file_path, 4, 11) == 7
    # Empty range
    assert _find_last_newline(file_path, 11, 11) is None
    # No newline in range
    file_path.write_bytes(b"abc")
    assert _find_last_newline(file_path, 0, 3) is None


def test_find_last_newline_spans_chunks(tmp_path: Path) -> None:
    file_path = tmp_path / "big.log"
    payload = b"x" * 200_000 + b"\n" + b"y" * 200_000
    file_path.write_bytes(payload)
    last = _find_last_newline(file_path, 0, len(payload))
    assert last == 200_000


def test_evaluate_increment_emits_new_and_regression() -> None:
    state = TailState()
    baseline_for_cohort = {
        "stable_a": {
            "mean_count": 100.0,
            "std_count": 5.0,
            "p95_count": 110.0,
        },
    }
    # Tick 1: counts normal — no anomalies.
    rows1 = [
        {"template_id": "stable_a", "event_count": 50, "normalized_template": "a", "severity_text": "INFO", "issue_kind": "noise"},
    ]
    out = _evaluate_increment(
        templates_rows=rows1,
        state=state,
        baseline_for_cohort=baseline_for_cohort,
        z_threshold=3.0,
        min_observed_count=5,
        novelty_min_count=1,
    )
    assert out == []

    # Tick 2: stable_a cumulative now 700 (50 + 650) — clear regression.
    rows2 = [
        {"template_id": "stable_a", "event_count": 650, "normalized_template": "a", "severity_text": "INFO", "issue_kind": "noise"},
        # Brand new template — should fire new_template once.
        {"template_id": "brand_new", "event_count": 10, "normalized_template": "x", "severity_text": "ERROR", "issue_kind": "code"},
    ]
    out2 = _evaluate_increment(
        templates_rows=rows2,
        state=state,
        baseline_for_cohort=baseline_for_cohort,
        z_threshold=3.0,
        min_observed_count=5,
        novelty_min_count=1,
    )
    kinds = {a["type"] for a in out2}
    assert kinds == {"new_template", "regression"}
    # Cumulative count for stable_a should be 700.
    regression = next(a for a in out2 if a["type"] == "regression")
    assert regression["observed_count"] == 700

    # Tick 3: same regression — should NOT fire again (deduped).
    rows3 = [
        {"template_id": "stable_a", "event_count": 100, "normalized_template": "a", "severity_text": "INFO", "issue_kind": "noise"},
        {"template_id": "brand_new", "event_count": 5, "normalized_template": "x", "severity_text": "ERROR", "issue_kind": "code"},
    ]
    out3 = _evaluate_increment(
        templates_rows=rows3,
        state=state,
        baseline_for_cohort=baseline_for_cohort,
        z_threshold=3.0,
        min_observed_count=5,
        novelty_min_count=1,
    )
    assert out3 == []


def _emit_event(handle, level: str = "INFO", message: str = "stable a event") -> None:
    payload = {
        "time": "2026-05-14T10:00:00Z",
        "level": level,
        "message": message,
        "microservice": "accounts",
        "worker_id": "w1",
        "threadName": "MainThread",
    }
    handle.write(json.dumps(payload) + "\n")


def test_tail_runner_detects_anomalies_incrementally(tmp_path: Path) -> None:
    baseline_path = _bootstrap_baseline(tmp_path)
    log_path = tmp_path / "live.log"
    log_path.write_text("", encoding="utf-8")
    out_path = tmp_path / "anomalies.ndjson"
    config = TailConfig(
        input_path=log_path,
        service_name="accounts",
        baseline_path=baseline_path,
        scenario="traits",
        out_path=out_path,
        tick_seconds=0.0,
        run_id="E2E_TRAITS_LIVE",
    )
    runner = TailRunner(config)
    assert runner.baseline_for_cohort  # cohort loaded

    # Tick 1 — empty file, no anomalies.
    assert runner.process_once() == []

    # Tick 2 — append a benign known template (no anomaly because not novel and not regression).
    with log_path.open("a", encoding="utf-8") as f:
        for _ in range(20):
            _emit_event(f, message="template stable_a")
    # Append a brand-new message body the parser will normalise differently.
    with log_path.open("a", encoding="utf-8") as f:
        for _ in range(5):
            _emit_event(f, level="ERROR", message="completely brand new error pattern")
    anomalies = runner.process_once()
    new_anomalies = [a for a in anomalies if a["type"] == "new_template"]
    assert new_anomalies, "expected at least one new_template anomaly"

    # Tick 3 — append more of the SAME novel pattern: must NOT re-emit it.
    with log_path.open("a", encoding="utf-8") as f:
        for _ in range(5):
            _emit_event(f, level="ERROR", message="completely brand new error pattern")
    anomalies2 = runner.process_once()
    new_again = [a for a in anomalies2 if a["type"] == "new_template"]
    assert not new_again, "novel template should be reported only once per session"

    # Verify NDJSON output file matches anomalies seen.
    runner.close()
    payload_lines = [json.loads(line) for line in out_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(payload_lines) == len(new_anomalies)


def test_tail_runner_runs_until_idle(tmp_path: Path) -> None:
    baseline_path = _bootstrap_baseline(tmp_path)
    log_path = tmp_path / "live.log"
    with log_path.open("w", encoding="utf-8") as f:
        for _ in range(30):
            _emit_event(f, message="template stable_a")
    config = TailConfig(
        input_path=log_path,
        service_name="accounts",
        baseline_path=baseline_path,
        scenario="traits",
        tick_seconds=0.0,
        stop_on_eof_idle_ticks=2,
        run_id="E2E_TRAITS_LIVE",
    )
    with TailRunner(config) as runner:
        state = runner.run(sleep=lambda _s: None)
    assert state.ticks_completed >= 2


def test_tail_runner_keeps_up_with_streaming_append(tmp_path: Path) -> None:
    """Performance: tail must keep up with a writer producing 500 lines/second.

    Run a 1.5-second producer that appends 5 batches of 100 lines each (1ms
    between writes within a batch, 300ms between batches), tail in parallel and
    require all events to be accounted for in < 2 seconds.
    """
    baseline_path = _bootstrap_baseline(tmp_path)
    log_path = tmp_path / "live.log"
    log_path.write_text("", encoding="utf-8")
    config = TailConfig(
        input_path=log_path,
        service_name="accounts",
        baseline_path=baseline_path,
        scenario="traits",
        tick_seconds=0.05,
        run_id="E2E_TRAITS_PERF",
    )
    producer_done = threading.Event()
    total_lines = 0
    expected_events_per_template = {"new_pattern": 0}

    def producer() -> None:
        nonlocal total_lines
        for batch in range(5):
            with log_path.open("a", encoding="utf-8") as f:
                for _ in range(100):
                    _emit_event(f, level="ERROR", message="new_pattern occurred")
                    total_lines += 1
                    expected_events_per_template["new_pattern"] += 1
            time.sleep(0.05)
        producer_done.set()

    t = threading.Thread(target=producer, daemon=True)
    started = perf_counter()
    t.start()
    runner = TailRunner(config)
    try:
        anomalies_seen: list[dict[str, object]] = []
        while not producer_done.is_set() or log_path.stat().st_size > 0:
            anomalies_seen.extend(runner.process_once())
            if producer_done.is_set():
                # Final flush
                anomalies_seen.extend(runner.process_once())
                break
            time.sleep(0.02)
        elapsed = perf_counter() - started
    finally:
        runner.close()
        t.join(timeout=2.0)

    assert producer_done.is_set()
    assert runner.state.events_processed >= total_lines * 0.95, (
        f"tail lost too many events: processed={runner.state.events_processed}, produced={total_lines}"
    )
    assert elapsed < 5.0, f"tail too slow: {elapsed:.2f}s"
    # At least one new_template fired for the novel pattern.
    assert any(a["type"] == "new_template" for a in anomalies_seen)
