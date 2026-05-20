"""Tail mode: stream a growing log file and emit anomalies vs. the baseline.

Designed to run side-by-side with a long-running test such as E2E so
the user sees regressions appear in real time rather than at the end.

The runner is incremental over byte offsets — every tick it asks the Rust hot
path to scan only the new bytes since the last tick (aligned to the most
recent newline so partial trailing lines are postponed to the next tick).

Anomalies are emitted to NDJSON. Each (kind, template_id) is reported at most
once per run to avoid the dashboard drowning in repeats.
"""
from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import pyarrow.parquet as pq

from .diff_engine import load_baseline_for
from .registry import derive_scenario
from .rust_engine import read_templates_ipc, scan_file_to_ipc


CHUNK_LOOKBACK = 65536


def _find_last_newline(file_path: Path, start: int, end: int) -> int | None:
    """Return the absolute offset of the last '\\n' in [start, end), or None."""
    if end <= start:
        return None
    pos = end
    with file_path.open("rb") as handle:
        while pos > start:
            read_start = max(start, pos - CHUNK_LOOKBACK)
            handle.seek(read_start)
            buf = handle.read(pos - read_start)
            idx = buf.rfind(b"\n")
            if idx >= 0:
                return read_start + idx
            pos = read_start
    return None


@dataclass
class TailState:
    """Cumulative state across ticks within a single tail session."""

    template_counts: dict[str, int] = field(default_factory=dict)
    template_meta: dict[str, dict[str, Any]] = field(default_factory=dict)
    seen_anomalies: set[tuple[str, str]] = field(default_factory=set)
    bytes_processed: int = 0
    events_processed: int = 0
    ticks_completed: int = 0


@dataclass
class TailConfig:
    input_path: Path
    service_name: str
    baseline_path: Path
    scenario: str | None = None
    z_threshold: float = 3.0
    min_observed_count: int = 5
    novelty_min_count: int = 1
    out_path: Path | None = None
    tick_seconds: float = 1.0
    max_runtime_seconds: float | None = None
    stop_on_eof_idle_ticks: int | None = None
    run_id: str = "TAIL"
    observed_timestamp: str | None = None
    include_raw: bool = False


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_baseline(baseline_path: Path, service: str, scenario: str) -> dict[str, dict[str, Any]]:
    if not baseline_path.exists():
        return {}
    table = pq.read_table(baseline_path)
    return load_baseline_for(table, service, scenario)


def _evaluate_increment(
    *,
    templates_rows: Iterable[dict[str, Any]],
    state: TailState,
    baseline_for_cohort: dict[str, dict[str, Any]],
    z_threshold: float,
    min_observed_count: int,
    novelty_min_count: int,
) -> list[dict[str, Any]]:
    """Update cumulative state with the new templates and return fresh anomalies."""
    anomalies: list[dict[str, Any]] = []
    for tpl in templates_rows:
        template_id = str(tpl.get("template_id") or "")
        if not template_id:
            continue
        count_delta = int(tpl.get("event_count") or 0)
        if count_delta <= 0:
            continue
        prior_total = state.template_counts.get(template_id, 0)
        new_total = prior_total + count_delta
        state.template_counts[template_id] = new_total
        if template_id not in state.template_meta:
            state.template_meta[template_id] = {
                "normalized_template": tpl.get("normalized_template"),
                "severity_text": tpl.get("severity_text"),
                "issue_kind": tpl.get("issue_kind"),
                "first_seen_at": _now(),
            }
        meta = state.template_meta[template_id]

        base = baseline_for_cohort.get(template_id)
        if base is None:
            if new_total >= novelty_min_count:
                key = ("new_template", template_id)
                if key not in state.seen_anomalies:
                    state.seen_anomalies.add(key)
                    anomalies.append(
                        {
                            "type": "new_template",
                            "ts": _now(),
                            "template_id": template_id,
                            "normalized_template": meta["normalized_template"],
                            "severity_text": meta["severity_text"],
                            "issue_kind": meta["issue_kind"],
                            "observed_count": new_total,
                        }
                    )
            continue
        mean = float(base.get("mean_count") or 0.0)
        std = float(base.get("std_count") or 0.0)
        p95 = float(base.get("p95_count") or 0.0)
        if new_total < min_observed_count or new_total <= p95:
            continue
        z = (new_total - mean) / std if std > 1e-9 else (math.inf if new_total > mean else 0.0)
        if z <= z_threshold:
            continue
        key = ("regression", template_id)
        if key in state.seen_anomalies:
            continue
        state.seen_anomalies.add(key)
        anomalies.append(
            {
                "type": "regression",
                "ts": _now(),
                "template_id": template_id,
                "normalized_template": meta["normalized_template"],
                "severity_text": meta["severity_text"],
                "observed_count": new_total,
                "baseline_mean": mean,
                "baseline_p95": p95,
                "z_score": z,
                "delta_factor": (new_total / mean) if mean > 0 else math.inf,
            }
        )
    return anomalies


def _scan_increment(
    *,
    input_path: Path,
    service_name: str,
    run_id: str,
    observed_timestamp: str,
    include_raw: bool,
    start_offset: int,
) -> tuple[list[dict[str, Any]], int]:
    """Scan from start_offset to EOF, return (template rows, bytes processed).

    Bytes processed are reported by the Rust scanner via the ``input_bytes``
    summary field — independent of file size, so we can trust it across ticks.
    """
    import tempfile
    with tempfile.TemporaryDirectory(prefix="logsreaper-tail-") as tmp_dir:
        events_ipc = Path(tmp_dir) / "events.arrow"
        templates_ipc = Path(tmp_dir) / "templates.arrow"
        summary = scan_file_to_ipc(
            input_path=input_path,
            events_out=events_ipc,
            templates_out=templates_ipc,
            service_name=service_name,
            run_id=run_id,
            observed_timestamp=observed_timestamp,
            include_raw=include_raw,
            start_offset=start_offset,
        )
        templates = read_templates_ipc(templates_ipc)
        bytes_processed = int(summary.get("input_bytes") or 0)
    return templates, bytes_processed


class TailRunner:
    def __init__(self, config: TailConfig) -> None:
        self.config = config
        self.scenario = config.scenario or derive_scenario(config.run_id)
        self.state = TailState()
        self.baseline_for_cohort = _load_baseline(
            config.baseline_path, config.service_name, self.scenario
        )
        self._last_offset: int = 0
        self._out_handle = None
        if config.out_path is not None:
            config.out_path.parent.mkdir(parents=True, exist_ok=True)
            self._out_handle = config.out_path.open("a", encoding="utf-8", buffering=1)

    def close(self) -> None:
        if self._out_handle is not None:
            self._out_handle.close()
            self._out_handle = None

    def __enter__(self) -> "TailRunner":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def emit(self, anomaly: dict[str, Any]) -> None:
        if self._out_handle is None:
            return
        self._out_handle.write(json.dumps(anomaly, ensure_ascii=False) + "\n")

    def process_once(self) -> list[dict[str, Any]]:
        """Run one tick. Returns the anomalies emitted on this tick."""
        path = self.config.input_path
        if not path.exists():
            return []
        size = path.stat().st_size
        if size <= self._last_offset:
            return []
        last_nl = _find_last_newline(path, self._last_offset, size)
        if last_nl is None:
            return []
        observed_ts = self.config.observed_timestamp or _now()
        templates_rows, bytes_processed = _scan_increment(
            input_path=path,
            service_name=self.config.service_name,
            run_id=self.config.run_id,
            observed_timestamp=observed_ts,
            include_raw=self.config.include_raw,
            start_offset=self._last_offset,
        )
        anomalies = _evaluate_increment(
            templates_rows=templates_rows,
            state=self.state,
            baseline_for_cohort=self.baseline_for_cohort,
            z_threshold=self.config.z_threshold,
            min_observed_count=self.config.min_observed_count,
            novelty_min_count=self.config.novelty_min_count,
        )
        self.state.bytes_processed += bytes_processed
        self.state.events_processed += sum(int(r.get("event_count") or 0) for r in templates_rows)
        self._last_offset = last_nl + 1
        for a in anomalies:
            self.emit(a)
        return anomalies

    def run(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        stop_callback: Callable[[TailState], bool] | None = None,
    ) -> TailState:
        """Run ticks until max_runtime_seconds or stop_on_eof_idle_ticks fires."""
        started_at = clock()
        idle_ticks = 0
        while True:
            anomalies = self.process_once()
            self.state.ticks_completed += 1
            if not anomalies:
                idle_ticks += 1
            else:
                idle_ticks = 0
            if self.config.stop_on_eof_idle_ticks is not None and idle_ticks >= self.config.stop_on_eof_idle_ticks:
                break
            if self.config.max_runtime_seconds is not None and (clock() - started_at) >= self.config.max_runtime_seconds:
                break
            if stop_callback is not None and stop_callback(self.state):
                break
            sleep(self.config.tick_seconds)
        return self.state
