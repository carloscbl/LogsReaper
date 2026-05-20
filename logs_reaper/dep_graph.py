"""Auto-inferred dependency / co-occurrence graph.

Two complementary graphs:

* **Service → infrastructure dependencies**: union of the dependencies that
  appear in ``connectivity_timeline`` across all runs of each service. The edge
  weight is the number of distinct runs where the dependency had at least one
  incident.

* **Template → template co-occurrence** (within the same run, same worker):
  for each ordered pair (template_a → template_b) where ``b`` appears within
  ``lag_seconds`` after ``a`` on the same ``worker_id``, count occurrences and
  derive a lift score. High-lift pairs reveal "X always precedes Y" — the
  building block for incident signatures.

Both helpers take a list of registry rows (or the registry table directly) and
return ready-to-render edge tables.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from .io import read_json


def service_dependency_edges(runs_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate (service, dependency, runs_with_incidents, total_incidents)."""
    counts: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: {"runs": 0, "incidents": 0})
    for run in runs_rows:
        run_dir = Path(run.get("run_dir") or "")
        if not run_dir.exists():
            continue
        meta_path = run_dir / "run.json"
        if not meta_path.exists():
            continue
        meta = read_json(meta_path)
        service = str(meta.get("service_name") or "")
        timeline = meta.get("connectivity_timeline") or {}
        for dep, payload in timeline.items():
            if not isinstance(payload, dict):
                continue
            incidents = payload.get("incidents") or []
            key = (service, dep)
            counts[key]["incidents"] += len(incidents)
            if incidents:
                counts[key]["runs"] += 1
            else:
                # Even seeing the dep with up-state still counts as "this service
                # depends on this infra" — useful for graph construction.
                counts[key]["runs"] += 1
    edges = [
        {
            "service_name": service,
            "dependency": dep,
            "runs_with_dependency": data["runs"],
            "total_incidents": data["incidents"],
        }
        for (service, dep), data in counts.items()
    ]
    edges.sort(key=lambda e: (e["service_name"], -e["total_incidents"], e["dependency"]))
    return edges


def _parse_ts(value: Any) -> float | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value)
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        return None


def template_cooccurrence_edges(
    events_table: pa.Table,
    *,
    lag_seconds: float = 30.0,
    min_count: int = 3,
) -> list[dict[str, Any]]:
    """Find ordered template pairs (a → b) where b follows a within ``lag_seconds``
    on the same worker. Returns edges with count and lift.
    """
    needed = {"template_id", "timestamp", "worker_id"}
    available = set(events_table.column_names)
    missing = needed - available
    if missing:
        raise ValueError(f"events table missing columns: {sorted(missing)}")
    rows = events_table.select(["template_id", "timestamp", "worker_id"]).to_pylist()
    # Sort per worker by timestamp, then walk a sliding window.
    by_worker: dict[str, list[tuple[float, str]]] = defaultdict(list)
    template_counts: Counter[str] = Counter()
    for row in rows:
        ts = _parse_ts(row.get("timestamp"))
        if ts is None:
            continue
        worker = str(row.get("worker_id") or "")
        template_id = str(row.get("template_id") or "")
        if not template_id:
            continue
        by_worker[worker].append((ts, template_id))
        template_counts[template_id] += 1
    pair_counts: Counter[tuple[str, str]] = Counter()
    for worker, events in by_worker.items():
        events.sort()
        for i, (ts_a, a) in enumerate(events):
            for j in range(i + 1, len(events)):
                ts_b, b = events[j]
                if ts_b - ts_a > lag_seconds:
                    break
                if a == b:
                    continue
                pair_counts[(a, b)] += 1
    total_events = sum(template_counts.values())
    edges: list[dict[str, Any]] = []
    for (a, b), count in pair_counts.items():
        if count < min_count:
            continue
        # Lift = observed_joint / (expected_joint under independence). Use simple
        # frequency-based estimate; good enough for ranking.
        p_a = template_counts[a] / total_events if total_events else 0
        p_b = template_counts[b] / total_events if total_events else 0
        expected = p_a * p_b * total_events
        lift = (count / expected) if expected > 0 else float("inf")
        edges.append(
            {
                "template_a": a,
                "template_b": b,
                "count": count,
                "p_a": p_a,
                "p_b": p_b,
                "lift": lift,
            }
        )
    edges.sort(key=lambda e: (-e["lift"], -e["count"]))
    return edges
