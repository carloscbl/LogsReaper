"""Notebook-friendly API on top of the registry.

```python
from logs_reaper import api
api.runs(service="accounts")          # → pa.Table
api.templates(run_id="…")             # → pa.Table
api.events(run_id="…", columns=[…])   # → pa.Table
api.baseline(service="accounts", scenario="traits")  # → pa.Table
api.diff_for(run_id="…")              # → dict
api.lineage_for(template_id="…")      # → list[dict] (predecessor chain)
```

Default registry directory is ``./runs`` but you can override
it once at import time by setting ``LOGS_REAPER_REGISTRY`` or by passing
``registry_dir=…`` to any function.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .diff_engine import compute_diff, load_baseline_for
from .lineage import jaccard, template_shingles


def _default_registry_dir() -> Path:
    raw = os.environ.get("LOGS_REAPER_REGISTRY")
    return Path(raw) if raw else Path(__file__).resolve().parents[1] / "runs"


def _registry_path(registry_dir: Path | None = None) -> Path:
    return (registry_dir or _default_registry_dir()) / "registry.parquet"


def _baseline_path(registry_dir: Path | None = None) -> Path:
    return (registry_dir or _default_registry_dir()) / "baseline.parquet"


def _template_registry_path(registry_dir: Path | None = None) -> Path:
    return (registry_dir or _default_registry_dir()) / "template_registry.parquet"


def runs(
    *,
    service: str | None = None,
    scenario: str | None = None,
    status: str | None = None,
    registry_dir: Path | None = None,
) -> pa.Table:
    """Return the registry table optionally filtered by service/scenario/status."""
    table = pq.read_table(_registry_path(registry_dir))
    rows = table.to_pylist()
    if service is not None:
        rows = [row for row in rows if row.get("service_name") == service]
    if scenario is not None:
        rows = [row for row in rows if row.get("scenario") == scenario]
    if status is not None:
        rows = [row for row in rows if row.get("status") == status]
    return pa.Table.from_pylist(rows, schema=table.schema) if rows else table.slice(0, 0)


def _find_run_dir(run_id: str, registry_dir: Path | None = None) -> Path:
    registry = pq.read_table(_registry_path(registry_dir))
    for row in registry.to_pylist():
        if row.get("run_id") == run_id:
            run_dir = row.get("run_dir")
            if run_dir:
                return Path(run_dir)
    raise KeyError(f"run_id {run_id!r} not in registry")


def templates(run_id: str, *, registry_dir: Path | None = None) -> pa.Table:
    run_dir = _find_run_dir(run_id, registry_dir)
    path = run_dir / "templates.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    return pq.read_table(path)


def events(
    run_id: str,
    *,
    columns: list[str] | None = None,
    registry_dir: Path | None = None,
) -> pa.Table:
    run_dir = _find_run_dir(run_id, registry_dir)
    path = run_dir / "events.parquet"
    if not path.exists():
        raise FileNotFoundError(path)
    return pq.read_table(path, columns=columns)


def baseline(
    *,
    service: str,
    scenario: str,
    registry_dir: Path | None = None,
) -> pa.Table:
    path = _baseline_path(registry_dir)
    if not path.exists():
        return pa.table({})
    table = pq.read_table(path)
    rows = [
        row
        for row in table.to_pylist()
        if row.get("service_name") == service and row.get("scenario") == scenario
    ]
    return pa.Table.from_pylist(rows, schema=table.schema) if rows else table.slice(0, 0)


def diff_for(
    run_id: str,
    *,
    registry_dir: Path | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    run_dir = _find_run_dir(run_id, registry_dir)
    return compute_diff(run_dir=run_dir, baseline_path=_baseline_path(registry_dir), **kwargs)


def lineage_for(
    template_id: str,
    *,
    registry_dir: Path | None = None,
    min_similarity: float = 0.5,
    top_k: int = 5,
) -> list[dict[str, Any]]:
    """Return the top-k baseline templates most similar to ``template_id``.

    Useful in a notebook to spelunk why a template was flagged "evolved" or to
    explore the neighbourhood of an unfamiliar template.
    """
    template_reg = pq.read_table(_template_registry_path(registry_dir)).to_pylist()
    target = next((row for row in template_reg if row.get("template_id") == template_id), None)
    if target is None:
        raise KeyError(f"template_id {template_id!r} not in template_registry")
    target_shingles = template_shingles(str(target.get("normalized_template") or ""))
    candidates: list[dict[str, Any]] = []
    for row in template_reg:
        if row.get("template_id") == template_id:
            continue
        score = jaccard(
            target_shingles,
            template_shingles(str(row.get("normalized_template") or "")),
        )
        if score >= min_similarity:
            candidates.append(
                {
                    "template_id": row.get("template_id"),
                    "service_name": row.get("service_name"),
                    "similarity_score": score,
                    "normalized_template": row.get("normalized_template"),
                    "severity_text": row.get("severity_text"),
                    "runs_seen_count": row.get("runs_seen_count"),
                }
            )
    candidates.sort(key=lambda r: -r["similarity_score"])
    return candidates[:top_k]
