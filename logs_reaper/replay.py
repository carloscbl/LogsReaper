"""Replayable timeline: unified chronological view of a run.

Builds one row per significant moment:

* ``boot`` — start of a new ``service_instance_seq``.
* ``first_code_error`` — first issue_kind=code event in each instance.
* ``template_burst`` — N events of the same template within a window.
* ``connectivity_down`` / ``connectivity_up`` — taken from
  ``connectivity_timeline.incidents`` in run.json.

The output is one ``pa.Table`` with a stable schema so the dashboard can
render it as a scrubbable Gantt-like view.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .io import read_json


REPLAY_SCHEMA = pa.schema(
    [
        ("ts", pa.string()),
        ("kind", pa.string()),
        ("label", pa.string()),
        ("instance_seq", pa.int64()),
        ("template_id", pa.string()),
        ("count", pa.int64()),
        ("dependency", pa.string()),
        ("severity_text", pa.string()),
    ]
)


def _empty_table() -> pa.Table:
    return pa.table(
        {field.name: pa.array([], type=field.type) for field in REPLAY_SCHEMA}, schema=REPLAY_SCHEMA
    )


def _parse_minute(ts: str | None) -> str | None:
    if not ts:
        return None
    s = ts
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s).replace(second=0, microsecond=0).isoformat()
    except ValueError:
        return None


def build_replay_timeline(
    run_dir: Path,
    *,
    burst_threshold: int = 50,
) -> pa.Table:
    """Build the chronological event list. Output sorted by ts ascending."""
    events_path = run_dir / "events.parquet"
    meta_path = run_dir / "run.json"
    if not events_path.exists():
        return _empty_table()
    table = pq.read_table(
        events_path,
        columns=[
            "service_instance_seq",
            "service_instance_started_at",
            "timestamp",
            "issue_kind",
            "template_id",
            "severity_text",
        ],
    )
    rows = table.to_pylist()
    items: list[dict[str, Any]] = []

    # Boots
    seen_boots: dict[int, str] = {}
    first_code_by_instance: dict[int, dict[str, Any]] = {}
    template_minute_counts: Counter[tuple[str, str]] = Counter()
    template_severity: dict[str, str] = {}
    for row in rows:
        seq = row.get("service_instance_seq")
        if seq is None:
            continue
        seq = int(seq)
        if seq not in seen_boots and row.get("service_instance_started_at"):
            seen_boots[seq] = str(row.get("service_instance_started_at"))
        if (
            row.get("issue_kind") == "code"
            and seq not in first_code_by_instance
            and row.get("timestamp")
        ):
            first_code_by_instance[seq] = {
                "ts": str(row.get("timestamp")),
                "template_id": str(row.get("template_id") or ""),
                "severity_text": str(row.get("severity_text") or ""),
            }
        template_id = str(row.get("template_id") or "")
        if template_id and row.get("timestamp"):
            minute = _parse_minute(str(row.get("timestamp")))
            if minute:
                template_minute_counts[(template_id, minute)] += 1
                template_severity.setdefault(template_id, str(row.get("severity_text") or ""))

    for seq, ts in seen_boots.items():
        items.append(
            {
                "ts": ts,
                "kind": "boot",
                "label": f"service instance #{seq} boot",
                "instance_seq": seq,
                "template_id": None,
                "count": None,
                "dependency": None,
                "severity_text": None,
            }
        )

    for seq, info in first_code_by_instance.items():
        items.append(
            {
                "ts": info["ts"],
                "kind": "first_code_error",
                "label": f"first code error in instance #{seq}",
                "instance_seq": seq,
                "template_id": info["template_id"],
                "count": 1,
                "dependency": None,
                "severity_text": info["severity_text"],
            }
        )

    for (template_id, minute), count in template_minute_counts.items():
        if count < burst_threshold:
            continue
        items.append(
            {
                "ts": minute,
                "kind": "template_burst",
                "label": f"burst of {count} events for {template_id[:12]}",
                "instance_seq": None,
                "template_id": template_id,
                "count": count,
                "dependency": None,
                "severity_text": template_severity.get(template_id, ""),
            }
        )

    if meta_path.exists():
        meta = read_json(meta_path)
        timeline = meta.get("connectivity_timeline") or {}
        for dep, payload in timeline.items():
            if not isinstance(payload, dict):
                continue
            for incident in payload.get("incidents") or []:
                items.append(
                    {
                        "ts": incident.get("down_at"),
                        "kind": "connectivity_down",
                        "label": f"{dep} down",
                        "instance_seq": None,
                        "template_id": None,
                        "count": None,
                        "dependency": dep,
                        "severity_text": None,
                    }
                )
                if incident.get("up_at"):
                    items.append(
                        {
                            "ts": incident.get("up_at"),
                            "kind": "connectivity_up",
                            "label": f"{dep} up",
                            "instance_seq": None,
                            "template_id": None,
                            "count": None,
                            "dependency": dep,
                            "severity_text": None,
                        }
                    )

    items.sort(key=lambda row: (row["ts"] or "", row["kind"]))
    if not items:
        return _empty_table()
    columns: dict[str, list[Any]] = {field.name: [] for field in REPLAY_SCHEMA}
    for row in items:
        for field in REPLAY_SCHEMA:
            columns[field.name].append(row.get(field.name))
    return pa.table(columns, schema=REPLAY_SCHEMA)
