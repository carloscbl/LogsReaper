from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .io import read_json, read_parquet


def export_dataset(
    *,
    input_dir: str | Path,
    out: str | Path,
    include_body: bool = False,
) -> int:
    input_path = Path(input_dir)
    out_path = Path(out)
    run = _read_run(input_path)
    templates = {row["template_id"]: row for row in read_parquet(input_path / "templates.parquet")}
    events = read_parquet(input_path / "events.parquet")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with out_path.open("w", encoding="utf-8") as handle:
        for event in events:
            template = templates.get(event["template_id"], {})
            payload: dict[str, Any] = {
                "run_id": event.get("run_id") or run.get("run_id"),
                "event_id": event.get("event_id"),
                "template_id": event.get("template_id"),
                "service_name": event.get("service_name"),
                "severity_text": event.get("severity_text"),
                "severity_number": event.get("severity_number"),
                "error_kind": event.get("error_kind"),
                "exception_type": event.get("exception_type"),
                "classification": template.get("classification") or event.get("classification"),
                "normalized_template": event.get("normalized_template"),
                "parse_status": event.get("parse_status"),
                "source": event.get("source"),
                "line_count": event.get("line_count"),
            }
            if include_body:
                payload["body"] = event.get("body")
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
            count += 1
    return count


def _read_run(path: Path) -> dict[str, Any]:
    run_path = path / "run.json"
    return read_json(run_path) if run_path.exists() else {}
