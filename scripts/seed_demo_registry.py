"""Seed a tiny demo registry so the Streamlit dashboard has something to show.

Creates:
  * 6 green TRAITS runs (stable baseline).
  * 6 green QUOTAS runs (different scenario).
  * 2 red TRAITS candidate runs (one with a spike on a baseline template, one
    with a brand-new error template + connectivity incident).
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from logs_reaper.io import EVENT_SCHEMA, TEMPLATE_SCHEMA, write_json, write_parquet, write_parquet_table
from logs_reaper.registry import build_registry
import pyarrow as pa


SERVICE = "accounts"


def _template(template_id: str, count: int, normalized: str, severity: str = "INFO", issue_kind: str = "noise") -> dict[str, object]:
    return {
        "template_id": template_id,
        "service_name": SERVICE,
        "severity_text": severity,
        "severity_number": 9 if severity == "INFO" else 17,
        "normalized_template": normalized,
        "error_kind": None,
        "exception_type": None,
        "event_count": count,
        "first_seen": "2026-05-14T10:00:00Z",
        "last_seen": "2026-05-14T10:30:00Z",
        "example_event_id": f"evt-{template_id}",
        "parse_status": "ok",
        "classification": "expected",
        "classification_reason": None,
        "baseline_match": True,
        "issue_kind": issue_kind,
    }


def _event_row(template_id: str, ts: str, seq: int, issue: str | None) -> dict[str, object]:
    base = {field.name: None for field in EVENT_SCHEMA}
    base.update(
        {
            "event_id": f"{template_id}-{ts}-{seq}",
            "timestamp": ts,
            "template_id": template_id,
            "service_name": SERVICE,
            "service_instance_seq": seq,
            "service_instance_started_at": "2026-05-14T10:00:00Z",
            "issue_kind": issue,
            "severity_text": "ERROR" if issue == "code" else "INFO",
            "worker_id": "w1",
        }
    )
    return base


def _make_run(
    runs_root: Path,
    *,
    run_id: str,
    created_at: str,
    templates: list[dict[str, object]],
    events: list[dict[str, object]] | None = None,
    runtime_counts: dict[str, int] | None = None,
    error_count: int = 0,
    kafka_incidents: int = 0,
) -> None:
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(run_dir / "templates.parquet", templates, TEMPLATE_SCHEMA)
    if events:
        events_table = pa.Table.from_pylist(events, schema=EVENT_SCHEMA)
        write_parquet_table(run_dir / "events.parquet", events_table, EVENT_SCHEMA)
    timeline = {}
    if kafka_incidents:
        timeline["kafka"] = {
            "state": "up",
            "incidents": [
                {
                    "down_at": "2026-05-14T10:05:00Z",
                    "up_at": "2026-05-14T10:05:30Z",
                    "duration_seconds": 30.0,
                }
            ] * kafka_incidents,
            "down_events": 0,
            "up_events": 0,
        }
    payload = {
        "tool": "LogsReaper",
        "tool_version": "0.1.0",
        "run_id": run_id,
        "created_at": created_at,
        "service_name": SERVICE,
        "input_globs": [],
        "input_files": [],
        "file_count": 1,
        "event_count": sum(int(t.get("event_count") or 0) for t in templates),
        "template_count": len(templates),
        "error_count": error_count,
        "hash_algorithm": "blake3",
        "runtime_counts": runtime_counts or {"code": 0, "infra": 0, "ops": 0, "noise": 100},
        "parse_status": {"ok": 100},
        "engine": "rust",
        "scan_duration_seconds": 1.5,
        "events_per_second": 1000.0,
        "throughput_gb_per_second": 0.05,
        "input_bytes": 1_048_576,
        "input_gigabytes": 0.001,
        "connectivity_timeline": timeline,
    }
    write_json(run_dir / "run.json", payload)


def main() -> None:
    runs_root = ROOT / "out"
    registry_out = ROOT / "runs"
    runs_root.mkdir(parents=True, exist_ok=True)

    # --- TRAITS baseline (6 green runs)
    traits_templates_base = [
        ("kafka_consume", "INFO", "Consuming message from kafka topic", "noise"),
        ("mongo_query", "INFO", "Mongo query executed in duration ms", "noise"),
        ("http_request", "INFO", "HTTP request handled in duration ms", "noise"),
        ("policy_apply", "INFO", "Policy applied for user", "noise"),
        ("rate_limited", "WARNING", "Request rate-limited for user", "ops"),
    ]
    for idx in range(6):
        _make_run(
            runs_root,
            run_id=f"E2E_TRAITS_{idx:03d}",
            created_at=f"2026-05-01T{idx:02d}:00:00Z",
            templates=[
                _template(tid, 200 + idx * 5, norm, sev, ik)
                for tid, sev, norm, ik in traits_templates_base
            ],
        )
    # TRAITS regressed candidate — kafka_consume blows up 10×.
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_REGRESSED_007",
        created_at="2026-05-08T00:00:00Z",
        templates=[
            _template("kafka_consume", 2200, "Consuming message from kafka topic"),
            *(
                _template(tid, 200, norm, sev, ik)
                for tid, sev, norm, ik in traits_templates_base[1:]
            ),
            _template(
                "kafka_lost",
                40,
                "Connection lost to kafka broker on host node-7",
                "ERROR",
                "infra",
            ),
        ],
        events=[
            _event_row("kafka_lost", "2026-05-08T00:01:00Z", 1, "code"),
        ],
        runtime_counts={"code": 1, "infra": 39, "ops": 0, "noise": 2400},
        error_count=1,
        kafka_incidents=2,
    )
    # TRAITS recovered run — back to baseline ranges.
    _make_run(
        runs_root,
        run_id="E2E_TRAITS_RECOVERED_008",
        created_at="2026-05-09T00:00:00Z",
        templates=[
            _template(tid, 210, norm, sev, ik)
            for tid, sev, norm, ik in traits_templates_base
        ],
    )

    # --- QUOTAS baseline (6 green runs)
    quotas_templates_base = [
        ("quota_check", "INFO", "Quota checked for tenant", "noise"),
        ("quota_grant", "INFO", "Quota granted for tenant", "noise"),
        ("quota_deny", "WARNING", "Quota denied for tenant", "ops"),
    ]
    for idx in range(6):
        _make_run(
            runs_root,
            run_id=f"E2E_QUOTAS_{idx:03d}",
            created_at=f"2026-05-01T{idx:02d}:30:00Z",
            templates=[
                _template(tid, 150 + idx * 3, norm, sev, ik)
                for tid, sev, norm, ik in quotas_templates_base
            ],
        )
    # QUOTAS candidate with novel template.
    _make_run(
        runs_root,
        run_id="E2E_QUOTAS_NEW_007",
        created_at="2026-05-08T00:30:00Z",
        templates=[
            *(
                _template(tid, 160, norm, sev, ik)
                for tid, sev, norm, ik in quotas_templates_base
            ),
            _template(
                "quota_overflow",
                25,
                "Tenant exceeded quota during traits provisioning",
                "ERROR",
                "code",
            ),
        ],
        events=[
            _event_row("quota_overflow", "2026-05-08T00:31:00Z", 1, "code"),
            _event_row("quota_check", "2026-05-08T00:31:01Z", 1, "noise"),
        ],
        runtime_counts={"code": 25, "infra": 0, "ops": 160, "noise": 320},
        error_count=1,
    )

    summary = build_registry(runs_root, registry_out, min_green_runs=2)
    print(
        f"Seeded registry at {summary['out_dir']}: "
        f"{summary['runs_total']} runs, baseline rows={summary['baseline_rows']}, "
        f"templates_total={summary['templates_total']}"
    )


if __name__ == "__main__":
    main()
