from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from logs_reaper.compare import compare_runs
from logs_reaper.dataset import export_dataset
from logs_reaper.io import read_parquet
from logs_reaper import scan as scan_module
from logs_reaper.scan import scan


def test_scan_json_text_traceback_and_malformed(tmp_path: Path) -> None:
    log_path = tmp_path / "mixed.log"
    log_path.write_text(
        "\n".join(
            [
                '{"time":"2026-05-14T10:00:00Z","level":"INFO","message":"LogLevel: INFO","microservice":"gateway-isp","worker_id":"w1","threadName":"MainThread"}',
                '{"time":"2026-05-14T10:00:01Z","level":"ERROR","message":"Failed account 507f1f77bcf86cd799439011 from 10.0.0.5:443","microservice":"gateway-isp"}',
                "2026-05-14T10:00:02Z\tERROR MainThread MainProcess\tmain.py:10\thandle\tfailed user 123",
                "Traceback (most recent call last):",
                '  File "/srv/app/main.py", line 10, in handle',
                "ValueError: bad user 123",
                "{malformed-json",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"

    scan(input_patterns=[str(log_path)], run_id="RUN_A", service_name="gateway-isp", out_dir=out_dir)

    events = read_parquet(out_dir / "events.parquet")
    templates = read_parquet(out_dir / "templates.parquet")
    errors = read_parquet(out_dir / "errors.parquet")
    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    run = json.loads((out_dir / "run.json").read_text(encoding="utf-8"))
    report = (out_dir / "report.md").read_text(encoding="utf-8")

    assert len(events) == 4
    assert len(templates) == 4
    assert len(errors) >= 2
    assert summary["parse_status"]["degraded"] == 1
    assert summary["scan_duration_seconds"] > 0
    assert summary["input_bytes"] == log_path.stat().st_size
    assert summary["throughput_gb_per_second"] > 0
    assert summary["events_per_second"] > 0
    assert run["scan_duration_seconds"] > 0
    assert run["events_per_second"] > 0
    assert run["engine"] == "rust"
    assert "Throughput GB/s:" in report
    assert "Events/s:" in report
    assert any("ValueError" in (event["body"] or "") for event in events)
    assert any(event["worker_id"] == "w1" for event in events)
    assert (out_dir / "report.md").exists()
    template_counts = {row["template_id"]: row["event_count"] for row in templates}
    event_counts: dict[str, int] = {}
    for event in events:
        event_counts[event["template_id"]] = event_counts.get(event["template_id"], 0) + 1
    assert template_counts == event_counts


def test_classification_rules_and_dataset_export(tmp_path: Path) -> None:
    log_path = tmp_path / "noise.ndjson"
    log_path.write_text(
        '{"time":"2026-05-14T10:00:00Z","level":"ERROR","message":"client disconnected with BrokenPipeError","microservice":"gateway-isp"}\n',
        encoding="utf-8",
    )
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        "known_noise:\n"
        "  - id: broken_pipe\n"
        "    severity: [ERROR]\n"
        "    template_regex: BrokenPipeError\n"
        "    reason: accepted noise\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"

    scan(input_patterns=[str(log_path)], run_id="RUN_NOISE", out_dir=out_dir, rules_path=rules_path)
    errors = read_parquet(out_dir / "errors.parquet")

    assert errors[0]["classification"] == "known-noise"
    dataset_path = tmp_path / "dataset.ndjson"
    count = export_dataset(input_dir=out_dir, out=dataset_path)
    rows = [json.loads(line) for line in dataset_path.read_text(encoding="utf-8").splitlines()]
    assert count == 1
    assert rows[0]["classification"] == "known-noise"
    assert "body" not in rows[0]


def _multi_boot_log() -> str:
    parts = [
        '{"time":"2026-05-14T10:00:00Z","level":"WARNING","message":"old kafka noise","microservice":"accounts"}',
        '{"time":"2026-05-14T10:00:05Z","level":"ERROR","message":"failure from previous boot","microservice":"accounts"}',
        '{"time":"2026-05-14T10:30:00Z","level":"INFO","message":"Starting gunicorn 26.0.0","microservice":"accounts"}',
        '{"time":"2026-05-14T10:30:00Z","level":"INFO","message":"Booting worker with pid: 11","microservice":"accounts"}',
        '{"time":"2026-05-14T10:30:01Z","level":"INFO","message":"app ready instance two","microservice":"accounts"}',
    ]
    # ~6 KiB of filler so the reverse-scan realignment window (4 KiB) cannot
    # cross into the previous boot.
    for index in range(60):
        parts.append(
            f'{{"time":"2026-05-14T10:30:{index % 60:02d}Z","level":"INFO","message":"filler line {index} '
            f'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","microservice":"accounts"}}'
        )
    parts.extend(
        [
            '{"time":"2026-05-14T11:00:00Z","level":"INFO","message":"Starting gunicorn 26.0.0","microservice":"accounts"}',
            '{"time":"2026-05-14T11:00:01Z","level":"INFO","message":"Booting worker with pid: 21","microservice":"accounts"}',
            '{"time":"2026-05-14T11:00:02Z","level":"ERROR","message":"latest boot failure","microservice":"accounts"}',
        ]
    )
    return "\n".join(parts) + "\n"


def test_instances_default_keeps_only_last_boot(tmp_path: Path) -> None:
    log_path = tmp_path / "multiboot.ndjson"
    log_path.write_text(_multi_boot_log(), encoding="utf-8")
    out_dir = tmp_path / "out"
    scan(
        input_patterns=[str(log_path)],
        run_id="MULTI_LAST",
        service_name="accounts",
        out_dir=out_dir,
    )
    events = read_parquet(out_dir / "events.parquet")
    run = json.loads((out_dir / "run.json").read_text(encoding="utf-8"))
    instances = run["instances"]
    # Reverse-scan anchors directly to the last boot, so Rust only ever sees that
    # one instance — older boots are not even parsed.
    assert instances["detected_count"] == 1
    assert instances["tail_anchor_offset"] is not None and instances["tail_anchor_offset"] > 0
    assert instances["parsed_input_bytes"] < instances["total_input_bytes"]
    assert run["event_count"] >= 1
    bodies = [event["body"] for event in events]
    assert any("latest boot failure" in body for body in bodies)
    assert not any("previous boot" in body for body in bodies)
    assert not any("old kafka noise" in body for body in bodies)


def test_instances_all_keeps_every_event(tmp_path: Path) -> None:
    log_path = tmp_path / "multiboot.ndjson"
    log_path.write_text(_multi_boot_log(), encoding="utf-8")
    out_dir = tmp_path / "out"
    scan(
        input_patterns=[str(log_path)],
        run_id="MULTI_ALL",
        service_name="accounts",
        out_dir=out_dir,
        instances="all",
    )
    events = read_parquet(out_dir / "events.parquet")
    run = json.loads((out_dir / "run.json").read_text(encoding="utf-8"))
    assert run["instances"]["filter_active"] is False
    assert run["instances"]["detected_count"] == 2
    seqs = {event["service_instance_seq"] for event in events}
    assert seqs == {0, 1, 2}


def test_instances_zero_boot_keeps_everything(tmp_path: Path) -> None:
    log_path = tmp_path / "quiet.ndjson"
    log_path.write_text(
        '{"time":"2026-05-14T10:00:00Z","level":"INFO","message":"hello there","microservice":"accounts"}\n'
        '{"time":"2026-05-14T10:00:05Z","level":"ERROR","message":"oops","microservice":"accounts"}\n',
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"
    scan(
        input_patterns=[str(log_path)],
        run_id="MULTI_NONE",
        service_name="accounts",
        out_dir=out_dir,
    )
    run = json.loads((out_dir / "run.json").read_text(encoding="utf-8"))
    events = read_parquet(out_dir / "events.parquet")
    assert run["instances"]["detected_count"] == 0
    assert run["instances"]["filter_active"] is False
    assert len(events) == 2
    assert all(event["service_instance_seq"] == 0 for event in events)


def test_issue_kind_split_and_connectivity_timeline(tmp_path: Path) -> None:
    log_path = tmp_path / "mixed_focus.ndjson"
    log_path.write_text(
        "\n".join(
            [
                '{"time":"2026-05-14T12:00:00Z","level":"INFO","message":"Starting gunicorn","microservice":"accounts"}',
                '{"time":"2026-05-14T12:00:05Z","level":"ERROR","message":"Traceback (most recent call last):\\n  File main.py, line 10, in handle\\nValueError: bad user 1","microservice":"accounts"}',
                '{"time":"2026-05-14T12:00:06Z","level":"WARNING","message":"ConfluentEventBusConsumer - error_cb: KafkaError{code=_ALL_BROKERS_DOWN,val=-187,str=\\"2/2 brokers are down\\"}","microservice":"accounts"}',
                '{"time":"2026-05-14T12:00:09Z","level":"WARNING","message":"ConfluentEventBusConsumer - error_cb: KafkaError{code=_TRANSPORT,val=-195,str=\\"kafka:9092/bootstrap: Connect to ipv4#172.18.0.2:9092 failed: Connection refused\\"}","microservice":"accounts"}',
                '{"time":"2026-05-14T12:00:30Z","level":"INFO","message":"rejoined group accounts-cg-1","microservice":"accounts"}',
                '{"time":"2026-05-14T12:00:35Z","level":"ERROR","message":"AttributeError: NoneType has no attribute foo","microservice":"accounts"}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"
    scan(
        input_patterns=[str(log_path)],
        run_id="FOCUS_SPLIT",
        service_name="accounts",
        out_dir=out_dir,
        instances="all",
        focus="both",
    )
    run = json.loads((out_dir / "run.json").read_text(encoding="utf-8"))
    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    report = (out_dir / "report.md").read_text(encoding="utf-8")
    issue_counts = summary["issue_kind_counts"]
    assert issue_counts.get("code", 0) >= 2
    assert issue_counts.get("infra", 0) >= 1
    assert "Code Issues" in report
    assert "Infrastructure Issues" in report
    timeline = run["connectivity_timeline"]
    kafka = timeline["kafka"]
    assert kafka["state"] == "up"
    assert kafka["down_events"] >= 2
    assert len(kafka["incidents"]) == 1
    incident = kafka["incidents"][0]
    assert incident["down_at"] == "2026-05-14T12:00:06Z"
    assert incident["up_at"] == "2026-05-14T12:00:30Z"
    assert incident["duration_seconds"] is not None and incident["duration_seconds"] >= 20.0


def test_focus_flag_renders_only_requested_lens(tmp_path: Path) -> None:
    log_path = tmp_path / "code_only.ndjson"
    log_path.write_text(
        '{"time":"2026-05-14T12:00:05Z","level":"ERROR","message":"AttributeError: nope","microservice":"accounts"}\n',
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"
    scan(
        input_patterns=[str(log_path)],
        run_id="FOCUS_CODE",
        service_name="accounts",
        out_dir=out_dir,
        focus="code",
    )
    report = (out_dir / "report.md").read_text(encoding="utf-8")
    assert "Code Issues" in report
    assert "Infrastructure Issues" not in report


def test_compare_marks_regression_and_fixed(tmp_path: Path) -> None:
    left_log = tmp_path / "left.ndjson"
    right_log = tmp_path / "right.ndjson"
    left_log.write_text(
        '{"time":"2026-05-14T10:00:00Z","level":"ERROR","message":"left failure account 1","microservice":"gateway-isp"}\n',
        encoding="utf-8",
    )
    right_log.write_text(
        '{"time":"2026-05-14T10:05:00Z","level":"ERROR","message":"right failure account 2","microservice":"gateway-isp"}\n',
        encoding="utf-8",
    )
    left_out = tmp_path / "left"
    right_out = tmp_path / "right"
    scan(input_patterns=[str(left_log)], run_id="LEFT", out_dir=left_out)
    scan(input_patterns=[str(right_log)], run_id="RIGHT", out_dir=right_out, baseline_dir=left_out)

    diff_path = tmp_path / "diff.md"
    payload = compare_runs(left_dir=left_out, right_dir=right_out, out=diff_path)

    assert payload["regression_count"] == 1
    assert payload["fixed_error_count"] == 1
    assert "regression" in diff_path.read_text(encoding="utf-8")
