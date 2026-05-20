from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from logs_reaper.autodiscovery import finalize_service_scan, prepare_service_scan


def test_prepare_service_scan_reuses_same_snapshot(monkeypatch, tmp_path: Path) -> None:
    service_dir = tmp_path / "accounts"
    registry = service_dir / "service-scan-registry.json"
    existing_capture = service_dir / "captures" / "existing.log"
    existing_capture.parent.mkdir(parents=True, exist_ok=True)
    existing_capture.write_text("same log\n", encoding="utf-8")
    scan_out = service_dir / "ACCOUNTS_OLD"
    scan_out.mkdir(parents=True)
    registry.write_text(
        json.dumps(
            {
                "runs": [
                    {
                        "fingerprint": "fp-1",
                        "run_id": "ACCOUNTS_OLD",
                        "scan_out_dir": str(scan_out),
                        "captured_log_path": str(existing_capture),
                        "container_id": "cid-1",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "logs_reaper.autodiscovery._discover_service_source",
        lambda **_: {
            "mode": "docker-logs",
            "service_name": "accounts",
            "captured_log_path": str(existing_capture),
            "container_id": "cid-1",
            "container_started_at": "2026-05-14T11:00:00Z",
            "content_digest": "same",
        },
    )
    monkeypatch.setattr("logs_reaper.autodiscovery._build_fingerprint", lambda source: "fp-1")

    prepared = prepare_service_scan(service_name="accounts", input_patterns=None, out_dir=service_dir, run_id=None)

    assert prepared["autodiscovery"]["reused_existing_scan"] is True
    assert prepared["run_id"] == "ACCOUNTS_OLD"
    assert prepared["out_dir"] == str(scan_out)
    assert prepared["baseline_dir"] is None


def test_finalize_service_scan_persists_registry(tmp_path: Path) -> None:
    registry_path = tmp_path / "service-scan-registry.json"
    autodiscovery = {
        "registry_path": str(registry_path),
        "fingerprint": "fp-2",
        "captured_log_path": str(tmp_path / "capture.log"),
        "container_id": "cid-2",
        "container_name": "sm-accounts-1",
        "container_started_at": "2026-05-14T11:00:00Z",
        "mode": "docker-logs",
        "status": "same_container_new_logs",
    }

    finalize_service_scan(
        service_name="accounts",
        run_id="ACCOUNTS_NEW",
        scan_out_dir=tmp_path / "ACCOUNTS_NEW",
        autodiscovery=autodiscovery,
        summary={"event_count": 10, "template_count": 2},
    )

    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    assert payload["runs"][0]["fingerprint"] == "fp-2"
    assert payload["runs"][0]["status"] == "same_container_new_logs"


def test_prepare_service_scan_suggests_previous_run_as_baseline(monkeypatch, tmp_path: Path) -> None:
    service_dir = tmp_path / "accounts"
    registry = service_dir / "service-scan-registry.json"
    previous_capture = service_dir / "captures" / "previous.log"
    previous_capture.parent.mkdir(parents=True, exist_ok=True)
    previous_capture.write_text("old log\n", encoding="utf-8")
    previous_scan = service_dir / "ACCOUNTS_PREV"
    previous_scan.mkdir(parents=True)
    registry.write_text(
        json.dumps(
            {
                "runs": [
                    {
                        "fingerprint": "fp-prev",
                        "run_id": "ACCOUNTS_PREV",
                        "scan_out_dir": str(previous_scan),
                        "captured_log_path": str(previous_capture),
                        "container_id": "cid-1",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    new_capture = service_dir / "captures" / "new.log"
    new_capture.write_text("new log\n", encoding="utf-8")
    monkeypatch.setattr(
        "logs_reaper.autodiscovery._discover_service_source",
        lambda **_: {
            "mode": "docker-logs",
            "service_name": "accounts",
            "captured_log_path": str(new_capture),
            "container_id": "cid-1",
            "container_name": "sm-accounts-1",
            "container_started_at": "2026-05-14T11:00:00Z",
            "content_digest": "new",
        },
    )
    monkeypatch.setattr("logs_reaper.autodiscovery._build_fingerprint", lambda source: "fp-new")

    prepared = prepare_service_scan(service_name="accounts", input_patterns=None, out_dir=service_dir, run_id=None)

    assert prepared["autodiscovery"]["status"] == "same_container_new_logs"
    assert prepared["baseline_dir"] == str(previous_scan)
    assert prepared["autodiscovery"]["baseline_run_id"] == "ACCOUNTS_PREV"


def test_prepare_service_scan_force_reprocesses_same_snapshot(monkeypatch, tmp_path: Path) -> None:
    service_dir = tmp_path / "accounts"
    registry = service_dir / "service-scan-registry.json"
    existing_capture = service_dir / "captures" / "existing.log"
    existing_capture.parent.mkdir(parents=True, exist_ok=True)
    existing_capture.write_text("same log\n", encoding="utf-8")
    previous_scan = service_dir / "ACCOUNTS_OLD"
    previous_scan.mkdir(parents=True)
    registry.write_text(
        json.dumps(
            {
                "runs": [
                    {
                        "fingerprint": "fp-1",
                        "run_id": "ACCOUNTS_OLD",
                        "scan_out_dir": str(previous_scan),
                        "captured_log_path": str(existing_capture),
                        "container_id": "cid-1",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "logs_reaper.autodiscovery._discover_service_source",
        lambda **_: {
            "mode": "docker-logs",
            "service_name": "accounts",
            "captured_log_path": str(existing_capture),
            "container_id": "cid-1",
            "container_name": "sm-accounts-1",
            "container_started_at": "2026-05-14T11:00:00Z",
            "content_digest": "same",
        },
    )
    monkeypatch.setattr("logs_reaper.autodiscovery._build_fingerprint", lambda source: "fp-1")

    prepared = prepare_service_scan(
        service_name="accounts",
        input_patterns=None,
        out_dir=service_dir,
        run_id=None,
        force_reprocess=True,
    )

    assert prepared["autodiscovery"]["reused_existing_scan"] is False
    assert prepared["autodiscovery"]["status"] == "same_snapshot"
    assert prepared["baseline_dir"] == str(previous_scan)
