"""Tests for the collect helpers and the offline HTML report.

`collect` se prueba sin docker real: monkeypatcheamos `asyncio.create_subprocess_exec`
para emular un docker logs streaming pequeño.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from logs_reaper.collect import CollectConfig, auto_detect_services, collect, resolve_services
from logs_reaper.report_html import write_report_html


# ---------- resolve_services -------------------------------------------------

def test_resolve_services_list_form():
    assert resolve_services("a,b , c") == ["a", "b", "c"]


def test_resolve_services_iterable():
    assert resolve_services(["x", " y "]) == ["x", "y"]


def test_resolve_services_all_invokes_autodetect(monkeypatch):
    monkeypatch.setattr("logs_reaper.collect.auto_detect_services", lambda *a, **kw: ["accounts", "gw"])
    assert resolve_services("all") == ["accounts", "gw"]


def test_auto_detect_services_parses_docker_ps(monkeypatch):
    class FakeProc:
        returncode = 0
        stdout = "sm-accounts-1\nsm-gateway-isp-1\nother-name-1\nsm-mongo-2\n"
    def fake_run(*args, **kwargs):
        return FakeProc()
    monkeypatch.setattr("subprocess.run", fake_run)
    services = auto_detect_services()
    assert services == ["accounts", "gateway-isp"]


# ---------- collect (mocked) -------------------------------------------------

class FakeAsyncSubprocess:
    """Emula asyncio subprocess con stdout que yield-ea chunks."""
    def __init__(self, chunks: list[bytes]):
        self.stdout = _FakeStream(chunks)
        self.returncode = 0

    async def wait(self):
        return self.returncode


class _FakeStream:
    def __init__(self, chunks):
        self._chunks = list(chunks)

    async def read(self, _n):
        if not self._chunks:
            return b""
        return self._chunks.pop(0)


@pytest.fixture()
def fake_exec(monkeypatch):
    def factory(per_service: dict[str, list[bytes]]):
        async def fake_create(prog, *args, **kwargs):
            # prog="docker"; args=("logs","-f","--tail","0",container,...)
            container = args[4]
            svc = container[len("sm-"):-len("-1")]
            chunks = per_service.get(svc, [b"hello\n"])
            return FakeAsyncSubprocess(chunks)
        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)
    return factory


def test_collect_writes_per_service_files(tmp_path: Path, fake_exec):
    fake_exec({
        "accounts":     [b"line1\n", b"line2\nline3\n"],
        "gateway-isp":  [b"only one line\n"],
    })
    out = tmp_path / "logs"
    cfg = CollectConfig(services=["accounts", "gateway-isp"], out_dir=out, duration_seconds=0.2)
    stats = collect(cfg)
    assert (out / "accounts.log").exists()
    assert (out / "gateway-isp.log").exists()
    acc = (out / "accounts.log").read_bytes()
    assert b"line1" in acc and b"line2" in acc and b"line3" in acc
    summary = json.loads((out / "collect_summary.json").read_text())
    assert set(summary["services"]) == {"accounts", "gateway-isp"}


# ---------- report_html ------------------------------------------------------

def test_write_report_html_basic(tmp_path: Path):
    per_service = {
        "accounts": {
            "run_dir": "/tmp/runs/accounts/R1",
            "delta_kind": "safe",
            "summary_counts": {"new": 2, "code_errors": 0, "policy_violations": 0},
            "code_errors_top": [],
            "policy_violations_top": [],
        },
        "gateway-isp": {
            "run_dir": "/tmp/runs/gateway-isp/R1",
            "delta_kind": "unsafe",
            "summary_counts": {"new": 0, "code_errors": 3, "policy_violations": 1},
            "code_errors_top": [
                {"observed_count": 25, "severity_text": "ERROR", "exception_type": "KeyError",
                 "template_id": "abc123def456"},
            ],
            "policy_violations_top": [
                {"observed_count": 2, "severity_text": "ERROR", "reason": "banned by policy",
                 "template_id": "xyz789"},
            ],
        },
    }
    out_html = tmp_path / "report.html"
    write_report_html(
        out_path=out_html,
        run_id="CI_1",
        per_service=per_service,
        collect_stats={"bytes_per_service": {"accounts": 100, "gateway-isp": 200},
                       "lines_per_service": {"accounts": 5, "gateway-isp": 10},
                       "duration_seconds": 60.5},
        registry_dir=Path("/tmp/registry"),
    )
    text = out_html.read_text()
    assert "<title>LogsReaper Report" in text
    assert "accounts" in text and "gateway-isp" in text
    assert "safe: 1" in text and "unsafe: 1" in text
    # secciones por severidad y patología:
    assert "KeyError" in text
    assert "banned by policy" in text
