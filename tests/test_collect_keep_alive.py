"""Smoke test for `collect --keep-alive`: el HTTP stats server sigue contestando
después de que --duration expira, hasta que llega SIGTERM/SIGINT.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import socket
import threading
import time
import urllib.request
from pathlib import Path

import pytest

from logs_reaper.collect import CollectConfig, collect


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture()
def fake_exec(monkeypatch):
    """Reusar el mock del test_collect_and_report sin importarlo."""
    class _FakeStream:
        def __init__(self, chunks):
            self._chunks = list(chunks)
        async def read(self, _n):
            if not self._chunks:
                return b""
            return self._chunks.pop(0)

    class _FakeProc:
        returncode = 0
        def __init__(self, chunks):
            self.stdout = _FakeStream(chunks)
        async def wait(self):
            return self.returncode
        def terminate(self): self.returncode = -15
        def kill(self):      self.returncode = -9

    async def fake_create(prog, *args, **kwargs):
        return _FakeProc([b"line1\nline2\n"])
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create)


def test_keep_alive_keeps_server_until_stop_event(tmp_path: Path, fake_exec):
    port = _free_port()
    stop_event = threading.Event()
    cfg = CollectConfig(
        services=["svc"],
        out_dir=tmp_path / "logs",
        duration_seconds=0.5,
        stats_port=port,
        stats_host="127.0.0.1",
        keep_alive=True,
        keep_alive_stop_event=stop_event,
    )

    result_holder: dict = {}

    def _runner():
        result_holder["stats"] = collect(cfg)

    thread = threading.Thread(target=_runner, daemon=True, name="collect-runner")
    thread.start()

    # Esperar a que el server esté arriba.
    deadline = time.time() + 4
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=0.2).read()
            break
        except Exception:
            time.sleep(0.05)

    # Después de duration_seconds=0.5, el server tiene que seguir respondiendo
    # y el config debe declarar ingestion_state=stopped.
    time.sleep(1.0)
    body = urllib.request.urlopen(f"http://127.0.0.1:{port}/api/config", timeout=2).read()
    config = json.loads(body)
    assert config["ingestion_state"] == "stopped", \
        "tras --duration el estado tiene que pasar a 'stopped'"
    assert config["keep_alive"] is True
    # collect_summary.json se ha persistido durante la transición.
    assert (tmp_path / "logs" / "collect_summary.json").exists()

    # El HTML debe reflejar el badge STOPPED.
    html_body = urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=2).read().decode()
    assert "pill stopped" in html_body
    assert "STOPPED" in html_body

    # Soltar el event para apagar el wait() y terminar el thread.
    stop_event.set()
    thread.join(timeout=5)
    assert not thread.is_alive(), "collect debe terminar tras stop_event.set()"
