"""Tests for the runtime stats tracker + HTTP server."""
from __future__ import annotations

import json
import socket
import time
import urllib.request

import pytest

from logs_reaper.runtime_stats import StatsServer, StatsTracker


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---------- StatsTracker -----------------------------------------------------

def test_tracker_registers_and_accumulates():
    t = StatsTracker(started_at=1000.0)
    t.add_chunk("accounts", chunk_bytes=100, chunk_lines=5, now=1001.0)
    t.add_chunk("accounts", chunk_bytes=50, chunk_lines=2, now=1002.0)
    snap = t.snapshot()
    acc = snap["services"]["accounts"]
    assert acc["bytes_total"] == 150
    assert acc["lines_total"] == 7
    assert snap["totals"]["bytes_total"] == 150


def test_tracker_rolling_rates_within_window():
    t = StatsTracker(started_at=time.time() - 30, windows=(5.0, 30.0))
    now = time.time()
    # 10 chunks de 100 B cada 0.5s en los últimos 5s
    for i in range(10):
        t.add_chunk("svc", chunk_bytes=100, chunk_lines=1, now=now - 0.5 * i)
    snap = t.snapshot()
    rates = snap["services"]["svc"]["rates"]
    # 1000 bytes en ~5s -> ~200 B/s en ventana 5s. Tolerancia ±50%.
    assert 100 <= rates["5s"]["bytes_per_sec"] <= 300


def test_tracker_idle_seconds_grows():
    t = StatsTracker()
    t.add_chunk("svc", chunk_bytes=1, chunk_lines=1, now=time.time() - 7.0)
    snap = t.snapshot()
    idle = snap["services"]["svc"]["idle_seconds"]
    assert idle is not None and idle > 5


def test_tracker_config_round_trip():
    t = StatsTracker()
    t.set_config({"services": ["a", "b"], "port": 9100})
    assert t.config_snapshot() == {"services": ["a", "b"], "port": 9100}


# ---------- StatsServer (real HTTP) -----------------------------------------

@pytest.fixture()
def server():
    tracker = StatsTracker()
    tracker.set_config({"services": ["a"], "port": 9100})
    tracker.add_chunk("a", chunk_bytes=42, chunk_lines=3)
    port = _free_port()
    srv = StatsServer(tracker, host="127.0.0.1", port=port)
    srv.start()
    # micro-espera para asegurar bind
    deadline = time.time() + 2
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=0.2).read()
            break
        except Exception:
            time.sleep(0.05)
    yield srv, tracker, port
    srv.stop()


def test_http_health(server):
    _, _, port = server
    body = urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=1).read()
    assert json.loads(body) == {"status": "ok"}


def test_http_stats_endpoint(server):
    _, _, port = server
    body = urllib.request.urlopen(f"http://127.0.0.1:{port}/api/stats", timeout=1).read()
    payload = json.loads(body)
    assert "services" in payload and "a" in payload["services"]
    assert payload["services"]["a"]["bytes_total"] == 42


def test_http_config_endpoint(server):
    _, _, port = server
    body = urllib.request.urlopen(f"http://127.0.0.1:{port}/api/config", timeout=1).read()
    payload = json.loads(body)
    assert payload == {"services": ["a"], "port": 9100}


def test_http_root_returns_html(server):
    _, _, port = server
    body = urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1).read()
    text = body.decode()
    assert "<title>LogsReaper" in text
    assert "Throughput" in text
    # auto-refresh metadata presente
    assert "http-equiv='refresh'" in text


def test_server_stop_is_idempotent(server):
    srv, _, _ = server
    srv.stop()
    srv.stop()
