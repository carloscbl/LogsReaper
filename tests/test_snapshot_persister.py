"""Tests for SnapshotPersister: thread-based JSON dump of the StatsTracker."""
from __future__ import annotations

import json
import time
from pathlib import Path

from logs_reaper.runtime_stats import SnapshotPersister, StatsTracker


def test_persister_writes_initial_snapshot(tmp_path: Path):
    t = StatsTracker(started_at=time.time())
    t.set_config({"services": ["a"]})
    t.add_chunk("a", chunk_bytes=10, chunk_lines=1)
    path = tmp_path / "snapshot.json"
    p = SnapshotPersister(t, path=path, interval=0.05)
    p.start()
    # esperar al primer write
    deadline = time.time() + 2
    while time.time() < deadline:
        if path.exists():
            break
        time.sleep(0.02)
    p.stop()
    assert path.exists()
    data = json.loads(path.read_text())
    assert "services" in data and "a" in data["services"]
    assert data["services"]["a"]["bytes_total"] == 10
    assert data["config"]["services"] == ["a"]


def test_persister_refreshes_periodically(tmp_path: Path):
    t = StatsTracker()
    path = tmp_path / "snap.json"
    p = SnapshotPersister(t, path=path, interval=0.05)
    p.start()
    time.sleep(0.15)
    t.add_chunk("x", chunk_bytes=50, chunk_lines=2)
    time.sleep(0.20)
    p.stop()
    data = json.loads(path.read_text())
    # Tras el segundo write, x debe estar reflejado.
    assert data["services"]["x"]["bytes_total"] == 50


def test_persister_stop_is_idempotent(tmp_path: Path):
    t = StatsTracker()
    p = SnapshotPersister(t, path=tmp_path / "x.json", interval=0.05)
    p.start()
    p.stop()
    p.stop()  # no debería crashear


def test_persister_atomic_write_replaces_tempfile(tmp_path: Path):
    t = StatsTracker()
    path = tmp_path / "atomic.json"
    p = SnapshotPersister(t, path=path, interval=0.05)
    p.start()
    time.sleep(0.15)
    p.stop()
    # No .tmp residual.
    leftovers = list(tmp_path.glob("*.tmp"))
    assert leftovers == []
