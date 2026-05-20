"""Tests for the AutoIndexer.

Usamos `run_once()` (no el loop thread) para tener control determinista del
flujo. Verificamos que:
* scan se omite cuando un log no existe o es demasiado pequeño.
* scan + index produce los parquets esperados.
* errores en scan no rompen el ciclo (recogidos en last_error).
"""
from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from logs_reaper.auto_index import AutoIndexer


def _fixture_log() -> str:
    return (
        "2026-05-15T10:00:00Z INFO ThreadPoolExecutor-0 MainProcess "
        "test_module.py:42 test_function Test message line 1\n"
        "2026-05-15T10:00:01Z ERROR ThreadPoolExecutor-1 MainProcess "
        "test_module.py:55 broken_function Error happened\n"
        "2026-05-15T10:00:02Z INFO ThreadPoolExecutor-0 MainProcess "
        "test_module.py:43 test_function Test message line 2\n"
    ) * 50  # repetimos para que pase del umbral min_log_bytes


def test_auto_indexer_skips_empty_logs(tmp_path: Path):
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    (logs_dir / "svc-empty.log").write_text("")  # vacío
    indexer = AutoIndexer(
        logs_dir=logs_dir,
        scans_root=tmp_path / "out",
        registry_dir=tmp_path / "runs",
        services_provider=lambda: ["svc-empty"],
        interval=99.0,
        min_log_bytes=10,
    )
    result = indexer.run_once()
    assert result["indexed"] == []
    assert not (tmp_path / "runs" / "registry.parquet").exists()


def test_auto_indexer_scans_and_indexes(tmp_path: Path):
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    (logs_dir / "accounts.log").write_text(_fixture_log())
    indexer = AutoIndexer(
        logs_dir=logs_dir,
        scans_root=tmp_path / "out",
        registry_dir=tmp_path / "runs",
        services_provider=lambda: ["accounts"],
        interval=99.0,
        min_log_bytes=100,
    )
    result = indexer.run_once()
    assert "accounts" in result["indexed"]
    assert indexer.last_run_at is not None
    registry_path = tmp_path / "runs" / "registry.parquet"
    assert registry_path.exists()
    table = pq.read_table(registry_path)
    services = [r.get("service_name") for r in table.to_pylist()]
    assert "accounts" in services
    # scan produjo carpeta con run_id estable
    assert (tmp_path / "out" / "accounts" / "LIVE_accounts" / "run.json").exists()


def test_auto_indexer_run_id_is_stable_between_runs(tmp_path: Path):
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    log = logs_dir / "accounts.log"
    log.write_text(_fixture_log())
    indexer = AutoIndexer(
        logs_dir=logs_dir,
        scans_root=tmp_path / "out",
        registry_dir=tmp_path / "runs",
        services_provider=lambda: ["accounts"],
        interval=99.0,
        min_log_bytes=100,
    )
    indexer.run_once()
    # crece el log y vuelve a indexar
    log.write_text(_fixture_log() * 2)
    indexer.run_once()
    # sigue habiendo UNA carpeta de scan, no dos
    out_dirs = list((tmp_path / "out" / "accounts").iterdir())
    assert len(out_dirs) == 1
    assert out_dirs[0].name == "LIVE_accounts"


def test_auto_indexer_records_errors_without_dying(tmp_path: Path):
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    # log inválido + log válido en el mismo ciclo
    (logs_dir / "bad.log").write_text("x" * 200)  # parsea pero ok
    (logs_dir / "ok.log").write_text(_fixture_log())
    indexer = AutoIndexer(
        logs_dir=logs_dir,
        scans_root=tmp_path / "out",
        registry_dir=tmp_path / "runs",
        services_provider=lambda: ["bad", "ok"],
        interval=99.0,
        min_log_bytes=100,
    )
    result = indexer.run_once()
    # ok debe estar indexado aunque bad falle o sea raro
    assert "ok" in result["indexed"]
    assert indexer.runs_completed == 1


def test_auto_indexer_skips_when_log_size_unchanged(tmp_path: Path):
    """Si el log no creció desde la última pasada, no relanza scan."""
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    (logs_dir / "accounts.log").write_text(_fixture_log())
    indexer = AutoIndexer(
        logs_dir=logs_dir,
        scans_root=tmp_path / "out",
        registry_dir=tmp_path / "runs",
        services_provider=lambda: ["accounts"],
        interval=99.0,
        min_log_bytes=100,
    )
    first = indexer.run_once()
    second = indexer.run_once()
    assert "accounts" in first["indexed"]
    assert "accounts" not in second["indexed"], "log no creció, skip esperado"


def test_auto_indexer_persists_history_with_deltas(tmp_path: Path):
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    log = logs_dir / "accounts.log"
    log.write_text(_fixture_log())
    indexer = AutoIndexer(
        logs_dir=logs_dir,
        scans_root=tmp_path / "out",
        registry_dir=tmp_path / "runs",
        services_provider=lambda: ["accounts"],
        interval=99.0,
        min_log_bytes=100,
    )
    indexer.run_once()
    log.write_text(_fixture_log() * 3)
    indexer.run_once()

    history_path = tmp_path / "runs" / "auto_index_history.json"
    assert history_path.exists()
    data = json.loads(history_path.read_text())
    series = data["services"]["accounts"]
    assert len(series) == 2
    second = series[1]
    # delta_log_bytes > 0 entre las dos pasadas
    assert second["delta_log_bytes"] > 0
    # events de la 2a pasada >= la 1a
    assert second["events"] >= series[0]["events"]


def test_auto_indexer_min_green_runs_one_builds_baseline_on_first_pass(tmp_path: Path):
    """Con min_green_runs=1, el primer scan ya genera filas de baseline."""
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    (logs_dir / "accounts.log").write_text(_fixture_log())
    indexer = AutoIndexer(
        logs_dir=logs_dir,
        scans_root=tmp_path / "out",
        registry_dir=tmp_path / "runs",
        services_provider=lambda: ["accounts"],
        interval=99.0,
        min_log_bytes=100,
        min_green_runs=1,
    )
    indexer.run_once()
    import pyarrow.parquet as _pq
    baseline = _pq.read_table(tmp_path / "runs" / "baseline.parquet").to_pylist()
    services_in_baseline = {r.get("service_name") for r in baseline}
    assert "accounts" in services_in_baseline, "boot baseline no creado con min_green_runs=1"
