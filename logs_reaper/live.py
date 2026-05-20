"""`logs-reaper live` — supervisor que une `collect` + dashboard Streamlit.

Diseño:

* `collect` corre como subprocess en background; lo monitorizamos por
  PID y propagamos SIGTERM/SIGINT cuando el supervisor recibe la señal.
* El subprocess persiste `stats_snapshot.json` cada N segundos.
* Streamlit corre en foreground en el puerto público, sirviendo el
  dashboard completo (incl. la tab "Live Ingest" que lee el snapshot).
* Cuando Streamlit termina (señal externa al supervisor), matamos el
  subprocess de `collect` también.
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def run_live(
    *,
    services: str,
    out_dir: Path,
    duration: float | None,
    streamlit_port: int,
    streamlit_host: str = "0.0.0.0",
    stats_port: int | None = None,
    snapshot_interval: float = 1.0,
    keep_alive: bool = True,
    registry_dir: Path | None = None,
    baselines_dir: Path | None = None,
    auto_index_interval: float = 5.0,
    auto_index_min_green_runs: int = 1,
    initial_tail: int = 2000,
    discover_interval: float = 10.0,
    extra_collect_args: list[str] | None = None,
) -> int:
    """Run collect + streamlit dashboard + auto-indexer until signaled."""
    out_dir = Path(out_dir)
    logs_dir = out_dir / "logs" / "live"
    scans_root = out_dir / "out"
    if registry_dir is None:
        registry_dir = out_dir / "runs"
    registry_dir = Path(registry_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)
    scans_root.mkdir(parents=True, exist_ok=True)
    registry_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = logs_dir / "stats_snapshot.json"

    collect_cmd = [
        "logs-reaper", "collect",
        "--services", services,
        "--out", str(logs_dir),
        "--snapshot-path", str(snapshot_path),
        "--snapshot-interval", str(snapshot_interval),
        "--tail", str(int(initial_tail)),
    ]
    if duration is not None:
        collect_cmd += ["--duration", str(duration)]
    if stats_port is not None:
        collect_cmd += ["--stats-port", str(stats_port)]
    if keep_alive:
        collect_cmd += ["--keep-alive"]
    if discover_interval and discover_interval > 0:
        collect_cmd += ["--discover-interval", str(discover_interval)]
    if extra_collect_args:
        collect_cmd += list(extra_collect_args)

    print(f"[live] starting collector: {' '.join(collect_cmd)}", flush=True)
    collector = subprocess.Popen(collect_cmd, stdout=sys.stdout, stderr=sys.stderr)

    # Auto-indexer en thread aparte: scan+index periódico para que las tabs
    # históricas del dashboard se vayan poblando con los logs ingeridos.
    indexer = None
    if auto_index_interval and auto_index_interval > 0:
        from .auto_index import AutoIndexer
        from .collect import resolve_services as _resolve

        def _services_provider() -> list[str]:
            try:
                return _resolve(services)
            except Exception:
                return []

        indexer = AutoIndexer(
            logs_dir=logs_dir,
            scans_root=scans_root,
            registry_dir=registry_dir,
            baselines_dir=baselines_dir,
            services_provider=_services_provider,
            interval=auto_index_interval,
            min_green_runs=auto_index_min_green_runs,
        )
        indexer.start()
        print(
            f"[live] auto-index every {auto_index_interval:.1f}s "
            f"(min_green_runs={auto_index_min_green_runs}, "
            f"scans -> {scans_root}, registry -> {registry_dir})",
            flush=True,
        )

    # Dashboard env: snapshot path explícito + registry si se da.
    env = os.environ.copy()
    env["LOGS_REAPER_SNAPSHOT"] = str(snapshot_path)
    env["LOGS_REAPER_REGISTRY"] = str(Path(registry_dir))

    dashboard_script = Path(__file__).resolve().parent / "dashboard.py"
    streamlit_cmd = [
        "python3", "-m", "streamlit", "run", str(dashboard_script),
        "--server.port", str(streamlit_port),
        "--server.address", streamlit_host,
        "--server.headless", "true",
        "--browser.gatherUsageStats", "false",
        "--server.fileWatcherType", "none",
    ]
    print(f"[live] starting dashboard on http://{streamlit_host}:{streamlit_port}/", flush=True)
    streamlit = subprocess.Popen(streamlit_cmd, stdout=sys.stdout, stderr=sys.stderr, env=env)

    def _shutdown(_signum=None, _frame=None):
        print("[live] shutting down child processes...", flush=True)
        if indexer is not None:
            indexer.stop()
        for proc, name in ((streamlit, "streamlit"), (collector, "collector")):
            if proc.poll() is None:
                try:
                    proc.terminate()
                except ProcessLookupError:
                    pass
        for proc, name in ((streamlit, "streamlit"), (collector, "collector")):
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass

    try:
        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT, _shutdown)
    except ValueError:
        pass

    try:
        # Wait until either child exits.
        while True:
            time.sleep(1)
            if streamlit.poll() is not None:
                print("[live] streamlit dashboard exited", flush=True)
                break
            if collector.poll() is not None and not keep_alive:
                print("[live] collector finished; keep_alive=False so we exit too", flush=True)
                break
    finally:
        _shutdown()

    return streamlit.returncode or 0
