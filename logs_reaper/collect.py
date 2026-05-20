"""docker logs -f streaming en paralelo via asyncio.

Diseñado para correr en un contenedor con el socket de docker montado:

    docker run -v /var/run/docker.sock:/var/run/docker.sock:ro logs-reaper \
        collect --services accounts,gateway-isp --duration 60 --out runs/<id>/

Para cada servicio levanta un subprocess `docker logs -f --tail=0
sm-<svc>-1` y vuelca la salida a `runs/<id>/<svc>.log`. Termina por:

* `--duration N` (segundos)
* SIGINT/SIGTERM externo
* todos los servicios desaparecidos

El comportamiento de "auto-detect" mira `docker ps --format {{.Names}}` y
toma todo lo que matchee `<prefix>-*-1` (default prefix `sm`).
"""
from __future__ import annotations

import asyncio
import json
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from .runtime_stats import SnapshotPersister, StatsServer, StatsTracker


@dataclass
class CollectConfig:
    services: list[str]
    out_dir: Path
    duration_seconds: float | None = None
    container_prefix: str = "sm"
    container_suffix: str = "-1"
    tail_initial: int = 0  # docker logs --tail
    metadata: dict[str, str] = field(default_factory=dict)
    stats_port: int | None = None
    stats_host: str = "0.0.0.0"
    snapshot_path: Path | None = None        # si se da, vuelca snapshot.json cada N segundos
    snapshot_interval: float = 1.0
    keep_alive: bool = False  # tras --duration, dejar el stats server vivo hasta SIGTERM
    keep_alive_stop_event: "threading.Event | None" = None  # hook de tests para parar el wait()
    # Si > 0 y cfg.services se resolvió desde "all", relanza auto_detect cada
    # N segundos y spawnea streams para containers sm-*-1 nuevos. Sin ello,
    # logs-reaper sólo ve los servicios que existían al arrancar.
    discover_interval: float = 0.0


@dataclass
class CollectStats:
    started_at: float
    finished_at: float | None = None
    bytes_per_service: dict[str, int] = field(default_factory=dict)
    lines_per_service: dict[str, int] = field(default_factory=dict)
    exit_codes: dict[str, int | None] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": (self.finished_at - self.started_at) if self.finished_at else None,
            "bytes_per_service": self.bytes_per_service,
            "lines_per_service": self.lines_per_service,
            "exit_codes": self.exit_codes,
        }


_SELF_SERVICE_NAME = "logs-reaper"


def auto_detect_services(prefix: str = "sm", suffix: str = "-1") -> list[str]:
    """Llama `docker ps` y devuelve los nombres lógicos <svc> que matcheen.

    Excluye al propio container (`logs-reaper`) — no tiene sentido que se
    monitorice a sí mismo cuando corre dentro del proyecto compose sm.
    """
    try:
        proc = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True, text=True, check=True, timeout=10,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        return []
    names: list[str] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line.startswith(f"{prefix}-"):
            continue
        if not line.endswith(suffix):
            continue
        svc = line[len(prefix) + 1 : -len(suffix)] if suffix else line[len(prefix) + 1 :]
        if svc and svc != _SELF_SERVICE_NAME:
            names.append(svc)
    return sorted(set(names))


def resolve_services(spec: str | Iterable[str]) -> list[str]:
    """`'all'` -> auto-detect. `'a,b,c'` -> ['a','b','c']. iterable -> list."""
    if isinstance(spec, str):
        if spec.strip().lower() == "all":
            return auto_detect_services()
        return [s.strip() for s in spec.split(",") if s.strip()]
    return [str(s).strip() for s in spec if str(s).strip()]


def _container_name(cfg: CollectConfig, svc: str) -> str:
    return f"{cfg.container_prefix}-{svc}{cfg.container_suffix}"


async def _stream_one(
    cfg: CollectConfig, svc: str, stats: CollectStats,
    procs: dict[str, "asyncio.subprocess.Process"],
    tracker: StatsTracker | None = None,
) -> None:
    container = _container_name(cfg, svc)
    out_path = cfg.out_dir / f"{svc}.log"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    proc = await asyncio.create_subprocess_exec(
        "docker", "logs", "-f", "--tail", str(cfg.tail_initial), container,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    procs[svc] = proc
    if tracker is not None:
        tracker.register_service(svc)
    bytes_seen = 0
    lines_seen = 0
    try:
        # buffering=0 + flush explícito tras cada chunk: garantiza que el
        # AutoIndexer (otro proceso, en otro thread) vea los bytes en disco
        # inmediatamente. Sin esto, servicios poco verbosos quedan
        # invisibles para el scan porque su .log se mantiene en buffer.
        with out_path.open("ab", buffering=0) as fh:
            assert proc.stdout is not None
            while True:
                chunk = await proc.stdout.read(64 * 1024)
                if not chunk:
                    break
                fh.write(chunk)
                bytes_seen += len(chunk)
                chunk_lines = chunk.count(b"\n")
                lines_seen += chunk_lines
                stats.bytes_per_service[svc] = bytes_seen
                stats.lines_per_service[svc] = lines_seen
                if tracker is not None:
                    tracker.add_chunk(
                        svc, chunk_bytes=len(chunk), chunk_lines=chunk_lines,
                    )
    except asyncio.CancelledError:
        # No reraise: el caller nos ha pedido parar y necesitamos hacer
        # cleanup del subprocess de forma ordenada.
        pass
    finally:
        if proc.returncode is None:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                try:
                    await proc.wait()
                except Exception:  # pragma: no cover - defensa
                    pass
        stats.exit_codes[svc] = proc.returncode


async def _run_collection(
    cfg: CollectConfig, stats: CollectStats, tracker: StatsTracker | None = None,
) -> None:
    procs: dict[str, asyncio.subprocess.Process] = {}
    tasks: dict[str, asyncio.Task] = {
        svc: asyncio.create_task(_stream_one(cfg, svc, stats, procs, tracker))
        for svc in cfg.services
    }

    shutdown = asyncio.Event()

    def _signal_terminate() -> None:
        shutdown.set()
        for proc in procs.values():
            if proc.returncode is None:
                try:
                    proc.terminate()
                except ProcessLookupError:
                    pass

    async def _duration_stopper() -> None:
        if cfg.duration_seconds is None:
            return
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=cfg.duration_seconds)
        except asyncio.TimeoutError:
            _signal_terminate()

    async def _discover_loop() -> None:
        """Cada `discover_interval`s consulta `docker ps` y spawnea streams
        para containers `sm-*-1` que han aparecido tras arrancar. Es la
        forma de capturar servicios que se levantan después de logs-reaper
        (típico en run_groups: cada grupo trae un set distinto)."""
        if cfg.discover_interval <= 0:
            return
        while True:
            await asyncio.sleep(cfg.discover_interval)
            try:
                found = auto_detect_services(cfg.container_prefix, cfg.container_suffix)
            except Exception:
                continue
            for svc in found:
                if svc in tasks:
                    continue
                cfg.services.append(svc)
                if tracker is not None:
                    tracker.register_service(svc)
                tasks[svc] = asyncio.create_task(
                    _stream_one(cfg, svc, stats, procs, tracker)
                )
                print(f"[collect] new service discovered: {svc}", flush=True)

    stopper = asyncio.create_task(_duration_stopper())
    discoverer = asyncio.create_task(_discover_loop())

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_terminate)
        except (NotImplementedError, RuntimeError):
            pass

    try:
        # Bucle robusto: el discoverer puede inyectar tareas mid-flight y
        # podemos arrancar sin ninguna (--services all sin containers todavía).
        # Salimos sólo cuando recibimos SIGTERM/SIGINT o expira --duration.
        # Sin shutdown explícito y tasks vacío, esperamos al discoverer.
        while not shutdown.is_set():
            pending = [t for t in tasks.values() if not t.done()]
            if pending:
                done, _ = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED,
                    timeout=cfg.discover_interval or 1.0,
                )
            else:
                # Nada que monitorizar todavía: dormimos un poco y volvemos
                # a comprobar (el discoverer está añadiendo en paralelo).
                try:
                    await asyncio.wait_for(shutdown.wait(), timeout=cfg.discover_interval or 1.0)
                except asyncio.TimeoutError:
                    pass
    finally:
        for t in (stopper, discoverer):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass


def collect(cfg: CollectConfig) -> CollectStats:
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    stats = CollectStats(started_at=time.time())

    tracker: StatsTracker | None = None
    server: StatsServer | None = None
    persister: SnapshotPersister | None = None

    def _build_config_snapshot(state: str) -> dict:
        return {
            "services": cfg.services,
            "out_dir": str(cfg.out_dir),
            "duration_seconds": cfg.duration_seconds,
            "container_prefix": cfg.container_prefix,
            "container_suffix": cfg.container_suffix,
            "tail_initial": cfg.tail_initial,
            "stats_port": cfg.stats_port,
            "containers": {svc: _container_name(cfg, svc) for svc in cfg.services},
            "metadata": cfg.metadata,
            "keep_alive": cfg.keep_alive,
            "ingestion_state": state,
            "ingestion_started_at": stats.started_at,
            "ingestion_stopped_at": stats.finished_at,
        }

    # El tracker se crea siempre que necesitemos exponer estado en algún sitio:
    # vía HTTP (stats_port), vía fichero (snapshot_path), o ambos.
    if cfg.stats_port or cfg.snapshot_path:
        tracker = StatsTracker(started_at=stats.started_at)
        tracker.set_config(_build_config_snapshot("running"))
        for svc in cfg.services:
            tracker.register_service(svc)

    if cfg.stats_port and tracker is not None:
        server = StatsServer(tracker, host=cfg.stats_host, port=cfg.stats_port)
        server.start()
        print(f"[collect] stats endpoint: {server.url}", flush=True)

    if cfg.snapshot_path and tracker is not None:
        persister = SnapshotPersister(
            tracker, path=Path(cfg.snapshot_path), interval=cfg.snapshot_interval,
        )
        persister.start()
        print(f"[collect] snapshot: {cfg.snapshot_path} (every {cfg.snapshot_interval:.1f}s)", flush=True)

    try:
        asyncio.run(_run_collection(cfg, stats, tracker))
    finally:
        stats.finished_at = time.time()
        (cfg.out_dir / "collect_summary.json").write_text(
            json.dumps({"services": cfg.services, "metadata": cfg.metadata, **stats.to_dict()}, indent=2)
        )

        # Keep the HTTP stats server alive after ingestion ends, so the
        # dashboard remains reachable until the operator stops the process.
        # Only do this when both a stats_port and keep_alive are requested;
        # otherwise the function returns and the caller decides.
        if server is not None and cfg.keep_alive:
            if tracker is not None:
                tracker.set_config(_build_config_snapshot("stopped"))
            print(
                f"[collect] ingestion stopped after "
                f"{stats.finished_at - stats.started_at:.1f}s — "
                f"stats dashboard still live at {server.url}",
                flush=True,
            )
            print("[collect] send SIGTERM/SIGINT to exit (e.g. `docker stop <container>`)", flush=True)
            shutdown = cfg.keep_alive_stop_event or threading.Event()

            def _on_signal(_signum, _frame):
                shutdown.set()

            # signal.signal sólo se puede instalar desde el main thread del
            # proceso. En tests `collect` puede correr en un thread aparte
            # — entonces confiamos en el `Event` inyectado.
            try:
                signal.signal(signal.SIGINT, _on_signal)
                signal.signal(signal.SIGTERM, _on_signal)
            except ValueError:
                pass
            try:
                shutdown.wait()
            except KeyboardInterrupt:
                pass
            print("[collect] shutdown signal received, stopping stats server.", flush=True)

        if server is not None:
            server.stop()
        if persister is not None:
            persister.stop()

    return stats
