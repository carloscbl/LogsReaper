"""Stats en tiempo real para `logs-reaper collect` / `ci-run`.

Dos piezas:

* :class:`StatsTracker` — ring-buffer con timestamps + bytes/lines por
  servicio. Provee `rate(window)` que devuelve bytes/s y lines/s para
  ventanas rolling (5s/30s/60s) sin asignaciones extras por update.

* :class:`StatsServer` — HTTP server (stdlib) en background thread que sirve:

    GET /              — HTML con auto-refresh (debug-friendly)
    GET /api/stats     — JSON con totales + rates por servicio
    GET /api/config    — JSON con la configuración usada (CollectConfig)
    GET /api/health    — 200 si el tracker está vivo

Sin dependencias externas. El server NO bloquea el event loop de asyncio
porque corre en thread aparte (ThreadingHTTPServer + handler stdlib).
"""
from __future__ import annotations

import html
import json
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


# Ventanas rolling expuestas en la API. Mantenemos el ring buffer
# dimensionado para la más larga.
_DEFAULT_WINDOWS = (5.0, 30.0, 60.0)
_MAX_WINDOW = max(_DEFAULT_WINDOWS)


@dataclass
class _ServiceState:
    bytes_total: int = 0
    lines_total: int = 0
    last_event_ts: float = 0.0          # wall-clock cuando llegó el último chunk
    last_log_timestamp_iso: str | None = None  # primera línea ISO en el chunk
    samples: deque[tuple[float, int, int]] = field(default_factory=lambda: deque(maxlen=4096))


class StatsTracker:
    """Almacena throughput por servicio con ventanas rolling de bajo coste.

    Thread-safe: `add_chunk` puede llamarse desde el event loop principal y
    `snapshot` desde el thread del HTTP server.
    """

    def __init__(self, *, started_at: float | None = None,
                 windows: tuple[float, ...] = _DEFAULT_WINDOWS) -> None:
        self.started_at = started_at if started_at is not None else time.time()
        self.windows = windows
        self._lock = threading.Lock()
        self._services: dict[str, _ServiceState] = {}
        self._config: dict[str, Any] = {}

    def set_config(self, config: dict[str, Any]) -> None:
        with self._lock:
            self._config = dict(config)

    def register_service(self, service: str) -> None:
        with self._lock:
            self._services.setdefault(service, _ServiceState())

    def add_chunk(self, service: str, *, chunk_bytes: int, chunk_lines: int,
                  log_ts_iso: str | None = None, now: float | None = None) -> None:
        now = now if now is not None else time.time()
        with self._lock:
            state = self._services.setdefault(service, _ServiceState())
            state.bytes_total += chunk_bytes
            state.lines_total += chunk_lines
            state.last_event_ts = now
            if log_ts_iso:
                state.last_log_timestamp_iso = log_ts_iso
            state.samples.append((now, chunk_bytes, chunk_lines))
            # Prune samples más antiguas que la mayor ventana — barato porque
            # el deque tiene maxlen ya. Hacemos una poda más explícita por TTL:
            cutoff = now - _MAX_WINDOW * 2
            while state.samples and state.samples[0][0] < cutoff:
                state.samples.popleft()

    def _rate_for(self, state: _ServiceState, window: float, now: float) -> tuple[float, float]:
        if not state.samples:
            return 0.0, 0.0
        cutoff = now - window
        b = ln = 0
        for ts, sb, sl in reversed(state.samples):
            if ts < cutoff:
                break
            b += sb
            ln += sl
        return b / window, ln / window

    def snapshot(self) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            services_out: dict[str, dict[str, Any]] = {}
            total_b = 0
            total_l = 0
            for svc, state in self._services.items():
                total_b += state.bytes_total
                total_l += state.lines_total
                rates = {}
                for w in self.windows:
                    bps, lps = self._rate_for(state, w, now)
                    rates[f"{int(w)}s"] = {"bytes_per_sec": bps, "lines_per_sec": lps}
                idle_seconds = (now - state.last_event_ts) if state.last_event_ts else None
                services_out[svc] = {
                    "bytes_total": state.bytes_total,
                    "lines_total": state.lines_total,
                    "rates": rates,
                    "last_event_ts": state.last_event_ts or None,
                    "idle_seconds": idle_seconds,
                    "last_log_timestamp_iso": state.last_log_timestamp_iso,
                }
            return {
                "now": now,
                "uptime_seconds": now - self.started_at,
                "totals": {
                    "bytes_total": total_b,
                    "lines_total": total_l,
                    "services": len(self._services),
                },
                "services": services_out,
            }

    def config_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._config)


# ----------------------------------------------------------------------------
# HTTP server
# ----------------------------------------------------------------------------

_HTML_TEMPLATE = """<!doctype html>
<html><head><meta charset='utf-8'/>
<title>LogsReaper — runtime stats</title>
<meta http-equiv='refresh' content='2'/>
<style>
  body { font-family: -apple-system, Segoe UI, Roboto, sans-serif;
         margin: 24px; color: #222; max-width: 1180px; }
  h1 { margin: 0 0 4px 0; }
  .sub { color: #666; font-size: 13px; }
  .pill { display: inline-block; padding: 2px 10px; border-radius: 999px;
          color: white; font-size: 12px; font-weight: 600; margin-left: 8px; }
  .pill.running { background: #198754; }
  .pill.stopped { background: #6c757d; }
  table { border-collapse: collapse; width: 100%; margin-top: 12px; font-size: 13px; }
  th, td { padding: 6px 10px; border-bottom: 1px solid #eee; text-align: right; }
  th:first-child, td:first-child { text-align: left; }
  th { background: #fafafa; }
  .stale { color: #c00; font-weight: 600; }
  .ok { color: #198754; }
  pre { background: #f4f4f4; padding: 10px; border-radius: 6px; font-size: 12px; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
  @media (max-width: 800px) { .grid { grid-template-columns: 1fr; } }
</style></head><body>
<h1>LogsReaper · runtime stats <span class='pill __STATE__'>__STATE_LABEL__</span></h1>
<div class='sub'>uptime: __UPTIME__s · refresh every 2s · port __PORT__</div>

<h2>Throughput</h2>
<table>
<thead><tr>
<th>service</th>
<th>bytes total</th><th>lines total</th>
<th>5s B/s</th><th>5s L/s</th>
<th>30s B/s</th><th>30s L/s</th>
<th>60s B/s</th><th>60s L/s</th>
<th>idle (s)</th>
</tr></thead><tbody>
__ROWS__
</tbody></table>

<div class='grid' style='margin-top:24px'>
<div>
  <h2>Totals</h2>
  <pre>__TOTALS__</pre>
</div>
<div>
  <h2>Configuration</h2>
  <pre>__CONFIG__</pre>
</div>
</div>

<h2>Raw snapshot (JSON)</h2>
<pre>__RAW__</pre>
</body></html>
"""


def _fmt_bytes(n: float) -> str:
    n = float(n)
    if n < 1024:
        return f"{n:.0f}"
    if n < 1024 * 1024:
        return f"{n/1024:.1f}K"
    return f"{n/(1024*1024):.2f}M"


def _render_rows(snapshot: dict[str, Any]) -> str:
    rows: list[str] = []
    for svc in sorted(snapshot["services"].keys()):
        s = snapshot["services"][svc]
        idle = s.get("idle_seconds")
        idle_str = f"{idle:.1f}" if idle is not None else "—"
        idle_class = "stale" if (idle or 0) > 5 else "ok"
        rates = s["rates"]
        rows.append(
            "<tr>"
            f"<td>{html.escape(svc)}</td>"
            f"<td>{_fmt_bytes(s['bytes_total'])}</td>"
            f"<td>{s['lines_total']}</td>"
            f"<td>{_fmt_bytes(rates['5s']['bytes_per_sec'])}</td>"
            f"<td>{rates['5s']['lines_per_sec']:.1f}</td>"
            f"<td>{_fmt_bytes(rates['30s']['bytes_per_sec'])}</td>"
            f"<td>{rates['30s']['lines_per_sec']:.1f}</td>"
            f"<td>{_fmt_bytes(rates['60s']['bytes_per_sec'])}</td>"
            f"<td>{rates['60s']['lines_per_sec']:.1f}</td>"
            f"<td class='{idle_class}'>{idle_str}</td>"
            "</tr>"
        )
    return "\n".join(rows) or "<tr><td colspan='10' style='color:#888'>no services yet</td></tr>"


class _StatsHandler(BaseHTTPRequestHandler):
    tracker: StatsTracker  # type: ignore[assignment]
    server_port: int       # set by StatsServer

    def log_message(self, *a, **kw):  # silenciar acceso log por defecto
        return

    def _send_json(self, code: int, body: dict[str, Any]) -> None:
        payload = json.dumps(body, indent=2, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):  # noqa: N802 - http handler signature
        if self.path.startswith("/api/health"):
            self._send_json(200, {"status": "ok"})
            return
        if self.path.startswith("/api/stats"):
            self._send_json(200, self.tracker.snapshot())
            return
        if self.path.startswith("/api/config"):
            self._send_json(200, self.tracker.config_snapshot())
            return
        if self.path in ("/", "/index.html"):
            snapshot = self.tracker.snapshot()
            config = self.tracker.config_snapshot()
            state = config.get("ingestion_state", "running")
            body = (
                _HTML_TEMPLATE
                .replace("__UPTIME__", f"{snapshot['uptime_seconds']:.0f}")
                .replace("__PORT__", str(self.server_port))
                .replace("__STATE__", state if state in ("running", "stopped") else "running")
                .replace("__STATE_LABEL__", state.upper())
                .replace("__ROWS__", _render_rows(snapshot))
                .replace("__TOTALS__", html.escape(json.dumps(snapshot["totals"], indent=2)))
                .replace("__CONFIG__", html.escape(json.dumps(config, indent=2, default=str)))
                .replace("__RAW__", html.escape(json.dumps(snapshot, indent=2, default=str)))
            )
            payload = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)
            return
        self.send_response(404)
        self.end_headers()


class SnapshotPersister:
    """Vuelca el tracker a un fichero JSON en disco cada `interval` segundos.

    Sirve como bridge entre el proceso de `collect` (que tiene el tracker
    en memoria) y el dashboard Streamlit (que corre en otro proceso y
    necesita leer el snapshot). Escritura atómica via `.tmp` + replace.
    """

    def __init__(self, tracker: StatsTracker, *, path: Path, interval: float = 1.0) -> None:
        self.tracker = tracker
        self.path = Path(path)
        self.interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _write_once(self) -> None:
        snapshot = self.tracker.snapshot()
        snapshot["config"] = self.tracker.config_snapshot()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(snapshot, indent=2, default=str))
        tmp.replace(self.path)

    def _loop(self) -> None:
        # Primer write inmediato para que el dashboard tenga algo al conectar.
        try:
            self._write_once()
        except Exception:
            pass
        while not self._stop.wait(self.interval):
            try:
                self._write_once()
            except Exception:
                # No queremos que un fallo de I/O (e.g. rotación de logs) tire el thread.
                continue

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="logs-reaper-snapshot-persister",
        )
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        # Flush final
        try:
            self._write_once()
        except Exception:
            pass
        self._thread = None


class StatsServer:
    """Wrapper sobre ThreadingHTTPServer que corre en thread daemon."""

    def __init__(self, tracker: StatsTracker, *, host: str = "0.0.0.0", port: int = 9100) -> None:
        self.tracker = tracker
        self.host = host
        self.port = port
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._server is not None:
            return
        tracker = self.tracker
        port = self.port

        class _Handler(_StatsHandler):
            pass
        _Handler.tracker = tracker  # type: ignore[assignment]
        _Handler.server_port = port

        self._server = ThreadingHTTPServer((self.host, self.port), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True, name="logs-reaper-stats")
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        if self._server is None:
            return
        try:
            self._server.shutdown()
            self._server.server_close()
        except Exception:
            pass
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._server = None
        self._thread = None

    @property
    def url(self) -> str:
        host = "localhost" if self.host in ("0.0.0.0", "::") else self.host
        return f"http://{host}:{self.port}/"
