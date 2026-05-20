"""Desktop notifications for LogsReaper (libnotify / D-Bus).

El contenedor `logs-reaper` se lanza en local con el bus de sesión del usuario
montado (ver ``scripts/start_logsreaper.sh``) y ``libnotify-bin`` instalado en
la imagen. Esto permite emitir notificaciones GNOME/KDE/etc. directamente
desde dentro del contenedor cuando aparece algo inesperado: errores de código
nuevos durante ``live`` o un delta ``risky``/``unsafe`` en ``ci-run``.

Diseño:

* Una única clase ``Notifier`` reutilizable; ``get_default()`` la cachea.
* ``notify(title, body, ...)`` con ``cooldown_key`` para evitar spam (el
  auto-indexer corre cada 5s; no queremos 12 popups/min si el error persiste).
* Si ``notify-send`` no existe o ``DBUS_SESSION_BUS_ADDRESS`` no está, hace
  fallback a stderr para que CI/headless siga viendo el aviso.
* El path al reporte se convierte de path-de-contenedor a path-de-host usando
  ``LOGS_REAPER_OUT`` (montaje del contenedor) y
  ``LOGSREAPER_OUT_HOST_PATH`` (path en host). Se incluye como ``file://`` en
  el body — GNOME lo renderiza clicable.

Env vars que reconocemos:

* ``LOGSREAPER_NOTIFY``     — ``0``/``off``/``false`` para desactivar.
* ``LOGSREAPER_OUT_HOST_PATH`` — host counterpart de ``LOGS_REAPER_OUT``.
* ``LOGSREAPER_DASHBOARD_URL`` — URL del dashboard live para enlazar.
* ``LOGSREAPER_APP_NAME``   — app name visible en el popup (default ``LogsReaper``).
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Iterable


_TRUE_STRS = {"1", "true", "yes", "on"}
_FALSE_STRS = {"0", "false", "no", "off", ""}


def _env_flag(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    val = raw.strip().lower()
    if val in _TRUE_STRS:
        return True
    if val in _FALSE_STRS:
        return False
    return default


class Notifier:
    """Thin wrapper alrededor de ``notify-send`` con rate-limit por key."""

    def __init__(
        self,
        *,
        app_name: str = "LogsReaper",
        enabled: bool | None = None,
        binary: str | None = None,
        default_cooldown: float = 60.0,
    ) -> None:
        self.app_name = os.environ.get("LOGSREAPER_APP_NAME", app_name)
        self.binary = binary or shutil.which("notify-send") or ""
        self.default_cooldown = default_cooldown
        self._last_at: dict[str, float] = {}
        self._lock = threading.Lock()
        if enabled is None:
            enabled = _env_flag("LOGSREAPER_NOTIFY", default=True)
        self.enabled = bool(enabled and self._desktop_available())

    def _desktop_available(self) -> bool:
        if not self.binary:
            return False
        # Si no hay bus, notify-send fallaría silenciosamente. Mejor no
        # marcar como disponible y caer al stderr fallback.
        return bool(os.environ.get("DBUS_SESSION_BUS_ADDRESS"))

    @staticmethod
    def container_to_host_path(path: str | os.PathLike) -> str:
        """Mapea un path de dentro del contenedor a su equivalente en host.

        Se usa para que el ``file://`` del popup abra el reporte en el host.
        Si no hay info de mapeo, devuelve el path tal cual.
        """
        p = str(path)
        out_ct = os.environ.get("LOGS_REAPER_OUT") or "/work/out"
        out_host = os.environ.get("LOGSREAPER_OUT_HOST_PATH")
        if out_host and p.startswith(out_ct):
            return out_host + p[len(out_ct):]
        return p

    def _should_send(self, key: str, cooldown: float) -> bool:
        now = time.monotonic()
        with self._lock:
            last = self._last_at.get(key, 0.0)
            if now - last < cooldown:
                return False
            self._last_at[key] = now
            return True

    def notify(
        self,
        title: str,
        body: str = "",
        *,
        urgency: str = "normal",
        cooldown_key: str | None = None,
        cooldown: float | None = None,
        report_path: str | os.PathLike | None = None,
        dashboard_url: str | None = None,
        extra_links: Iterable[str] = (),
    ) -> bool:
        """Lanza una notificación. Devuelve True si se intentó enviar.

        ``cooldown_key`` agrupa notificaciones equivalentes; mientras esté
        en cooldown se descartan. ``report_path``/``dashboard_url`` se
        añaden al body como links clicables.
        """
        cd = self.default_cooldown if cooldown is None else float(cooldown)
        key = cooldown_key or title
        if not self._should_send(key, cd):
            return False

        link_lines: list[str] = []
        if report_path:
            host_path = self.container_to_host_path(report_path)
            link_lines.append(f"file://{host_path}")
        if dashboard_url is None:
            dashboard_url = os.environ.get("LOGSREAPER_DASHBOARD_URL")
        if dashboard_url:
            link_lines.append(dashboard_url)
        for link in extra_links:
            if link:
                link_lines.append(link)

        full_body = body
        if link_lines:
            sep = "\n" if body else ""
            full_body = f"{body}{sep}" + "\n".join(link_lines)

        if not self.enabled:
            # Fallback siempre útil en CI/headless.
            print(f"[notify] {title} — {full_body}".replace("\n", " | "),
                  file=sys.stderr, flush=True)
            return False

        cmd = [
            self.binary,
            "--app-name", self.app_name,
            "--urgency", urgency,
            "--category", "logsreaper",
            "--icon", "dialog-warning",
            title,
            full_body,
        ]
        try:
            subprocess.run(cmd, check=False, timeout=5,
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            return True
        except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
            print(f"[notify] failed ({exc}); body: {full_body}",
                  file=sys.stderr, flush=True)
            return False


_default: Notifier | None = None
_default_lock = threading.Lock()


def get_default() -> Notifier:
    global _default
    with _default_lock:
        if _default is None:
            _default = Notifier()
        return _default


def notify_unexpected_errors(
    *,
    title: str,
    summary_lines: list[str],
    report_path: str | os.PathLike | None = None,
    dashboard_url: str | None = None,
    cooldown_key: str | None = None,
    urgency: str = "critical",
) -> bool:
    """Helper de alto nivel: agrega líneas tipo ``svc: +N errores`` en el body."""
    body = "\n".join(summary_lines) if summary_lines else ""
    return get_default().notify(
        title=title,
        body=body,
        urgency=urgency,
        cooldown_key=cooldown_key,
        report_path=report_path,
        dashboard_url=dashboard_url,
    )
