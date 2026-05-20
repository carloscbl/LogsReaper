"""auto_index — refresco incremental mientras corre `collect`.

Pensado para que `logs-reaper live` lo lance como thread daemon. Cada
`interval` segundos:

1. **Tick incremental** (barato): para cada `<svc>.log` que haya crecido
   desde el offset persistido, llama `incremental.delta_scan_tick` que
   procesa SÓLO los bytes nuevos vía `scan_file_to_ipc(start_offset=N)`.
   El Rust hot path acepta byte-range; nada se reescanea desde 0.
2. **Materialize** (caro, periódico): cada `materialize_every_ticks` o
   cuando algún servicio acumula `materialize_threshold_fragments`,
   consolida los fragments → `events.parquet` + `templates.parquet` +
   `summary.json` + corre Drain. El dashboard lee siempre los parquet
   consolidados, así que ve snapshots atómicos.
3. **Registry / baselines** se reconstruyen sólo cuando algún servicio
   materializó en ese tick — antes lo hacíamos en cada barrido.

Skip-if-unchanged ya no es por tamaño bruto: el estado v2 trackea
`(inode, offset, sequence)` por servicio. Rotación o truncate dispara
un purge automático.
"""
from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any

from .incremental import IncrementalState, delta_scan_tick, materialize


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _current_log_size(logs_dir: Path, svc: str) -> int:
    """Best-effort current size for a service's log; returns 0 if missing."""
    try:
        return int((logs_dir / f"{svc}.log").stat().st_size)
    except OSError:
        return 0


class AutoIndexer:
    """Loop en background que repite scan+index hasta que se le diga parar."""

    def __init__(
        self,
        *,
        logs_dir: Path,
        scans_root: Path,
        registry_dir: Path,
        baselines_dir: Path | None = None,
        services_provider,
        interval: float = 5.0,
        run_id_prefix: str = "LIVE",
        min_log_bytes: int = 64,
        min_green_runs: int = 1,
        history_window: int = 200,
        initial_pass: bool = True,
        idle_interval_max: float = 60.0,
        idle_backoff_factor: float = 2.0,
        idle_cycles_before_backoff: int = 2,
        state_file: Path | None = None,
        materialize_every_ticks: int = 6,
        materialize_threshold_fragments: int = 32,
        registry_rebuild_every_materializes: int = 6,
        max_materializes_per_cycle: int = 3,
    ) -> None:
        self.logs_dir = Path(logs_dir)
        self.scans_root = Path(scans_root)
        self.registry_dir = Path(registry_dir)
        self.baselines_dir = Path(baselines_dir) if baselines_dir else None
        self.services_provider = services_provider
        self.base_interval = float(interval)
        self.interval = self.base_interval
        self.run_id_prefix = run_id_prefix
        self.min_log_bytes = min_log_bytes
        self.min_green_runs = min_green_runs
        self.history_window = history_window
        self.initial_pass = initial_pass
        self.idle_interval_max = max(float(idle_interval_max), self.base_interval)
        self.idle_backoff_factor = max(float(idle_backoff_factor), 1.0)
        self.idle_cycles_before_backoff = max(int(idle_cycles_before_backoff), 1)
        self.materialize_every_ticks = max(int(materialize_every_ticks), 1)
        self.materialize_threshold_fragments = max(int(materialize_threshold_fragments), 1)
        # build_registry() walks every run's templates.parquet and rewrites the
        # aggregated template_registry + baseline parquet files; with ~30 active
        # services that's a 60-80% CPU burst even when each individual
        # materialize was cheap. Running it every Nth materialize keeps the
        # cross-service Code-Errors view fresh on a coarser cadence (default
        # ~3× materialize_every_ticks·interval ≈ 3 min) while idle CPU stays
        # in the single-digit range.
        self.registry_rebuild_every_materializes = max(
            int(registry_rebuild_every_materializes), 1
        )
        self._materializes_since_registry: int = 0
        # Cap how many services materialize in a single cycle. With 30 active
        # services running materialize on each one serially was a 60-84% CPU
        # burst on the supervisor; spreading them across cycles keeps the
        # worst-case burst bounded while the queue still drains in
        # max_materializes_per_cycle · materialize_every_ticks · interval per
        # full sweep. Priority is by pending_events (busiest services first).
        self.max_materializes_per_cycle = max(int(max_materializes_per_cycle), 1)
        self.history_path = self.registry_dir / "auto_index_history.json"
        # v2 state file: (inode, offset, sequence) per service. Migrates v1
        # transparently on first read. Persisted so a docker restart doesn't
        # re-process bytes that were already in the materialized parquet.
        self.state_path = Path(state_file) if state_file else (self.registry_dir / "auto_index_state.json")
        self.state = IncrementalState(self.state_path)

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._history: dict[str, deque[dict[str, Any]]] = {}
        self._last_per_service: dict[str, dict[str, Any]] = {}
        self._idle_cycles: int = 0
        self._tick_counter: int = 0
        # Services that still have fragments pending materialize since last cycle.
        self._dirty_services: set[str] = set()

        self.last_run_at: float | None = None
        self.last_error: str | None = None
        self.runs_completed: int = 0
        self.services_indexed_last: list[str] = []

    # ------------------------------------------------------------------
    def _delta_tick(self, svc: str) -> tuple[dict[str, Any] | None, int]:
        """Run one incremental tick for `svc`.

        Returns (delta_summary_or_None, current_log_size). The summary is
        None when there is no new work — most ticks return None for most
        services because heartbeats don't add enough bytes.
        """
        log_path = self.logs_dir / f"{svc}.log"
        if not log_path.exists():
            return None, 0
        try:
            current_size = log_path.stat().st_size
        except OSError:
            return None, 0
        if current_size < self.min_log_bytes:
            return None, current_size
        run_id = f"{self.run_id_prefix}_{svc}"
        run_dir = self.scans_root / svc / run_id
        try:
            summary = delta_scan_tick(
                svc=svc,
                log_path=log_path,
                run_dir=run_dir,
                state=self.state,
                service_name=svc,
                run_id=run_id,
            )
        except Exception as exc:
            self.last_error = f"delta {svc}: {exc}"
            return None, current_size
        if summary is None:
            return None, current_size
        if not summary.get("empty_fragment"):
            self._dirty_services.add(svc)
        return summary, current_size

    def _materialize_service(self, svc: str) -> dict[str, Any] | None:
        """Consolidate pending fragments for `svc` into the canonical parquet."""
        run_id = f"{self.run_id_prefix}_{svc}"
        run_dir = self.scans_root / svc / run_id
        try:
            return materialize(
                svc=svc,
                run_dir=run_dir,
                state=self.state,
                service_name=svc,
                rules_path=None,
                baseline_dir=None,
            )
        except Exception as exc:
            self.last_error = f"materialize {svc}: {exc}"
            return None

    def _rebuild_registry(self) -> dict[str, Any]:
        from .registry import build_registry
        summary = build_registry(
            runs_root=self.scans_root,
            out_dir=self.registry_dir,
            min_green_runs=self.min_green_runs,
        )
        if self.baselines_dir is not None:
            from .baselines import partition_baselines
            try:
                partition_baselines(
                    aggregate_dir=self.registry_dir,
                    baselines_dir=self.baselines_dir,
                )
            except Exception as exc:
                self.last_error = f"partition: {exc}"
        return summary

    def _read_scan_summary(self, svc: str) -> dict[str, Any]:
        """Lee summary.json producido por scan; tolera ausencia."""
        run_id = f"{self.run_id_prefix}_{svc}"
        path = self.scans_root / svc / run_id / "summary.json"
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError:
            return {}

    def _stats_for_service(self, svc: str, log_size: int) -> dict[str, Any]:
        summary = self._read_scan_summary(svc)
        issue_counts = summary.get("issue_kind_event_counts") or {}
        return {
            "service": svc,
            "scan_at": time.time(),
            "log_size_bytes": log_size,
            "events": _safe_int(summary.get("event_count")),
            "templates": _safe_int(summary.get("template_count")),
            "errors": _safe_int(summary.get("error_count")),
            "code_errors": _safe_int(issue_counts.get("code")),
            "infra_errors": _safe_int(issue_counts.get("infra")),
            "connectivity_incidents": _safe_int(summary.get("connectivity_incident_count")),
        }

    def _absorb_stats(self, current: dict[str, Any]) -> dict[str, Any]:
        """Compara con el último ciclo y devuelve deltas."""
        svc = current["service"]
        prev = self._last_per_service.get(svc, {})
        deltas = {
            "delta_events": current["events"] - _safe_int(prev.get("events")),
            "delta_templates": current["templates"] - _safe_int(prev.get("templates")),
            "delta_errors": current["errors"] - _safe_int(prev.get("errors")),
            "delta_code_errors": current["code_errors"] - _safe_int(prev.get("code_errors")),
            "delta_log_bytes": current["log_size_bytes"] - _safe_int(prev.get("log_size_bytes")),
        }
        entry = {**current, **deltas}
        self._last_per_service[svc] = current
        return entry

    def _persist_history(self) -> None:
        """Vuelca el historial agregado a un JSON para que el dashboard lo lea."""
        snapshot = {
            "updated_at": time.time(),
            "runs_completed": self.runs_completed,
            "last_error": self.last_error,
            "interval_seconds": self.interval,
            "min_green_runs": self.min_green_runs,
            "history_window": self.history_window,
            "services": {
                svc: list(entries) for svc, entries in self._history.items()
            },
            "current": dict(self._last_per_service),
        }
        self.history_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.history_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(snapshot, indent=2, default=str))
        os.replace(tmp, self.history_path)

    def run_once(self) -> dict[str, Any]:
        services = self.services_provider() or []
        self._tick_counter += 1

        # ---- Phase 1: incremental tick per service (cheap) ---------------
        # In a steady-state idle cluster most services contribute heartbeats
        # below MIN_DELTA_BYTES — delta_scan_tick returns None for them,
        # short-circuiting before any FFI work.
        ticked: list[str] = []
        for svc in services:
            try:
                summary, _log_size = self._delta_tick(svc)
            except Exception as exc:
                self.last_error = f"tick {svc}: {exc}"
                continue
            if summary is not None:
                ticked.append(svc)

        # ---- Phase 2: materialize when due ------------------------------
        # Trigger by cadence OR backlog. The cadence ensures the dashboard
        # parquet doesn't go too stale; the backlog cap ensures a noisy
        # service forces consolidation before its fragments balloon.
        force_by_backlog = any(
            self.state.get(svc).pending_fragments >= self.materialize_threshold_fragments
            for svc in self._dirty_services
        )
        due_by_cadence = (self._tick_counter % self.materialize_every_ticks) == 0
        materialized: list[str] = []
        per_service_entries: list[dict[str, Any]] = []
        if self._dirty_services and (force_by_backlog or due_by_cadence):
            # Pick the top-N services by pending_events (busiest first). Anyone
            # not selected stays in _dirty_services and gets first dibs next
            # cycle — the queue drains FIFO-ish without ever materializing the
            # full backlog in a single 80%+ CPU burst.
            candidates = sorted(
                self._dirty_services,
                key=lambda s: -int(self.state.get(s).pending_events),
            )
            to_materialize = candidates[: self.max_materializes_per_cycle]
            for svc in to_materialize:
                run_summary = self._materialize_service(svc)
                if run_summary is None:
                    continue
                materialized.append(svc)
                stats = self._stats_for_service(svc, _current_log_size(self.logs_dir, svc))
                entry = self._absorb_stats(stats)
                with self._lock:
                    hist = self._history.setdefault(svc, deque(maxlen=self.history_window))
                    hist.append(entry)
                per_service_entries.append(entry)
            self._dirty_services.difference_update(materialized)
            if materialized:
                self._materializes_since_registry += 1
                if self._materializes_since_registry >= self.registry_rebuild_every_materializes:
                    try:
                        self._rebuild_registry()
                    except Exception as exc:
                        self.last_error = f"index: {exc}"
                    finally:
                        self._materializes_since_registry = 0

        # ---- Persist + bookkeeping --------------------------------------
        try:
            self.state.save_atomic()
        except Exception as exc:
            self.last_error = f"state-save: {exc}"

        self.services_indexed_last = materialized
        self.last_run_at = time.time()
        self.runs_completed += 1
        try:
            self._persist_history()
        except Exception as exc:
            self.last_error = f"history: {exc}"
        # Adaptive back-off: based on whether ANYTHING ticked (not just
        # materialized) — that's the right signal that the cluster is busy.
        if ticked:
            self._idle_cycles = 0
            self.interval = self.base_interval
        else:
            self._idle_cycles += 1
            if self._idle_cycles >= self.idle_cycles_before_backoff:
                self.interval = min(
                    self.interval * self.idle_backoff_factor,
                    self.idle_interval_max,
                )
        try:
            self._maybe_notify(per_service_entries)
        except Exception as exc:
            self.last_error = f"notify: {exc}"
        return {
            "ticked": ticked,
            "materialized": materialized,
            "runs_completed": self.runs_completed,
            "entries": per_service_entries,
            "last_error": self.last_error,
            "next_interval": self.interval,
            "idle_cycles": self._idle_cycles,
            "tick_counter": self._tick_counter,
        }

    def _maybe_notify(self, entries: list[dict[str, Any]]) -> None:
        """Desktop notification cuando hay errores nuevos en este ciclo.

        Se considera "inesperado": code_errors crecieron desde el ciclo
        anterior, o ante la falta de delta_code_errors, errors totales
        crecieron. Cooldown por servicio para no spammear.
        """
        flagged: list[str] = []
        for entry in entries:
            d_code = _safe_int(entry.get("delta_code_errors"))
            d_err = _safe_int(entry.get("delta_errors"))
            d_tpl = _safe_int(entry.get("delta_templates"))
            if d_code <= 0 and d_err <= 0:
                continue
            svc = entry.get("service", "?")
            bits = []
            if d_code > 0:
                bits.append(f"+{d_code} code-errors")
            elif d_err > 0:
                bits.append(f"+{d_err} errors")
            if d_tpl > 0:
                bits.append(f"+{d_tpl} new templates")
            flagged.append(f"{svc}: {', '.join(bits)}")
        if not flagged:
            return
        from .notifier import notify_unexpected_errors
        notify_unexpected_errors(
            title=f"LogsReaper: {len(flagged)} servicio(s) con errores inesperados",
            summary_lines=flagged,
            cooldown_key=f"auto-index:{','.join(sorted(e.split(':',1)[0] for e in flagged))}",
            urgency="critical",
        )

    # ------------------------------------------------------------------
    def _loop(self) -> None:
        if self.initial_pass:
            try:
                self.run_once()
            except Exception as exc:
                self.last_error = f"initial: {exc}"
        while not self._stop.is_set():
            if self._stop.wait(self.interval):
                return
            try:
                self.run_once()
            except Exception as exc:
                self.last_error = f"loop: {exc}"

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, daemon=True, name="logs-reaper-auto-index")
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._thread = None
