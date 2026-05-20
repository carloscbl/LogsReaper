"""Incremental scan + materialize for `logs-reaper live`.

The previous AutoIndexer reran the full Rust scan on every tick over the
*entire* log file, even when only a few hundred bytes were new. With 30
services and large `gateway-isp.log` / `mongodb.log` files this saturated
the CPU at idle. This module replaces that with two phases:

1. `delta_scan_tick(svc)` — called per service per tick. Reads only the
   new bytes (offset persisted in `IncrementalState`) and writes a small
   fragment `_rust/events_{seq:04d}.arrow` + `_rust/templates_{seq:04d}.arrow`.
   Costs O(delta_bytes), not O(total_bytes). Skips when the file hasn't
   grown (returns None).

2. `materialize(svc)` — called less frequently (every N ticks, or when
   backlog is large). Consolidates the persisted parquet + any pending
   fragments into a fresh `events.parquet` / `templates.parquet` /
   `summary.json` and runs the Drain phase. Fragments are deleted on
   success. The dashboard reads only the materialized parquet, so it sees
   atomic snapshots.

State file shape (v2):
    {
      "version": 2,
      "updated_at": <epoch>,
      "services": {
        "<svc>": {
          "inode": <int>, "offset": <int>, "sequence": <int>,
          "materialized_offset": <int>, "materialized_at": <epoch|null>,
          "pending_fragments": <int>
        }
      }
    }

Rotation/truncation:
    inode change OR current_size < last_offset → state is reset, all
    fragments + parquet for that svc are discarded, scan restarts at
    offset=0. Rare but supported.
"""
from __future__ import annotations

import json
import os
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .rust_engine import scan_file_to_ipc

STATE_VERSION = 2
FRAGMENT_GLOB = "events_*.arrow"
TEMPLATE_FRAGMENT_GLOB = "templates_*.arrow"


@dataclass
class ServiceState:
    """Per-service tracking. `offset` is the next byte to read (aligned to \\n)."""
    inode: int = 0
    offset: int = 0
    sequence: int = 0
    materialized_offset: int = 0
    materialized_at: float | None = None
    pending_fragments: int = 0
    last_error: str | None = None
    # Free-form delta counters since the last materialize, surfaced to dashboard.
    pending_events: int = 0
    pending_templates: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "inode": self.inode,
            "offset": self.offset,
            "sequence": self.sequence,
            "materialized_offset": self.materialized_offset,
            "materialized_at": self.materialized_at,
            "pending_fragments": self.pending_fragments,
            "pending_events": self.pending_events,
            "pending_templates": self.pending_templates,
            "last_error": self.last_error,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ServiceState":
        return cls(
            inode=int(data.get("inode") or 0),
            offset=int(data.get("offset") or 0),
            sequence=int(data.get("sequence") or 0),
            materialized_offset=int(data.get("materialized_offset") or 0),
            materialized_at=(float(data["materialized_at"]) if data.get("materialized_at") else None),
            pending_fragments=int(data.get("pending_fragments") or 0),
            pending_events=int(data.get("pending_events") or 0),
            pending_templates=int(data.get("pending_templates") or 0),
            last_error=data.get("last_error"),
        )


class IncrementalState:
    """File-backed state, atomic on save. Loads v1 (legacy `last_log_size`)
    and migrates to v2 transparently — legacy entries are treated as fully
    materialized with offset=last_log_size."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.services: dict[str, ServiceState] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text())
        except (OSError, json.JSONDecodeError):
            return
        version = int(payload.get("version") or 1)
        if version == 1:
            # Migrate: treat persisted sizes as already-materialized offsets so
            # we don't reprocess the full file on first incremental tick.
            sizes = payload.get("last_log_size") or {}
            for svc, raw in sizes.items():
                try:
                    n = int(raw)
                except (TypeError, ValueError):
                    continue
                self.services[svc] = ServiceState(offset=n, materialized_offset=n)
            return
        for svc, body in (payload.get("services") or {}).items():
            try:
                self.services[svc] = ServiceState.from_dict(body)
            except Exception:
                continue

    def save_atomic(self) -> None:
        payload = {
            "version": STATE_VERSION,
            "updated_at": time.time(),
            "services": {svc: st.to_dict() for svc, st in self.services.items()},
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2, default=str))
        os.replace(tmp, self.path)

    def get(self, svc: str) -> ServiceState:
        return self.services.setdefault(svc, ServiceState())

    def reset(self, svc: str, new_inode: int = 0) -> None:
        self.services[svc] = ServiceState(inode=new_inode)


# ---------------------------------------------------------------------------
# Tick: produce a fragment
# ---------------------------------------------------------------------------

# Heartbeats sometimes write a few bytes per second. Below this threshold
# we skip the FFI call entirely — saves the mmap+memchr cycle even if it
# was cheap. The Rust hot path bottoms out around ~10 ms per FFI even on
# zero events because of pyo3 marshalling.
MIN_DELTA_BYTES_DEFAULT = 4 * 1024


def delta_scan_tick(
    *,
    svc: str,
    log_path: Path,
    run_dir: Path,
    state: IncrementalState,
    service_name: str | None = None,
    run_id: str | None = None,
    min_delta_bytes: int = MIN_DELTA_BYTES_DEFAULT,
    observed_timestamp: str | None = None,
) -> dict[str, Any] | None:
    """Process [offset..EOF] of `log_path`, write a fragment, persist state.

    Returns the Rust scan summary dict augmented with `fragment_path`
    and `fragment_seq`. Returns None when there is no work to do
    (file missing, below min delta, etc.).
    """
    if not log_path.exists():
        return None
    try:
        st = log_path.stat()
    except OSError:
        return None
    current_size = st.st_size
    current_inode = st.st_ino

    svc_state = state.get(svc)

    # Rotation / truncation: inode changed, or size shrank below our offset.
    # Reset state and discard prior fragments + parquet so we start clean.
    rotated = (
        svc_state.inode and svc_state.inode != current_inode
    ) or current_size < svc_state.offset
    if rotated:
        _purge_service_artifacts(run_dir)
        state.reset(svc, new_inode=current_inode)
        svc_state = state.get(svc)

    if not svc_state.inode:
        svc_state.inode = current_inode

    delta_bytes = current_size - svc_state.offset
    if delta_bytes < max(min_delta_bytes, 1):
        # Nothing meaningful new. We do NOT touch state — the file may still
        # be growing slowly and we'll catch up on a later tick.
        return None

    rust_dir = run_dir / "_rust"
    rust_dir.mkdir(parents=True, exist_ok=True)
    next_seq = svc_state.sequence + 1
    events_frag = rust_dir / f"events_{next_seq:04d}.arrow"
    templates_frag = rust_dir / f"templates_{next_seq:04d}.arrow"

    ts = observed_timestamp or _iso_now()
    summary = scan_file_to_ipc(
        input_path=log_path,
        events_out=events_frag,
        templates_out=templates_frag,
        service_name=service_name or svc,
        run_id=run_id or f"LIVE_{svc}",
        observed_timestamp=ts,
        start_offset=int(svc_state.offset),
    )

    new_offset = int(summary.get("new_offset") or svc_state.offset)
    event_count = int(summary.get("event_count") or 0)
    template_count = int(summary.get("template_count") or 0)

    # Empty fragment → don't keep the file. Happens when the delta contained
    # only continuation lines or content that produced no events.
    if event_count == 0:
        events_frag.unlink(missing_ok=True)
        templates_frag.unlink(missing_ok=True)
    else:
        svc_state.sequence = next_seq
        svc_state.pending_fragments += 1
        svc_state.pending_events += event_count
        svc_state.pending_templates += template_count

    # Always advance offset, even on empty fragment — those bytes are
    # consumed and don't need to be retried.
    svc_state.offset = new_offset
    svc_state.last_error = None

    summary["fragment_path"] = str(events_frag)
    summary["fragment_templates_path"] = str(templates_frag)
    summary["fragment_seq"] = next_seq
    summary["delta_bytes"] = int(summary.get("bytes_processed") or 0)
    summary["log_size_bytes"] = current_size
    summary["log_inode"] = current_inode
    summary["empty_fragment"] = event_count == 0
    return summary


def _iso_now() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Materialize: consolidate fragments → parquet
# ---------------------------------------------------------------------------


SNAPSHOT_FILENAME = "events_snapshot.arrow"


def materialize(
    *,
    svc: str,
    run_dir: Path,
    state: IncrementalState,
    service_name: str | None = None,
    rules_path: Path | None = None,
    baseline_dir: Path | None = None,
    autodiscovery: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Consolidate fragments + previous snapshot into the canonical parquet
    output (`events.parquet`, `templates.parquet`, `summary.json`).

    Incremental contract: a single `_state/events_snapshot.arrow` holds the
    consolidated events table from the previous materialize. On each call
    we concatenate it with the newly produced fragments before running the
    rest of the pipeline (Drain, classify, write parquet). The new
    consolidated table is then written back to the snapshot atomically, so
    history is preserved across materializes — fragments only carry the
    delta, the snapshot carries the running total.

    Deletes consumed fragments on success. Returns the run summary dict, or
    None if there's nothing new to materialize (no fragments AND no
    snapshot to refresh).
    """
    rust_dir = run_dir / "_rust"
    state_dir = run_dir / "_state"
    snapshot_path = state_dir / SNAPSHOT_FILENAME

    fragments: list[Path] = []
    template_fragments: list[Path] = []
    if rust_dir.exists():
        fragments = sorted(rust_dir.glob(FRAGMENT_GLOB))
        template_fragments = sorted(rust_dir.glob(TEMPLATE_FRAGMENT_GLOB))

    snapshot_exists = snapshot_path.exists()
    if not fragments and not snapshot_exists:
        return None
    # When there are no fresh fragments but the snapshot exists, we still
    # have nothing to refresh — short-circuit.
    if not fragments:
        return None

    # Snapshot from the previous materialize goes FIRST so concat keeps the
    # natural temporal order (older events before newer). The snapshot
    # shares the Rust fragments' schema (we persisted it pre-lookup-columns
    # for exactly this reason).
    consolidated_events_inputs: list[Path] = []
    if snapshot_exists:
        consolidated_events_inputs.append(snapshot_path)
    consolidated_events_inputs.extend(fragments)

    new_snapshot = state_dir / (SNAPSHOT_FILENAME + ".new")
    from .scan import scan_from_ipc_fragments

    summary = scan_from_ipc_fragments(
        events_ipc_paths=consolidated_events_inputs,
        templates_ipc_paths=template_fragments,
        run_id=f"LIVE_{svc}",
        out_dir=run_dir,
        service_name=service_name or svc,
        rules_path=rules_path,
        baseline_dir=baseline_dir,
        autodiscovery=autodiscovery,
        events_snapshot_out=new_snapshot,
    )

    # scan_from_ipc_fragments wrote the new snapshot atomically inside that
    # function (tmp + os.replace). It lives at `new_snapshot` — promote it
    # to the canonical name. If no snapshot was produced (table was empty),
    # we keep the existing one (this batch was no-op).
    if new_snapshot.exists():
        os.replace(new_snapshot, snapshot_path)

    svc_state = state.get(svc)
    svc_state.materialized_offset = svc_state.offset
    svc_state.materialized_at = time.time()
    svc_state.pending_fragments = 0
    svc_state.pending_events = 0
    svc_state.pending_templates = 0

    # Drop the consumed fragments. The snapshot now holds the consolidated
    # state; keeping the .arrow shards would double-count on the next
    # materialize.
    for path in fragments + template_fragments:
        path.unlink(missing_ok=True)

    return summary


def _purge_service_artifacts(run_dir: Path) -> None:
    """Used on rotation/truncation: wipe everything under the LIVE_<svc> dir
    so the next tick starts from a clean state. Keeps run_dir itself."""
    if not run_dir.exists():
        return
    for child in run_dir.iterdir():
        if child.is_dir():
            shutil.rmtree(child, ignore_errors=True)
        else:
            try:
                child.unlink()
            except OSError:
                pass
