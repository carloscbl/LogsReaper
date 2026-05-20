"""Thin Python facade over the Rust scan core (pyo3 in-process binding).

Hard-requires the `logs_reaper_core` extension (built with
`maturin develop --release --features python` from `rust/logs_reaper_core`)
and `pyarrow`. There is no subprocess or JSON fallback — Python is purely a
wrapper around the in-process Rust engine.

The Rust core streams parsed RecordBatches over a bounded crossbeam channel
straight into Arrow IPC files (uncompressed — the on-disk bytes ARE the Arrow
in-memory buffer layout). Python re-opens each file via `pa.ipc.open_file`,
which memory-maps the contents and exposes a zero-copy `pa.Table`. That makes
the scan→table boundary essentially free compared with parquet round-trips.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

try:
    import logs_reaper_core as _logs_reaper_core  # type: ignore[import-not-found]
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "logs_reaper_core (pyo3 extension) is required. Build it from "
        "./rust/logs_reaper_core with "
        "`maturin develop --release --features python`."
    ) from exc

try:
    import pyarrow as _pa  # type: ignore[import-not-found]
    import pyarrow.ipc as _pa_ipc  # type: ignore[import-not-found]
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "pyarrow is required to materialise Rust scan output into Python tables."
    ) from exc


def scan_file_to_ipc(
    *,
    input_path: str | Path,
    events_out: str | Path,
    templates_out: str | Path,
    service_name: str,
    run_id: str,
    observed_timestamp: str,
    include_raw: bool = False,
    start_offset: int = 0,
    progress_callback: Callable[[int, int, int], None] | None = None,
) -> dict[str, Any]:
    """Drive the Rust streaming scan that writes events + templates as Arrow IPC files.

    Arrow IPC is the on-disk shape of the in-memory Arrow buffers (no compression, no
    re-encoding), so Python re-opens these via `pa.ipc.open_file` and gets a zero-copy
    `pa.Table` — much cheaper than a parquet round-trip.
    """
    _ = progress_callback  # reserved for a future progress-channel binding
    resolved_input = Path(input_path).resolve()
    summary = _logs_reaper_core.scan_file_to_ipc(
        str(resolved_input),
        str(Path(events_out).resolve()),
        str(Path(templates_out).resolve()),
        service_name,
        run_id,
        observed_timestamp,
        include_raw,
        int(start_offset),
    )
    summary = dict(summary)
    # Compatibility: older Rust builds (before the byte-range refactor)
    # don't emit `new_offset`. Synthesise it from start_offset + bytes
    # actually processed so incremental.delta_scan_tick can advance state
    # even against those builds. The new build's value is preferred when
    # present because it's clamped to the last \n we observed.
    if "new_offset" not in summary:
        processed = int(summary.get("bytes_processed") or summary.get("input_bytes") or 0)
        summary["new_offset"] = int(start_offset) + processed
    if "bytes_processed" not in summary:
        summary["bytes_processed"] = int(summary.get("input_bytes") or 0)
    return summary


def read_events_ipc(path: str | Path) -> _pa.Table:
    """Open the Rust-produced events IPC file zero-copy via mmap."""
    with _pa.memory_map(str(path), "r") as source:
        return _pa_ipc.open_stream(source).read_all()


def read_templates_ipc(path: str | Path) -> list[dict[str, Any]]:
    with _pa.memory_map(str(path), "r") as source:
        return _pa_ipc.open_stream(source).read_all().to_pylist()
