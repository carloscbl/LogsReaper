"""Thin Python facade over the Rust normalizer.

The implementation lives entirely in the `logs_reaper_core` pyo3 extension
built from `rust/logs_reaper_core`. This module exposes a stable Python API
(matching the previous pure-Python surface) so legacy call sites and tests
keep working — but every function delegates straight into Rust.

If the extension is not importable, fail loudly at import time: there is no
Python fallback by design.
"""

from __future__ import annotations

try:
    from logs_reaper_core import (  # type: ignore[import-not-found]
        determine_error_kind_py as _determine_error_kind,
        extract_exception_type_py as _extract_exception_type,
        normalize_message_py as _normalize_message,
        strip_ansi_py as _strip_ansi,
    )
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "logs_reaper_core (pyo3 extension) is required. Build it from "
        "./rust/logs_reaper_core with "
        "`maturin develop --release --features python`."
    ) from exc


def strip_ansi(value: str) -> str:
    return _strip_ansi(value)


def normalize_message(value: object | None) -> str:
    return _normalize_message("" if value is None else str(value))


def extract_exception_type(value: object | None) -> str | None:
    return _extract_exception_type("" if value is None else str(value))


def determine_error_kind(
    body: object | None,
    severity_text: str | None,
    exception_type: str | None = None,
) -> str:
    return _determine_error_kind(
        "" if body is None else str(body),
        severity_text or "",
        exception_type,
    )
