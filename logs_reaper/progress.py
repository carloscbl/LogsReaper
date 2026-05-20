from __future__ import annotations

import os
import sys
import threading
from time import monotonic
from typing import IO


class ProgressReporter:
    """Minimal, dependency-free progress reporter writing to stderr.

    When stderr is not a TTY, falls back to occasional plain log lines so logs
    captured to a file remain readable without ANSI control codes.
    """

    def __init__(self, stream: IO[str] | None = None, enabled: bool | None = None) -> None:
        self._stream = stream if stream is not None else sys.stderr
        self._tty = enabled if enabled is not None else _is_tty(self._stream)
        self._lock = threading.Lock()
        self._last_render = 0.0
        self._last_phase: str | None = None

    @property
    def enabled(self) -> bool:
        return self._tty

    def phase(self, label: str) -> None:
        with self._lock:
            self._last_phase = label
            if self._tty:
                self._stream.write(f"\r\033[K[logs-reaper] {label}\n")
                self._stream.flush()
            else:
                self._stream.write(f"[logs-reaper] {label}\n")
                self._stream.flush()

    def update(self, *, bytes_read: int, bytes_total: int, events: int, prefix: str = "parsing") -> None:
        now = monotonic()
        is_terminal_frame = bytes_total > 0 and bytes_read >= bytes_total
        if self._tty and not is_terminal_frame and (now - self._last_render) < 0.08:
            return
        with self._lock:
            self._last_render = now
            if self._tty:
                width = max(10, _terminal_width() - 50)
                if bytes_total > 0:
                    ratio = min(1.0, bytes_read / bytes_total)
                else:
                    ratio = 0.0
                filled = int(ratio * width)
                bar = "█" * filled + "·" * (width - filled)
                line = (
                    f"\r\033[K[{prefix}] [{bar}] "
                    f"{_format_bytes(bytes_read)}/{_format_bytes(bytes_total)} "
                    f"({ratio * 100:5.1f}%) events={events:,}"
                )
                self._stream.write(line)
                self._stream.flush()
            else:
                # Avoid spamming non-TTY logs: roughly once per second.
                if (now - self._last_render) < 1.0:
                    return
                self._stream.write(
                    f"[{prefix}] {_format_bytes(bytes_read)}/{_format_bytes(bytes_total)} events={events}\n"
                )
                self._stream.flush()

    def finish(self) -> None:
        if not self._tty:
            return
        with self._lock:
            self._stream.write("\r\033[K")
            self._stream.flush()


def _is_tty(stream: IO[str]) -> bool:
    try:
        return stream.isatty()
    except (AttributeError, ValueError):
        return False


def _terminal_width() -> int:
    try:
        return os.get_terminal_size().columns
    except OSError:
        return 100


def _format_bytes(value: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    size = float(value)
    index = 0
    while size >= 1024 and index < len(units) - 1:
        size /= 1024
        index += 1
    if index == 0:
        return f"{int(size)} {units[index]}"
    return f"{size:7.2f} {units[index]}"
