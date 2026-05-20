from __future__ import annotations

import re
from pathlib import Path
from typing import Any

BOOT_PATTERNS = (
    re.compile(r"\bStarting gunicorn\b", re.IGNORECASE),
    re.compile(r"\bStarting Granian\b", re.IGNORECASE),
    re.compile(r"\bGranian server starting\b", re.IGNORECASE),
    re.compile(r"\bBooting worker with pid\b", re.IGNORECASE),
    re.compile(r"\bApplication startup complete\b", re.IGNORECASE),
)
# Primary markers are those emitted at the very first moment of a service boot.
PRIMARY_BOOT_PATTERNS = (
    re.compile(rb"Starting gunicorn", re.IGNORECASE),
    re.compile(rb"Starting Granian", re.IGNORECASE),
    re.compile(rb"Granian server starting", re.IGNORECASE),
    re.compile(rb"Application startup complete", re.IGNORECASE),
    re.compile(rb"Booting worker with pid", re.IGNORECASE),
)
REVERSE_BLOCK_BYTES = 64 * 1024
BOOT_REALIGN_WINDOW = 4 * 1024  # small look-back: cover the boot opening salvo without crossing into older boots
BOOT_HEAD_PATTERNS = (
    re.compile(rb"Starting gunicorn", re.IGNORECASE),
    re.compile(rb"Starting Granian", re.IGNORECASE),
    re.compile(rb"Granian server starting", re.IGNORECASE),
)
BOOT_COOLDOWN_SECONDS = 30
INSTANCES_ALL = "all"
INSTANCES_LAST = "last"


def locate_last_boot_offset(path: Path) -> int | None:
    """Return the byte offset of the start of the line containing the FIRST marker
    of the most recent service boot, or None if no marker was found.

    Strategy:
      1. Reverse-scan in REVERSE_BLOCK_BYTES blocks looking for ANY primary marker.
         As soon as the latest match in the file is located, stop.
      2. Once we have a match, re-scan a BOOT_REALIGN_WINDOW window ending just
         after that match looking for the earliest marker. That earliest marker
         is the true start of the boot (e.g. 'Starting gunicorn' is logged before
         'Booting worker with pid:' for the same restart).
    """
    try:
        size = path.stat().st_size
    except OSError:
        return None
    if size == 0:
        return None
    max_pattern_len = max(len(pattern.pattern) for pattern in PRIMARY_BOOT_PATTERNS)
    overlap = max_pattern_len - 1
    with path.open("rb") as handle:
        latest_match: int | None = None
        end = size
        while end > 0 and latest_match is None:
            start = max(0, end - REVERSE_BLOCK_BYTES)
            read_start = max(0, start - overlap) if end != size else start
            handle.seek(read_start)
            block = handle.read(end - read_start)
            block_best: int | None = None
            for pattern in PRIMARY_BOOT_PATTERNS:
                last = None
                for match in pattern.finditer(block):
                    last = match.start()
                if last is not None:
                    candidate = read_start + last
                    if block_best is None or candidate > block_best:
                        block_best = candidate
            if block_best is not None:
                latest_match = block_best
                break
            end = start
        if latest_match is None:
            return None
        # Realign to the earliest HEAD marker (Starting gunicorn/Granian)
        # within BOOT_REALIGN_WINDOW bytes before latest_match. We only honor
        # head markers here so a tightly-packed previous boot does not steal
        # the anchor.
        window_end = min(size, latest_match + max_pattern_len + 1024)
        window_start = max(0, window_end - BOOT_REALIGN_WINDOW)
        handle.seek(window_start)
        window = handle.read(window_end - window_start)
        earliest_head = None
        for pattern in BOOT_HEAD_PATTERNS:
            for match in pattern.finditer(window):
                candidate = window_start + match.start()
                if candidate <= latest_match and (earliest_head is None or candidate < earliest_head):
                    earliest_head = candidate
        anchor = earliest_head if earliest_head is not None else latest_match
        return _line_start_offset(handle, anchor)


def _line_start_offset(handle, match_offset: int) -> int:
    """Walk backwards from match_offset until we find a line break, return offset of the byte AFTER it."""
    window = 4096
    pos = match_offset
    while pos > 0:
        read_start = max(0, pos - window)
        handle.seek(read_start)
        chunk = handle.read(pos - read_start)
        idx = chunk.rfind(b"\n")
        if idx != -1:
            return read_start + idx + 1
        pos = read_start
    return 0


def annotate_instances(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Assign service_instance_seq and service_instance_started_at to events.

    Events are walked in their stored order (already source+offset sorted by Rust).
    Boot markers within BOOT_COOLDOWN_SECONDS of the previous one are coalesced.
    Events before the first boot belong to instance 0 (pre-existing logs).
    Returns the list of detected instance dicts in order.
    """
    instances: list[dict[str, Any]] = []
    current_seq = 0
    current_started_at: str | None = None
    last_boot_seconds: float | None = None
    for event in events:
        body = event.get("body") or ""
        timestamp = event.get("timestamp") or event.get("observed_timestamp")
        if _matches_boot(body):
            event_seconds = _timestamp_to_seconds(timestamp)
            if last_boot_seconds is None or event_seconds is None or (event_seconds - last_boot_seconds) > BOOT_COOLDOWN_SECONDS:
                current_seq += 1
                current_started_at = timestamp
                instances.append(
                    {
                        "seq": current_seq,
                        "started_at": timestamp,
                        "first_event_id": event.get("event_id"),
                        "event_count": 0,
                    }
                )
            if event_seconds is not None:
                last_boot_seconds = event_seconds
        event["service_instance_seq"] = current_seq
        event["service_instance_started_at"] = current_started_at
        if instances and current_seq >= 1:
            instances[current_seq - 1]["event_count"] += 1
    return instances


def select_instance_seqs(instances: list[dict[str, Any]], spec: str) -> set[int] | None:
    """Return the set of instance seq numbers to keep, or None for 'no filter'.

    spec is one of:
      - 'last'  -> keep only the most recent instance (or pre-boot if no boots)
      - 'all'   -> None (keep everything)
      - integer string -> keep the last N instances
    """
    normalized = (spec or INSTANCES_LAST).strip().lower()
    if normalized == INSTANCES_ALL:
        return None
    if not instances:
        return None
    if normalized == INSTANCES_LAST:
        return {instances[-1]["seq"]}
    if normalized.isdigit():
        count = int(normalized)
        if count <= 0:
            return {instances[-1]["seq"]}
        return {item["seq"] for item in instances[-count:]}
    raise ValueError(f"Unsupported --instances value: {spec!r}")


def filter_events_by_instance(events: list[dict[str, Any]], keep: set[int] | None) -> list[dict[str, Any]]:
    if keep is None:
        return events
    return [event for event in events if event.get("service_instance_seq") in keep]


def _matches_boot(body: str) -> bool:
    return any(pattern.search(body) for pattern in BOOT_PATTERNS)


def _timestamp_to_seconds(value: str | None) -> float | None:
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        from datetime import datetime

        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None
