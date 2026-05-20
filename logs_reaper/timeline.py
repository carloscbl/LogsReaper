from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

DOWN_PATTERNS = {
    "kafka": (
        re.compile(r"\b_ALL_BROKERS_DOWN\b"),
        re.compile(r"\b_TRANSPORT\b"),
        re.compile(r"\bbrokers?\s+are\s+down\b", re.IGNORECASE),
        re.compile(r"kafka:[^\"']*?Connect\s+to\s+\S+\s+failed", re.IGNORECASE),
        re.compile(r"\bbroker\s+transport\s+failure\b", re.IGNORECASE),
        re.compile(r"\bGroupCoordinator\b[^\"]*?\bfailed\b", re.IGNORECASE),
    ),
    "mongo": (
        re.compile(r"\bMongo(?:DB)?[^\n]{0,80}\b(?:unreachable|timed?\s*out|not\s+master|closed|disconnect)\b", re.IGNORECASE),
        re.compile(r"\bServerSelectionTimeoutError\b"),
        re.compile(r"\bAutoReconnect\b"),
    ),
    "elasticsearch": (
        re.compile(r"\belasticsearch\b[^\n]{0,80}\b(?:unreachable|timed?\s*out|refused|circuit_breaking)\b", re.IGNORECASE),
        re.compile(r"\bConnectionTimeout\b.*\belasticsearch\b", re.IGNORECASE),
    ),
}

UP_PATTERNS = {
    "kafka": (
        re.compile(r"\b(?:re)?joined\s+group\b", re.IGNORECASE),
        re.compile(r"\bGroup\s+\S+\s+heartbeat\b", re.IGNORECASE),
        re.compile(r"\bbroker\s+\S+\s+(?:up|connected)\b", re.IGNORECASE),
        re.compile(r"\bpartitions\s+assigned\b", re.IGNORECASE),
        re.compile(r"\bsubscribed\s+to\s+topic\b", re.IGNORECASE),
        re.compile(r"\bmessage\s+(?:produced|delivered)\b", re.IGNORECASE),
    ),
    "mongo": (
        re.compile(r"\bConnected\s+to\s+mongo(?:db)?\b", re.IGNORECASE),
        re.compile(r"\bmongo(?:db)?\b[^\n]{0,40}\b(?:reconnected|connected|topology\s+opened)\b", re.IGNORECASE),
    ),
    "elasticsearch": (
        re.compile(r"\belasticsearch\b[^\n]{0,40}\b(?:connected|ready)\b", re.IGNORECASE),
    ),
}


def build_connectivity_timeline(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Walk ordered events and build a per-dependency timeline of down/up windows.

    Returns a dict like:
      {
        "kafka": {"incidents": [...], "current_state": "up|down|unknown", "first_event_at": ts, "last_event_at": ts},
        "mongo": {...},
        ...
      }
    Each incident is a dict {"down_at": ts, "up_at": ts | None, "duration_seconds": float | None,
                              "down_events": int, "first_sample": str}.
    """
    timelines: dict[str, dict[str, Any]] = {}
    for service in DOWN_PATTERNS:
        timelines[service] = {
            "state": "unknown",
            "incidents": [],
            "down_events": 0,
            "up_events": 0,
        }
    for event in events:
        body = event.get("body") or event.get("normalized_template") or ""
        timestamp = _normalize_timestamp(event.get("timestamp") or event.get("observed_timestamp"))
        for service, down_regexes in DOWN_PATTERNS.items():
            entry = timelines[service]
            if any(pattern.search(body) for pattern in down_regexes):
                entry["down_events"] += 1
                if entry["state"] != "down":
                    entry["state"] = "down"
                    entry["incidents"].append(
                        {
                            "down_at": timestamp,
                            "up_at": None,
                            "duration_seconds": None,
                            "down_events": 1,
                            "first_sample": _trim(body),
                        }
                    )
                elif entry["incidents"]:
                    entry["incidents"][-1]["down_events"] += 1
                continue
            up_regexes = UP_PATTERNS.get(service, ())
            if any(pattern.search(body) for pattern in up_regexes):
                entry["up_events"] += 1
                if entry["state"] == "down" and entry["incidents"]:
                    incident = entry["incidents"][-1]
                    incident["up_at"] = timestamp
                    incident["duration_seconds"] = _duration_seconds(incident.get("down_at"), timestamp)
                    entry["state"] = "up"
                elif entry["state"] != "down":
                    entry["state"] = "up"
    return timelines


def _normalize_timestamp(value: str | None) -> str | None:
    if not value:
        return None
    return str(value)


def _duration_seconds(start: str | None, end: str | None) -> float | None:
    if not start or not end:
        return None
    try:
        s = _parse(start)
        e = _parse(end)
        return max((e - s).total_seconds(), 0.0)
    except ValueError:
        return None


def _parse(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text)


def _trim(body: str, limit: int = 140) -> str:
    body = " ".join(body.split())
    if len(body) > limit:
        return body[: limit - 1] + "…"
    return body
