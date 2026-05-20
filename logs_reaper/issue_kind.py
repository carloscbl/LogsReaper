from __future__ import annotations

import re
from typing import Any

# Exception types that almost always point at infrastructure / external
# dependencies rather than at our own code logic.
INFRA_EXCEPTION_TYPES = {
    "KafkaError",
    "KafkaException",
    "BrokerNotAvailableError",
    "NetworkException",
    "ConnectionError",
    "ConnectionRefusedError",
    "ConnectionResetError",
    "ConnectionAbortedError",
    "TimeoutError",
    "ReadTimeout",
    "ReadTimeoutError",
    "ConnectTimeout",
    "ConnectTimeoutError",
    "BrokenPipeError",
    "OperationFailure",
    "AutoReconnect",
    "ServerSelectionTimeoutError",
    "NetworkTimeout",
    "DNSError",
    "ESConnectionError",
    "ESConnectionTimeout",
    "DisconnectionError",
}

# Exception types that almost always reveal a programming defect.
CODE_EXCEPTION_TYPES = {
    "AssertionError",
    "AttributeError",
    "KeyError",
    "IndexError",
    "TypeError",
    "ValueError",
    "NameError",
    "ZeroDivisionError",
    "RecursionError",
    "RuntimeError",
    "NotImplementedError",
    "ImportError",
    "ModuleNotFoundError",
    "UnboundLocalError",
    "SyntaxError",
    "IndentationError",
    "LookupError",
}

INFRA_BODY_PATTERNS = (
    re.compile(r"\bbrokers?\s+are\s+down\b", re.IGNORECASE),
    re.compile(r"\bconnection\s+refused\b", re.IGNORECASE),
    re.compile(r"\bno\s+route\s+to\s+host\b", re.IGNORECASE),
    re.compile(r"\bbroker\s+transport\s+failure\b", re.IGNORECASE),
    re.compile(r"\b_ALL_BROKERS_DOWN\b"),
    re.compile(r"\b_TRANSPORT\b"),
    re.compile(r"\bGroupCoordinator\b"),
    re.compile(r"\bfailed\s+to\s+get\s+metadata\b", re.IGNORECASE),
    re.compile(r"\bmongo(?:db)?\b.*\b(?:not\s+master|reconnect|topology|closed)\b", re.IGNORECASE),
    re.compile(r"\bgetaddrinfo\b", re.IGNORECASE),
    re.compile(r"\btemporary\s+failure\s+in\s+name\s+resolution\b", re.IGNORECASE),
    re.compile(r"\belasticsearch\b.*\b(?:timeout|unreachable|refused)\b", re.IGNORECASE),
    re.compile(r"\bredis\b.*\b(?:timeout|unreachable|refused)\b", re.IGNORECASE),
    re.compile(r"\bkafka\b.*\b(?:rebalanc|reassign|coordinator)\b", re.IGNORECASE),
    re.compile(r"\b(?:read|write)\s+timeout\b", re.IGNORECASE),
)

CODE_BODY_PATTERNS = (
    re.compile(r"\bTraceback\s+\(most\s+recent\s+call\s+last\)\b"),
    re.compile(r"\b(?:in|line)\s+\d+,?\s+in\s+\w+"),  # python traceback frames
    re.compile(r"\bUnhandled\s+exception\b", re.IGNORECASE),
    re.compile(r"\bassert(?:ion)?\s+(?:failed|error)\b", re.IGNORECASE),
)


def classify_issue_kind(row: dict[str, Any]) -> str:
    """Return 'code', 'infra' or 'unknown' for a template/event row."""
    exception_type = row.get("exception_type") or ""
    body = row.get("normalized_template") or row.get("body") or ""
    severity = str(row.get("severity_text") or "").upper()
    error_kind = row.get("error_kind") or ""

    if exception_type in INFRA_EXCEPTION_TYPES:
        return "infra"
    if exception_type in CODE_EXCEPTION_TYPES:
        return "code"
    for pattern in INFRA_BODY_PATTERNS:
        if pattern.search(body):
            return "infra"
    for pattern in CODE_BODY_PATTERNS:
        if pattern.search(body):
            return "code"
    if error_kind == "traceback":
        return "code"
    # Anything still flagged at ERROR/CRITICAL/FATAL severity after the infra
    # filters above is a code-side signal: either an explicit `log.error(...)`
    # in our code (error_kind=log_error, no exception attached) or a typed
    # exception that didn't match the infra allowlist. Without this, plain
    # logger.error calls fall to "unknown" and silently skip the Code Errors
    # tab even though they are exactly what an engineer wants to see.
    if severity in {"ERROR", "CRITICAL", "FATAL"}:
        return "code"
    return "unknown"


def annotate_issue_kind(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        row["issue_kind"] = classify_issue_kind(row)
