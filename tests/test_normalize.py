from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from logs_reaper.normalize import determine_error_kind, normalize_message


def test_normalize_high_cardinality_tokens() -> None:
    value = (
        "Failed user 123e4567-e89b-12d3-a456-426614174000 "
        "from 10.0.0.5:443 path /api/accounts/507f1f77bcf86cd799439011 "
        "trace 4bf92f3577b34da6a3ce929d0e0e4736 at 2026-05-14T09:00:00Z"
    )

    normalized = normalize_message(value)

    assert "<UUID>" in normalized
    assert "<IP>" in normalized
    assert "<PORT>" in normalized
    assert "<PATH>" in normalized
    assert "<TIMESTAMP>" in normalized
    assert "123e4567" not in normalized
    assert normalize_message(value) == normalized


def test_determine_error_kind_prefers_exception_type() -> None:
    body = "Traceback (most recent call last):\nValueError: bad account"

    assert determine_error_kind(body, "ERROR") == "ValueError"
    assert determine_error_kind("plain error", "ERROR") == "log_error"
