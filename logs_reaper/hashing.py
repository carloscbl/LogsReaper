from __future__ import annotations

import hashlib
from collections.abc import Iterable

try:  # Optional fast path. The Rust extension uses blake3 as the primary path.
    import blake3  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
    blake3 = None


HASH_ALGORITHM = "blake3-128" if blake3 else "blake2b-128"


def stable_hash(parts: Iterable[object | None], *, digest_bytes: int = 16) -> str:
    payload = "\x1f".join("" if part is None else str(part) for part in parts)
    data = payload.encode("utf-8", errors="replace")
    if blake3:
        return blake3.blake3(data).hexdigest(length=digest_bytes)
    return hashlib.blake2b(data, digest_size=digest_bytes).hexdigest()
