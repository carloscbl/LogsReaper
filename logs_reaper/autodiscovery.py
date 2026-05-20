from __future__ import annotations

import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .hashing import stable_hash
from .io import read_json, write_json

DEFAULT_OUT_DIR = Path(__file__).resolve().parents[1] / "out"
DOCKER_LOG_LIMIT_BYTES = 16 * 1024 * 1024


def prepare_service_scan(
    *,
    service_name: str,
    input_patterns: list[str] | None,
    out_dir: str | Path | None,
    run_id: str | None,
    since: str | None = None,
    force_reprocess: bool = False,
) -> dict[str, Any]:
    service_slug = _slug(service_name)
    service_out_dir = Path(out_dir) if out_dir else DEFAULT_OUT_DIR / service_slug
    service_out_dir.mkdir(parents=True, exist_ok=True)
    registry_path = service_out_dir / "service-scan-registry.json"
    registry = _load_registry(registry_path)

    if input_patterns:
        resolved_run_id = run_id or _default_run_id(service_slug)
        resolved_out_dir = service_out_dir / resolved_run_id if out_dir is None else Path(out_dir)
        return {
            "input_patterns": input_patterns,
            "run_id": resolved_run_id,
            "out_dir": str(resolved_out_dir),
            "baseline_dir": None,
            "service_autodetected": False,
            "autodiscovery": None,
        }

    source = _discover_service_source(service_name=service_name, service_slug=service_slug, since=since)
    fingerprint = _build_fingerprint(source)
    previous = _find_previous_match(registry, fingerprint)
    status = _classify_snapshot(registry, source, fingerprint)
    previous_run = _find_previous_run_for_container(registry, source)

    if previous and Path(previous["scan_out_dir"]).exists() and not force_reprocess:
        return {
            "input_patterns": [previous["captured_log_path"]],
            "run_id": previous["run_id"],
            "out_dir": previous["scan_out_dir"],
            "baseline_dir": previous.get("baseline_dir"),
            "service_autodetected": True,
            "autodiscovery": {
                "mode": source["mode"],
                "status": "same_snapshot",
                "reused_existing_scan": True,
                "fingerprint": fingerprint,
                "container_id": source.get("container_id"),
                "captured_log_path": previous["captured_log_path"],
                "registry_path": str(registry_path),
                "matched_run_id": previous["run_id"],
            },
        }

    resolved_run_id = run_id or _default_run_id(service_slug)
    resolved_out_dir = Path(out_dir) if out_dir else service_out_dir / resolved_run_id
    return {
        "input_patterns": [source["captured_log_path"]],
        "run_id": resolved_run_id,
        "out_dir": str(resolved_out_dir),
        "baseline_dir": _select_baseline_dir(status, previous, previous_run),
        "service_autodetected": True,
        "autodiscovery": {
            "mode": source["mode"],
            "status": status,
            "reused_existing_scan": False,
            "force_reprocess": force_reprocess,
            "fingerprint": fingerprint,
            "container_id": source.get("container_id"),
            "container_name": source.get("container_name"),
            "container_started_at": source.get("container_started_at"),
            "captured_log_path": source["captured_log_path"],
            "registry_path": str(registry_path),
            "baseline_run_id": _select_baseline_run_id(status, previous, previous_run),
            "baseline_dir": _select_baseline_dir(status, previous, previous_run),
        },
    }


def finalize_service_scan(
    *,
    service_name: str,
    run_id: str,
    scan_out_dir: str | Path,
    autodiscovery: dict[str, Any] | None,
    summary: dict[str, Any],
) -> None:
    if not autodiscovery:
        return
    registry_path = Path(str(autodiscovery["registry_path"]))
    registry = _load_registry(registry_path)
    fingerprint = str(autodiscovery["fingerprint"])
    captured_log_path = str(autodiscovery["captured_log_path"])
    entry = {
        "service_name": service_name,
        "run_id": run_id,
        "scan_out_dir": str(Path(scan_out_dir)),
        "captured_log_path": captured_log_path,
        "fingerprint": fingerprint,
        "container_id": autodiscovery.get("container_id"),
        "container_name": autodiscovery.get("container_name"),
        "container_started_at": autodiscovery.get("container_started_at"),
        "mode": autodiscovery.get("mode"),
        "status": autodiscovery.get("status"),
        "baseline_dir": autodiscovery.get("baseline_dir"),
        "baseline_run_id": autodiscovery.get("baseline_run_id"),
        "event_count": summary.get("event_count"),
        "template_count": summary.get("template_count"),
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    runs = [item for item in registry.get("runs", []) if item.get("fingerprint") != fingerprint]
    runs.append(entry)
    registry["runs"] = sorted(runs, key=lambda item: str(item.get("recorded_at", "")))
    write_json(registry_path, registry)


def _discover_service_source(*, service_name: str, service_slug: str, since: str | None) -> dict[str, Any]:
    container = _find_container(service_name)
    mounted_logs = _mounted_log_files(container)
    timestamp = _utc_stamp()
    capture_dir = DEFAULT_OUT_DIR / service_slug / "captures"
    capture_dir.mkdir(parents=True, exist_ok=True)

    if mounted_logs:
        digest_parts: list[object] = []
        for path in mounted_logs:
            stat = path.stat()
            digest_parts.extend([str(path.resolve()), stat.st_mtime_ns, stat.st_size])
        digest = stable_hash(digest_parts)
        capture_path = capture_dir / f"{timestamp}-mounted-{digest}.inputs.json"
        capture_path.write_text(json.dumps([str(path) for path in mounted_logs], indent=2) + "\n", encoding="utf-8")
        return {
            "mode": "mounted-files",
            "service_name": service_name,
            "captured_log_path": str(capture_path),
            "input_files": [str(path) for path in mounted_logs],
            "container_id": container["id"],
            "container_name": container["name"],
            "container_started_at": container["started_at"],
            "content_digest": digest,
        }

    raw_log = _capture_docker_logs(container["name"], since=since)
    digest = stable_hash([raw_log])
    capture_path = capture_dir / f"{timestamp}-docker-{digest}.log"
    capture_path.write_text(raw_log, encoding="utf-8")
    return {
        "mode": "docker-logs",
        "service_name": service_name,
        "captured_log_path": str(capture_path),
        "container_id": container["id"],
        "container_name": container["name"],
        "container_started_at": container["started_at"],
        "content_digest": digest,
    }


def _find_container(service_name: str) -> dict[str, str]:
    format_string = "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}"
    proc = subprocess.run(
        ["docker", "ps", "--format", format_string],
        check=True,
        capture_output=True,
        text=True,
    )
    service_key = service_name.lower()
    candidates: list[dict[str, str]] = []
    for line in proc.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) != 4:
            continue
        container_id, name, image, status = parts
        haystack = f"{name} {image}".lower()
        score = 0
        if re.search(rf"(^|[-_]){re.escape(service_key)}($|[-_])", name.lower()):
            score += 3
        if re.search(rf"(^|[-_/]){re.escape(service_key)}($|[:._/-])", image.lower()):
            score += 2
        if service_key in haystack:
            score += 1
        if score:
            candidates.append({"id": container_id, "name": name, "image": image, "status": status, "score": str(score)})
    if not candidates:
        raise RuntimeError(f"No running Docker container matched service {service_name!r}")
    winner = max(candidates, key=lambda item: (int(item["score"]), item["name"]))
    inspect = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.StartedAt}}", winner["id"]],
        check=True,
        capture_output=True,
        text=True,
    )
    winner["started_at"] = inspect.stdout.strip()
    return winner


def _mounted_log_files(container: dict[str, str]) -> list[Path]:
    proc = subprocess.run(
        ["docker", "inspect", "--format", "{{json .Mounts}}", container["id"]],
        check=True,
        capture_output=True,
        text=True,
    )
    mounts = json.loads(proc.stdout or "[]")
    log_dirs: list[Path] = []
    for mount in mounts:
        source = mount.get("Source")
        destination = mount.get("Destination", "")
        if not source:
            continue
        if destination.endswith("/log") or source.endswith("/log"):
            path = Path(source)
            if path.exists() and path.is_dir():
                log_dirs.append(path)
    files: list[Path] = []
    for directory in log_dirs:
        for path in sorted(directory.rglob("*")):
            if path.is_file() and path.name != ".gitignore" and path.stat().st_size > 0:
                files.append(path)
    return files


def _capture_docker_logs(container_name: str, *, since: str | None) -> str:
    cmd = ["docker", "logs"]
    if since:
        cmd.extend(["--since", since])
    cmd.append(container_name)
    proc = subprocess.run(cmd, check=True, capture_output=True)
    raw = (proc.stdout or b"") + (proc.stderr or b"")
    if len(raw) > DOCKER_LOG_LIMIT_BYTES:
        raw = raw[-DOCKER_LOG_LIMIT_BYTES:]
    return raw.decode("utf-8", errors="replace")


def _build_fingerprint(source: dict[str, Any]) -> str:
    return stable_hash(
        [
            source.get("service_name"),
            source.get("mode"),
            source.get("container_id"),
            source.get("container_started_at"),
            source.get("content_digest"),
        ]
    )


def _find_previous_match(registry: dict[str, Any], fingerprint: str) -> dict[str, Any] | None:
    for item in reversed(registry.get("runs", [])):
        if item.get("fingerprint") == fingerprint:
            return item
    return None


def _find_previous_run_for_container(registry: dict[str, Any], source: dict[str, Any]) -> dict[str, Any] | None:
    for item in reversed(registry.get("runs", [])):
        if item.get("container_id") != source.get("container_id"):
            continue
        scan_out_dir = item.get("scan_out_dir")
        if scan_out_dir and Path(str(scan_out_dir)).exists():
            return item
    return None


def _select_baseline_dir(
    status: str,
    previous_same_snapshot: dict[str, Any] | None,
    previous_run: dict[str, Any] | None,
) -> str | None:
    if status == "same_snapshot" and previous_same_snapshot:
        return str(previous_same_snapshot.get("scan_out_dir")) if previous_same_snapshot.get("scan_out_dir") else None
    if previous_run:
        return str(previous_run.get("scan_out_dir")) if previous_run.get("scan_out_dir") else None
    return None


def _select_baseline_run_id(
    status: str,
    previous_same_snapshot: dict[str, Any] | None,
    previous_run: dict[str, Any] | None,
) -> str | None:
    if status == "same_snapshot" and previous_same_snapshot:
        return str(previous_same_snapshot.get("run_id")) if previous_same_snapshot.get("run_id") else None
    if previous_run:
        return str(previous_run.get("run_id")) if previous_run.get("run_id") else None
    return None


def _classify_snapshot(registry: dict[str, Any], source: dict[str, Any], fingerprint: str) -> str:
    previous = registry.get("runs", [])
    if any(item.get("fingerprint") == fingerprint for item in previous):
        return "same_snapshot"
    for item in reversed(previous):
        if item.get("container_id") == source.get("container_id"):
            return "same_container_new_logs"
    return "new_container"


def _load_registry(path: Path) -> dict[str, Any]:
    if path.exists():
        payload = read_json(path)
        if isinstance(payload, dict):
            payload.setdefault("runs", [])
            return payload
    return {"runs": []}


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _default_run_id(service_slug: str) -> str:
    return f"{service_slug.upper()}_{_utc_stamp()}"


def _slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "-", value).strip("-").lower()
