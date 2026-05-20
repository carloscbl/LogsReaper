"""Per-service baselines bajo `./baselines/<service>/`.

Tres responsabilidades:

* :func:`partition_baselines` — toma los parquet agregados que produce
  `build_registry` y los particiona por `service_name`, escribiendo
  `baseline.parquet` + `template_registry.parquet` + `baseline_overrides.json`
  (subset) por servicio.

* :func:`classify_delta` — dado un diff de un run, decide si el cambio es
  ``safe`` (auto-commiteable), ``unsafe`` (necesita revisión) o ``no-change``.

* :func:`commit_baselines` — wrapper sobre git para commitear los ficheros
  particionados con un mensaje estándar. Default ``dry_run=True``.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


# Severities consideradas "safe" para auto-commit. ERROR/CRITICAL/FATAL nunca lo son.
_SAFE_SEVERITIES = {"INFO", "DEBUG", "NOTICE"}
# Orden parcial de severidad para detectar shifts "hacia arriba".
_SEVERITY_ORDER = {
    "DEBUG": 0, "INFO": 1, "NOTICE": 2, "WARN": 3, "WARNING": 3,
    "ERROR": 4, "CRITICAL": 5, "FATAL": 5,
}


def _services_in_table(table: pa.Table) -> list[str]:
    if table.num_rows == 0 or "service_name" not in table.column_names:
        return []
    return sorted({str(s) for s in table.column("service_name").to_pylist() if s})


def _filter_by_service(table: pa.Table, service: str) -> pa.Table:
    if table.num_rows == 0:
        return table
    mask = pc.equal(table.column("service_name"), pa.scalar(service))
    return table.filter(mask)


def _partition_overrides(master: dict[str, Any], service: str) -> dict[str, Any]:
    """Subset del fichero master de overrides para un servicio dado."""
    out: dict[str, Any] = {"version": master.get("version", 1), "service": service, "overrides": {}}
    prefix = f"{service}::"
    for key, entry in (master.get("overrides") or {}).items():
        if key.startswith(prefix):
            out["overrides"][key] = entry
    return out


def partition_baselines(
    *,
    aggregate_dir: Path,
    baselines_dir: Path,
    services: list[str] | None = None,
) -> dict[str, Any]:
    """Vuelca por servicio los parquet agregados que viven en ``aggregate_dir``.

    aggregate_dir debe contener:
        baseline.parquet, template_registry.parquet, baseline_overrides.json (opt)

    baselines_dir/<service>/ es la salida (se crea si falta).
    """
    baseline_path = aggregate_dir / "baseline.parquet"
    templates_path = aggregate_dir / "template_registry.parquet"
    overrides_path = aggregate_dir / "baseline_overrides.json"

    if not baseline_path.exists():
        return {"services": [], "baselines_dir": str(baselines_dir), "skipped": "no baseline.parquet"}

    baseline_table = pq.read_table(baseline_path)
    templates_table = pq.read_table(templates_path) if templates_path.exists() else pa.table({})
    overrides_master: dict[str, Any] = {}
    if overrides_path.exists():
        try:
            overrides_master = json.loads(overrides_path.read_text())
        except json.JSONDecodeError:
            overrides_master = {}

    found = _services_in_table(baseline_table)
    target_services = services if services else found

    baselines_dir.mkdir(parents=True, exist_ok=True)
    written: list[dict[str, Any]] = []
    for svc in target_services:
        svc_dir = baselines_dir / svc
        svc_dir.mkdir(parents=True, exist_ok=True)
        svc_baseline = _filter_by_service(baseline_table, svc)
        _tmp = svc_dir / "baseline.parquet.tmp"
        pq.write_table(svc_baseline, _tmp, compression="zstd", use_dictionary=True)
        os.replace(_tmp, svc_dir / "baseline.parquet")
        if templates_table.num_rows:
            svc_templates = _filter_by_service(templates_table, svc)
            _tmp_t = svc_dir / "template_registry.parquet.tmp"
            pq.write_table(
                svc_templates,
                _tmp_t,
                compression="zstd",
                use_dictionary=True,
            )
            os.replace(_tmp_t, svc_dir / "template_registry.parquet")
        svc_overrides = _partition_overrides(overrides_master, svc)
        (svc_dir / "baseline_overrides.json").write_text(json.dumps(svc_overrides, indent=2, sort_keys=True))
        written.append({
            "service": svc,
            "baseline_rows": svc_baseline.num_rows,
            "overrides_count": len(svc_overrides["overrides"]),
        })
    return {"services": written, "baselines_dir": str(baselines_dir)}


def classify_delta(diff: dict[str, Any]) -> str:
    """Decide si el delta de un diff es safe / unsafe / no-change.

    safe   -> nuevos templates sólo INFO/DEBUG/NOTICE, 0 code_errors,
              0 policy_violations, 0 severity-shifts hacia arriba.
    unsafe -> cualquier indicador de regresión o impacto en errores.
    no-change -> diff vacío.
    """
    counts = diff.get("summary_counts") or {}
    if (
        counts.get("new", 0) == 0
        and counts.get("missing", 0) == 0
        and counts.get("regressed", 0) == 0
        and counts.get("severity_shifted", 0) == 0
        and counts.get("code_errors", 0) == 0
        and counts.get("policy_violations", 0) == 0
    ):
        return "no-change"

    if counts.get("code_errors", 0) > 0:
        return "unsafe"
    if counts.get("policy_violations", 0) > 0:
        return "unsafe"
    if counts.get("regressed", 0) > 0:
        return "unsafe"

    # Severity-shift hacia arriba (e.g. INFO -> ERROR) es unsafe.
    for entry in diff.get("severity_shifted") or []:
        prev = _SEVERITY_ORDER.get(str(entry.get("previous_severity") or "").upper(), 0)
        cur = _SEVERITY_ORDER.get(str(entry.get("current_severity") or "").upper(), 0)
        if cur > prev:
            return "unsafe"

    # Nuevos templates: sólo permitimos severidades "tranquilas".
    for entry in diff.get("new_templates") or []:
        sev = str(entry.get("severity_text") or "").upper()
        if sev and sev not in _SAFE_SEVERITIES:
            return "unsafe"

    return "safe"


def append_changelog(
    service_dir: Path,
    *,
    run_id: str,
    delta_kind: str,
    diff_counts: dict[str, Any],
    extra: str = "",
) -> None:
    """Añade una línea al CHANGELOG.md del servicio."""
    changelog = service_dir / "CHANGELOG.md"
    ts = datetime.now(timezone.utc).isoformat()
    lines = [
        f"## {ts}  ({delta_kind})  run={run_id}",
        "",
        f"- new: {diff_counts.get('new', 0)}  missing: {diff_counts.get('missing', 0)}  "
        f"regressed: {diff_counts.get('regressed', 0)}  "
        f"severity_shifted: {diff_counts.get('severity_shifted', 0)}  "
        f"code_errors: {diff_counts.get('code_errors', 0)}  "
        f"policy_violations: {diff_counts.get('policy_violations', 0)}",
    ]
    if extra:
        lines.append(f"- {extra}")
    lines.append("")
    body = "\n".join(lines) + "\n"
    if changelog.exists():
        body = body + changelog.read_text()
    changelog.write_text(body)


def commit_baselines(
    *,
    repo_root: Path,
    baselines_dir: Path,
    services: list[str],
    message: str,
    push: bool = False,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Stage + commit (+push) los cambios de baseline para los servicios dados.

    Default ``dry_run=True`` para que `ci-run` por defecto NUNCA toque git
    sin un flag explícito. El llamador debe poner ``dry_run=False`` cuando
    ``--commit-policy=safe-auto`` está activo Y el delta es ``safe``.
    """
    rel = baselines_dir.relative_to(repo_root) if baselines_dir.is_relative_to(repo_root) else baselines_dir
    paths_to_add: list[str] = []
    for svc in services:
        svc_rel = rel / svc
        if (repo_root / svc_rel).exists():
            paths_to_add.append(str(svc_rel))

    if not paths_to_add:
        return {"status": "no-paths"}

    def _run(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "-C", str(repo_root), *args],
            capture_output=True, text=True, check=False,
        )

    if dry_run:
        return {
            "status": "dry-run",
            "paths": paths_to_add,
            "message": message,
            "push": push,
        }

    add = _run("add", "--", *paths_to_add)
    if add.returncode != 0:
        return {"status": "error", "stage": "add", "stderr": add.stderr}

    status = _run("status", "--porcelain", "--", *paths_to_add)
    if not status.stdout.strip():
        return {"status": "clean", "paths": paths_to_add}

    commit = _run("commit", "-m", message)
    if commit.returncode != 0:
        return {"status": "error", "stage": "commit", "stderr": commit.stderr}

    if push:
        pushed = _run("push")
        if pushed.returncode != 0:
            return {"status": "committed-not-pushed", "stderr": pushed.stderr}

    return {"status": "committed", "paths": paths_to_add, "pushed": push}
