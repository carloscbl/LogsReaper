"""ci-run — orquestador para entornos CI.

Pensado para correr DENTRO de la imagen logs-reaper, con el socket de docker
montado. Coordina:

1. ``collect`` (streaming docker logs -f en background).
2. Espera ``--duration`` segundos (el CI corre la suite E2E en paralelo).
3. ``scan`` por servicio sobre los logs recolectados.
4. ``index`` global + particionado a ``baselines/<service>/``.
5. ``diff`` por servicio contra el baseline previo.
6. ``report-html`` agregado.
7. Opcionalmente, commit del delta a git si la política es ``safe-auto`` y
   ``classify_delta`` devuelve ``safe`` para todos los servicios afectados.

Salidas: ``<out>/runs/<run_id>/`` con artifacts del scan, ``<out>/report.html``
y ``<out>/ci_summary.json`` con el resumen agregado.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .baselines import (
    append_changelog,
    classify_delta,
    commit_baselines,
    partition_baselines,
)
from .collect import CollectConfig, collect, resolve_services
from .diff_engine import compute_diff
from .registry import build_registry
from .scan import scan as scan_run


def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def _generate_run_id(prefix: str) -> str:
    return f"{prefix}_{int(time.time())}"


def _scan_service(
    *, service: str, log_path: Path, out_dir: Path, run_id: str
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    return scan_run(
        input_patterns=[str(log_path)],
        run_id=run_id,
        out_dir=str(out_dir),
        service_name=service,
        lib_versions={},
        rules_path=None,
        baseline_dir=None,
        include_raw=False,
        autodiscovery=None,
        instances="last",
        focus="both",
    )


def run_ci(
    *,
    services_spec: str,
    out_root: Path,
    baselines_dir: Path,
    duration_seconds: float,
    commit_policy: str = "off",
    repo_root: Path | None = None,
    run_id_prefix: str = "CI",
    push: bool = False,
    stats_port: int | None = None,
) -> dict[str, Any]:
    """Pipeline completo. Devuelve un summary JSON-serialisable."""
    services = resolve_services(services_spec)
    if not services:
        raise ValueError("ci-run: no services resolved (try --services all with sm-* containers running)")

    out_root.mkdir(parents=True, exist_ok=True)
    run_id = _generate_run_id(run_id_prefix)
    logs_dir = out_root / "logs" / run_id
    scans_dir = out_root / "out"          # raíz que `index` walk-ea
    registry_dir = out_root / "runs"
    report_path = out_root / "report.html"

    # 1) Collect
    print(f"[ci-run] services={services} duration={duration_seconds}s logs_dir={logs_dir}", flush=True)
    collect_cfg = CollectConfig(
        services=services,
        out_dir=logs_dir,
        duration_seconds=duration_seconds,
        metadata={"run_id": run_id, "started_at": _ts()},
        stats_port=stats_port,
    )
    collect_stats = collect(collect_cfg)

    # 2) Scan por servicio
    scan_results: dict[str, dict[str, Any]] = {}
    for svc in services:
        log_file = logs_dir / f"{svc}.log"
        if not log_file.exists() or log_file.stat().st_size == 0:
            print(f"[ci-run] skipping scan for {svc}: no logs collected")
            continue
        scan_out = scans_dir / svc / run_id
        print(f"[ci-run] scanning {svc} -> {scan_out}", flush=True)
        scan_results[svc] = _scan_service(
            service=svc, log_path=log_file, out_dir=scan_out, run_id=run_id,
        )

    # 3) Index agregado + partición por servicio
    print(f"[ci-run] indexing under {registry_dir} and partitioning to {baselines_dir}", flush=True)
    index_summary = build_registry(runs_root=scans_dir, out_dir=registry_dir)
    partition_summary = partition_baselines(
        aggregate_dir=registry_dir,
        baselines_dir=baselines_dir,
        services=services,
    )

    # 4) Diff por servicio contra el baseline (recién recompuesto).
    per_service: dict[str, dict[str, Any]] = {}
    for svc, scan_meta in scan_results.items():
        run_dir = Path(scan_meta["out_dir"])
        diff = compute_diff(
            run_dir=run_dir,
            baseline_path=registry_dir / "baseline.parquet",
            overrides_dir=registry_dir,
        )
        kind = classify_delta(diff)
        per_service[svc] = {
            "run_dir": str(run_dir),
            "delta_kind": kind,
            "summary_counts": diff.get("summary_counts", {}),
            "code_errors_top": (diff.get("code_errors") or [])[:3],
            "policy_violations_top": (diff.get("policy_violations") or [])[:3],
        }
        append_changelog(
            baselines_dir / svc,
            run_id=run_id,
            delta_kind=kind,
            diff_counts=diff.get("summary_counts", {}),
        )

    # 5) Report HTML agregado
    try:
        from .report_html import write_report_html
        write_report_html(
            out_path=report_path,
            run_id=run_id,
            per_service=per_service,
            collect_stats=collect_stats.to_dict(),
            registry_dir=registry_dir,
        )
        report_written = True
    except Exception as exc:  # pragma: no cover - reporte es best-effort
        print(f"[ci-run] WARN: report-html failed: {exc}", flush=True)
        report_written = False

    # 6) Commit policy
    commit_result: dict[str, Any] = {"status": "off"}
    if commit_policy == "safe-auto" and repo_root is not None:
        safe_services = [s for s, info in per_service.items() if info["delta_kind"] == "safe"]
        if safe_services:
            message = (
                f"chore(baselines): {len(safe_services)} service(s) safe-update [{run_id}]\n\n"
                + "\n".join(
                    f"- {s}: new={per_service[s]['summary_counts'].get('new',0)} "
                    f"templates"
                    for s in safe_services
                )
            )
            commit_result = commit_baselines(
                repo_root=repo_root,
                baselines_dir=baselines_dir,
                services=safe_services,
                message=message,
                push=push,
                dry_run=False,
            )
        else:
            commit_result = {"status": "skip", "reason": "no safe deltas"}

    summary = {
        "run_id": run_id,
        "started_at": collect_stats.started_at,
        "finished_at": collect_stats.finished_at,
        "services": services,
        "duration_seconds": duration_seconds,
        "collect": collect_stats.to_dict(),
        "index": index_summary,
        "partition": partition_summary,
        "per_service": per_service,
        "report_html": str(report_path) if report_written else None,
        "commit_policy": commit_policy,
        "commit_result": commit_result,
    }
    (out_root / "ci_summary.json").write_text(json.dumps(summary, indent=2, default=str))
    _notify_ci_summary(per_service, report_path if report_written else None, run_id)
    return summary


def _notify_ci_summary(
    per_service: dict[str, dict[str, Any]],
    report_path: Path | None,
    run_id: str,
) -> None:
    """Desktop notification al final del pipeline si hay deltas no-safe."""
    flagged: list[str] = []
    for svc, info in per_service.items():
        kind = info.get("delta_kind") or "unknown"
        counts = info.get("summary_counts") or {}
        new_n = int(counts.get("new") or 0)
        code_n = len(info.get("code_errors_top") or [])
        if kind == "safe" and new_n == 0 and code_n == 0:
            continue
        bits = [f"delta={kind}"]
        if new_n:
            bits.append(f"+{new_n} new")
        if code_n:
            bits.append(f"{code_n} code-errors")
        flagged.append(f"{svc}: {', '.join(bits)}")
    if not flagged:
        return
    try:
        from .notifier import notify_unexpected_errors
        notify_unexpected_errors(
            title=f"LogsReaper {run_id}: {len(flagged)} servicio(s) con deltas no-safe",
            summary_lines=flagged,
            report_path=report_path,
            cooldown_key=f"ci-run:{run_id}",
            urgency="critical",
        )
    except Exception as exc:  # pragma: no cover - best-effort
        print(f"[ci-run] WARN: notifier failed: {exc}", flush=True)
