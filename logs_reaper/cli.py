from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .autodiscovery import finalize_service_scan, prepare_service_scan
from .compare import compare_runs
from .dataset import export_dataset
from .diff_engine import compute_diff, diff_to_table
from .io import resolve_inputs, write_json
from .registry import build_registry
from .scan import scan


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="logs-reaper", description="Parse, template, classify, and compare logs.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser("scan", help="Parse input logs and emit LogsReaper IR.")
    scan_parser.add_argument("--input", action="append", default=[], dest="inputs", help="Input glob, file, or dir.")
    scan_parser.add_argument("--run-id", default=None, help="Stable run id. Auto-generated when omitted.")
    scan_parser.add_argument("--service", default=None, help="Default service.name. If --input is omitted, autodetect from Docker.")
    scan_parser.add_argument("--lib", action="append", default=[], help="Library version as name=version. Repeatable.")
    scan_parser.add_argument("--rules", default=None, help="YAML rules catalog. Defaults to configs/default-rules.yaml.")
    scan_parser.add_argument("--baseline", default=None, help="Previous scan output directory used as expected baseline.")
    scan_parser.add_argument("--include-raw", action="store_true", help="Persist raw log records in events.parquet.")
    scan_parser.add_argument("--force", action="store_true", help="Reprocess even if the autodetected snapshot matches a previous scan.")
    scan_parser.add_argument("--out", default=None, help="Output directory. Defaults to ./out/<service>/<run_id>.")
    scan_parser.add_argument(
        "--since",
        default=None,
        help="When autodetecting from Docker, pass through to docker logs --since, for example 15m or 2026-05-14T11:00:00Z.",
    )
    scan_parser.add_argument(
        "--instances",
        default="last",
        help=(
            "Which service instance(s) (boots) to include: 'last' (default), 'all', or an integer N for the last N. "
            "Boots are detected from log markers like 'Starting gunicorn' or 'Booting worker with pid:'."
        ),
    )
    scan_parser.add_argument(
        "--focus",
        default="both",
        choices=["both", "code", "infra"],
        help=(
            "Which lens the report highlights: 'code' (engineer focus: tracebacks, code exceptions), "
            "'infra' (ops focus: kafka/mongo/network issues, connectivity timeline), or 'both' (default)."
        ),
    )
    scan_parser.set_defaults(func=_cmd_scan)

    compare_parser = subparsers.add_parser("compare", help="Compare two scan output directories.")
    compare_parser.add_argument("--left", required=True, help="Baseline/left scan output directory.")
    compare_parser.add_argument("--right", required=True, help="Candidate/right scan output directory.")
    compare_parser.add_argument("--out", required=True, help="Output markdown path or directory.")
    compare_parser.add_argument("--frequency-ratio", type=float, default=2.0)
    compare_parser.add_argument("--min-count", type=int, default=5)
    compare_parser.set_defaults(func=_cmd_compare)

    dataset_parser = subparsers.add_parser("export-dataset", help="Export event-level NDJSON for ML/debugging.")
    dataset_parser.add_argument("--input", required=True, help="Scan output directory.")
    dataset_parser.add_argument("--out", required=True, help="Output NDJSON path.")
    dataset_parser.add_argument("--include-body", action="store_true", help="Include original event body in dataset.")
    dataset_parser.set_defaults(func=_cmd_export_dataset)

    index_parser = subparsers.add_parser(
        "index",
        help="Refresh the cross-run registry, template registry and baseline parquet files.",
    )
    index_parser.add_argument(
        "--root",
        default=None,
        help="Root with one folder per scan run. Defaults to ./out.",
    )
    index_parser.add_argument(
        "--out",
        default=None,
        help="Output directory for registry/baseline parquet. Defaults to ./runs.",
    )
    index_parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Ignore prior state and reprocess every run from scratch.",
    )
    index_parser.add_argument(
        "--scenario-regex",
        default=None,
        help="Optional regex applied to run_id to extract a scenario name (named group 'scenario' or first group).",
    )
    index_parser.add_argument(
        "--min-green-runs",
        type=int,
        default=2,
        help="Minimum number of green runs per (service, scenario) cohort before a baseline row is emitted.",
    )
    index_parser.add_argument(
        "--baselines-dir",
        default=None,
        help="If given, partition the aggregated baseline by service into <baselines-dir>/<service>/.",
    )
    index_parser.set_defaults(func=_cmd_index)

    diff_parser = subparsers.add_parser(
        "diff",
        help="Compare a run against the baseline (use after `index`).",
    )
    diff_parser.add_argument("--run-dir", required=True, help="Scan output directory of the run under inspection.")
    diff_parser.add_argument(
        "--baseline",
        default=None,
        help="Path to baseline.parquet (defaults to ./runs/baseline.parquet).",
    )
    diff_parser.add_argument("--out", default=None, help="Output directory (defaults to <run-dir>).")
    diff_parser.add_argument("--z-threshold", type=float, default=3.0)
    diff_parser.add_argument("--min-observed-count", type=int, default=5)
    diff_parser.add_argument("--scenario", default=None, help="Override scenario derivation for the cohort lookup.")
    diff_parser.set_defaults(func=_cmd_diff)

    dashboard_parser = subparsers.add_parser(
        "dashboard",
        help="Launch the Streamlit dashboard against an existing registry.",
    )
    dashboard_parser.add_argument(
        "--registry",
        default=None,
        help="Registry directory (defaults to ./runs).",
    )
    dashboard_parser.add_argument(
        "--port",
        type=int,
        default=8501,
        help="Port for the Streamlit server (default 8501).",
    )
    dashboard_parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Bind address. Use 0.0.0.0 to expose on the LAN.",
    )
    dashboard_parser.set_defaults(func=_cmd_dashboard)

    tail_parser = subparsers.add_parser(
        "tail",
        help="Stream a growing log file and emit anomalies vs the baseline.",
    )
    tail_parser.add_argument("--input", required=True, help="Path to the (growing) log file to tail.")
    tail_parser.add_argument("--service", required=True, help="Service name used to look up the baseline cohort.")
    tail_parser.add_argument("--scenario", default=None, help="Scenario override (defaults to derive from run-id).")
    tail_parser.add_argument(
        "--baseline",
        default=None,
        help="baseline.parquet path (defaults to ./runs/baseline.parquet).",
    )
    tail_parser.add_argument("--out", default=None, help="Anomalies NDJSON output (default: stdout-only).")
    tail_parser.add_argument("--tick", type=float, default=1.0, help="Polling interval in seconds.")
    tail_parser.add_argument("--max-runtime", type=float, default=None, help="Stop after N seconds.")
    tail_parser.add_argument(
        "--stop-on-eof",
        type=int,
        default=None,
        help="Stop after N idle ticks (no new anomalies and file not growing).",
    )
    tail_parser.add_argument("--run-id", default="TAIL", help="Run id used by the scanner (default: TAIL).")
    tail_parser.add_argument("--z-threshold", type=float, default=3.0)
    tail_parser.add_argument("--min-observed-count", type=int, default=5)
    tail_parser.set_defaults(func=_cmd_tail)

    collect_parser = subparsers.add_parser(
        "collect",
        help="Stream `docker logs -f` from one or more services into per-service files.",
    )
    collect_parser.add_argument(
        "--services",
        required=True,
        help="Comma-separated logical service names, or 'all' for auto-detect (sm-*-1).",
    )
    collect_parser.add_argument(
        "--out",
        required=True,
        help="Output directory for per-service .log files (one per service).",
    )
    collect_parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Stop after N seconds. If omitted, run until SIGINT/SIGTERM.",
    )
    collect_parser.add_argument("--prefix", default="sm", help="Container name prefix (default 'sm').")
    collect_parser.add_argument("--suffix", default="-1", help="Container name suffix (default '-1').")
    collect_parser.add_argument("--tail", type=int, default=0, help="docker logs --tail (default 0 = future only).")
    collect_parser.add_argument(
        "--stats-port",
        type=int,
        default=None,
        help="If set, expose a runtime stats dashboard at http://0.0.0.0:<port>/ during collection.",
    )
    collect_parser.add_argument("--stats-host", default="0.0.0.0", help="Bind address for stats server.")
    collect_parser.add_argument(
        "--keep-alive",
        action="store_true",
        help="After --duration ends, keep the stats HTTP server alive until SIGTERM/SIGINT so the dashboard stays reachable.",
    )
    collect_parser.add_argument(
        "--snapshot-path",
        default=None,
        help="Path to write a stats_snapshot.json refreshed every --snapshot-interval seconds (read by the Streamlit dashboard).",
    )
    collect_parser.add_argument("--snapshot-interval", type=float, default=1.0, help="Seconds between snapshot writes.")
    collect_parser.add_argument(
        "--discover-interval",
        type=float,
        default=0.0,
        help="If > 0, re-run docker discovery every N seconds and stream new sm-*-1 containers as they appear.",
    )
    collect_parser.set_defaults(func=_cmd_collect)

    ci_parser = subparsers.add_parser(
        "ci-run",
        help="End-to-end CI pipeline: collect logs, scan, index, diff, report-html, optional commit.",
    )
    ci_parser.add_argument("--services", required=True, help="'all' or comma-separated logical service names.")
    ci_parser.add_argument("--out", required=True, help="Root output directory (logs+out+runs+report).")
    ci_parser.add_argument(
        "--baselines-dir",
        default=None,
        help="Where to partition baselines per service. Defaults to <repo>/./baselines.",
    )
    ci_parser.add_argument("--duration", type=float, required=True, help="Seconds to collect logs (suite runs in parallel).")
    ci_parser.add_argument(
        "--commit-policy",
        choices=["off", "safe-auto"],
        default="off",
        help="Whether to auto-commit safe baseline deltas. Default off (artifact-only).",
    )
    ci_parser.add_argument("--repo-root", default=None, help="Git repo root. Required for safe-auto commit.")
    ci_parser.add_argument("--push", action="store_true", help="git push after committing. Default off.")
    ci_parser.add_argument("--run-id-prefix", default="CI", help="Prefix for the generated run_id.")
    ci_parser.add_argument(
        "--stats-port",
        type=int,
        default=None,
        help="Expose the live ingestion dashboard at http://0.0.0.0:<port>/ during collect phase.",
    )
    ci_parser.set_defaults(func=_cmd_ci_run)

    live_parser = subparsers.add_parser(
        "live",
        help="Run collect (background) + Streamlit dashboard (foreground) — the unified live UI.",
    )
    live_parser.add_argument("--services", required=True, help="'all' or comma-separated logical names.")
    live_parser.add_argument("--out", required=True, help="Output root (logs+runs+snapshot land here).")
    live_parser.add_argument("--duration", type=float, default=None, help="Stop collect after N seconds (server stays up).")
    live_parser.add_argument("--streamlit-port", type=int, default=9110, help="Public Streamlit port.")
    live_parser.add_argument("--streamlit-host", default="0.0.0.0", help="Bind address for Streamlit.")
    live_parser.add_argument("--stats-port", type=int, default=None, help="Optional internal HTTP /api/stats port.")
    live_parser.add_argument("--snapshot-interval", type=float, default=1.0)
    live_parser.add_argument(
        "--no-keep-alive",
        dest="keep_alive",
        action="store_false",
        default=True,
        help="Exit when collect finishes (default keeps dashboard alive after duration ends).",
    )
    live_parser.add_argument("--registry", default=None, help="Registry directory (defaults to <out>/runs).")
    live_parser.add_argument(
        "--baselines-dir",
        default=None,
        help="Where to partition baselines per service after each index pass.",
    )
    live_parser.add_argument(
        "--auto-index-interval",
        type=float,
        default=5.0,
        help="Seconds between automatic scan+index passes. 0 disables auto-indexing.",
    )
    live_parser.add_argument(
        "--auto-index-min-green-runs",
        type=int,
        default=1,
        help="min_green_runs threshold for the baseline (default 1 so boot scans count).",
    )
    live_parser.add_argument(
        "--initial-tail",
        type=int,
        default=2000,
        help="docker logs --tail N at startup so silent services still get a boot baseline.",
    )
    live_parser.add_argument(
        "--discover-interval",
        type=float,
        default=10.0,
        help="Re-run service auto-discovery every N seconds and stream new containers (default 10s; 0 disables).",
    )
    live_parser.set_defaults(func=_cmd_live)

    report_parser = subparsers.add_parser(
        "report-html",
        help="Render a single-file HTML report from an existing ci_summary.json.",
    )
    report_parser.add_argument("--summary", required=True, help="Path to ci_summary.json produced by ci-run.")
    report_parser.add_argument("--out", required=True, help="Output HTML path.")
    report_parser.set_defaults(func=_cmd_report_html)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except Exception as exc:  # pragma: no cover - CLI guard
        print(f"logs-reaper: error: {exc}", file=sys.stderr)
        return 1
    return 0


def _cmd_scan(args: argparse.Namespace) -> None:
    if not args.inputs and not args.service:
        raise ValueError("scan requires either --input or --service")
    prepared = prepare_service_scan(
        service_name=args.service or "unknown-service",
        input_patterns=args.inputs or None,
        out_dir=args.out,
        run_id=args.run_id,
        since=args.since,
        force_reprocess=args.force,
    )
    autodiscovery = prepared["autodiscovery"]
    if autodiscovery and autodiscovery.get("reused_existing_scan"):
        print(
            f"Reused existing scan {prepared['run_id']} at {prepared['out_dir']} "
            f"({autodiscovery['status']}, fingerprint={autodiscovery['fingerprint']})"
        )
        return
    baseline_dir = args.baseline or prepared.get("baseline_dir")
    result = scan(
        input_patterns=prepared["input_patterns"],
        run_id=prepared["run_id"],
        out_dir=prepared["out_dir"],
        service_name=args.service,
        lib_versions=_parse_lib_versions(args.lib),
        rules_path=args.rules,
        baseline_dir=baseline_dir,
        include_raw=args.include_raw,
        autodiscovery=autodiscovery,
        instances=args.instances,
        focus=args.focus,
    )
    finalize_service_scan(
        service_name=args.service or "unknown-service",
        run_id=prepared["run_id"],
        scan_out_dir=result["out_dir"],
        autodiscovery=autodiscovery,
        summary=result["summary"],
    )
    if autodiscovery:
        baseline_msg = f", baseline={baseline_dir}" if baseline_dir else ""
        print(
            f"Wrote LogsReaper IR to {result['out_dir']} "
            f"({autodiscovery['status']}, source={autodiscovery['mode']}{baseline_msg})"
        )
    else:
        print(f"Wrote LogsReaper IR to {result['out_dir']}")


def _cmd_compare(args: argparse.Namespace) -> None:
    payload = compare_runs(
        left_dir=args.left,
        right_dir=args.right,
        out=args.out,
        frequency_ratio=args.frequency_ratio,
        min_count=args.min_count,
    )
    out_path = Path(args.out)
    md_path = out_path if out_path.suffix.lower() == ".md" else out_path / "diff.md"
    print(
        f"Wrote diff to {md_path} "
        f"({payload['regression_count']} regressions, {payload['fixed_error_count']} fixed errors)"
    )


def _cmd_export_dataset(args: argparse.Namespace) -> None:
    count = export_dataset(input_dir=args.input, out=args.out, include_body=args.include_body)
    print(f"Wrote {count} dataset rows to {args.out}")


def _cmd_diff(args: argparse.Namespace) -> None:
    import pyarrow.parquet as pq

    run_dir = Path(args.run_dir)
    baseline = (
        Path(args.baseline)
        if args.baseline
        else Path(__file__).resolve().parents[1] / "runs" / "baseline.parquet"
    )
    out_dir = Path(args.out) if args.out else run_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    diff = compute_diff(
        run_dir=run_dir,
        baseline_path=baseline,
        z_threshold=args.z_threshold,
        min_observed_count=args.min_observed_count,
        scenario_override=args.scenario,
    )
    table = diff_to_table(diff)
    pq.write_table(table, out_dir / "diff.parquet", compression="zstd", use_dictionary=True)
    write_json(out_dir / "diff.json", diff)
    counts = diff["summary_counts"]
    print(
        f"diff vs baseline ({diff['baseline_status']}, cohort={diff['baseline_cohort_size']}): "
        f"new={counts['new']} missing={counts['missing']} regressed={counts['regressed']} "
        f"severity_shifted={counts['severity_shifted']} connectivity={counts['connectivity_regressions']} "
        f"code_errors={counts.get('code_errors', 0)} ({counts.get('code_error_events', 0)} events)"
    )
    code_errors = diff.get("code_errors") or []
    if code_errors:
        print("\nTop code errors:")
        for entry in code_errors[:5]:
            template = (entry.get("normalized_template") or "").strip().replace("\n", " ")
            print(
                f"  [{entry['observed_count']:>4}x] {entry.get('severity_text','?'):8s} "
                f"{'NEW' if entry.get('is_new') else 'REG'}: {template[:120]}"
            )


def _cmd_dashboard(args: argparse.Namespace) -> None:
    import os
    import subprocess

    registry = Path(args.registry) if args.registry else Path(__file__).resolve().parents[1] / "runs"
    if not (registry / "registry.parquet").exists():
        raise FileNotFoundError(
            f"No registry.parquet found in {registry}. Run `logs-reaper index` first."
        )
    dashboard_script = Path(__file__).resolve().parent / "dashboard.py"
    env = os.environ.copy()
    env["LOGS_REAPER_REGISTRY"] = str(registry)
    cmd = [
        "python3",
        "-m",
        "streamlit",
        "run",
        str(dashboard_script),
        "--server.port",
        str(args.port),
        "--server.address",
        args.host,
        "--server.headless",
        "true",
        "--browser.gatherUsageStats",
        "false",
    ]
    print(f"Launching dashboard on http://{args.host}:{args.port} (registry={registry})")
    subprocess.run(cmd, env=env, check=False)


def _cmd_tail(args: argparse.Namespace) -> None:
    from .tail import TailConfig, TailRunner

    baseline = (
        Path(args.baseline)
        if args.baseline
        else Path(__file__).resolve().parents[1] / "runs" / "baseline.parquet"
    )
    config = TailConfig(
        input_path=Path(args.input),
        service_name=args.service,
        baseline_path=baseline,
        scenario=args.scenario,
        z_threshold=args.z_threshold,
        min_observed_count=args.min_observed_count,
        out_path=Path(args.out) if args.out else None,
        tick_seconds=args.tick,
        max_runtime_seconds=args.max_runtime,
        stop_on_eof_idle_ticks=args.stop_on_eof,
        run_id=args.run_id,
    )
    with TailRunner(config) as runner:
        print(
            f"Tailing {args.input} (service={args.service}, scenario={runner.scenario}, "
            f"baseline_size={len(runner.baseline_for_cohort)}, tick={args.tick}s)"
        )
        state = runner.run()
        print(
            f"Tail finished after {state.ticks_completed} ticks, "
            f"{state.events_processed} events, {len(state.seen_anomalies)} anomalies."
        )


def _cmd_index(args: argparse.Namespace) -> None:
    root = Path(args.root) if args.root else Path(__file__).resolve().parents[1] / "out"
    out_dir = Path(args.out) if args.out else Path(__file__).resolve().parents[1] / "runs"
    summary = build_registry(
        runs_root=root,
        out_dir=out_dir,
        rebuild=args.rebuild,
        scenario_regex=args.scenario_regex,
        min_green_runs=args.min_green_runs,
    )
    print(
        f"Indexed {summary['runs_total']} runs ({summary['runs_new_or_changed']} new/changed, "
        f"{summary['runs_skipped_unchanged']} unchanged) -> {summary['out_dir']} "
        f"[templates={summary['templates_total']}, baseline_rows={summary['baseline_rows']}]"
    )
    if args.baselines_dir:
        from .baselines import partition_baselines

        result = partition_baselines(
            aggregate_dir=out_dir,
            baselines_dir=Path(args.baselines_dir),
        )
        services = result.get("services") or []
        print(
            f"Partitioned baseline into {len(services)} service folder(s) "
            f"under {result['baselines_dir']}"
        )


def _cmd_collect(args: argparse.Namespace) -> None:
    from .collect import CollectConfig, collect, resolve_services

    services = resolve_services(args.services)
    if not services and getattr(args, "discover_interval", 0.0) <= 0:
        raise ValueError(
            "collect: no services resolved. Pass --services name1,name2 or 'all' "
            "when docker containers <prefix>-*-<suffix> are running. "
            "Alternatively pass --discover-interval N to wait for containers to appear."
        )
    cfg = CollectConfig(
        services=services,
        out_dir=Path(args.out),
        duration_seconds=args.duration,
        container_prefix=args.prefix,
        container_suffix=args.suffix,
        tail_initial=args.tail,
        stats_port=args.stats_port,
        stats_host=args.stats_host,
        keep_alive=args.keep_alive,
        snapshot_path=Path(args.snapshot_path) if args.snapshot_path else None,
        snapshot_interval=args.snapshot_interval,
        discover_interval=getattr(args, "discover_interval", 0.0),
    )
    print(f"Collecting {len(services)} service(s) into {cfg.out_dir}: {', '.join(services)}")
    stats = collect(cfg)
    total_bytes = sum(stats.bytes_per_service.values())
    total_lines = sum(stats.lines_per_service.values())
    print(
        f"Collected {total_bytes} bytes / {total_lines} lines across {len(services)} service(s) "
        f"in {(stats.finished_at or 0) - stats.started_at:.1f}s"
    )


def _cmd_ci_run(args: argparse.Namespace) -> None:
    from .ci_run import run_ci

    repo_root = Path(args.repo_root).resolve() if args.repo_root else None
    default_baselines = (
        repo_root / "tools" / "LogsReaper" / "baselines"
        if repo_root
        else Path(__file__).resolve().parents[1] / "baselines"
    )
    baselines_dir = Path(args.baselines_dir) if args.baselines_dir else default_baselines

    if args.commit_policy == "safe-auto" and repo_root is None:
        raise ValueError("--commit-policy=safe-auto requires --repo-root pointing to the git work tree.")

    summary = run_ci(
        services_spec=args.services,
        out_root=Path(args.out),
        baselines_dir=baselines_dir,
        duration_seconds=args.duration,
        commit_policy=args.commit_policy,
        repo_root=repo_root,
        run_id_prefix=args.run_id_prefix,
        push=args.push,
        stats_port=args.stats_port,
    )
    print(f"ci-run finished. run_id={summary['run_id']} services={summary['services']}")
    for svc, info in summary["per_service"].items():
        print(f"  {svc:>20s}  {info['delta_kind']:>9s}  {info['summary_counts']}")
    print(f"  report:  {summary.get('report_html')}")
    print(f"  commit:  {summary['commit_result']}")


def _cmd_live(args: argparse.Namespace) -> None:
    from .live import run_live

    registry_dir = Path(args.registry) if args.registry else Path(args.out) / "runs"
    baselines_dir = Path(args.baselines_dir) if args.baselines_dir else None
    rc = run_live(
        services=args.services,
        out_dir=Path(args.out),
        duration=args.duration,
        streamlit_port=args.streamlit_port,
        streamlit_host=args.streamlit_host,
        stats_port=args.stats_port,
        snapshot_interval=args.snapshot_interval,
        keep_alive=args.keep_alive,
        registry_dir=registry_dir,
        baselines_dir=baselines_dir,
        auto_index_interval=args.auto_index_interval,
        auto_index_min_green_runs=args.auto_index_min_green_runs,
        initial_tail=args.initial_tail,
        discover_interval=args.discover_interval,
    )
    raise SystemExit(rc)


def _cmd_report_html(args: argparse.Namespace) -> None:
    from .report_html import write_report_html

    summary = read_json_path(args.summary)
    out = write_report_html(
        out_path=Path(args.out),
        run_id=summary.get("run_id", "?"),
        per_service=summary.get("per_service", {}),
        collect_stats=summary.get("collect", {}),
        registry_dir=Path(summary.get("index", {}).get("out_dir", "?")),
    )
    print(f"Wrote {out}")


def read_json_path(path: str) -> dict:
    import json as _json
    return _json.loads(Path(path).read_text())


def _parse_lib_versions(values: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"--lib expects name=version, got {value!r}")
        name, version = value.split("=", 1)
        result[name.strip()] = version.strip()
    return result


if __name__ == "__main__":
    raise SystemExit(main())
