from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare LogsReaper and drain3 throughput on the same NDJSON input.")
    parser.add_argument("--input", required=True, help="NDJSON input file.")
    parser.add_argument("--service", default="synthetic")
    parser.add_argument("--message-key", default="message")
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--drain3-python", default="/tmp/logsreaper-bench-venv/bin/python")
    parser.add_argument("--out-json", default=None)
    parser.add_argument("--out-md", default=None)
    parser.add_argument("--drain-depth", type=int, default=None)
    parser.add_argument("--drain-sim-th", type=float, default=None)
    parser.add_argument("--drain-max-children", type=int, default=None)
    return parser


def _run_trial(
    *,
    python_bin: str,
    engine: str,
    input_path: Path,
    service: str,
    message_key: str,
    drain_depth: int | None,
    drain_sim_th: float | None,
    drain_max_children: int | None,
    trial_label: str,
) -> dict[str, object]:
    script_path = Path(__file__).with_name("engine_bench.py")
    cmd = [
        python_bin,
        str(script_path),
        "--engine",
        engine,
        "--input",
        str(input_path),
        "--service",
        service,
        "--run-id",
        f"BENCH_{engine.upper()}",
        "--message-key",
        message_key,
        "--trial-label",
        trial_label,
    ]
    if drain_depth is not None:
        cmd.extend(["--drain-depth", str(drain_depth)])
    if drain_sim_th is not None:
        cmd.extend(["--drain-sim-th", str(drain_sim_th)])
    if drain_max_children is not None:
        cmd.extend(["--drain-max-children", str(drain_max_children)])
    completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def _summarize(samples: list[dict[str, object]]) -> dict[str, object]:
    mbps = [float(item["throughput_mb_per_second"]) for item in samples]
    eps = [float(item["events_per_second"]) for item in samples]
    rss = [float(item["max_rss_mb"]) for item in samples]
    elapsed = [float(item["elapsed_seconds"]) for item in samples]
    template_counts = [int(item["template_count"]) for item in samples]
    event_counts = [int(item["event_count"]) for item in samples]
    return {
        "trials": len(samples),
        "throughput_mb_per_second_mean": statistics.mean(mbps),
        "throughput_mb_per_second_stdev": statistics.stdev(mbps) if len(mbps) > 1 else 0.0,
        "events_per_second_mean": statistics.mean(eps),
        "events_per_second_stdev": statistics.stdev(eps) if len(eps) > 1 else 0.0,
        "max_rss_mb_mean": statistics.mean(rss),
        "max_rss_mb_stdev": statistics.stdev(rss) if len(rss) > 1 else 0.0,
        "elapsed_seconds_mean": statistics.mean(elapsed),
        "elapsed_seconds_stdev": statistics.stdev(elapsed) if len(elapsed) > 1 else 0.0,
        "template_count_mean": statistics.mean(template_counts),
        "event_count_mean": statistics.mean(event_counts),
        "raw_samples": samples,
    }


def _markdown_report(payload: dict[str, object]) -> str:
    run = payload["run"]
    lr = payload["engines"]["logsreaper"]
    dr = payload["engines"]["drain3"]
    speedup = lr["throughput_mb_per_second_mean"] / max(dr["throughput_mb_per_second_mean"], 1e-9)
    return "\n".join(
        [
            "# LogsReaper vs drain3",
            "",
            f"- Date: {run['created_at']}",
            f"- Machine: {run['machine']}",
            f"- Input: `{run['input_path']}`",
            f"- Input bytes: {run['input_bytes']}",
            f"- Warmup trials: {run['warmup']}",
            f"- Measured trials: {run['trials']}",
            "",
            "## Scope",
            "",
            "- `LogsReaper`: Rust hot path via `logs_reaper.rust_engine.scan_file_to_ipc`.",
            f"- `drain3`: Python `TemplateMiner.add_log_message()` over `{run['message_key']}` after NDJSON `json.loads`.",
            "- This is not a full feature-parity comparison: Drain3 is a template miner, while LogsReaper also parses and aggregates structured output.",
            "",
            "## Results",
            "",
            "| Engine | MB/s mean | MB/s stdev | Events/s mean | RSS MB mean | Template count mean |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
            f"| LogsReaper | {lr['throughput_mb_per_second_mean']:.2f} | {lr['throughput_mb_per_second_stdev']:.2f} | {lr['events_per_second_mean']:.0f} | {lr['max_rss_mb_mean']:.1f} | {lr['template_count_mean']:.0f} |",
            f"| drain3 | {dr['throughput_mb_per_second_mean']:.2f} | {dr['throughput_mb_per_second_stdev']:.2f} | {dr['events_per_second_mean']:.0f} | {dr['max_rss_mb_mean']:.1f} | {dr['template_count_mean']:.0f} |",
            "",
            f"- Throughput ratio `LogsReaper / drain3`: {speedup:.2f}x",
            "",
        ]
    )


def main() -> int:
    args = _build_parser().parse_args()
    input_path = Path(args.input).resolve()
    created_at = datetime.now(timezone.utc).isoformat()
    engines = {
        "logsreaper": {"python": sys.executable, "samples": []},
        "drain3": {"python": args.drain3_python, "samples": []},
    }
    for engine_name, meta in engines.items():
        for idx in range(args.warmup):
            _run_trial(
                python_bin=meta["python"],
                engine=engine_name,
                input_path=input_path,
                service=args.service,
                message_key=args.message_key,
                drain_depth=args.drain_depth,
                drain_sim_th=args.drain_sim_th,
                drain_max_children=args.drain_max_children,
                trial_label=f"warmup-{idx + 1}",
            )
        for idx in range(args.trials):
            sample = _run_trial(
                python_bin=meta["python"],
                engine=engine_name,
                input_path=input_path,
                service=args.service,
                message_key=args.message_key,
                drain_depth=args.drain_depth,
                drain_sim_th=args.drain_sim_th,
                drain_max_children=args.drain_max_children,
                trial_label=f"measured-{idx + 1}",
            )
            meta["samples"].append(sample)

    payload = {
        "run": {
            "created_at": created_at,
            "machine": platform.platform(),
            "python_logsreaper": sys.executable,
            "python_drain3": args.drain3_python,
            "input_path": str(input_path),
            "input_bytes": input_path.stat().st_size,
            "warmup": args.warmup,
            "trials": args.trials,
            "message_key": args.message_key,
        },
        "engines": {
            "logsreaper": _summarize(engines["logsreaper"]["samples"]),
            "drain3": _summarize(engines["drain3"]["samples"]),
        },
    }
    markdown = _markdown_report(payload)
    if args.out_json:
        Path(args.out_json).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.out_md:
        Path(args.out_md).write_text(markdown + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
