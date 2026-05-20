from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one benchmark trial for a single engine.")
    parser.add_argument("--engine", required=True, choices=["logsreaper", "drain3"])
    parser.add_argument("--input", required=True, help="NDJSON input file.")
    parser.add_argument("--service", default="synthetic")
    parser.add_argument("--run-id", default="BENCH")
    parser.add_argument("--message-key", default="message")
    parser.add_argument("--drain-depth", type=int, default=None)
    parser.add_argument("--drain-sim-th", type=float, default=None)
    parser.add_argument("--drain-max-children", type=int, default=None)
    parser.add_argument("--trial-label", default="trial")
    return parser


def _max_rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def _run_logsreaper(input_path: Path, service: str, run_id: str) -> dict[str, object]:
    from logs_reaper.rust_engine import scan_file_to_ipc

    started = perf_counter()
    with tempfile.TemporaryDirectory(prefix="logsreaper-bench-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        summary = scan_file_to_ipc(
            input_path=input_path,
            events_out=tmp_path / "events.arrow",
            templates_out=tmp_path / "templates.arrow",
            service_name=service,
            run_id=run_id,
            observed_timestamp=datetime.now(timezone.utc).isoformat(),
            include_raw=False,
            start_offset=0,
        )
    elapsed = perf_counter() - started
    input_bytes = int(summary.get("input_bytes") or input_path.stat().st_size)
    event_count = int(summary.get("event_count") or 0)
    template_count = int(summary.get("template_count") or 0)
    return {
        "engine": "logsreaper",
        "elapsed_seconds": elapsed,
        "input_bytes": input_bytes,
        "event_count": event_count,
        "template_count": template_count,
        "throughput_mb_per_second": (input_bytes / (1024 * 1024)) / max(elapsed, 1e-9),
        "events_per_second": event_count / max(elapsed, 1e-9),
        "max_rss_mb": _max_rss_mb(),
    }


def _run_drain3(
    input_path: Path,
    message_key: str,
    drain_depth: int | None,
    drain_sim_th: float | None,
    drain_max_children: int | None,
) -> dict[str, object]:
    from drain3.template_miner import TemplateMiner
    from drain3.template_miner_config import TemplateMinerConfig

    config = TemplateMinerConfig()
    config.profiling_enabled = False
    if drain_depth is not None:
        config.drain_depth = drain_depth
    if drain_sim_th is not None:
        config.drain_sim_th = drain_sim_th
    if drain_max_children is not None:
        config.drain_max_children = drain_max_children

    miner = TemplateMiner(config=config)
    started = perf_counter()
    event_count = 0
    with input_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            message = payload.get(message_key)
            if not isinstance(message, str):
                continue
            miner.add_log_message(message)
            event_count += 1
    elapsed = perf_counter() - started
    input_bytes = input_path.stat().st_size
    cluster_count = len(miner.drain.clusters)
    return {
        "engine": "drain3",
        "elapsed_seconds": elapsed,
        "input_bytes": input_bytes,
        "event_count": event_count,
        "template_count": cluster_count,
        "throughput_mb_per_second": (input_bytes / (1024 * 1024)) / max(elapsed, 1e-9),
        "events_per_second": event_count / max(elapsed, 1e-9),
        "max_rss_mb": _max_rss_mb(),
        "drain_config": {
            "drain_depth": config.drain_depth,
            "drain_sim_th": config.drain_sim_th,
            "drain_max_children": config.drain_max_children,
            "parametrize_numeric_tokens": config.parametrize_numeric_tokens,
        },
    }


def main() -> int:
    args = _build_parser().parse_args()
    input_path = Path(args.input).resolve()
    os.environ.setdefault("PYTHONHASHSEED", "0")
    if args.engine == "logsreaper":
        result = _run_logsreaper(input_path, args.service, args.run_id)
    else:
        result = _run_drain3(
            input_path,
            args.message_key,
            args.drain_depth,
            args.drain_sim_th,
            args.drain_max_children,
        )
    result["trial_label"] = args.trial_label
    result["input_path"] = str(input_path)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
