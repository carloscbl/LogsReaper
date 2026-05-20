from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--target-mb", type=int, default=128)
    args = parser.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    target = args.target_mb * 1024 * 1024
    written = 0
    index = 0
    with out.open("w", encoding="utf-8") as handle:
        while written < target:
            payload = {
                "time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "level": "ERROR" if index % 100 == 0 else "INFO",
                "message": f"request {index} from 10.0.{index % 255}.{index % 31} completed in {index % 500} ms",
                "microservice": "synthetic",
                "worker_id": f"worker-{index % 8}",
                "threadName": f"Thread-{index % 32}",
            }
            line = json.dumps(payload, sort_keys=True) + "\n"
            handle.write(line)
            written += len(line.encode("utf-8"))
            index += 1
    print(f"wrote {index} rows to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
