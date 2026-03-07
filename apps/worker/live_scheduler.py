from __future__ import annotations

import argparse
import json
import time
from datetime import datetime

from apps.api.service import PlatformService


def run_scheduler_once(service: PlatformService, *, max_jobs: int = 10) -> list[dict]:
    started = datetime.utcnow().isoformat()
    print(f"[live-scheduler] cycle_start={started} max_jobs={max_jobs}")
    results = service.run_live_poll_cycle(max_jobs=max_jobs)
    completed = sum(1 for r in results if str(r.get("status")) == "completed")
    failed = sum(1 for r in results if str(r.get("status")) == "failed")
    print(
        "[live-scheduler] cycle_done "
        + json.dumps(
            {
                "started_at": started,
                "jobs_selected": len(results),
                "completed": completed,
                "failed": failed,
            }
        )
    )
    for row in results:
        print(
            "[live-scheduler] run "
            + json.dumps(
                {
                    "source_config_id": row.get("source_config_id"),
                    "status": row.get("status"),
                    "rows_stored": row.get("rows_stored"),
                    "retry_count": row.get("retry_count"),
                    "error": row.get("error"),
                },
                default=str,
            )
        )
    return results


def scheduler_loop(
    service: PlatformService,
    *,
    interval_sec: int,
    max_jobs: int = 10,
    max_cycles: int | None = None,
) -> None:
    cycle = 0
    while True:
        run_scheduler_once(service, max_jobs=max_jobs)
        cycle += 1
        if max_cycles is not None and cycle >= max_cycles:
            print(f"[live-scheduler] stopping after {cycle} cycles")
            return
        time.sleep(max(interval_sec, 1))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run polling connector scheduler loop")
    parser.add_argument("--interval-sec", type=int, default=60, help="Seconds between poll cycles")
    parser.add_argument("--max-jobs", type=int, default=10, help="Maximum due polling jobs per cycle")
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=1,
        help="Number of cycles to run (default=1 for safe local/dev use)",
    )
    args = parser.parse_args()

    service = PlatformService()
    scheduler_loop(
        service,
        interval_sec=args.interval_sec,
        max_jobs=args.max_jobs,
        max_cycles=args.max_cycles,
    )


if __name__ == "__main__":
    main()
