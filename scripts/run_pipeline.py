"""Run the RetailDataOps Replay -> Bronze -> Silver -> Gold pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from retaildataops.orchestrator import (
    OrchestrationError,
    PipelinePaths,
    build_request,
    configure_logging,
    run_pipeline,
)


def main() -> int:
    """Parse arguments and run the end-to-end pipeline."""
    parser = argparse.ArgumentParser(
        description="Run Replay, Bronze, Silver, and Gold in order."
    )
    selection = parser.add_argument_group("batch selection")
    selection.add_argument("--batch-id")
    selection.add_argument("--start-date")
    selection.add_argument("--end-date")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--landing-dir", default="data/landing")
    parser.add_argument("--bronze-dir", default="data/bronze")
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--gold-dir", default="data/gold")
    parser.add_argument(
        "--retries",
        type=int,
        default=0,
        help="Additional attempts per failed stage (default: 0).",
    )
    args = parser.parse_args()
    configure_logging()

    try:
        request = build_request(
            start_date=args.start_date,
            end_date=args.end_date,
            batch_id=args.batch_id,
            paths=PipelinePaths(
                raw_dir=Path(args.raw_dir),
                landing_dir=Path(args.landing_dir),
                bronze_dir=Path(args.bronze_dir),
                silver_dir=Path(args.silver_dir),
                gold_dir=Path(args.gold_dir),
            ),
            retries=args.retries,
            project_root=Path(__file__).resolve().parent.parent,
        )
        return run_pipeline(request)
    except OrchestrationError as err:
        print(f"PIPELINE INPUT FAILED: {err}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
