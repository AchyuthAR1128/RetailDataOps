"""Command-line script to run the RetailDataOps Replay Simulator."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure src is on Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from retaildataops.replay import ReplayError, ReplaySimulator


def main() -> int:
    """Run CLI interface for the replay simulator."""
    parser = argparse.ArgumentParser(
        description="Run RetailDataOps M5 dataset replay simulator for a date range."
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="ISO start date YYYY-MM-DD (e.g. 2011-01-29)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="ISO end date YYYY-MM-DD (e.g. 2011-02-04)",
    )
    parser.add_argument(
        "--raw-dir",
        default="data/raw",
        help="Path to raw dataset directory (default: data/raw)",
    )
    parser.add_argument(
        "--landing-dir",
        default="data/landing",
        help="Path to landing directory (default: data/landing)",
    )

    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    landing_dir = Path(args.landing_dir)

    print("=" * 70)
    print("RETAILDATAOPS — M5 REPLAY SIMULATOR")
    print("=" * 70)
    print(f"Raw Directory:     {raw_dir.resolve()}")
    print(f"Landing Directory: {landing_dir.resolve()}")
    print(f"Requested Scope:   {args.start_date} to {args.end_date}")
    print("-" * 70)

    try:
        simulator = ReplaySimulator(raw_dir=raw_dir, landing_dir=landing_dir)
        manifest = simulator.run(
            start_date_str=args.start_date, end_date_str=args.end_date
        )
        print("REPLAY SUCCESSFUL ✅")
        print(f"Batch ID:                  {manifest.batch_id}")
        d_start = manifest.day_keys[0]
        d_end = manifest.day_keys[-1]
        print(f"Day Keys ({len(manifest.day_keys)}):         {d_start} to {d_end}")
        print(f"Sales Slice Rows:          {manifest.sales_row_count:,}")
        print(f"Calendar Slice Rows:       {manifest.calendar_row_count:,}")
        print(f"Sell Prices Slice Rows:    {manifest.sell_prices_row_count:,}")
        print(f"Composite Checksum:        {manifest.checksums['composite_checksum']}")
        print(
            f"Landing Path:              {(landing_dir / manifest.batch_id).resolve()}"
        )
        print("=" * 70)
        return 0
    except ReplayError as err:
        print(f"\nERROR: Replay failed — {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
