"""CLI script to run Bronze Ingestion for a given landing batch_id.

Usage:
    python scripts/run_bronze.py --batch-id batch_20110129_20110204
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from retaildataops.bronze import BronzeError, BronzeIngestor

if os.name == "nt":
    hadoop_dir = Path(tempfile.gettempdir()) / "hadoop_home"
    bin_dir = hadoop_dir / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    exe_file = bin_dir / "winutils.exe"

    if not exe_file.exists() or exe_file.stat().st_size < 100:
        cs_file = bin_dir / "winutils.cs"
        cs_file.write_text(
            "class Program {" " static int Main(string[] args) { return 0; } }"
        )
        csc = r"C:\Windows\Microsoft.NET" r"\Framework64\v4.0.30319\csc.exe"
        if Path(csc).exists():
            subprocess.run(
                [csc, f"/out:{exe_file}", str(cs_file)],
                capture_output=True,
                check=False,
            )

    os.environ["HADOOP_HOME"] = str(hadoop_dir)


def get_spark_session(
    app_name: str = "RetailDataOps-Bronze",
) -> SparkSession:
    """Create and return a SparkSession configured for Delta Lake."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.fs.nativeio.enabled", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> None:
    """CLI entry point for Bronze ingestion."""
    parser = argparse.ArgumentParser(
        description="Ingest landing CSV batch into Bronze Delta tables."
    )
    parser.add_argument(
        "--batch-id",
        required=True,
        help="Batch ID directory name (e.g., batch_20110129_20110204)",
    )
    parser.add_argument(
        "--landing-dir",
        default="data/landing",
        help="Path to landing directory (default: data/landing)",
    )
    parser.add_argument(
        "--bronze-dir",
        default="data/bronze",
        help="Path to bronze Delta tables directory (default: data/bronze)",
    )

    args = parser.parse_args()

    landing_path = Path(args.landing_dir)
    bronze_path = Path(args.bronze_dir)

    print(f"Starting Bronze Ingestion for batch: {args.batch_id}")
    print(f"Landing Directory: {landing_path.resolve()}")
    print(f"Bronze Directory:  {bronze_path.resolve()}")

    spark = get_spark_session()

    try:
        ingestor = BronzeIngestor(
            spark=spark,
            landing_dir=landing_path,
            bronze_dir=bronze_path,
        )
        counts = ingestor.ingest_batch(args.batch_id)

        print("\nIngestion completed successfully!")
        print(f"  - bronze_calendar:    {counts['calendar']:,} rows")
        print(f"  - bronze_sell_prices: {counts['sell_prices']:,} rows")
        print(f"  - bronze_sales:       {counts['sales']:,} rows")
    except BronzeError as err:
        print(
            f"\nBRONZE INGESTION FAILED: {err}",
            file=sys.stderr,
        )
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
