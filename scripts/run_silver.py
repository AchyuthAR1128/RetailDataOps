"""CLI runner for the Bronze-to-Silver transformation."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from retaildataops.silver import SilverError, SilverTransformer, load_manifest_day_keys


def get_spark_session() -> SparkSession:
    """Create a local SparkSession configured for Delta Lake."""
    builder = (
        SparkSession.builder.appName("RetailDataOps-Silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
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
    """Run the Silver transformation from Bronze Delta tables."""
    parser = argparse.ArgumentParser(
        description="Transform Bronze Delta tables to Silver."
    )
    parser.add_argument("--bronze-dir", default="data/bronze")
    parser.add_argument("--landing-dir", default="data/landing")
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument(
        "--batch-id",
        help="Optional batch to process; otherwise process all landing manifests.",
    )
    args = parser.parse_args()

    manifests: dict[str, dict[str, object]] = {}
    for manifest_path in sorted(Path(args.landing_dir).glob("*/manifest.json")):
        with open(manifest_path, encoding="utf-8") as manifest_file:
            manifest = json.load(manifest_file)
        batch_id = manifest.get("batch_id")
        if not isinstance(batch_id, str):
            raise SilverError(f"Manifest '{manifest_path}' has no valid batch_id.")
        if args.batch_id is not None and batch_id != args.batch_id:
            continue
        manifests[batch_id] = manifest
    if args.batch_id is not None and args.batch_id not in manifests:
        raise SilverError(
            f"Manifest for requested batch '{args.batch_id}' was not found."
        )

    spark = get_spark_session()
    try:
        transformer = SilverTransformer(spark)
        day_keys = load_manifest_day_keys(manifests)
        sales = spark.read.format("delta").load(str(Path(args.bronze_dir) / "sales"))
        if args.batch_id is not None:
            sales = sales.filter(sales.batch_id == args.batch_id)
        frame = transformer.transform(
            sales,
            spark.read.format("delta").load(str(Path(args.bronze_dir) / "calendar")),
            spark.read.format("delta").load(str(Path(args.bronze_dir) / "sell_prices")),
            day_keys,
        )
        written = transformer.write(frame, args.silver_dir)
        print(f"Silver transformation completed; newly written rows: {written:,}")
    except SilverError as err:
        print(f"SILVER TRANSFORMATION FAILED: {err}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
