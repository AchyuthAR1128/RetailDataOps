"""CLI runner for Gold analytical tables."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from retaildataops.gold import GoldError, GoldTransformer


def get_spark_session() -> SparkSession:
    """Create a local SparkSession configured for Delta Lake."""
    builder = (
        SparkSession.builder.appName("RetailDataOps-Gold")
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
    """Read Silver and write all Gold analytical tables."""
    parser = argparse.ArgumentParser(description="Build Gold analytical Delta tables.")
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--gold-dir", default="data/gold")
    args = parser.parse_args()

    spark = get_spark_session()
    try:
        silver = spark.read.format("delta").load(str(Path(args.silver_dir)))
        transformer = GoldTransformer(spark)
        outputs = [
            (
                "daily_sales_by_store",
                transformer.daily_sales_by_store(silver),
                ["date", "store_id", "state_id"],
            ),
            (
                "weekly_sales_by_store",
                transformer.weekly_sales_by_store(silver),
                ["wm_yr_wk", "store_id", "state_id"],
            ),
            (
                "product_category_performance",
                transformer.product_category_performance(silver),
                ["item_id", "dept_id", "cat_id", "store_id", "state_id"],
            ),
            (
                "price_sales_analysis",
                transformer.price_sales_analysis(silver),
                ["wm_yr_wk", "store_id", "item_id", "cat_id", "dept_id"],
            ),
        ]
        for name, frame, keys in outputs:
            count = transformer.write_table(frame, Path(args.gold_dir) / name, keys)
            print(f"{name}: {count:,} newly written rows")
    except GoldError as err:
        print(f"GOLD TRANSFORMATION FAILED: {err}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
