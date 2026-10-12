"""Deterministic analytical aggregations over validated Silver data."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as fn


class GoldError(Exception):
    """Base exception for Gold analytical transformations."""


class GoldSchemaError(GoldError):
    """Raised when Silver is missing required columns or has invalid keys."""


class GoldDuplicateKeyError(GoldError):
    """Raised when Silver or a Gold result has duplicate keys."""


class GoldAggregationError(GoldError):
    """Raised when an aggregation result cannot be reconciled."""


class GoldIdempotencyConflictError(GoldError):
    """Raised when an existing Gold key has different content."""


SILVER_REQUIRED_COLUMNS = [
    "batch_id",
    "id",
    "item_id",
    "dept_id",
    "cat_id",
    "store_id",
    "state_id",
    "d",
    "date",
    "wm_yr_wk",
    "sales_units",
    "sell_price",
]


def _require_columns(frame: DataFrame, required: Sequence[str]) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise GoldSchemaError(f"Silver is missing required columns: {missing}.")


def _duplicate_count(frame: DataFrame, keys: Sequence[str]) -> int:
    return frame.groupBy(*keys).count().filter(fn.col("count") > 1).count()


def _hash_columns(frame: DataFrame, columns: Sequence[str]) -> Column:
    values = [
        fn.coalesce(fn.col(column).cast("string"), fn.lit("__NULL__"))
        for column in columns
    ]
    return fn.sha2(fn.concat_ws("|", *values), 256)


def _sum_sales() -> Column:
    """Sum known sales while retaining null when no sales value is known."""
    return fn.sum("sales_units").cast("long")


class GoldTransformer:
    """Build analytical Gold tables from the validated Silver fact grain."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def validate_silver(self, silver: DataFrame) -> None:
        """Validate Silver columns, keys, and key nullability before aggregation."""
        _require_columns(silver, SILVER_REQUIRED_COLUMNS)
        key_columns = [
            "batch_id",
            "id",
            "d",
            "date",
            "wm_yr_wk",
            "item_id",
            "dept_id",
            "cat_id",
            "store_id",
            "state_id",
        ]
        null_key_filter = fn.lit(False)
        for column in key_columns:
            null_key_filter = null_key_filter | fn.col(column).isNull()
        if silver.filter(null_key_filter).limit(1).count():
            raise GoldSchemaError(f"Silver contains null key columns: {key_columns}.")
        if _duplicate_count(silver, key_columns) > 0:
            raise GoldDuplicateKeyError(
                f"Silver contains duplicate keys: {key_columns}."
            )
        if silver.filter(fn.col("sales_units") < fn.lit(0)).limit(1).count():
            raise GoldSchemaError("Silver contains negative sales_units.")

    def _aggregate(
        self,
        silver: DataFrame,
        keys: Sequence[str],
        extra_metrics: bool = False,
    ) -> DataFrame:
        self.validate_silver(silver)
        aggregations = [
            _sum_sales().alias("sales_units_total"),
            fn.count("sales_units").cast("long").alias("sales_observation_count"),
            fn.sum(fn.when(fn.col("sales_units").isNull(), 1).otherwise(0))
            .cast("long")
            .alias("null_sales_count"),
            fn.count(fn.lit(1)).cast("long").alias("record_count"),
        ]
        if extra_metrics:
            aggregations.extend(
                [
                    fn.avg("sell_price").cast("double").alias("average_sell_price"),
                    fn.sum(
                        fn.when(
                            fn.col("sales_units").isNotNull()
                            & fn.col("sell_price").isNotNull(),
                            fn.col("sales_units") * fn.col("sell_price"),
                        )
                    )
                    .cast("double")
                    .alias("revenue_total"),
                ]
            )
        result = silver.groupBy(*keys).agg(*aggregations)
        if _duplicate_count(result, keys) > 0:
            raise GoldDuplicateKeyError(
                f"Gold result has duplicate keys: {list(keys)}."
            )
        return result

    def daily_sales_by_store(self, silver: DataFrame) -> DataFrame:
        """Return one row per calendar date and store."""
        return self._aggregate(
            silver,
            ["date", "store_id", "state_id"],
            extra_metrics=True,
        ).select(
            "date",
            "store_id",
            "state_id",
            "sales_units_total",
            "sales_observation_count",
            "null_sales_count",
            "record_count",
            "average_sell_price",
            "revenue_total",
        )

    def weekly_sales_by_store(self, silver: DataFrame) -> DataFrame:
        """Return one row per M5 week and store."""
        return self._aggregate(
            silver,
            ["wm_yr_wk", "store_id", "state_id"],
            extra_metrics=True,
        ).select(
            "wm_yr_wk",
            "store_id",
            "state_id",
            "sales_units_total",
            "sales_observation_count",
            "null_sales_count",
            "record_count",
            "average_sell_price",
            "revenue_total",
        )

    def product_category_performance(self, silver: DataFrame) -> DataFrame:
        """Return one row per item, department, category, and store."""
        return self._aggregate(
            silver,
            ["item_id", "dept_id", "cat_id", "store_id", "state_id"],
            extra_metrics=True,
        ).select(
            "item_id",
            "dept_id",
            "cat_id",
            "store_id",
            "state_id",
            "sales_units_total",
            "sales_observation_count",
            "null_sales_count",
            "record_count",
            "average_sell_price",
            "revenue_total",
        )

    def price_sales_analysis(self, silver: DataFrame) -> DataFrame:
        """Return weekly item/store price and sales measures without causal claims."""
        return self._aggregate(
            silver,
            ["wm_yr_wk", "store_id", "item_id", "cat_id", "dept_id"],
            extra_metrics=True,
        ).select(
            "wm_yr_wk",
            "store_id",
            "item_id",
            "cat_id",
            "dept_id",
            "average_sell_price",
            "sales_units_total",
            "sales_observation_count",
            "null_sales_count",
            "record_count",
            "revenue_total",
        )

    def write_table(
        self,
        frame: DataFrame,
        output_dir: str | Path,
        key_columns: Sequence[str],
    ) -> int:
        """Write a Gold table append-only and idempotently."""
        _require_columns(frame, key_columns)
        if any(
            frame.filter(fn.col(column).isNull()).limit(1).count()
            for column in key_columns
        ):
            raise GoldSchemaError(
                f"Gold output has null key columns: {list(key_columns)}."
            )
        if _duplicate_count(frame, key_columns) > 0:
            raise GoldDuplicateKeyError(
                f"Gold output has duplicate keys: {list(key_columns)}."
            )

        hash_columns = list(frame.columns)
        candidate = frame.withColumn(
            "gold_row_hash", _hash_columns(frame, hash_columns)
        )
        path = str(Path(output_dir))
        if not DeltaTable.isDeltaTable(self.spark, path):
            candidate.write.format("delta").mode("overwrite").save(path)
            return candidate.count()

        existing = self.spark.read.format("delta").load(path)
        _require_columns(existing, [*key_columns, "gold_row_hash"])
        if _duplicate_count(existing, key_columns) > 0:
            raise GoldDuplicateKeyError(
                f"Existing Gold table has duplicate keys: {list(key_columns)}."
            )
        overlap = candidate.alias("incoming").join(
            existing.select(*key_columns, "gold_row_hash").alias("existing"),
            on=list(key_columns),
            how="inner",
        )
        if (
            overlap.filter(
                fn.col("incoming.gold_row_hash") != fn.col("existing.gold_row_hash")
            )
            .limit(1)
            .count()
        ):
            raise GoldIdempotencyConflictError(
                "Existing Gold table contains conflicting content for a key."
            )
        new_rows = candidate.join(
            existing.select(*key_columns),
            on=list(key_columns),
            how="left_anti",
        )
        new_count = new_rows.count()
        if new_count:
            new_rows.write.format("delta").mode("append").save(path)
        return new_count
