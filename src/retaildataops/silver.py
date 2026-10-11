"""Silver transformations for the RetailDataOps M5 data contract."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as fn


class SilverError(Exception):
    """Base exception for Silver transformation errors."""


class SilverSchemaError(SilverError):
    """Raised when a required Silver input column is missing."""


class SilverDuplicateKeyError(SilverError):
    """Raised when an input or output business key is duplicated."""


class SilverDataQualityError(SilverError):
    """Raised when Silver input data violates a quality rule."""


class SilverJoinError(SilverError):
    """Raised when a required dimension join does not match."""


class SilverRowCountError(SilverError):
    """Raised when a Silver row-count reconciliation fails."""


class SilverIdempotencyConflictError(SilverError):
    """Raised when an existing Silver key has different content."""


SALES_COLUMNS = [
    "id",
    "item_id",
    "dept_id",
    "cat_id",
    "store_id",
    "state_id",
    "batch_id",
]
CALENDAR_COLUMNS = [
    "date",
    "wm_yr_wk",
    "weekday",
    "wday",
    "month",
    "year",
    "d",
    "event_name_1",
    "event_type_1",
    "event_name_2",
    "event_type_2",
    "snap_CA",
    "snap_TX",
    "snap_WI",
]
PRICE_COLUMNS = ["store_id", "item_id", "wm_yr_wk", "sell_price"]
SILVER_KEY_COLUMNS = ["batch_id", "id", "d"]


def _require_columns(frame: DataFrame, required: Sequence[str], name: str) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise SilverSchemaError(f"{name} is missing required columns: {missing}.")


def _duplicate_count(frame: DataFrame, keys: Sequence[str]) -> int:
    return frame.groupBy(*keys).count().filter(fn.col("count") > 1).count()


def _hash_columns(frame: DataFrame, columns: Sequence[str]) -> Column:
    values = [
        fn.coalesce(fn.col(column).cast("string"), fn.lit("__NULL__"))
        for column in columns
    ]
    return fn.sha2(fn.concat_ws("|", *values), 256)


class SilverTransformer:
    """Transform append-only Bronze tables into a validated Silver fact table."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def transform(
        self,
        sales: DataFrame,
        calendar: DataFrame,
        prices: DataFrame,
        day_keys_by_batch: Mapping[str, Sequence[str]],
    ) -> DataFrame:
        """Unpivot sales and enrich each daily record with calendar and price data."""
        _require_columns(sales, SALES_COLUMNS, "sales")
        _require_columns(calendar, CALENDAR_COLUMNS, "calendar")
        _require_columns(prices, PRICE_COLUMNS, "sell_prices")

        if not day_keys_by_batch:
            raise SilverSchemaError("day_keys_by_batch cannot be empty.")

        missing_batches = [
            row.batch_id
            for row in sales.select("batch_id").distinct().collect()
            if row.batch_id not in day_keys_by_batch
        ]
        if missing_batches:
            raise SilverSchemaError(
                f"Missing manifest day_keys for batches: {sorted(missing_batches)}."
            )

        if _duplicate_count(calendar, ["d"]) > 0:
            raise SilverDuplicateKeyError("Calendar contains duplicate d keys.")
        if _duplicate_count(prices, ["store_id", "item_id", "wm_yr_wk"]) > 0:
            raise SilverDuplicateKeyError(
                "Sell prices contains duplicate store_id/item_id/wm_yr_wk keys."
            )
        if (
            prices.filter(
                fn.col("sell_price").isNull() | (fn.col("sell_price") <= fn.lit(0))
            )
            .limit(1)
            .count()
        ):
            raise SilverDataQualityError(
                "Sell prices contains null or non-positive prices."
            )

        unpivoted: DataFrame | None = None
        expected_rows = 0
        for batch_id, day_keys in day_keys_by_batch.items():
            keys = list(day_keys)
            if not keys or len(keys) != len(set(keys)):
                raise SilverSchemaError(
                    f"Manifest day_keys for batch '{batch_id}' must be "
                    "non-empty and unique."
                )
            missing_days = sorted(set(keys) - set(sales.columns))
            if missing_days:
                raise SilverSchemaError(
                    f"Sales is missing manifest day columns for batch '{batch_id}': "
                    f"{missing_days}."
                )
            batch_sales = sales.filter(fn.col("batch_id") == fn.lit(batch_id))
            batch_count = batch_sales.count()
            expected_rows += batch_count * len(keys)
            expressions = ", ".join(
                f"'{day_key}', CAST(`{day_key}` AS BIGINT)" for day_key in keys
            )
            batch_long = batch_sales.select(
                *SALES_COLUMNS,
                fn.expr(f"stack({len(keys)}, {expressions})").alias("d", "sales_units"),
            )
            unpivoted = (
                batch_long
                if unpivoted is None
                else unpivoted.unionByName(batch_long, allowMissingColumns=True)
            )

        if unpivoted is None:
            raise SilverRowCountError("No sales records were available to unpivot.")
        actual_rows = unpivoted.count()
        if actual_rows != expected_rows:
            raise SilverRowCountError(
                f"Unpivot row count mismatch: expected {expected_rows}, "
                f"got {actual_rows}."
            )
        if (
            unpivoted.filter(
                fn.col("sales_units").isNotNull() & (fn.col("sales_units") < fn.lit(0))
            )
            .limit(1)
            .count()
        ):
            raise SilverDataQualityError("Sales contains negative sales_units.")
        if _duplicate_count(unpivoted, SILVER_KEY_COLUMNS) > 0:
            raise SilverDuplicateKeyError(
                f"Sales contains duplicate Silver keys: {SILVER_KEY_COLUMNS}."
            )

        calendar_join = calendar.select(
            *CALENDAR_COLUMNS,
            fn.lit(True).alias("_calendar_match"),
        )
        enriched = unpivoted.join(calendar_join, on="d", how="left")
        if enriched.filter(fn.col("_calendar_match").isNull()).limit(1).count():
            raise SilverJoinError(
                "One or more sales day keys did not match calendar.d."
            )

        price_join = prices.select(
            *PRICE_COLUMNS,
            fn.lit(True).alias("_price_match"),
        )
        enriched = enriched.join(
            price_join,
            on=["store_id", "item_id", "wm_yr_wk"],
            how="left",
        )
        if enriched.filter(fn.col("_price_match").isNull()).limit(1).count():
            raise SilverJoinError(
                "One or more sales records did not match sell_prices by "
                "store_id/item_id/wm_yr_wk."
            )

        result = enriched.select(
            *SALES_COLUMNS,
            "d",
            "date",
            "wm_yr_wk",
            "sales_units",
            "sell_price",
            "weekday",
            "wday",
            "month",
            "year",
            "event_name_1",
            "event_type_1",
            "event_name_2",
            "event_type_2",
            "snap_CA",
            "snap_TX",
            "snap_WI",
        ).withColumn(
            "silver_row_hash",
            _hash_columns(
                enriched,
                [
                    "batch_id",
                    "id",
                    "d",
                    "sales_units",
                    "sell_price",
                    "date",
                    "wm_yr_wk",
                ],
            ),
        )
        if result.count() != expected_rows:
            raise SilverRowCountError(
                f"Enriched row count mismatch: expected {expected_rows}, "
                f"got {result.count()}."
            )
        return result

    def write(
        self,
        frame: DataFrame,
        output_dir: str | Path,
    ) -> int:
        """Write Silver records idempotently and return newly written row count."""
        _require_columns(frame, [*SILVER_KEY_COLUMNS, "silver_row_hash"], "silver")
        if _duplicate_count(frame, SILVER_KEY_COLUMNS) > 0:
            raise SilverDuplicateKeyError(
                "Silver input contains duplicate business keys."
            )

        path = str(Path(output_dir))
        if not DeltaTable.isDeltaTable(self.spark, path):
            frame.write.format("delta").mode("overwrite").save(path)
            return frame.count()

        existing = self.spark.read.format("delta").load(path)
        _require_columns(
            existing, [*SILVER_KEY_COLUMNS, "silver_row_hash"], "existing Silver"
        )
        if _duplicate_count(existing, SILVER_KEY_COLUMNS) > 0:
            raise SilverDuplicateKeyError(
                "Existing Silver contains duplicate business keys."
            )

        overlapping = frame.alias("incoming").join(
            existing.select(*SILVER_KEY_COLUMNS, "silver_row_hash").alias("existing"),
            on=SILVER_KEY_COLUMNS,
            how="inner",
        )
        if (
            overlapping.filter(
                fn.col("incoming.silver_row_hash") != fn.col("existing.silver_row_hash")
            )
            .limit(1)
            .count()
        ):
            raise SilverIdempotencyConflictError(
                "Existing Silver contains conflicting content for a business key."
            )

        new_rows = frame.join(
            existing.select(*SILVER_KEY_COLUMNS),
            on=SILVER_KEY_COLUMNS,
            how="left_anti",
        )
        new_count = new_rows.count()
        if new_count:
            new_rows.write.format("delta").mode("append").save(path)
        return new_count


def load_manifest_day_keys(
    manifest_data: Mapping[str, Mapping[str, object]],
) -> dict[str, list[str]]:
    """Extract and validate day_keys from manifest records."""
    result: dict[str, list[str]] = {}
    for batch_id, manifest in manifest_data.items():
        day_keys = manifest.get("day_keys")
        if not isinstance(day_keys, list) or not all(
            isinstance(value, str) for value in day_keys
        ):
            raise SilverSchemaError(
                f"Manifest '{batch_id}' has invalid day_keys; expected a string list."
            )
        result[batch_id] = day_keys
    return result
