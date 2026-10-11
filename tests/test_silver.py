"""Focused tests for the Bronze-to-Silver transformation."""

from __future__ import annotations

import datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from retaildataops.silver import (
    SilverDataQualityError,
    SilverDuplicateKeyError,
    SilverIdempotencyConflictError,
    SilverJoinError,
    SilverSchemaError,
    SilverTransformer,
)


def _inputs(spark: SparkSession) -> tuple[DataFrame, DataFrame, DataFrame]:
    sales = spark.createDataFrame(
        [
            (
                "item_1_CA_1",
                "item_1",
                "dept",
                "cat",
                "CA_1",
                "CA",
                "b1",
                1,
                0,
                None,
                None,
            ),
            (
                "item_1_CA_1_b2",
                "item_1",
                "dept",
                "cat",
                "CA_1",
                "CA",
                "b2",
                None,
                None,
                3,
                None,
            ),
        ],
        StructType(
            [
                *[
                    StructField(name, StringType(), False)
                    for name in [
                        "id",
                        "item_id",
                        "dept_id",
                        "cat_id",
                        "store_id",
                        "state_id",
                        "batch_id",
                    ]
                ],
                *[
                    StructField(name, LongType(), True)
                    for name in ["d_1", "d_2", "d_3", "d_4"]
                ],
            ]
        ),
    )
    calendar = spark.createDataFrame(
        [
            (
                datetime.date(2011, 1, 29),
                11101,
                "Saturday",
                1,
                1,
                2011,
                "d_1",
                None,
                None,
                None,
                None,
                0,
                0,
                0,
            ),
            (
                datetime.date(2011, 1, 30),
                11101,
                "Sunday",
                2,
                1,
                2011,
                "d_2",
                None,
                None,
                None,
                None,
                0,
                0,
                0,
            ),
            (
                datetime.date(2011, 1, 31),
                11101,
                "Monday",
                3,
                1,
                2011,
                "d_3",
                "Event",
                "Type",
                None,
                None,
                1,
                0,
                0,
            ),
            (
                datetime.date(2011, 2, 1),
                11101,
                "Tuesday",
                4,
                2,
                2011,
                "d_4",
                None,
                None,
                None,
                None,
                0,
                0,
                0,
            ),
        ],
        StructType(
            [
                StructField("date", DateType(), False),
                StructField("wm_yr_wk", IntegerType(), False),
                StructField("weekday", StringType(), False),
                StructField("wday", IntegerType(), False),
                StructField("month", IntegerType(), False),
                StructField("year", IntegerType(), False),
                StructField("d", StringType(), False),
                StructField("event_name_1", StringType(), True),
                StructField("event_type_1", StringType(), True),
                StructField("event_name_2", StringType(), True),
                StructField("event_type_2", StringType(), True),
                StructField("snap_CA", IntegerType(), False),
                StructField("snap_TX", IntegerType(), False),
                StructField("snap_WI", IntegerType(), False),
            ]
        ),
    )
    prices = spark.createDataFrame(
        [("CA_1", "item_1", 11101, 9.58)],
        ["store_id", "item_id", "wm_yr_wk", "sell_price"],
    )
    return sales, calendar, prices


def test_unpivot_joins_and_preserves_zero_and_null(
    spark_session: SparkSession,
) -> None:
    sales, calendar, prices = _inputs(spark_session)
    result = SilverTransformer(spark_session).transform(
        sales, calendar, prices, {"b1": ["d_1", "d_2"], "b2": ["d_3", "d_4"]}
    )

    rows = {
        (row.batch_id, row.d): row.sales_units
        for row in result.select("batch_id", "d", "sales_units").collect()
    }
    assert rows[("b1", "d_1")] == 1
    assert rows[("b1", "d_2")] == 0
    assert rows[("b2", "d_3")] == 3
    assert rows[("b2", "d_4")] is None
    assert result.filter(fn.col("event_name_1") == "Event").count() == 1
    assert result.select("sell_price").distinct().collect()[0].sell_price == 9.58
    assert result.count() == 4


def test_null_sales_are_not_zero(spark_session: SparkSession) -> None:
    sales, calendar, prices = _inputs(spark_session)
    result = SilverTransformer(spark_session).transform(
        sales, calendar, prices, {"b1": ["d_1", "d_2"], "b2": ["d_3", "d_4"]}
    )
    first_row = (
        result.filter((fn.col("batch_id") == "b1") & (fn.col("d") == "d_1"))
        .select("sales_units")
        .first()
    )
    third_row = (
        result.filter((fn.col("batch_id") == "b2") & (fn.col("d") == "d_3"))
        .select("sales_units")
        .first()
    )
    assert first_row is not None
    assert third_row is not None
    assert first_row.sales_units == 1
    assert third_row.sales_units == 3


def test_missing_calendar_match_fails(spark_session: SparkSession) -> None:
    sales, calendar, prices = _inputs(spark_session)
    with pytest.raises(SilverJoinError, match="calendar"):
        SilverTransformer(spark_session).transform(
            sales,
            calendar.filter(fn.col("d") != "d_4"),
            prices,
            {"b1": ["d_1", "d_2"], "b2": ["d_3", "d_4"]},
        )


def test_missing_price_match_fails(spark_session: SparkSession) -> None:
    sales, calendar, prices = _inputs(spark_session)
    with pytest.raises(SilverJoinError, match="sell_prices"):
        SilverTransformer(spark_session).transform(
            sales,
            calendar,
            prices.filter(fn.col("item_id") != "item_1"),
            {"b1": ["d_1", "d_2"], "b2": ["d_3", "d_4"]},
        )


def test_duplicate_price_keys_fail(spark_session: SparkSession) -> None:
    sales, calendar, prices = _inputs(spark_session)
    duplicate = prices.unionByName(prices)
    with pytest.raises(SilverDuplicateKeyError, match="Sell prices"):
        SilverTransformer(spark_session).transform(
            sales,
            calendar,
            duplicate,
            {"b1": ["d_1", "d_2"], "b2": ["d_3", "d_4"]},
        )


def test_negative_sales_and_invalid_price_fail(spark_session: SparkSession) -> None:
    sales, calendar, prices = _inputs(spark_session)
    with pytest.raises(SilverDataQualityError, match="negative"):
        SilverTransformer(spark_session).transform(
            sales.withColumn("d_1", fn.lit(-1)),
            calendar,
            prices,
            {"b1": ["d_1", "d_2"], "b2": ["d_3", "d_4"]},
        )
    with pytest.raises(SilverDataQualityError, match="prices"):
        SilverTransformer(spark_session).transform(
            sales,
            calendar,
            prices.withColumn("sell_price", fn.lit(0.0)),
            {"b1": ["d_1", "d_2"], "b2": ["d_3", "d_4"]},
        )


def test_missing_manifest_day_keys_fail(spark_session: SparkSession) -> None:
    sales, calendar, prices = _inputs(spark_session)
    with pytest.raises(SilverSchemaError, match="b2"):
        SilverTransformer(spark_session).transform(
            sales, calendar, prices, {"b1": ["d_1", "d_2"]}
        )


def test_duplicate_sales_keys_fail(spark_session: SparkSession) -> None:
    sales, calendar, prices = _inputs(spark_session)
    duplicate = sales.unionByName(sales.limit(1))
    with pytest.raises(SilverDuplicateKeyError, match="duplicate Silver keys"):
        SilverTransformer(spark_session).transform(
            duplicate, calendar, prices, {"b1": ["d_1", "d_2"], "b2": ["d_3", "d_4"]}
        )


def test_write_is_idempotent_and_rejects_conflict(
    spark_session: SparkSession, tmp_path: Path
) -> None:
    sales, calendar, prices = _inputs(spark_session)
    transformer = SilverTransformer(spark_session)
    result = transformer.transform(
        sales, calendar, prices, {"b1": ["d_1", "d_2"], "b2": ["d_3", "d_4"]}
    )
    output = tmp_path / "silver"
    assert transformer.write(result, output) == 4
    assert transformer.write(result, output) == 0
    conflicting = result.withColumn(
        "silver_row_hash",
        fn.when(
            (fn.col("batch_id") == "b1") & (fn.col("d") == "d_1"),
            fn.lit("conflict"),
        ).otherwise(fn.col("silver_row_hash")),
    )
    with pytest.raises(SilverIdempotencyConflictError, match="conflicting"):
        transformer.write(conflicting, output)
