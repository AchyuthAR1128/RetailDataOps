"""Focused tests for Gold analytical aggregations."""

from __future__ import annotations

import datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from retaildataops.gold import (
    GoldDuplicateKeyError,
    GoldIdempotencyConflictError,
    GoldSchemaError,
    GoldTransformer,
)


def _silver(spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField(name, StringType(), False)
            for name in [
                "batch_id",
                "id",
                "item_id",
                "dept_id",
                "cat_id",
                "store_id",
                "state_id",
                "d",
            ]
        ]
        + [
            StructField("date", DateType(), False),
            StructField("wm_yr_wk", IntegerType(), False),
            StructField("sales_units", LongType(), True),
            StructField("sell_price", DoubleType(), True),
        ]
    )
    return spark.createDataFrame(
        [
            (
                "b1",
                "id_1",
                "item_1",
                "dept_1",
                "cat_1",
                "CA_1",
                "CA",
                "d_1",
                datetime.date(2011, 1, 29),
                11101,
                2,
                10.0,
            ),
            (
                "b1",
                "id_2",
                "item_1",
                "dept_1",
                "cat_1",
                "CA_1",
                "CA",
                "d_2",
                datetime.date(2011, 1, 30),
                11101,
                0,
                10.0,
            ),
            (
                "b1",
                "id_3",
                "item_2",
                "dept_2",
                "cat_2",
                "TX_1",
                "TX",
                "d_3",
                datetime.date(2011, 1, 31),
                11101,
                None,
                5.0,
            ),
            (
                "b2",
                "id_4",
                "item_1",
                "dept_1",
                "cat_1",
                "CA_1",
                "CA",
                "d_4",
                datetime.date(2011, 2, 1),
                11102,
                3,
                11.0,
            ),
        ],
        schema,
    )


def test_daily_and_weekly_totals_and_null_metrics(
    spark_session: SparkSession,
) -> None:
    transformer = GoldTransformer(spark_session)
    silver = _silver(spark_session)

    daily = transformer.daily_sales_by_store(silver)
    ca_daily = daily.filter(fn.col("store_id") == "CA_1").orderBy("date").collect()
    assert ca_daily[0].sales_units_total == 2
    assert ca_daily[1].sales_units_total == 0
    assert ca_daily[0].revenue_total == 20.0
    assert ca_daily[0].null_sales_count == 0

    weekly = transformer.weekly_sales_by_store(silver)
    tx_week = weekly.filter(fn.col("store_id") == "TX_1").first()
    assert tx_week is not None
    assert tx_week.sales_units_total is None
    assert tx_week.sales_observation_count == 0
    assert tx_week.null_sales_count == 1


def test_product_and_price_analytics_group_by_dimensions(
    spark_session: SparkSession,
) -> None:
    transformer = GoldTransformer(spark_session)
    silver = _silver(spark_session)
    product = transformer.product_category_performance(silver)
    item = product.filter(fn.col("item_id") == "item_1").first()
    assert item is not None
    assert item.sales_units_total == 5
    assert item.record_count == 3

    price = transformer.price_sales_analysis(silver)
    row = price.filter(fn.col("item_id") == "item_1").orderBy("wm_yr_wk").first()
    assert row is not None
    assert row.average_sell_price == 10.0
    assert row.revenue_total == 20.0


def test_null_input_keys_and_duplicate_silver_fail(
    spark_session: SparkSession,
) -> None:
    transformer = GoldTransformer(spark_session)
    silver = _silver(spark_session)
    with pytest.raises(GoldSchemaError, match="null key"):
        transformer.daily_sales_by_store(
            silver.withColumn("store_id", fn.lit(None).cast("string"))
        )
    with pytest.raises(GoldDuplicateKeyError, match="duplicate"):
        transformer.daily_sales_by_store(silver.unionByName(silver.limit(1)))


def test_write_is_idempotent_and_conflicts_are_explicit(
    spark_session: SparkSession, tmp_path: Path
) -> None:
    transformer = GoldTransformer(spark_session)
    daily = transformer.daily_sales_by_store(_silver(spark_session))
    output = tmp_path / "daily"
    keys = ["date", "store_id", "state_id"]
    assert transformer.write_table(daily, output, keys) == 4
    assert transformer.write_table(daily, output, keys) == 0
    conflict = daily.withColumn(
        "sales_units_total",
        fn.when(fn.col("store_id") == "CA_1", 999).otherwise(
            fn.col("sales_units_total")
        ),
    )
    with pytest.raises(GoldIdempotencyConflictError, match="conflicting"):
        transformer.write_table(conflict, output, keys)
