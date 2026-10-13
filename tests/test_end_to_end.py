"""Synthetic end-to-end validation of the RetailDataOps pipeline."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from retaildataops.orchestrator import PipelinePaths, PipelineRequest, run_pipeline


def _write_csv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(rows)


def _create_synthetic_raw_data(raw_dir: Path) -> None:
    _write_csv(
        raw_dir / "calendar.csv",
        [
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
        ],
        [
            [
                "2011-01-29",
                "11101",
                "Saturday",
                "1",
                "1",
                "2011",
                "d_1",
                "",
                "",
                "",
                "",
                "0",
                "0",
                "0",
            ],
            [
                "2011-01-30",
                "11101",
                "Sunday",
                "2",
                "1",
                "2011",
                "d_2",
                "Event",
                "Type",
                "",
                "",
                "1",
                "0",
                "0",
            ],
        ],
    )
    _write_csv(
        raw_dir / "sales_train_validation.csv",
        [
            "id",
            "item_id",
            "dept_id",
            "cat_id",
            "store_id",
            "state_id",
            "d_1",
            "d_2",
        ],
        [
            ["item_1_CA_1_validation", "item_1", "dept_1", "cat_1", "CA_1", "CA", 1, 2],
            ["item_2_CA_1_validation", "item_2", "dept_1", "cat_1", "CA_1", "CA", 3, 4],
        ],
    )
    _write_csv(
        raw_dir / "sell_prices.csv",
        ["store_id", "item_id", "wm_yr_wk", "sell_price"],
        [
            ["CA_1", "item_1", "11101", "10.0"],
            ["CA_1", "item_2", "11101", "5.0"],
        ],
    )


def _pipeline_request(tmp_path: Path) -> PipelineRequest:
    root = Path(__file__).resolve().parent.parent
    return PipelineRequest(
        start_date="2011-01-29",
        end_date="2011-01-30",
        batch_id="batch_20110129_20110130",
        paths=PipelinePaths(
            raw_dir=tmp_path / "raw",
            landing_dir=tmp_path / "landing",
            bronze_dir=tmp_path / "bronze",
            silver_dir=tmp_path / "silver",
            gold_dir=tmp_path / "gold",
        ),
        project_root=root,
        python_executable=sys.executable,
    )


def _read_delta(spark: SparkSession, path: Path) -> DataFrame:
    return spark.read.format("delta").load(str(path))


def test_synthetic_pipeline_runs_and_reruns_without_duplicates(
    tmp_path: Path,
    spark_session: SparkSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _pipeline_request(tmp_path)
    request.paths.raw_dir.mkdir()
    _create_synthetic_raw_data(request.paths.raw_dir)
    monkeypatch.setenv("SPARK_LOCAL_IP", "127.0.0.1")

    assert run_pipeline(request) == 0

    landing_batch = request.paths.landing_dir / request.batch_id
    assert (landing_batch / "manifest.json").exists()
    bronze_sales = _read_delta(spark_session, request.paths.bronze_dir / "sales")
    bronze_calendar = _read_delta(spark_session, request.paths.bronze_dir / "calendar")
    bronze_prices = _read_delta(spark_session, request.paths.bronze_dir / "sell_prices")
    silver = _read_delta(spark_session, request.paths.silver_dir)
    daily = _read_delta(spark_session, request.paths.gold_dir / "daily_sales_by_store")
    weekly = _read_delta(
        spark_session, request.paths.gold_dir / "weekly_sales_by_store"
    )
    product = _read_delta(
        spark_session, request.paths.gold_dir / "product_category_performance"
    )
    price_sales = _read_delta(
        spark_session, request.paths.gold_dir / "price_sales_analysis"
    )

    assert bronze_sales.count() == 2
    assert bronze_calendar.count() == 2
    assert bronze_prices.count() == 2
    assert silver.count() == 4
    assert {"batch_id", "id", "d", "date", "wm_yr_wk", "sales_units"} <= set(
        silver.columns
    )
    assert silver.select("date", "wm_yr_wk", "sell_price").count() == 4
    assert {"date", "store_id", "sales_units_total", "revenue_total"} <= set(
        daily.columns
    )
    assert {
        "wm_yr_wk",
        "store_id",
        "sales_units_total",
        "revenue_total",
    } <= set(weekly.columns)
    assert {
        "item_id",
        "dept_id",
        "cat_id",
        "store_id",
        "sales_units_total",
    } <= set(product.columns)
    assert {
        "wm_yr_wk",
        "store_id",
        "item_id",
        "average_sell_price",
        "sales_units_total",
    } <= set(price_sales.columns)
    assert [
        row.sales_units_total
        for row in daily.orderBy("date").select("sales_units_total").collect()
    ] == [4, 6]
    weekly_metrics = weekly.select("sales_units_total", "revenue_total").first()
    assert weekly_metrics is not None
    assert weekly_metrics.sales_units_total == 10
    assert weekly_metrics.revenue_total == 65.0
    assert [
        row.sales_units_total
        for row in product.orderBy("item_id").select("sales_units_total").collect()
    ] == [3, 7]
    assert price_sales.count() == 2
    assert sorted(
        row.sales_units_total
        for row in price_sales.select("sales_units_total").collect()
    ) == [3, 7]
    assert [
        row.revenue_total
        for row in daily.orderBy("date").select("revenue_total").collect()
    ] == [25.0, 40.0]

    counts_before = {
        "bronze_sales": bronze_sales.count(),
        "silver": silver.count(),
        "daily": daily.count(),
        "weekly": weekly.count(),
        "product": product.count(),
        "price_sales": price_sales.count(),
    }
    assert run_pipeline(request) == 0
    assert (
        _read_delta(spark_session, request.paths.bronze_dir / "sales").count()
        == counts_before["bronze_sales"]
    )
    assert (
        _read_delta(spark_session, request.paths.silver_dir).count()
        == counts_before["silver"]
    )
    assert (
        _read_delta(
            spark_session, request.paths.gold_dir / "daily_sales_by_store"
        ).count()
        == counts_before["daily"]
    )
    assert (
        _read_delta(
            spark_session, request.paths.gold_dir / "weekly_sales_by_store"
        ).count()
        == counts_before["weekly"]
    )
    assert (
        _read_delta(
            spark_session, request.paths.gold_dir / "product_category_performance"
        ).count()
        == counts_before["product"]
    )
    assert (
        _read_delta(
            spark_session, request.paths.gold_dir / "price_sales_analysis"
        ).count()
        == counts_before["price_sales"]
    )


def test_synthetic_pipeline_failure_stops_before_downstream_stages(
    tmp_path: Path,
) -> None:
    request = _pipeline_request(tmp_path)
    request.paths.raw_dir.mkdir()
    _write_csv(request.paths.raw_dir / "calendar.csv", ["date"], [])

    assert run_pipeline(request) != 0
    assert not request.paths.bronze_dir.exists()
    assert not request.paths.silver_dir.exists()
    assert not request.paths.gold_dir.exists()
