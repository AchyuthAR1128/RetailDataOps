"""Unit and integration test suite for Bronze Ingestion module."""

from __future__ import annotations

import csv
import datetime
import hashlib
import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from retaildataops.bronze import (
    BronzeIngestor,
    DuplicateBusinessKeyError,
    IdempotencyConflictError,
    ManifestValidationError,
    RowCountMismatchError,
    SchemaValidationError,
    compute_canonical_row_hash,
)


def create_mock_landing_batch(
    landing_dir: Path,
    batch_id: str = "batch_20110129_20110130",
    sales_rows: list[list[str]] | None = None,
    cal_rows: list[list[str]] | None = None,
    price_rows: list[list[str]] | None = None,
    tamper_file: str | None = None,
    start_date: str = "2011-01-29",
    end_date: str = "2011-01-30",
    day_keys: list[str] | None = None,
) -> Path:
    """Create a temporary mock landing batch with manifest and CSVs."""
    batch_dir = landing_dir / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)

    # Default Calendar rows (2 days: d_1, d_2)
    if cal_rows is None:
        cal_rows = [
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
                "",
                "",
                "",
                "",
                "0",
                "0",
                "0",
            ],
        ]

    # Default Prices rows (2 items x 1 store)
    if price_rows is None:
        price_rows = [
            ["store_id", "item_id", "wm_yr_wk", "sell_price"],
            ["CA_1", "HOBBIES_1_001", "11101", "9.58"],
            ["CA_1", "HOBBIES_1_002", "11101", "5.97"],
        ]

    # Default Sales rows (2 items x 1 store, d_1 and d_2)
    if sales_rows is None:
        sales_rows = [
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
                "HOBBIES_1_001_CA_1_validation",
                "HOBBIES_1_001",
                "HOBBIES_1",
                "HOBBIES",
                "CA_1",
                "CA",
                "0",
                "1",
            ],
            [
                "HOBBIES_1_002_CA_1_validation",
                "HOBBIES_1_002",
                "HOBBIES_1",
                "HOBBIES",
                "CA_1",
                "CA",
                "5",
                "0",
            ],
        ]

    if day_keys is None:
        day_keys = ["d_1", "d_2"]

    # Write CSV files
    cal_file = batch_dir / "calendar_slice.csv"
    with open(cal_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(cal_rows)

    prices_file = batch_dir / "sell_prices_slice.csv"
    with open(prices_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(price_rows)

    sales_file = batch_dir / "sales_slice.csv"
    with open(sales_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(sales_rows)

    def sha256_file(p: Path) -> str:
        h = hashlib.sha256()
        with open(p, "rb") as fh:
            while chunk := fh.read(65536):
                h.update(chunk)
        return h.hexdigest()

    sales_sha = sha256_file(sales_file)
    cal_sha = sha256_file(cal_file)
    prices_sha = sha256_file(prices_file)

    if tamper_file == "sales_slice.csv":
        sales_sha = (
            "00000000000000000000000000000000" "00000000000000000000000000000000"
        )

    manifest_data = {
        "batch_id": batch_id,
        "source_files": [
            "sales_train_validation.csv",
            "calendar.csv",
            "sell_prices.csv",
        ],
        "start_date": start_date,
        "end_date": end_date,
        "day_keys": day_keys,
        "sales_row_count": len(sales_rows) - 1,
        "calendar_row_count": len(cal_rows) - 1,
        "sell_prices_row_count": len(price_rows) - 1,
        "files": {
            "sales_slice": "sales_slice.csv",
            "calendar_slice": "calendar_slice.csv",
            "sell_prices_slice": "sell_prices_slice.csv",
        },
        "checksums": {
            "sales_slice": sales_sha,
            "calendar_slice": cal_sha,
            "sell_prices_slice": prices_sha,
        },
        "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    manifest_file = batch_dir / "manifest.json"
    with open(manifest_file, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)

    return batch_dir


@pytest.fixture
def temp_dirs(tmp_path: Path) -> tuple[Path, Path]:
    landing = tmp_path / "landing"
    bronze = tmp_path / "bronze"
    landing.mkdir()
    bronze.mkdir()
    return landing, bronze


class TestBronzeIngestion:
    """Test suite for Bronze Ingestion engine rules and edge cases."""

    def test_deterministic_row_hash_computation(self) -> None:
        """Verify compute_canonical_row_hash is reproducible."""
        vals1 = ["2011-01-29", 11101, "Saturday", None, 1]
        vals2 = ["2011-01-29", 11101, "Saturday", None, 1]
        vals3 = ["2011-01-29", 11101, "Saturday", "2", 1]

        h1 = compute_canonical_row_hash(vals1)
        h2 = compute_canonical_row_hash(vals2)
        h3 = compute_canonical_row_hash(vals3)

        assert h1 == h2
        assert h1 != h3
        assert len(h1) == 64

    def test_manifest_checksum_mismatch_raises_error(
        self,
        spark_session: SparkSession,
        temp_dirs: tuple[Path, Path],
    ) -> None:
        """Tampered file checksum causes ManifestValidationError."""
        landing_dir, bronze_dir = temp_dirs
        create_mock_landing_batch(landing_dir, tamper_file="sales_slice.csv")

        ingestor = BronzeIngestor(spark_session, landing_dir, bronze_dir)
        with pytest.raises(ManifestValidationError, match="Checksum mismatch"):
            ingestor.ingest_batch("batch_20110129_20110130")

    def test_missing_manifest_raises_error(
        self,
        spark_session: SparkSession,
        temp_dirs: tuple[Path, Path],
    ) -> None:
        """Missing manifest.json raises ManifestValidationError."""
        landing_dir, bronze_dir = temp_dirs
        batch_dir = landing_dir / "batch_missing_manifest"
        batch_dir.mkdir()

        ingestor = BronzeIngestor(spark_session, landing_dir, bronze_dir)
        with pytest.raises(ManifestValidationError, match="Manifest file missing"):
            ingestor.ingest_batch("batch_missing_manifest")

    def test_successful_first_ingestion(
        self,
        spark_session: SparkSession,
        temp_dirs: tuple[Path, Path],
    ) -> None:
        """First ingestion creates Delta tables with metadata."""
        landing_dir, bronze_dir = temp_dirs
        create_mock_landing_batch(landing_dir)

        ingestor = BronzeIngestor(spark_session, landing_dir, bronze_dir)
        counts = ingestor.ingest_batch("batch_20110129_20110130")

        assert counts["calendar"] == 2
        assert counts["sell_prices"] == 2
        assert counts["sales"] == 2

        # Read back Delta tables
        sales_df = spark_session.read.format("delta").load(str(bronze_dir / "sales"))
        assert sales_df.count() == 2
        assert "batch_id" in sales_df.columns
        assert "row_hash" in sales_df.columns

    def test_same_batch_identical_rerun_is_noop(
        self,
        spark_session: SparkSession,
        temp_dirs: tuple[Path, Path],
    ) -> None:
        """Re-ingesting the exact same batch is a safe no-op."""
        landing_dir, bronze_dir = temp_dirs
        create_mock_landing_batch(landing_dir)

        ingestor = BronzeIngestor(spark_session, landing_dir, bronze_dir)
        ingestor.ingest_batch("batch_20110129_20110130")
        counts2 = ingestor.ingest_batch("batch_20110129_20110130")

        assert counts2["sales"] == 2

        sales_df = spark_session.read.format("delta").load(str(bronze_dir / "sales"))
        assert sales_df.count() == 2

    def test_same_batch_conflicting_rerun_raises_error(
        self,
        spark_session: SparkSession,
        temp_dirs: tuple[Path, Path],
    ) -> None:
        """Replaying batch_id with modified contents raises error."""
        landing_dir, bronze_dir = temp_dirs
        create_mock_landing_batch(landing_dir)

        ingestor = BronzeIngestor(spark_session, landing_dir, bronze_dir)
        ingestor.ingest_batch("batch_20110129_20110130")

        # Create altered batch with same batch_id
        alt_sales = [
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
                "HOBBIES_1_001_CA_1_validation",
                "HOBBIES_1_001",
                "HOBBIES_1",
                "HOBBIES",
                "CA_1",
                "CA",
                "99",  # Modified value -> hash changes
                "1",
            ],
            [
                "HOBBIES_1_002_CA_1_validation",
                "HOBBIES_1_002",
                "HOBBIES_1",
                "HOBBIES",
                "CA_1",
                "CA",
                "5",
                "0",
            ],
        ]
        create_mock_landing_batch(landing_dir, sales_rows=alt_sales)

        with pytest.raises(
            IdempotencyConflictError,
            match="conflicting row keys or hashes",
        ):
            ingestor.ingest_batch("batch_20110129_20110130")

    def test_intra_batch_duplicate_business_keys_raises_error(
        self,
        spark_session: SparkSession,
        temp_dirs: tuple[Path, Path],
    ) -> None:
        """Duplicate business keys raise DuplicateBusinessKeyError."""
        landing_dir, bronze_dir = temp_dirs
        dup_sales = [
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
                "SAME_ID",
                "HOBBIES_1_001",
                "HOBBIES_1",
                "HOBBIES",
                "CA_1",
                "CA",
                "0",
                "1",
            ],
            [
                "SAME_ID",
                "HOBBIES_1_001",
                "HOBBIES_1",
                "HOBBIES",
                "CA_1",
                "CA",
                "5",
                "0",
            ],
        ]
        create_mock_landing_batch(landing_dir, sales_rows=dup_sales)

        ingestor = BronzeIngestor(spark_session, landing_dir, bronze_dir)
        with pytest.raises(
            DuplicateBusinessKeyError,
            match="duplicate business keys",
        ):
            ingestor.ingest_batch("batch_20110129_20110130")

    def test_unexpected_non_d_column_raises_schema_error(
        self,
        spark_session: SparkSession,
        temp_dirs: tuple[Path, Path],
    ) -> None:
        """Unexpected non-d_* column raises SchemaValidationError."""
        landing_dir, bronze_dir = temp_dirs
        bad_sales = [
            [
                "id",
                "item_id",
                "dept_id",
                "cat_id",
                "store_id",
                "state_id",
                "extra_bad_col",
                "d_1",
                "d_2",
            ],
            [
                "HOBBIES_1_001_CA_1_validation",
                "HOBBIES_1_001",
                "HOBBIES_1",
                "HOBBIES",
                "CA_1",
                "CA",
                "bad",
                "0",
                "1",
            ],
        ]
        create_mock_landing_batch(landing_dir, sales_rows=bad_sales)

        ingestor = BronzeIngestor(spark_session, landing_dir, bronze_dir)
        with pytest.raises(SchemaValidationError, match="Unexpected non-d"):
            ingestor.ingest_batch("batch_20110129_20110130")

    def test_unexpected_d_column_not_in_manifest_raises_schema_error(
        self,
        spark_session: SparkSession,
        temp_dirs: tuple[Path, Path],
    ) -> None:
        """d_* column not in manifest.day_keys raises error."""
        landing_dir, bronze_dir = temp_dirs
        bad_sales = [
            [
                "id",
                "item_id",
                "dept_id",
                "cat_id",
                "store_id",
                "state_id",
                "d_1",
                "d_999",
            ],
            [
                "HOBBIES_1_001_CA_1_validation",
                "HOBBIES_1_001",
                "HOBBIES_1",
                "HOBBIES",
                "CA_1",
                "CA",
                "0",
                "1",
            ],
        ]
        create_mock_landing_batch(landing_dir, sales_rows=bad_sales)

        ingestor = BronzeIngestor(spark_session, landing_dir, bronze_dir)
        with pytest.raises(SchemaValidationError, match="Unexpected d"):
            ingestor.ingest_batch("batch_20110129_20110130")

    def test_allowed_d_schema_evolution(
        self,
        spark_session: SparkSession,
        temp_dirs: tuple[Path, Path],
    ) -> None:
        """New batch with new d_* columns evolves sales schema."""
        landing_dir, bronze_dir = temp_dirs

        # Batch 1 (d_1, d_2)
        create_mock_landing_batch(landing_dir, batch_id="batch_1")
        ingestor = BronzeIngestor(spark_session, landing_dir, bronze_dir)
        ingestor.ingest_batch("batch_1")

        # Batch 2 (d_3, d_4)
        b2_sales = [
            [
                "id",
                "item_id",
                "dept_id",
                "cat_id",
                "store_id",
                "state_id",
                "d_3",
                "d_4",
            ],
            [
                "HOBBIES_1_001_CA_1_validation",
                "HOBBIES_1_001",
                "HOBBIES_1",
                "HOBBIES",
                "CA_1",
                "CA",
                "3",
                "4",
            ],
            [
                "HOBBIES_1_002_CA_1_validation",
                "HOBBIES_1_002",
                "HOBBIES_1",
                "HOBBIES",
                "CA_1",
                "CA",
                "0",
                "1",
            ],
        ]
        cal2 = [
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
                "2011-01-31",
                "11101",
                "Monday",
                "3",
                "1",
                "2011",
                "d_3",
                "",
                "",
                "",
                "",
                "0",
                "0",
                "0",
            ],
            [
                "2011-02-01",
                "11101",
                "Tuesday",
                "3",
                "2",
                "2011",
                "d_4",
                "",
                "",
                "",
                "",
                "0",
                "0",
                "0",
            ],
        ]
        prices2 = [
            ["store_id", "item_id", "wm_yr_wk", "sell_price"],
            ["CA_1", "HOBBIES_1_001", "11101", "9.58"],
        ]

        create_mock_landing_batch(
            landing_dir,
            batch_id="batch_2",
            sales_rows=b2_sales,
            cal_rows=cal2,
            price_rows=prices2,
            start_date="2011-01-31",
            end_date="2011-02-01",
            day_keys=["d_3", "d_4"],
        )

        ingestor.ingest_batch("batch_2")

        sales_df = spark_session.read.format("delta").load(str(bronze_dir / "sales"))
        assert "d_1" in sales_df.columns
        assert "d_2" in sales_df.columns
        assert "d_3" in sales_df.columns
        assert "d_4" in sales_df.columns
        assert sales_df.count() == 4

    def test_parsed_row_count_mismatch_raises_error(
        self,
        spark_session: SparkSession,
        temp_dirs: tuple[Path, Path],
    ) -> None:
        """Row count mismatch raises RowCountMismatchError."""
        landing_dir, bronze_dir = temp_dirs
        create_mock_landing_batch(landing_dir)

        # Alter manifest sales_row_count
        batch_dir = landing_dir / "batch_20110129_20110130"
        manifest_file = batch_dir / "manifest.json"
        with open(manifest_file, encoding="utf-8") as f:
            data = json.load(f)

        data["sales_row_count"] = 999
        with open(manifest_file, "w", encoding="utf-8") as f:
            json.dump(data, f)

        # Altering sales_row_count keeps CSV checksums valid
        # because manifest.json itself is not SHA256-verified.
        ingestor = BronzeIngestor(spark_session, landing_dir, bronze_dir)
        with pytest.raises(
            RowCountMismatchError,
            match="Sales row count mismatch",
        ):
            ingestor.ingest_batch("batch_20110129_20110130")

    def test_partial_failure_recovery(
        self,
        spark_session: SparkSession,
        temp_dirs: tuple[Path, Path],
    ) -> None:
        """Partial table write recovers safely on rerun."""
        landing_dir, bronze_dir = temp_dirs
        create_mock_landing_batch(landing_dir)

        ingestor = BronzeIngestor(spark_session, landing_dir, bronze_dir)

        # Write only calendar manually to simulate partial failure
        cal_path = landing_dir / "batch_20110129_20110130" / "calendar_slice.csv"
        cal_raw_df = (
            spark_session.read.format("csv")
            .option("header", "true")
            .schema(from_bronze_schema("calendar"))
            .load(str(cal_path))
        )
        cal_cols = [
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
        cal_df = ingestor._add_metadata_and_hash(
            cal_raw_df,
            "batch_20110129_20110130",
            "calendar_slice.csv",
            cal_cols,
        )
        ingestor._write_table(
            cal_df,
            "calendar",
            "batch_20110129_20110130",
            ["date"],
            allow_merge_schema=False,
        )

        # Verify calendar exists, sales does not
        assert (bronze_dir / "calendar").exists()
        assert not (bronze_dir / "sales").exists()

        # Rerun full ingest: calendar no-op, rest completes
        counts = ingestor.ingest_batch("batch_20110129_20110130")
        assert counts["sales"] == 2

        cal_df_final = spark_session.read.format("delta").load(
            str(bronze_dir / "calendar")
        )
        sales_df_final = spark_session.read.format("delta").load(
            str(bronze_dir / "sales")
        )

        assert cal_df_final.count() == 2
        assert sales_df_final.count() == 2


def from_bronze_schema(name: str) -> StructType:
    """Return the Bronze schema struct for the given dataset."""
    from retaildataops.bronze import (
        CALENDAR_STRUCT,
        SELL_PRICES_STRUCT,
    )

    if name == "calendar":
        return CALENDAR_STRUCT
    return SELL_PRICES_STRUCT
