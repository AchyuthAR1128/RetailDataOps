"""Bronze ingestion module for RetailDataOps.

Ingests raw landing CSV batches into Delta Lake Bronze tables with:
- Manifest and physical file SHA256 checksum verification
- Strict CSV schema and header validation
- Dynamic sales day_keys validation against manifest
- Intra-batch business key deduplication check
- Deterministic canonical row_hash computation
- Append-only Delta writes with controlled d_* schema evolution
- Inter-batch idempotency and partial-failure recovery
"""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


class BronzeError(Exception):
    """Base exception for Bronze ingestion errors."""


class ManifestValidationError(BronzeError):
    """Raised when manifest validation or checksum check fails."""


class SchemaValidationError(BronzeError):
    """Raised when input schema or header is invalid."""


class RowCountMismatchError(BronzeError):
    """Raised when parsed row count does not match manifest row count."""


class DuplicateBusinessKeyError(BronzeError):
    """Raised when duplicate business keys exist within the landing batch."""


class IdempotencyConflictError(BronzeError):
    """Raised when replaying an existing batch with conflicting metadata or content."""


# Base metadata columns required for sales matrix
SALES_BASE_META_COLS = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"]

# Calendar schema definition
CALENDAR_STRUCT = StructType(
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
)

# Sell prices schema definition
SELL_PRICES_STRUCT = StructType(
    [
        StructField("store_id", StringType(), False),
        StructField("item_id", StringType(), False),
        StructField("wm_yr_wk", IntegerType(), False),
        StructField("sell_price", DoubleType(), False),
    ]
)


def compute_canonical_row_hash(values: list[Any]) -> str:
    """Compute deterministic SHA256 hex digest for a row's source values.

    Args:
        values: Ordered list of source column values.

    Returns:
        Hex string of SHA256 digest.
    """
    formatted: list[str] = []
    for v in values:
        if v is None:
            formatted.append("__NULL__")
        else:
            formatted.append(str(v))
    canonical_str = "|".join(formatted)
    return hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()


class BronzeIngestor:
    """Bronze Ingestion pipeline engine operating on PySpark and Delta Lake."""

    def __init__(
        self,
        spark: SparkSession,
        landing_dir: str | Path,
        bronze_dir: str | Path,
    ) -> None:
        """Initialize BronzeIngestor.

        Args:
            spark: Active SparkSession configured with Delta extensions.
            landing_dir: Directory containing landing batches.
            bronze_dir: Directory where Bronze Delta tables are stored.
        """
        self.spark = spark
        self.landing_dir = Path(landing_dir)
        self.bronze_dir = Path(bronze_dir)

    @staticmethod
    def _compute_file_sha256(file_path: Path) -> str:
        """Compute SHA256 checksum of a file on disk."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _verify_manifest_and_checksums(self, batch_id: str) -> dict[str, Any]:
        """Load manifest.json, verify required fields and physical SHA256 checksums."""
        batch_dir = self.landing_dir / batch_id
        if not batch_dir.exists():
            raise ManifestValidationError(
                f"Landing batch directory '{batch_dir}' does not exist."
            )

        manifest_path = batch_dir / "manifest.json"
        if not manifest_path.exists():
            raise ManifestValidationError(
                f"Manifest file missing in batch directory '{batch_dir}'."
            )

        with open(manifest_path, encoding="utf-8") as f:
            manifest_data: dict[str, Any] = json.load(f)

        required_keys = [
            "batch_id",
            "source_files",
            "start_date",
            "end_date",
            "day_keys",
            "sales_row_count",
            "calendar_row_count",
            "sell_prices_row_count",
            "checksums",
        ]
        for key in required_keys:
            if key not in manifest_data:
                raise ManifestValidationError(
                    f"Manifest in '{batch_id}' missing required field '{key}'."
                )

        if manifest_data["batch_id"] != batch_id:
            raise ManifestValidationError(
                f"Manifest batch_id '{manifest_data['batch_id']}' "
                f"does not match requested '{batch_id}'."
            )

        checksums = manifest_data.get("checksums", {})
        files_map = manifest_data.get("files", {})

        sales_name = files_map.get("sales_slice", "sales_slice.csv")
        cal_name = files_map.get("calendar_slice", "calendar_slice.csv")
        prices_name = files_map.get("sell_prices_slice", "sell_prices_slice.csv")

        file_checks = [
            (batch_dir / sales_name, "sales_slice"),
            (batch_dir / cal_name, "calendar_slice"),
            (batch_dir / prices_name, "sell_prices_slice"),
        ]

        for file_path, check_key in file_checks:
            if not file_path.exists():
                raise ManifestValidationError(
                    f"Required landing file '{file_path.name}' is missing."
                )

            expected_sha = checksums.get(check_key)
            if not expected_sha:
                raise ManifestValidationError(
                    f"Checksum key '{check_key}' missing in manifest.json."
                )

            actual_sha = self._compute_file_sha256(file_path)
            if actual_sha != expected_sha:
                raise ManifestValidationError(
                    f"Checksum mismatch for '{file_path.name}': "
                    f"expected {expected_sha}, got {actual_sha}."
                )

        return manifest_data

    @staticmethod
    def _read_csv_header(file_path: Path) -> list[str]:
        """Read header columns from a CSV file."""
        with open(file_path, encoding="utf-8") as f:
            reader = csv.reader(f)
            return next(reader)

    def _validate_sales_header(self, file_path: Path, day_keys: list[str]) -> list[str]:
        """Validate sales CSV header against base metadata and manifest day_keys."""
        header = self._read_csv_header(file_path)
        expected_cols = SALES_BASE_META_COLS + day_keys

        if header != expected_cols:
            unexpected_non_d = [
                c
                for c in header
                if c not in SALES_BASE_META_COLS and not c.startswith("d_")
            ]
            unexpected_d = [
                c for c in header if c.startswith("d_") and c not in day_keys
            ]
            missing_cols = [c for c in expected_cols if c not in header]

            raise SchemaValidationError(
                f"Sales CSV header mismatch. Expected {expected_cols}. "
                f"Missing: {missing_cols}, Unexpected non-d: {unexpected_non_d}, "
                f"Unexpected d: {unexpected_d}."
            )

        return header

    @staticmethod
    def _add_metadata_and_hash(
        df: DataFrame, batch_id: str, source_file: str, source_cols: list[str]
    ) -> DataFrame:
        """Add metadata columns and deterministic row_hash to DataFrame."""
        concat_exprs = [
            fn.coalesce(fn.col(c).cast("string"), fn.lit("__NULL__"))
            for c in source_cols
        ]
        hash_expr = fn.sha2(fn.concat_ws("|", *concat_exprs), 256)

        return (
            df.withColumn("batch_id", fn.lit(batch_id))
            .withColumn("source_file", fn.lit(source_file))
            .withColumn("ingest_ts", fn.current_timestamp())
            .withColumn("row_hash", hash_expr)
        )

    @staticmethod
    def _check_intra_batch_duplicates(
        df: DataFrame, key_cols: list[str], dataset_name: str
    ) -> None:
        """Verify business key uniqueness within the incoming batch."""
        full_keys = ["batch_id", *key_cols]
        dup_count = df.groupBy(full_keys).count().filter(fn.col("count") > 1).count()
        if dup_count > 0:
            raise DuplicateBusinessKeyError(
                f"Found {dup_count} duplicate business keys "
                f"in batch for '{dataset_name}'."
            )

    def _write_table(
        self,
        df: DataFrame,
        table_name: str,
        batch_id: str,
        key_cols: list[str],
        allow_merge_schema: bool = False,
    ) -> None:
        """Write DataFrame to Bronze Delta table with append-only idempotency checks."""
        table_path = self.bronze_dir / table_name
        table_path.parent.mkdir(parents=True, exist_ok=True)
        table_str = str(table_path)

        delta_exists = DeltaTable.isDeltaTable(self.spark, table_str)

        if delta_exists:
            existing_df = self.spark.read.format("delta").load(table_str)
            existing_batch = existing_df.filter(fn.col("batch_id") == batch_id)

            if not existing_batch.isEmpty():
                incoming_count = df.count()
                existing_count = existing_batch.count()

                if incoming_count != existing_count:
                    raise IdempotencyConflictError(
                        f"Existing batch '{batch_id}' in '{table_name}' has "
                        f"row count {existing_count}, incoming has {incoming_count}."
                    )

                full_keys = [*key_cols, "row_hash"]
                incoming_keys = df.select(*full_keys)
                existing_keys = existing_batch.select(*full_keys)

                diff1 = incoming_keys.subtract(existing_keys)
                diff2 = existing_keys.subtract(incoming_keys)

                if not diff1.isEmpty() or not diff2.isEmpty():
                    raise IdempotencyConflictError(
                        f"Existing batch '{batch_id}' in '{table_name}' has "
                        "conflicting row keys or hashes."
                    )

                # Identical batch already present — safe no-op
                return

        writer = df.write.format("delta").mode("append")
        if allow_merge_schema:
            writer = writer.option("mergeSchema", "true")

        writer.save(table_str)

    def ingest_batch(self, batch_id: str) -> dict[str, int]:
        """Execute Bronze ingestion for the given landing batch_id.

        Args:
            batch_id: Landing batch directory name.

        Returns:
            Dict mapping dataset names to ingested row counts.
        """
        # Step 1: Pre-flight manifest & physical checksum verification
        manifest = self._verify_manifest_and_checksums(batch_id)
        batch_dir = self.landing_dir / batch_id
        files_map = manifest.get("files", {})

        sales_name = files_map.get("sales_slice", "sales_slice.csv")
        cal_name = files_map.get("calendar_slice", "calendar_slice.csv")
        prices_name = files_map.get("sell_prices_slice", "sell_prices_slice.csv")

        sales_path = batch_dir / sales_name
        cal_path = batch_dir / cal_name
        prices_path = batch_dir / prices_name

        day_keys = manifest["day_keys"]

        # Step 2: Validate Sales Header against manifest day_keys
        sales_source_cols = self._validate_sales_header(sales_path, day_keys)

        # Step 3: Parse CSVs with explicit schemas in FAILFAST mode
        try:
            cal_raw_df = (
                self.spark.read.format("csv")
                .option("header", "true")
                .option("mode", "FAILFAST")
                .schema(CALENDAR_STRUCT)
                .load(str(cal_path))
            )
            cal_count = cal_raw_df.count()

            prices_raw_df = (
                self.spark.read.format("csv")
                .option("header", "true")
                .option("mode", "FAILFAST")
                .schema(SELL_PRICES_STRUCT)
                .load(str(prices_path))
            )
            prices_count = prices_raw_df.count()

            # Dynamic sales schema construction
            sales_fields = [
                StructField(c, StringType(), False) for c in SALES_BASE_META_COLS
            ] + [StructField(dk, IntegerType(), False) for dk in day_keys]
            sales_struct = StructType(sales_fields)

            sales_raw_df = (
                self.spark.read.format("csv")
                .option("header", "true")
                .option("mode", "FAILFAST")
                .schema(sales_struct)
                .load(str(sales_path))
            )
            sales_count = sales_raw_df.count()

        except Exception as err:
            raise SchemaValidationError(
                f"Failed parsing CSVs in batch '{batch_id}' with FAILFAST: {err}"
            ) from err

        # Step 4: Reconcile parsed row counts with manifest
        if cal_count != manifest["calendar_row_count"]:
            raise RowCountMismatchError(
                f"Calendar row count mismatch: parsed {cal_count}, "
                f"manifest {manifest['calendar_row_count']}."
            )
        if prices_count != manifest["sell_prices_row_count"]:
            raise RowCountMismatchError(
                f"Sell prices row count mismatch: parsed {prices_count}, "
                f"manifest {manifest['sell_prices_row_count']}."
            )
        if sales_count != manifest["sales_row_count"]:
            raise RowCountMismatchError(
                f"Sales row count mismatch: parsed {sales_count}, "
                f"manifest {manifest['sales_row_count']}."
            )

        # Step 5: Add Metadata & Deterministic Row Hash
        cal_source_cols = [f.name for f in CALENDAR_STRUCT.fields]
        prices_source_cols = [f.name for f in SELL_PRICES_STRUCT.fields]

        cal_df = self._add_metadata_and_hash(
            cal_raw_df, batch_id, cal_name, cal_source_cols
        )
        prices_df = self._add_metadata_and_hash(
            prices_raw_df, batch_id, prices_name, prices_source_cols
        )
        sales_df = self._add_metadata_and_hash(
            sales_raw_df, batch_id, sales_name, sales_source_cols
        )

        # Step 6: Intra-batch Duplicate Business Key Check
        self._check_intra_batch_duplicates(cal_df, ["date"], "calendar")
        self._check_intra_batch_duplicates(
            prices_df, ["store_id", "item_id", "wm_yr_wk"], "sell_prices"
        )
        self._check_intra_batch_duplicates(sales_df, ["id"], "sales")

        # Step 7: Write to Bronze Delta Tables (Append-Only)
        self._write_table(
            cal_df, "calendar", batch_id, ["date"], allow_merge_schema=False
        )
        self._write_table(
            prices_df,
            "sell_prices",
            batch_id,
            ["store_id", "item_id", "wm_yr_wk"],
            allow_merge_schema=False,
        )
        self._write_table(sales_df, "sales", batch_id, ["id"], allow_merge_schema=True)

        return {
            "calendar": cal_count,
            "sell_prices": prices_count,
            "sales": sales_count,
        }
