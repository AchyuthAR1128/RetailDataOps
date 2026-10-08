"""Unit tests for the M5 Replay Simulator module."""

from __future__ import annotations

import csv
import datetime
import json
from pathlib import Path

import pytest

from retaildataops.replay import (
    IdempotencyConflictError,
    InvalidDateRangeError,
    MissingSourceFileError,
    ReplaySimulator,
)


@pytest.fixture
def mock_raw_dir(tmp_path: Path) -> Path:
    """Create a temporary raw directory with lightweight mock M5 CSV files."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    # Mock calendar.csv (days d_1 to d_3 covering 2011-01-29 to 2011-01-31)
    calendar_file = raw_dir / "calendar.csv"
    with open(calendar_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
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
            ]
        )
        writer.writerow(
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
            ]
        )
        writer.writerow(
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
            ]
        )
        writer.writerow(
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
            ]
        )

    # Mock sales_train_validation.csv (2 items x 1 store)
    sales_file = raw_dir / "sales_train_validation.csv"
    with open(sales_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "id",
                "item_id",
                "dept_id",
                "cat_id",
                "store_id",
                "state_id",
                "d_1",
                "d_2",
                "d_3",
            ]
        )
        writer.writerow(
            [
                "HOBBIES_1_001_CA_1_validation",
                "HOBBIES_1_001",
                "HOBBIES_1",
                "HOBBIES",
                "CA_1",
                "CA",
                "0",
                "1",
                "2",
            ]
        )
        writer.writerow(
            [
                "HOBBIES_1_002_CA_1_validation",
                "HOBBIES_1_002",
                "HOBBIES_1",
                "HOBBIES",
                "CA_1",
                "CA",
                "5",
                "0",
                "1",
            ]
        )

    # Mock sell_prices.csv
    prices_file = raw_dir / "sell_prices.csv"
    with open(prices_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["store_id", "item_id", "wm_yr_wk", "sell_price"])
        writer.writerow(["CA_1", "HOBBIES_1_001", "11101", "9.58"])
        writer.writerow(["CA_1", "HOBBIES_1_002", "11101", "5.97"])

    return raw_dir


@pytest.fixture
def landing_dir(tmp_path: Path) -> Path:
    """Create a temporary landing directory."""
    landing = tmp_path / "landing"
    landing.mkdir()
    return landing


class TestReplaySimulator:
    """Test suite for ReplaySimulator."""

    def test_batch_id_generation_deterministic(self) -> None:
        """Batch IDs are deterministically formatted."""
        d1 = datetime.date(2011, 1, 29)
        d2 = datetime.date(2011, 1, 31)
        batch_id = ReplaySimulator.generate_batch_id(d1, d2)
        assert batch_id == "batch_20110129_20110131"

    def test_date_range_validation_valid(
        self, mock_raw_dir: Path, landing_dir: Path
    ) -> None:
        """Valid date range executes replay simulation successfully."""
        sim = ReplaySimulator(raw_dir=mock_raw_dir, landing_dir=landing_dir)
        manifest = sim.run("2011-01-29", "2011-01-30")
        assert manifest.batch_id == "batch_20110129_20110130"
        assert manifest.day_keys == ["d_1", "d_2"]
        assert manifest.sales_row_count == 2
        assert manifest.calendar_row_count == 2
        assert manifest.sell_prices_row_count == 2
        assert len(manifest.checksums["composite_checksum"]) == 64

    def test_date_range_validation_invalid_format(
        self, mock_raw_dir: Path, landing_dir: Path
    ) -> None:
        """Invalid date format raises InvalidDateRangeError."""
        sim = ReplaySimulator(raw_dir=mock_raw_dir, landing_dir=landing_dir)
        with pytest.raises(InvalidDateRangeError, match="Invalid start_date format"):
            sim.run("2011/01/29", "2011-01-30")

    def test_date_range_validation_reversed_dates(
        self, mock_raw_dir: Path, landing_dir: Path
    ) -> None:
        """start_date after end_date raises InvalidDateRangeError."""
        sim = ReplaySimulator(raw_dir=mock_raw_dir, landing_dir=landing_dir)
        with pytest.raises(InvalidDateRangeError, match="cannot be after end_date"):
            sim.run("2011-01-31", "2011-01-29")

    def test_date_range_out_of_bounds(
        self, mock_raw_dir: Path, landing_dir: Path
    ) -> None:
        """Date range outside calendar raises InvalidDateRangeError."""
        sim = ReplaySimulator(raw_dir=mock_raw_dir, landing_dir=landing_dir)
        with pytest.raises(InvalidDateRangeError, match="does not match any dates"):
            sim.run("2025-01-01", "2025-01-05")

    def test_missing_source_file(self, tmp_path: Path, landing_dir: Path) -> None:
        """Missing raw source files raise MissingSourceFileError."""
        empty_raw = tmp_path / "empty_raw"
        empty_raw.mkdir()
        sim = ReplaySimulator(raw_dir=empty_raw, landing_dir=landing_dir)
        with pytest.raises(MissingSourceFileError, match="Source file"):
            sim.run("2011-01-29", "2011-01-30")

    def test_deterministic_checksum_and_manifest(
        self, mock_raw_dir: Path, landing_dir: Path
    ) -> None:
        """Manifest contains all required fields and SHA256 checksums."""
        sim = ReplaySimulator(raw_dir=mock_raw_dir, landing_dir=landing_dir)
        manifest = sim.run("2011-01-29", "2011-01-30")

        manifest_file = landing_dir / manifest.batch_id / "manifest.json"
        assert manifest_file.exists()

        with open(manifest_file, encoding="utf-8") as f:
            data = json.load(f)

        assert data["source_files"] == [
            "sales_train_validation.csv",
            "calendar.csv",
            "sell_prices.csv",
        ]
        assert data["start_date"] == "2011-01-29"
        assert data["end_date"] == "2011-01-30"
        assert "sales_slice" in data["checksums"]
        assert "composite_checksum" in data["checksums"]

    def test_existing_batch_dir_without_manifest(
        self, mock_raw_dir: Path, landing_dir: Path
    ) -> None:
        """Missing manifest in existing batch dir raises error."""
        sim = ReplaySimulator(raw_dir=mock_raw_dir, landing_dir=landing_dir)
        d1 = datetime.date(2011, 1, 29)
        d2 = datetime.date(2011, 1, 30)
        batch_id = ReplaySimulator.generate_batch_id(d1, d2)
        batch_dir = landing_dir / batch_id
        batch_dir.mkdir(parents=True)

        with pytest.raises(IdempotencyConflictError, match=r"without a manifest\.json"):
            sim.run("2011-01-29", "2011-01-30")

    def test_repeated_replay_idempotency_success(
        self, mock_raw_dir: Path, landing_dir: Path
    ) -> None:
        """Replaying identical batch twice returns existing manifest without error."""
        sim = ReplaySimulator(raw_dir=mock_raw_dir, landing_dir=landing_dir)
        manifest1 = sim.run("2011-01-29", "2011-01-30")
        manifest2 = sim.run("2011-01-29", "2011-01-30")

        assert manifest1.batch_id == manifest2.batch_id
        assert manifest1.checksums == manifest2.checksums

    def test_idempotency_conflict_error_on_modified_content(
        self, mock_raw_dir: Path, landing_dir: Path
    ) -> None:
        """Replaying over existing batch with modified content
        raises IdempotencyConflictError."""
        sim = ReplaySimulator(raw_dir=mock_raw_dir, landing_dir=landing_dir)
        manifest = sim.run("2011-01-29", "2011-01-30")

        # Tamper with landing file content
        sales_file = landing_dir / manifest.batch_id / "sales_slice.csv"
        sales_file.write_text("tampered content", encoding="utf-8")

        with pytest.raises(
            IdempotencyConflictError, match="already exists with conflicting"
        ):
            sim.run("2011-01-29", "2011-01-30")
