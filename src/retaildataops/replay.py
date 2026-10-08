"""M5 Replay Simulator module for RetailDataOps.

Simulates daily source data delivery from historical M5 datasets into
the landing zone with deterministic batch IDs, SHA256 checksums, and manifest metadata.
"""

from __future__ import annotations

import csv
import datetime
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


class ReplayError(Exception):
    """Base exception for replay simulator errors."""


class InvalidDateRangeError(ReplayError):
    """Raised when the specified date range is invalid or out of bounds."""


class MissingSourceFileError(ReplayError):
    """Raised when expected source files are missing in raw directory."""


class IdempotencyConflictError(ReplayError):
    """Raised when an existing batch has conflicting content or metadata."""


@dataclass
class Manifest:
    """Batch manifest metadata schema."""

    batch_id: str
    source_files: list[str]
    start_date: str
    end_date: str
    day_keys: list[str]
    sales_row_count: int
    calendar_row_count: int
    sell_prices_row_count: int
    files: dict[str, str]
    checksums: dict[str, str]
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        """Convert manifest instance to dictionary representation."""
        return asdict(self)


class ReplaySimulator:
    """Deterministic, idempotent replay simulator for M5 datasets."""

    def __init__(
        self,
        raw_dir: str | Path,
        landing_dir: str | Path,
        sales_filename: str = "sales_train_validation.csv",
        calendar_filename: str = "calendar.csv",
        sell_prices_filename: str = "sell_prices.csv",
    ) -> None:
        """Initialize the ReplaySimulator.

        Args:
            raw_dir: Base directory containing raw source files.
            landing_dir: Target landing directory for generated batches.
            sales_filename: Name of raw sales CSV.
            calendar_filename: Name of raw calendar CSV.
            sell_prices_filename: Name of raw sell prices CSV.
        """
        self.raw_dir = Path(raw_dir)
        self.landing_dir = Path(landing_dir)
        self.sales_filename = sales_filename
        self.calendar_filename = calendar_filename
        self.sell_prices_filename = sell_prices_filename

    def _resolve_raw_path(self, filename: str) -> Path:
        """Resolve source file path in raw directory or subdirectories."""
        direct_path = self.raw_dir / filename
        if direct_path.exists():
            return direct_path

        # Check subdirectories (e.g. data/raw/m5-forecasting-accuracy/)
        for child in self.raw_dir.iterdir() if self.raw_dir.exists() else []:
            if child.is_dir() and (child / filename).exists():
                return child / filename

        raise MissingSourceFileError(
            f"Source file '{filename}' not found in '{self.raw_dir}' or subdirectories."
        )

    def _validate_dates(
        self, start_date_str: str, end_date_str: str
    ) -> tuple[datetime.date, datetime.date]:
        """Parse and validate date string arguments."""
        try:
            start_date = datetime.date.fromisoformat(start_date_str)
        except ValueError as err:
            raise InvalidDateRangeError(
                f"Invalid start_date format '{start_date_str}'. Expected YYYY-MM-DD."
            ) from err

        try:
            end_date = datetime.date.fromisoformat(end_date_str)
        except ValueError as err:
            raise InvalidDateRangeError(
                f"Invalid end_date format '{end_date_str}'. Expected YYYY-MM-DD."
            ) from err

        if start_date > end_date:
            msg = (
                f"start_date '{start_date_str}' cannot be "
                f"after end_date '{end_date_str}'."
            )
            raise InvalidDateRangeError(msg)

        return start_date, end_date

    @staticmethod
    def generate_batch_id(start_date: datetime.date, end_date: datetime.date) -> str:
        """Generate deterministic batch_id from date range."""
        s_str = start_date.strftime("%Y%m%d")
        e_str = end_date.strftime("%Y%m%d")
        return f"batch_{s_str}_{e_str}"

    @staticmethod
    def compute_file_sha256(file_path: Path) -> str:
        """Compute SHA256 checksum of a file on disk."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()

    def run(
        self,
        start_date_str: str,
        end_date_str: str,
    ) -> Manifest:
        """Execute replay simulation for the given date range.

        Args:
            start_date_str: ISO start date (YYYY-MM-DD).
            end_date_str: ISO end date (YYYY-MM-DD).

        Returns:
            Manifest instance describing the replayed batch.
        """
        start_date, end_date = self._validate_dates(start_date_str, end_date_str)
        batch_id = self.generate_batch_id(start_date, end_date)
        batch_dir = self.landing_dir / batch_id
        manifest_path = batch_dir / "manifest.json"

        # Resolve raw file paths
        calendar_path = self._resolve_raw_path(self.calendar_filename)
        sales_path = self._resolve_raw_path(self.sales_filename)
        sell_prices_path = self._resolve_raw_path(self.sell_prices_filename)

        # Step 1: Read calendar to map dates -> day_keys (d_1..) and active wm_yr_wks
        day_keys: list[str] = []
        active_weeks: set[str] = set()
        calendar_slice_rows: list[list[str]] = []
        calendar_header: list[str] = []

        with open(calendar_path, encoding="utf-8") as f:
            reader = csv.reader(f)
            calendar_header = next(reader)
            date_idx = calendar_header.index("date")
            d_idx = calendar_header.index("d")
            wm_yr_wk_idx = calendar_header.index("wm_yr_wk")

            for row in reader:
                row_date = datetime.date.fromisoformat(row[date_idx])
                if start_date <= row_date <= end_date:
                    day_keys.append(row[d_idx])
                    active_weeks.add(row[wm_yr_wk_idx])
                    calendar_slice_rows.append(row)

        if not day_keys:
            raise InvalidDateRangeError(
                f"Date range [{start_date_str}, {end_date_str}] "
                "does not match any dates in calendar.csv."
            )

        # Step 2: Check idempotency if batch directory already exists
        if batch_dir.exists():
            if not manifest_path.exists():
                raise IdempotencyConflictError(
                    f"Batch directory '{batch_dir}' already exists without a "
                    "manifest.json. Manual intervention required — replay will "
                    "not overwrite existing batch data."
                )

            with open(manifest_path, encoding="utf-8") as f:
                existing_manifest_data = json.load(f)

            # Check metadata match
            if (
                existing_manifest_data.get("start_date") == start_date_str
                and existing_manifest_data.get("end_date") == end_date_str
                and existing_manifest_data.get("day_keys") == day_keys
            ):
                # Verify disk file checksums
                sales_file = batch_dir / "sales_slice.csv"
                cal_file = batch_dir / "calendar_slice.csv"
                price_file = batch_dir / "sell_prices_slice.csv"

                if sales_file.exists() and cal_file.exists() and price_file.exists():
                    chk = existing_manifest_data.get("checksums", {})
                    s_sha = self.compute_file_sha256(sales_file)
                    s_ok = s_sha == chk.get("sales_slice")
                    c_ok = self.compute_file_sha256(cal_file) == chk.get(
                        "calendar_slice"
                    )
                    p_ok = self.compute_file_sha256(price_file) == chk.get(
                        "sell_prices_slice"
                    )
                    if s_ok and c_ok and p_ok:
                        # Identical batch exists — return existing manifest
                        return Manifest(**existing_manifest_data)

            raise IdempotencyConflictError(
                f"Batch directory '{batch_dir}' already exists with conflicting "
                "metadata or content. Manual intervention required — replay will "
                "not overwrite existing batch data."
            )

        # Create batch directory if not exists
        batch_dir.mkdir(parents=True, exist_ok=True)

        # Step 3: Write calendar_slice.csv
        cal_slice_path = batch_dir / "calendar_slice.csv"
        with open(cal_slice_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(calendar_header)
            writer.writerows(calendar_slice_rows)

        # Step 4: Stream and write sales_slice.csv
        sales_slice_path = batch_dir / "sales_slice.csv"
        sales_row_count = 0
        with (
            open(sales_path, encoding="utf-8") as f_in,
            open(sales_slice_path, "w", encoding="utf-8", newline="") as f_out,
        ):
            reader = csv.reader(f_in)
            writer = csv.writer(f_out)
            header = next(reader)

            meta_cols = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"]
            meta_indices = [header.index(col) for col in meta_cols]
            day_indices = [header.index(dk) for dk in day_keys]

            slice_header = meta_cols + day_keys
            writer.writerow(slice_header)

            for row in reader:
                sales_row_count += 1
                meta_vals = [row[idx] for idx in meta_indices]
                day_vals = [row[idx] for idx in day_indices]
                writer.writerow(meta_vals + day_vals)

        # Step 5: Stream and write sell_prices_slice.csv
        sell_prices_slice_path = batch_dir / "sell_prices_slice.csv"
        sell_prices_row_count = 0
        with (
            open(sell_prices_path, encoding="utf-8") as f_in,
            open(sell_prices_slice_path, "w", encoding="utf-8", newline="") as f_out,
        ):
            reader = csv.reader(f_in)
            writer = csv.writer(f_out)
            header = next(reader)
            writer.writerow(header)

            wm_yr_wk_idx = header.index("wm_yr_wk")
            for row in reader:
                if row[wm_yr_wk_idx] in active_weeks:
                    sell_prices_row_count += 1
                    writer.writerow(row)

        # Compute checksums
        cal_sha = self.compute_file_sha256(cal_slice_path)
        sales_sha = self.compute_file_sha256(sales_slice_path)
        prices_sha = self.compute_file_sha256(sell_prices_slice_path)

        composite_input = f"{sales_sha}:{cal_sha}:{prices_sha}".encode()
        composite_sha = hashlib.sha256(composite_input).hexdigest()

        created_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

        manifest = Manifest(
            batch_id=batch_id,
            source_files=[
                self.sales_filename,
                self.calendar_filename,
                self.sell_prices_filename,
            ],
            start_date=start_date_str,
            end_date=end_date_str,
            day_keys=day_keys,
            sales_row_count=sales_row_count,
            calendar_row_count=len(calendar_slice_rows),
            sell_prices_row_count=sell_prices_row_count,
            files={
                "sales_slice": sales_slice_path.name,
                "calendar_slice": cal_slice_path.name,
                "sell_prices_slice": sell_prices_slice_path.name,
            },
            checksums={
                "sales_slice": sales_sha,
                "calendar_slice": cal_sha,
                "sell_prices_slice": prices_sha,
                "composite_checksum": composite_sha,
            },
            created_at=created_at,
        )

        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest.to_dict(), f, indent=2)

        return manifest
