# RetailDataOps — M5 Replay Simulator Technical Specification

> **Module:** `src/retaildataops/replay.py`
> **CLI Runner:** `scripts/run_replay.py`
> **Test Suite:** `tests/test_replay.py`
> **Status:** IMPLEMENTED (Phase 3 Step 1)

---

## 1. Purpose

The **M5 Replay Simulator** creates a deterministic, reproducible landing zone by slicing historical Walmart M5 data into daily/date-range batches. Because M5 is a static historical dataset, the simulator mimics an operational source system that delivers daily sales and price files into a landing directory prior to raw ingestion into the Medallion Bronze layer.

---

## 2. Inputs & Location Resolution

The simulator reads from the raw data layer in read-only mode (`r`):

* **Sales File**: `sales_train_validation.csv` (or `sales_train_evaluation.csv`)
* **Calendar File**: `calendar.csv`
* **Prices File**: `sell_prices.csv`

**Location Resolution Algorithm**:
1. Checks `data/raw/<filename>` directly.
2. Checks subdirectories such as `data/raw/m5-forecasting-accuracy/<filename>`.
3. If not found, raises `MissingSourceFileError`.

---

## 3. Outputs & Landing Directory Structure

Output files are created under `data/landing/<batch_id>/`:

```
data/landing/
└── batch_20110129_20110204/
    ├── calendar_slice.csv         # Sliced calendar rows for requested date range
    ├── sales_slice.csv            # Sliced sales matrix (metadata + d_1..d_7)
    ├── sell_prices_slice.csv      # Sliced prices for active weeks in date range
    └── manifest.json              # Batch audit metadata & SHA256 checksums
```

---

## 4. Deterministic Batch Concept & Naming

* **Batch ID Naming Scheme**: `batch_YYYYMMDD_YYYYMMDD` (derived from start and end dates).
* **Deterministic Filtering**:
  * Date range translates to specific day keys (`d_1`..`d_N`) via `calendar.csv`.
  * `sales_slice.csv` extracts exact column indices matching `id_cols + day_keys`.
  * `sell_prices_slice.csv` filters rows matching active `wm_yr_wk` values for the date range.

---

## 5. Manifest Specification (`manifest.json`)

Each landing batch contains a `manifest.json` providing complete auditability:

```json
{
  "batch_id": "batch_20110129_20110204",
  "source_files": [
    "sales_train_validation.csv",
    "calendar.csv",
    "sell_prices.csv"
  ],
  "start_date": "2011-01-29",
  "end_date": "2011-02-04",
  "day_keys": ["d_1", "d_2", "d_3", "d_4", "d_5", "d_6", "d_7"],
  "sales_row_count": 30490,
  "calendar_row_count": 7,
  "sell_prices_row_count": 21343,
  "files": {
    "sales_slice": "sales_slice.csv",
    "calendar_slice": "calendar_slice.csv",
    "sell_prices_slice": "sell_prices_slice.csv"
  },
  "checksums": {
    "sales_slice": "a1b2c3d4...",
    "calendar_slice": "e5f6g7h8...",
    "sell_prices_slice": "i9j0k1l2...",
    "composite_checksum": "m3n4o5p6..."
  },
  "created_at": "2026-10-08T14:05:00Z"
}
```

* **`created_at`**: Audit metadata timestamp that is intentionally runtime-dependent. While sliced files (`sales_slice.csv`, etc.), batch IDs, and SHA256 checksums are completely deterministic, `created_at` reflects the exact run time and is NOT included in idempotency validation.

---

## 6. Idempotency & Conflict Rules

The simulator strictly enforces idempotency without silent overwrites:

1. **New Batch**: When `data/landing/<batch_id>/` does not exist, files and `manifest.json` are created normally.
2. **Missing Manifest**: If the `batch_id` directory exists but `manifest.json` is missing, raises `IdempotencyConflictError`. It never writes into an existing batch directory unless existing manifest/checksums prove it is an identical batch.
3. **Identical Replay**: When `batch_id` directory and `manifest.json` exist:
   - Reads existing `manifest.json`.
   - Verifies `start_date`, `end_date`, `day_keys`, and SHA256 checksums of files on disk. (Note: `created_at` is excluded from this comparison.)
   - If **identical**, logs/returns the existing manifest without rewriting files (Idempotent Success).
4. **Conflicting Replay**: If the directory exists but manifest metadata or file checksums do not match:
   - Raises `IdempotencyConflictError`.
   - Never deletes or overwrites existing landing batch data automatically.

---

## 7. Performance & Memory Efficiency

- **Line-by-Line Streaming**: Uses Python's native `csv.reader` and `csv.writer` to stream sales matrix rows.
- **Low Memory Overhead**: Memory usage scales with the number of day columns requested (e.g. 7 columns), not total dataset size (58M+ unpivoted rows).

---

## 8. Limitations

- **Source Scope**: Replays historical dates up to `d_1913` (validation) or `d_1941` (evaluation).
- **Format**: Produces sliced CSV files in the landing zone prior to Delta Lake Bronze ingestion in Phase 3 Step 2.

---

## 9. Usage Examples

### Command Line Interface (CLI)

```bash
# Run replay simulator for a 7-day batch
python scripts/run_replay.py --start-date 2011-01-29 --end-date 2011-02-04

# Run with custom directories
python scripts/run_replay.py --start-date 2011-01-29 --end-date 2011-02-04 --raw-dir data/raw --landing-dir data/landing
```

### Python API

```python
from retaildataops.replay import ReplaySimulator

simulator = ReplaySimulator(raw_dir="data/raw", landing_dir="data/landing")
manifest = simulator.run(start_date_str="2011-01-29", end_date_str="2011-02-04")
print(f"Created batch {manifest.batch_id} with checksum {manifest.checksums['composite_checksum']}")
```
