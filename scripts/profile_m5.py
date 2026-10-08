"""Profile M5 Forecasting dataset files in data/raw/.

Inspects CSV files without modifying them and prints complete profiling metrics.
Uses Python standard library for maximum portability and zero extra dependencies.
"""

from __future__ import annotations

import csv
import math
from collections import Counter
from pathlib import Path


def find_data_dir() -> Path:
    """Find data/raw directory, searching root or nested subdirectories."""
    base_dir = Path("data/raw")
    if not base_dir.exists():
        raise FileNotFoundError("Directory 'data/raw' does not exist.")

    # Check if CSV files are directly in data/raw
    csv_files = list(base_dir.glob("*.csv"))
    if csv_files:
        return base_dir

    # Check subdirectories (e.g. data/raw/m5-forecasting-accuracy)
    subdirs = [d for d in base_dir.iterdir() if d.is_dir()]
    for subdir in subdirs:
        if list(subdir.glob("*.csv")):
            return subdir

    raise FileNotFoundError("No CSV files found in 'data/raw' or its subdirectories.")


def profile_file_general(file_path: Path) -> dict:
    """Perform general profiling on a CSV file."""
    file_size_bytes = file_path.stat().st_size
    file_size_mb = file_size_bytes / (1024 * 1024)

    with open(file_path, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        col_count = len(header)

        row_count = 0
        first_5: list[list[str]] = []
        last_5: list[list[str]] = []

        # Track nulls per column
        null_counts = [0] * col_count

        # For duplicate row detection
        # (sampled hash set if very large, or full for medium files)
        # Note: for sell_prices (6.8M rows) full row string hashing uses
        # ~200MB RAM in Python.
        seen_row_hashes: set[int] = set()
        duplicate_rows = 0

        for row in reader:
            row_count += 1
            if len(first_5) < 5:
                first_5.append(row)

            # Keep a sliding window of last 5
            if len(last_5) >= 5:
                last_5.pop(0)
            last_5.append(row)

            # Null check (empty string)
            for i, val in enumerate(row):
                if val == "" or val is None:
                    null_counts[i] += 1

            # Duplicate check
            row_tuple = tuple(row)
            h = hash(row_tuple)
            if h in seen_row_hashes:
                duplicate_rows += 1
            else:
                seen_row_hashes.add(h)

    return {
        "file_name": file_path.name,
        "file_size_bytes": file_size_bytes,
        "file_size_mb": file_size_mb,
        "row_count": row_count,
        "col_count": col_count,
        "header": header,
        "first_5": first_5,
        "last_5": last_5,
        "null_counts": null_counts,
        "duplicate_rows": duplicate_rows,
    }


def profile_calendar(file_path: Path) -> None:
    """Detailed profiling for calendar.csv."""
    print("=" * 80)
    print(f"PROFILING FILE: {file_path.name}")
    print("=" * 80)

    gen = profile_file_general(file_path)
    print(
        f"File Size: {gen['file_size_mb']:.2f} MB "
        f"({gen['file_size_bytes']:,} bytes)"
    )
    print(f"Row Count: {gen['row_count']:,}")
    print(f"Column Count: {gen['col_count']}")
    print(f"Duplicate Rows: {gen['duplicate_rows']:,}")
    print(f"\nColumns ({gen['col_count']}):")

    with open(file_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    header = gen["header"]
    for i, col in enumerate(header):
        null_cnt = gen["null_counts"][i]
        null_pct = (null_cnt / gen["row_count"]) * 100 if gen["row_count"] > 0 else 0
        vals = [r[col] for r in rows if r[col] != ""]
        unique_cnt = len(set(vals))
        print(
            f"  - {col:<15} | Nulls: {null_cnt:>5,} ({null_pct:>5.1f}%) "
            f"| Unique: {unique_cnt:>5,}"
        )

    # Specific calendar checks
    dates = [r["date"] for r in rows]
    wm_yr_wks = [r["wm_yr_wk"] for r in rows]
    weekdays = Counter(r["weekday"] for r in rows)
    events_1 = Counter(r["event_name_1"] for r in rows if r["event_name_1"])
    event_types_1 = Counter(r["event_type_1"] for r in rows if r["event_type_1"])
    events_2 = Counter(r["event_name_2"] for r in rows if r["event_name_2"])
    event_types_2 = Counter(r["event_type_2"] for r in rows if r["event_type_2"])
    snap_ca = Counter(r["snap_CA"] for r in rows)
    snap_tx = Counter(r["snap_TX"] for r in rows)
    snap_wi = Counter(r["snap_WI"] for r in rows)

    print("\nCALENDAR SPECIFIC METRICS:")
    print(f"  - Date Range: {min(dates)} to {max(dates)}")
    print(f"  - Unique wm_yr_wk values: {len(set(wm_yr_wks))}")
    print(f"  - Weekday counts: {dict(weekdays)}")
    print(
        f"  - Event Name 1 count: {len(events_1)} unique events, "
        f"top 5: {events_1.most_common(5)}"
    )
    print(f"  - Event Type 1 breakdown: {dict(event_types_1)}")
    print(
        f"  - Event Name 2 count: {len(events_2)} unique events, list: {dict(events_2)}"
    )
    print(f"  - Event Type 2 breakdown: {dict(event_types_2)}")
    print(f"  - SNAP CA breakdown (0/1): {dict(snap_ca)}")
    print(f"  - SNAP TX breakdown (0/1): {dict(snap_tx)}")
    print(f"  - SNAP WI breakdown (0/1): {dict(snap_wi)}")

    print("\nFIRST 5 ROWS:")
    for r in gen["first_5"]:
        print("  ", r)

    print("\nLAST 5 ROWS:")
    for r in gen["last_5"]:
        print("  ", r)


def profile_sell_prices(file_path: Path) -> None:
    """Detailed profiling for sell_prices.csv."""
    print("\n" + "=" * 80)
    print(f"PROFILING FILE: {file_path.name}")
    print("=" * 80)

    file_size_bytes = file_path.stat().st_size
    file_size_mb = file_size_bytes / (1024 * 1024)

    stores: set[str] = set()
    items: set[str] = set()
    weeks: set[str] = set()
    business_keys: set[tuple[str, str, str]] = set()

    row_count = 0
    duplicate_business_keys = 0
    null_counts = {"store_id": 0, "item_id": 0, "wm_yr_wk": 0, "sell_price": 0}

    prices: list[float] = []
    min_price = float("inf")
    max_price = float("-inf")
    sum_price = 0.0

    first_5: list[list[str]] = []
    last_5: list[list[str]] = []

    with open(file_path, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

        for row in reader:
            row_count += 1
            if len(first_5) < 5:
                first_5.append(row)
            if len(last_5) >= 5:
                last_5.pop(0)
            last_5.append(row)

            store_id, item_id, wm_yr_wk, sell_price_str = row[0], row[1], row[2], row[3]

            if not store_id:
                null_counts["store_id"] += 1
            if not item_id:
                null_counts["item_id"] += 1
            if not wm_yr_wk:
                null_counts["wm_yr_wk"] += 1
            if not sell_price_str:
                null_counts["sell_price"] += 1

            stores.add(store_id)
            items.add(item_id)
            weeks.add(wm_yr_wk)

            key = (store_id, item_id, wm_yr_wk)
            if key in business_keys:
                duplicate_business_keys += 1
            else:
                business_keys.add(key)

            if sell_price_str:
                p = float(sell_price_str)
                if p < min_price:
                    min_price = p
                if p > max_price:
                    max_price = p
                sum_price += p
                # Sample 100k prices for std dev calculation
                if row_count % 68 == 0:
                    prices.append(p)

    mean_price = sum_price / row_count if row_count > 0 else 0.0
    # Calculate std dev on sample
    variance = (
        sum((x - mean_price) ** 2 for x in prices) / len(prices) if prices else 0.0
    )
    std_price = math.sqrt(variance)

    print(f"File Size: {file_size_mb:.2f} MB ({file_size_bytes:,} bytes)")
    print(f"Row Count: {row_count:,}")
    print(f"Column Count: {len(header)}")
    print(f"Header: {header}")
    print(f"Null Counts: {null_counts}")
    print("\nSELL PRICES SPECIFIC METRICS:")
    print(f"  - Unique Stores: {len(stores)} ({sorted(list(stores))})")
    print(f"  - Unique Items: {len(items):,}")
    print(f"  - Unique Weeks (wm_yr_wk): {len(weeks)}")
    print(
        f"  - Unique (store_id, item_id, wm_yr_wk) "
        f"Business Keys: {len(business_keys):,}"
    )
    print(f"  - Duplicate Business Keys: {duplicate_business_keys:,}")
    print(f"  - Min Sell Price: ${min_price:.2f}")
    print(f"  - Max Sell Price: ${max_price:.2f}")
    print(f"  - Mean Sell Price: ${mean_price:.2f}")
    print(f"  - Std Dev Sell Price (sampled): ${std_price:.2f}")

    print("\nFIRST 5 ROWS:")
    for r in first_5:
        print("  ", r)

    print("\nLAST 5 ROWS:")
    for r in last_5:
        print("  ", r)


def profile_sales(file_path: Path, calendar_file: Path | None = None) -> None:
    """Detailed profiling for sales_train_validation/evaluation.csv."""
    print("\n" + "=" * 80)
    print(f"PROFILING FILE: {file_path.name}")
    print("=" * 80)

    file_size_bytes = file_path.stat().st_size
    file_size_mb = file_size_bytes / (1024 * 1024)

    with open(file_path, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        col_count = len(header)

        id_cols = header[:6]  # id, item_id, dept_id, cat_id, store_id, state_id
        day_cols = header[6:]  # d_1 ... d_N

        row_count = 0
        items: set[str] = set()
        depts: set[str] = set()
        cats: set[str] = set()
        stores: set[str] = set()
        states: set[str] = set()
        item_store_combos: set[tuple[str, str]] = set()

        first_5: list[list[str]] = []
        last_5: list[list[str]] = []

        total_nulls = 0

        for row in reader:
            row_count += 1
            if len(first_5) < 5:
                first_5.append(row[:6] + [f"({len(day_cols)} day values...)"])
            if len(last_5) >= 5:
                last_5.pop(0)
            last_5.append(row[:6] + [f"({len(day_cols)} day values...)"])

            item_id, dept_id, cat_id, store_id, state_id = (
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
            )
            items.add(item_id)
            depts.add(dept_id)
            cats.add(cat_id)
            stores.add(store_id)
            states.add(state_id)
            item_store_combos.add((item_id, store_id))

            # Null check in metadata
            for val in row[:6]:
                if val == "":
                    total_nulls += 1

    print(f"File Size: {file_size_mb:.2f} MB ({file_size_bytes:,} bytes)")
    print(f"Row Count: {row_count:,}")
    print(f"Column Count: {col_count:,}")
    print(f"Metadata Columns (6): {id_cols}")
    print(f"Day Columns ({len(day_cols)}): {day_cols[0]} through {day_cols[-1]}")
    print(f"Metadata Null Count: {total_nulls}")
    print("\nSALES SPECIFIC METRICS:")
    print(f"  - Unique items: {len(items):,}")
    print(f"  - Unique departments: {len(depts)} ({sorted(list(depts))})")
    print(f"  - Unique categories: {len(cats)} ({sorted(list(cats))})")
    print(f"  - Unique stores: {len(stores)} ({sorted(list(stores))})")
    print(f"  - Unique states: {len(states)} ({sorted(list(states))})")
    print(
        f"  - Unique item/store combinations: {len(item_store_combos):,} "
        f"(matches row_count: {len(item_store_combos) == row_count})"
    )

    # Calendar mapping check
    if calendar_file and calendar_file.exists():
        with open(calendar_file, encoding="utf-8") as f:
            cal_reader = csv.DictReader(f)
            cal_rows = {r["d"]: r["date"] for r in cal_reader}

        start_day, end_day = day_cols[0], day_cols[-1]
        start_date = cal_rows.get(start_day, "Unknown")
        end_date = cal_rows.get(end_day, "Unknown")
        print(
            f"  - Calendar Date Coverage: {start_day} ({start_date}) "
            f"to {end_day} ({end_date})"
        )

    print("\nFIRST 5 ROWS (Metadata + Summary):")
    for r in first_5:
        print("  ", r)

    print("\nLAST 5 ROWS (Metadata + Summary):")
    for r in last_5:
        print("  ", r)


def main() -> None:
    data_dir = find_data_dir()
    print(f"Found dataset directory: {data_dir.resolve()}\n")

    calendar_path = data_dir / "calendar.csv"
    sell_prices_path = data_dir / "sell_prices.csv"
    val_path = data_dir / "sales_train_validation.csv"
    eval_path = data_dir / "sales_train_evaluation.csv"
    sub_path = data_dir / "sample_submission.csv"

    if calendar_path.exists():
        profile_calendar(calendar_path)

    if sell_prices_path.exists():
        profile_sell_prices(sell_prices_path)

    if val_path.exists():
        profile_sales(val_path, calendar_path)

    if eval_path.exists():
        profile_sales(eval_path, calendar_path)

    if sub_path.exists():
        print("\n" + "=" * 80)
        print(f"PROFILING FILE: {sub_path.name}")
        print("=" * 80)
        gen = profile_file_general(sub_path)
        print(
            f"File Size: {gen['file_size_mb']:.2f} MB "
            f"({gen['file_size_bytes']:,} bytes)"
        )
        print(f"Row Count: {gen['row_count']:,}")
        print(f"Column Count: {gen['col_count']}")
        print(f"Header: {gen['header'][:5]} ... {gen['header'][-1]}")
        print(f"Null Counts Total: {sum(gen['null_counts'])}")


if __name__ == "__main__":
    main()
