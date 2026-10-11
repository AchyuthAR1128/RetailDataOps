# RetailDataOps Silver Transformation

The Silver layer converts the append-only Bronze M5 tables into a validated
daily fact table at the grain `(batch_id, id, d)`.

## Inputs

`SilverTransformer.transform` reads:

- Bronze `sales`, including its batch lineage columns and dynamically evolved
  `d_*` columns.
- Bronze `calendar`, keyed by `d`.
- Bronze `sell_prices`, keyed by `store_id`, `item_id`, and `wm_yr_wk`.
- A `day_keys_by_batch` mapping loaded from the landing manifests.

Only the `d_*` columns declared by each batch manifest are unpivoted. This is
important because Bronze schema evolution represents columns absent from older
batches as `NULL`; those nulls are not converted to zero or treated as
historical observations.

## Output

The Silver Delta table contains item/store dimensions, `d`, `date`,
`wm_yr_wk`, `sales_units`, `sell_price`, calendar attributes, event fields,
SNAP flags, `batch_id`, and a deterministic `silver_row_hash`.

## Validation

The transformation fails explicitly for missing columns, duplicate calendar or
price keys, duplicate Silver keys, negative sales, null/non-positive prices,
unmatched calendar or price joins, missing manifest day keys, and row-count
reconciliation failures. It never silently drops unmatched records.

`SilverTransformer.write` is append-only and idempotent. Rewriting identical
keys is a no-op; a key with a different deterministic hash raises
`SilverIdempotencyConflictError`.

## Usage

```bash
python scripts/run_silver.py \
  --bronze-dir data/bronze \
  --landing-dir data/landing \
  --silver-dir data/silver
```
