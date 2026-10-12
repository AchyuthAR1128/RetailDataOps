# RetailDataOps Gold Analytics

Gold tables are deterministic analytical projections of the validated Silver
daily fact table. They are historical aggregates over the M5 dataset, not
real-time inventory measures or causal models.

## Tables and grains

| Table | Grain |
|---|---|
| `daily_sales_by_store` | `date`, `store_id`, `state_id` |
| `weekly_sales_by_store` | `wm_yr_wk`, `store_id`, `state_id` |
| `product_category_performance` | `item_id`, `dept_id`, `cat_id`, `store_id`, `state_id` |
| `price_sales_analysis` | `wm_yr_wk`, `store_id`, `item_id`, `cat_id`, `dept_id` |

Every table includes `sales_units_total`, `sales_observation_count`,
`null_sales_count`, and `record_count`. A null sales value is excluded from
the numeric total and counted explicitly in `null_sales_count`; it is never
silently converted to zero.

The daily and weekly tables also include average observed selling price and a
`revenue_total` proxy calculated as `sales_units * sell_price` only where both
values are present. These are descriptive historical measures. They do not
establish causal price effects, real-time inventory, confirmed stockouts, or
measured business impact.

## Validation and idempotency

Gold validates required Silver columns, non-null keys, duplicate Silver keys,
and non-negative sales. Output keys are checked for duplicates and nulls.
`GoldTransformer.write_table` writes Delta output idempotently: identical
reruns are no-ops, while conflicting content for an existing key raises an
explicit error.

## Usage

```bash
python scripts/run_gold.py \
  --silver-dir data/silver \
  --gold-dir data/gold
```
