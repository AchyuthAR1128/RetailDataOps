# RetailDataOps — Formal Data Contract

> **Contract Version:** 1.0.0
> **Status:** APPROVED (Phase 2 Step 3)
> **Source Platform:** Walmart M5 Forecasting Dataset (Makridakis Open Forecasting Center)
> **Target System:** RetailDataOps Medallion Platform

---

## 1. Overview & Scope

This data contract establishes formal data specifications, structural schemas, data quality expectations, relationship constraints, and analytical target definitions for the raw source data of the **RetailDataOps** platform.

All expectations in this document are grounded in empirical observations from the Phase 2 profiling step (`scripts/profile_m5.py`). Rules are tagged with one of three classifications:
- **`[OBSERVED]`**: Verified directly against empirical dataset structure and profiling output.
- **`[ASSUMPTION]`**: Inferred business rule or architectural constraint not directly specified by source metadata.
- **`[FUTURE VALIDATION]`**: Quality rule to be enforced during automated ingestion pipelines in later phases.

---

## 2. Project Limitations & Context

- **Historical Static Data `[OBSERVED]`**: M5 is a static, historical competition dataset spanning from 2011-01-29 to 2016-05-22.
- **No Direct Walmart API Connection `[OBSERVED]`**: RetailDataOps does not connect to internal, live Walmart ERP/POS systems.
- **No Physical Inventory Quantities `[OBSERVED]`**: The dataset contains daily sales unit counts (`sales`), but no stock-on-hand, warehouse inventory levels, or reorder point data.
- **No Real Stockout Labels `[OBSERVED]`**: True stockouts are not explicitly flagged in the source data.
- **Simulated Replay Stream `[ASSUMPTION]`**: Daily source ingestion will be simulated by streaming historical dates day-by-day in later phases.
- **Derived Demand-Risk Metric `[ASSUMPTION]`**: Demand-risk metrics created in later Gold/Analytical layers will be derived proxies (e.g., zero-sales streaks during high-demand weeks), not direct stockout measurements.

---

## 3. Source Datasets Contracts

### 3.1 `calendar.csv`

- **Purpose**: Dimension table mapping day identifiers (`d_1`..`d_1969`) to calendar dates, weekdays, years, SNAP eligibility flags, and cultural/religious/national events.
- **Expected Location**: `data/raw/calendar.csv` (or `data/raw/m5-forecasting-accuracy/calendar.csv`)
- **Source Grain**: **One row per calendar day** (`d`). `[OBSERVED]`
- **Row Count**: **1,969** rows `[OBSERVED]`
- **Date Range**: `2011-01-29` to `2016-06-19` `[OBSERVED]`

#### Schema Specification:
| Field Name | Data Type | Nullable? | Key Type | Constraint / Valid Values | Status |
|---|---|---|---|---|---|
| `date` | `DATE` (YYYY-MM-DD) | No | Primary Key | `2011-01-29` to `2016-06-19`, Unique | `[OBSERVED]` |
| `wm_yr_wk` | `INTEGER` | No | Foreign Key | `11101` to `11621` (282 unique weeks) | `[OBSERVED]` |
| `weekday` | `VARCHAR` | No | Attribute | `Saturday`, `Sunday`, `Monday`, `Tuesday`, `Wednesday`, `Thursday`, `Friday` | `[OBSERVED]` |
| `wday` | `INTEGER` | No | Attribute | Range: `1` (Saturday) to `7` (Friday) | `[OBSERVED]` |
| `month` | `INTEGER` | No | Attribute | Range: `1` to `12` | `[OBSERVED]` |
| `year` | `INTEGER` | No | Attribute | Range: `2011` to `2016` | `[OBSERVED]` |
| `d` | `VARCHAR` | No | Alternate Key | `d_1` to `d_1969`, Format: `d_\d+`, Unique | `[OBSERVED]` |
| `event_name_1` | `VARCHAR` | Yes (85.3%) | Attribute | 30 unique event names or NULL | `[OBSERVED]` |
| `event_type_1` | `VARCHAR` | Yes (85.3%) | Attribute | `Sporting`, `Cultural`, `National`, `Religious` or NULL | `[OBSERVED]` |
| `event_name_2` | `VARCHAR` | Yes (99.7%) | Attribute | 4 unique event names or NULL | `[OBSERVED]` |
| `event_type_2` | `VARCHAR` | Yes (99.7%) | Attribute | `Cultural`, `Religious` or NULL | `[OBSERVED]` |
| `snap_CA` | `INTEGER` | No | Flag | `0` or `1` | `[OBSERVED]` |
| `snap_TX` | `INTEGER` | No | Flag | `0` or `1` | `[OBSERVED]` |
| `snap_WI` | `INTEGER` | No | Flag | `0` or `1` | `[OBSERVED]` |

#### Key Rules & Quality Constraints:
- **`date` Uniqueness `[OBSERVED]`**: Exactly 1,969 unique dates. No duplicate dates allowed.
- **`d` Uniqueness `[OBSERVED]`**: Exactly 1,969 unique day keys (`d_1` to `d_1969`). No duplicate day keys allowed.
- **SNAP Values `[OBSERVED]`**: Must be strictly `0` or `1`. Breakdown: 1,319 zeros, 650 ones per state.
- **Event Nullability `[OBSERVED]`**: `event_name_1` is null for 1,680 days (85.3%). `event_name_2` is null for 1,964 days (99.7%). Nulls are expected and valid.

---

### 3.2 `sell_prices.csv`

- **Purpose**: Dimension/Fact table containing store-level weekly sell prices for each item SKU.
- **Expected Location**: `data/raw/sell_prices.csv` (or `data/raw/m5-forecasting-accuracy/sell_prices.csv`)
- **Source Grain**: **One row per store, per item SKU, per week** (`store_id`, `item_id`, `wm_yr_wk`). `[OBSERVED]`
- **Row Count**: **6,841,121** rows `[OBSERVED]`

#### Schema Specification:
| Field Name | Data Type | Nullable? | Key Type | Constraint / Valid Values | Status |
|---|---|---|---|---|---|
| `store_id` | `VARCHAR` | No | Composite PK / FK | 10 stores: `CA_1`..`CA_4`, `TX_1`..`TX_3`, `WI_1`..`WI_3` | `[OBSERVED]` |
| `item_id` | `VARCHAR` | No | Composite PK / FK | 3,049 item SKUs | `[OBSERVED]` |
| `wm_yr_wk` | `INTEGER` | No | Composite PK / FK | Range: `11101` to `11621` (282 weeks) | `[OBSERVED]` |
| `sell_price` | `NUMERIC(10,2)`| No | Measure | Range: `$0.01` to `$107.32`, Mean: `$4.41` | `[OBSERVED]` |

#### Key Rules & Quality Constraints:
- **Composite Primary Key Uniqueness `[OBSERVED]`**: `(store_id, item_id, wm_yr_wk)` has zero duplicates across 6,841,121 rows.
- **Nullability `[OBSERVED]`**: Zero null values across all 4 columns.
- **Non-Negative Price Rule `[OBSERVED]`**: `sell_price > 0.0`. Minimum observed price is `$0.01`. Zero or negative prices are invalid.

---

### 3.3 `sales_train_validation.csv`

- **Purpose**: Raw transactional sales matrix containing historical daily unit sales for validation.
- **Expected Location**: `data/raw/sales_train_validation.csv`
- **Source Grain (Raw Wide Format)**: **One row per item SKU per store**, with daily unit sales flattened as columns `d_1` through `d_1913`. `[OBSERVED]`
- **Row Count**: **30,490** rows (3,049 items × 10 stores) `[OBSERVED]`
- **Column Count**: **1,919** columns (6 metadata columns + 1,913 day columns) `[OBSERVED]`
- **Date Coverage**: `d_1` (2011-01-29) through `d_1913` (2016-04-24) `[OBSERVED]`

#### Schema Specification:
| Field Name | Data Type | Nullable? | Key Type | Constraint / Valid Values | Status |
|---|---|---|---|---|---|
| `id` | `VARCHAR` | No | Primary Key | Format: `{item_id}_{store_id}_validation` (30,490 unique) | `[OBSERVED]` |
| `item_id` | `VARCHAR` | No | Attribute / FK | 3,049 unique items (e.g., `HOBBIES_1_001`) | `[OBSERVED]` |
| `dept_id` | `VARCHAR` | No | Attribute | 7 departments: `FOODS_1`..`3`, `HOBBIES_1`..`2`, `HOUSEHOLD_1`..`2` | `[OBSERVED]` |
| `cat_id` | `VARCHAR` | No | Attribute | 3 categories: `FOODS`, `HOBBIES`, `HOUSEHOLD` | `[OBSERVED]` |
| `store_id` | `VARCHAR` | No | Attribute / FK | 10 stores: `CA_1`..`CA_4`, `TX_1`..`TX_3`, `WI_1`..`WI_3` | `[OBSERVED]` |
| `state_id` | `VARCHAR` | No | Attribute | 3 states: `CA`, `TX`, `WI` | `[OBSERVED]` |
| `d_1` .. `d_1913` | `INTEGER` | No | Measure | Daily unit sales quantity ($\ge 0$) | `[OBSERVED]` |

---

### 3.4 `sales_train_evaluation.csv`

- **Purpose**: Extended historical sales matrix for competition evaluation phase.
- **Expected Location**: `data/raw/sales_train_evaluation.csv`
- **Source Grain (Raw Wide Format)**: **One row per item SKU per store**, with daily unit sales flattened as columns `d_1` through `d_1941`. `[OBSERVED]`
- **Row Count**: **30,490** rows `[OBSERVED]`
- **Column Count**: **1,947** columns (6 metadata columns + 1,941 day columns) `[OBSERVED]`
- **Date Coverage**: `d_1` (2011-01-29) through `d_1941` (2016-05-22) — extends validation set by 28 days. `[OBSERVED]`

---

### 3.5 `sample_submission.csv`

- **Purpose**: Submission template schema for validation (28 days) and evaluation (28 days) predictions.
- **Expected Location**: `data/raw/sample_submission.csv`
- **Shape**: **60,980** rows × **29** columns (`id`, `F1`..`F28`) `[OBSERVED]`

---

## 4. Target Analytical Grain (Silver Layer Specification)

While raw source sales files are formatted in wide matrices (1,913 / 1,941 day columns per row), the normalized target analytical model in the downstream Silver layer will adhere to the following strict grain:

- **Target Silver Grain**: **ONE ROW = ONE ITEM SKU + ONE STORE + ONE CALENDAR DATE** `[ASSUMPTION]`
- **Silver Primary Key**: `(item_id, store_id, date)` `[ASSUMPTION]`
- **Uniqueness Expectation**: The tuple `(item_id, store_id, date)` MUST be 100% unique in the unpivoted Silver sales table. `[FUTURE VALIDATION]`
- **Total Unpivoted Fact Sales Rows**:
  - Validation scope: $30,490 \text{ rows} \times 1,913 \text{ days} = \mathbf{58,327,370 \text{ rows}}$ `[OBSERVED]`
  - Evaluation scope: $30,490 \text{ rows} \times 1,941 \text{ days} = \mathbf{59,181,090 \text{ rows}}$ `[OBSERVED]`

---

## 5. Dataset Relationships & Referential Integrity

```
┌───────────────────────────┐                     ┌───────────────────────────┐
│       calendar.csv        │                     │      sell_prices.csv      │
├───────────────────────────┤                     ├───────────────────────────┤
│ PK: date                  │                     │ PK: (store_id, item_id,   │
│ AK: d                     │                     │      wm_yr_wk)            │
│ FK: wm_yr_wk              │                     │ FK: store_id, item_id,    │
└─────────────┬─────────────┘                     │     wm_yr_wk              │
              │ (d / date)                        └─────────────┬─────────────┘
              │                                                 │ (store_id, item_id,
              ▼                                                 │  wm_yr_wk)
┌───────────────────────────────────────────────────────────────▼─────────────┐
│                       sales_train_validation/evaluation                      │
├─────────────────────────────────────────────────────────────────────────────┤
│ Wide Source Grain:   (item_id, store_id) x d_1..d_N                         │
│ Target Silver Grain: (item_id, store_id, date)                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Relationships:
1. **Sales → Calendar (`sales.d` → `calendar.d`) `[OBSERVED]`**:
   - Every day column (`d_1`..`d_1913`/`d_1941`) maps to exactly one calendar row via `calendar.d`, yielding a deterministic `date`.
2. **Sales → Sell Prices (`(store_id, item_id, wm_yr_wk)`) `[OBSERVED]`**:
   - Every sales observation for a given item, store, and date resolves to a specific week `wm_yr_wk` in `calendar.csv`.
   - The combination `(store_id, item_id, wm_yr_wk)` links sales observations directly to `sell_prices.csv`.
3. **Referential Integrity Expectations `[FUTURE VALIDATION]`**:
   - 100% of store IDs in sales MUST exist in `sell_prices.csv` and `calendar.csv` SNAP flags.
   - 100% of item IDs in sales MUST exist in `sell_prices.csv`.
   - 100% of `d` keys in sales MUST exist in `calendar.csv`.

---

## 6. Formal Data Quality Rules & Validation Suite

| Rule ID | Category | Target Field / Table | Rule Description | Enforcement | Classification |
|---|---|---|---|---|---|
| **DQ-01** | Structure | All CSVs | All expected columns must be present with exact header names. | Ingestion Check | `[FUTURE VALIDATION]` |
| **DQ-02** | Nullability | `sales.*` metadata | Zero null values in `id`, `item_id`, `dept_id`, `cat_id`, `store_id`, `state_id`. | Ingestion Check | `[OBSERVED]` |
| **DQ-03** | Nullability | `sell_prices.*` | Zero null values in `store_id`, `item_id`, `wm_yr_wk`, `sell_price`. | Ingestion Check | `[OBSERVED]` |
| **DQ-04** | Nullability | `calendar.date`, `d` | Zero null values in `date`, `d`, `wm_yr_wk`, `weekday`, `wday`, `month`, `year`. | Ingestion Check | `[OBSERVED]` |
| **DQ-05** | Uniqueness | `calendar.date` | `date` must be 100% unique across all 1,969 rows. | Ingestion Check | `[OBSERVED]` |
| **DQ-06** | Uniqueness | `calendar.d` | `d` key must be 100% unique across all 1,969 rows. | Ingestion Check | `[OBSERVED]` |
| **DQ-07** | Uniqueness | `sell_prices` PK | `(store_id, item_id, wm_yr_wk)` composite key must have 0 duplicates. | Ingestion Check | `[OBSERVED]` |
| **DQ-08** | Uniqueness | `sales` PK (wide) | `(item_id, store_id)` must be 100% unique (30,490 rows). | Ingestion Check | `[OBSERVED]` |
| **DQ-09** | Value Domain | `store_id` | Must belong to set `{'CA_1','CA_2','CA_3','CA_4','TX_1','TX_2','TX_3','WI_1','WI_2','WI_3'}`. | Ingestion Check | `[OBSERVED]` |
| **DQ-10** | Value Domain | `state_id` | Must belong to set `{'CA', 'TX', 'WI'}`. | Ingestion Check | `[OBSERVED]` |
| **DQ-11** | Value Domain | Hierarchy | `cat_id` and `dept_id` must follow valid mappings (`FOODS` → `FOODS_1..3`, `HOBBIES` → `HOBBIES_1..2`, `HOUSEHOLD` → `HOUSEHOLD_1..2`). | Ingestion Check | `[OBSERVED]` |
| **DQ-12** | Range | `sales.unit_sales` | Daily unit sales must be non-negative integer ($\ge 0$). | Transformation | `[FUTURE VALIDATION]` |
| **DQ-13** | Range | `sell_prices.sell_price` | Sell price must be positive float ($> 0.00$). Observed min is `$0.01`. | Ingestion Check | `[OBSERVED]` |
| **DQ-14** | Value Domain | `snap_*` flags | `snap_CA`, `snap_TX`, `snap_WI` must be strictly `0` or `1`. | Ingestion Check | `[OBSERVED]` |
| **DQ-15** | Ref Integrity | Sales → Calendar | Every day column `d_N` in sales must map to a valid `d` in `calendar.csv`. | Transformation | `[FUTURE VALIDATION]` |
| **DQ-16** | Ref Integrity | Sales → Sell Prices | Every `(store_id, item_id, wm_yr_wk)` in sales must exist in `sell_prices.csv` for active weeks. | Transformation | `[FUTURE VALIDATION]` |
| **DQ-17** | Anomaly | Row Count Check | Ingestion fails if `sales` row count $\neq 30,490$ or `calendar` row count $\neq 1,969$. | Ingestion Check | `[FUTURE VALIDATION]` |
| **DQ-18** | Freshness | Streaming Replay | Simulated daily batch arrivals must follow strict chronological day sequence (`d_t` before `d_{t+1}`). | Pipeline Engine | `[ASSUMPTION]` |

---

## 7. Data Contract Acceptance Criteria

The data contract is considered **ACCEPTED and VALIDATED** because:

- [x] **Grains are explicit**: Source grains (wide sales, weekly price, daily calendar) and target Silver grain `(item_id, store_id, date)` are clearly defined.
- [x] **Keys are explicit**: Primary keys, alternate keys, and composite business keys are explicitly listed.
- [x] **Relationships are explicit**: Foreign key linkages between sales, calendar, and sell prices are documented.
- [x] **Nullable fields are explicit**: Event nullability in calendar (85.3% / 99.7%) vs zero-null metadata fields are documented.
- [x] **Validation rules are explicit**: 18 concrete data quality rules with enforcement stages are defined.
- [x] **Assumptions are clearly labeled**: Every non-observed rule is tagged with `[ASSUMPTION]` or `[FUTURE VALIDATION]`.
- [x] **Limitations are documented**: Historical scope, non-monitored inventory, and simulated replay constraints are explicitly recorded.

---

*End of Data Contract — RetailDataOps Platform*
