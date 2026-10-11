# Retail Sales Warehouse

## Overview

This repository implements a daily pipeline that downloads a retail transactions dataset from Kaggle and loads it
into ClickHouse. dbt then models the data into a star schema (store and product dimensions, a sales fact table),
a daily sales aggregate, a monthly like-for-like status per store, and an ABC analysis of products. Airflow runs
the pipeline every day at midnight, and the whole stack starts with one `docker compose up`.

| Assignment requirement | Where |
|---|---|
| Download the dataset with a scheduled job and load it into a database | `download_dataset`, `load_raw_sales` → `raw.raw_sales` |
| Store dimension: unique stores with generated names | `dim_store` |
| Product dimension: unique products with generated names | `dim_product` |
| Sales fact table | `fact_sales` |
| Monthly like-for-like status per store | `dim_store_lfl` |
| Daily aggregate per day × store × product | `agg_sales_daily` |
| ABC analysis by quantity and by amount, written into the product dimension | `product_abc` → `dim_product.abc_quantity`, `dim_product.abc_amount` |
| Orchestration with Airflow, daily at midnight | DAG `retail_daily` |
| Everything runs with Docker Compose | `docker-compose.yml` |

## Architecture

Airflow runs the pipeline once a day. Python tasks download the CSV from Kaggle and load it, as text,
into the `raw` database in ClickHouse. dbt then types, cleans and models the data into the `analytics`
database. Postgres only stores Airflow's own metadata; no business data goes there.

```
Kaggle
  │  CSV (Kaggle API)
  ▼
┌─ Airflow DAG retail_daily (daily at 00:00 Asia/Tbilisi) ──────────────┐
│                                                                       │
│  download_dataset ──► ./data/Retail_Transaction_Dataset.csv           │
│        │                                                              │
│  load_raw_sales   ──► ClickHouse raw.raw_sales (every column String)  │
│        │                                                              │
│  dbt_staging      ──► analytics.stg_sales                             │
│        │                                                              │
│  dbt_core         ──► analytics.dim_store, analytics.fact_sales       │
│        │                                                              │
│  dbt_aggregates   ──► analytics.agg_sales_daily                       │
│        │                                                              │
│  dbt_analytics    ──► analytics.dim_store_lfl, analytics.product_abc, │
│                       analytics.dim_product                           │
└───────────────────────────────────────────────────────────────────────┘
```

`dim_product` is built in the last task because it needs the ABC classes from `product_abc`.
Each dbt task runs `dbt build`: it builds that layer's models and runs their tests before the next task starts.

| Service | Image | Role |
|---|---|---|
| `clickhouse` | `clickhouse/clickhouse-server:26.8.17.4` | Data warehouse. Databases `raw` (written by the loader) and `analytics` (written by dbt). |
| `postgres` | `postgres:16.9` | Airflow metadata only: DAG runs, task states, users. |
| `airflow-init` | `retail-airflow:dev` (built locally) | Runs once: creates the Airflow metadata tables and the admin user, then exits. |
| `airflow-scheduler` | `retail-airflow:dev` | Starts the DAG at midnight and runs its tasks (`LocalExecutor`: tasks run as processes inside this container). |
| `airflow-webserver` | `retail-airflow:dev` | Airflow UI on http://localhost:8080. |

The `retail-airflow` image is `apache/airflow:2.10.5-python3.11` plus:
- the pipeline's Python packages (`pandas`, `clickhouse-connect`, `kaggle`, `pytest`), installed with Airflow's constraints file;
- dbt (`dbt-core 1.12.5`, `dbt-clickhouse 1.10.3`) in its own virtualenv, `/opt/dbt-venv`, so its dependencies
  never conflict with Airflow's. Airflow calls it as a shell command.

Code is not copied into the image: `dags/`, `src/`, `dbt/`, `tests/` and `data/` are mounted from the repository,
so a change is picked up without a rebuild. ClickHouse and Postgres keep their data in named volumes, which survive
`docker compose down`. Both ports are bound to `127.0.0.1` only.

## Quick start

### Requirements

- Docker with Docker Compose v2 (`docker compose`, not `docker-compose`).
- About 4 GB of memory for Docker (the stack uses ~2.2 GB when idle) and ~3.5 GB of disk for the images.
- A Kaggle account and an API token: on kaggle.com, open Settings → API and create a new token.
- Tested on Linux and macOS. On Windows, use Docker Desktop with WSL2 and clone the repository inside WSL.

### Setup

```bash
git clone https://github.com/neochrist/retail-dwh
cd retail-dwh
cp .env.example .env
```

Edit `.env`:

| Variable | What to set |
|---|---|
| `KAGGLE_API_TOKEN` | Your Kaggle API token. |
| `CLICKHOUSE_PASSWORD`, `POSTGRES_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD` | Any passwords. Use only letters and digits for `POSTGRES_PASSWORD`: it is inserted into a connection URL. |
| `AIRFLOW_UID` | **Linux only**: your user id, so Airflow can write to the mounted `data/` and `dbt/` folders. |

On Linux, set `AIRFLOW_UID` with:

```bash
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
```

Start the stack. The first start builds the Airflow image, which takes a few minutes:

```bash
docker compose up -d
```

Run every `docker compose` command from the repository root: Compose finds `docker-compose.yml` and `.env`
in the current folder.

### Run the pipeline

The DAG is unpaused when it is created, so Airflow starts one run as soon as the scheduler is up.
After that it runs every day at 00:00 (Asia/Tbilisi). A full run takes under a minute.

To start a run by hand:

```bash
docker compose exec airflow-scheduler airflow dags trigger retail_daily
```

or open the Airflow UI at http://localhost:8080 (user and password from `.env`) and press ▶ on `retail_daily`.

Check the result of the runs:

```bash
docker compose exec airflow-scheduler airflow dags list-runs -d retail_daily
```

### Look at the results

ClickHouse has a web SQL editor at http://localhost:8123/play. Enter `CLICKHOUSE_USER` and
`CLICKHOUSE_PASSWORD` from `.env` in the top-right fields, then run for example:

```sql
select product_id, product_name, abc_quantity, abc_amount
from analytics.dim_product
order by product_id;
```

```
┌─product_id─┬─product_name─────┬─abc_quantity─┬─abc_amount─┐
│ A          │ Fresh Select A   │ C            │ C          │
│ B          │ Classic Choice B │ C            │ C          │
│ C          │ Organic Blend C  │ A            │ A          │
│ D          │ Classic Blend D  │ B            │ B          │
└────────────┴──────────────────┴──────────────┴────────────┘
```

```sql
select lfl_status, count() as store_months
from analytics.dim_store_lfl
group by lfl_status;
```

```
┌─lfl_status─┬─store_months─┐
│          0 │          248 │
└────────────┴──────────────┘
```

Every status is 0 with this dataset; see [Like-for-like](#like-for-like-dim_store_lfl) for why.

The same queries work from the terminal:

```bash
docker compose exec clickhouse sh -c \
  'clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --query "select count() from analytics.fact_sales"'
```

The single quotes make the container's shell, not yours, fill in the user and password, so the password never
appears in your shell history.

To stop the stack: `docker compose down` (data is kept in volumes). To delete all data as well:
`docker compose down -v`.

## Project layout

```
.
├── docker-compose.yml          # the five services
├── docker/airflow/Dockerfile   # Airflow image + pipeline packages + dbt in /opt/dbt-venv
├── .env.example                # settings template; copy to .env
├── dags/
│   └── retail_daily.py         # the DAG: six tasks, schedule, retries
├── src/ingestion/
│   └── raw_sales.py            # download from Kaggle, read, validate, load into raw.raw_sales
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml            # ClickHouse connection, read from environment variables
│   ├── models/
│   │   ├── staging/            # _sources.yml, stg_sales
│   │   └── marts/
│   │       ├── core/           # dim_store, fact_sales
│   │       ├── aggregates/     # agg_sales_daily
│   │       └── analytics/      # dim_store_lfl, product_abc, dim_product
│   └── tests/                  # singular tests: reconciliation, grain checks, store-code parsing
├── tests/
│   ├── test_raw_sales.py       # loader tests (run locally and in the container)
│   └── test_dags.py            # DAG integrity test (container only)
├── data/                       # downloaded CSV, not committed
├── pyproject.toml, uv.lock     # local Python tooling: ruff, pytest
└── .pre-commit-config.yaml     # ruff and pytest before every commit
```

Each dbt folder has a `.yml` file next to its models with descriptions, tests and unit tests.

## Pipeline (Airflow DAG)

One DAG, `retail_daily`, in `dags/retail_daily.py`:

| Task | Type | What it does |
|---|---|---|
| `download_dataset` | Python | Downloads and unzips the CSV into `data/`. Deletes the old file first, so a failed download can't leave yesterday's file behind. |
| `load_raw_sales` | Python | Reads the CSV with every value as text, validates it, then truncates `raw.raw_sales` and inserts all rows. Fails if the table's row count differs from the file's. |
| `dbt_staging` | Bash (dbt) | Builds and tests `models/staging`. |
| `dbt_core` | Bash (dbt) | Builds and tests `models/marts/core`. |
| `dbt_aggregates` | Bash (dbt) | Builds and tests `models/marts/aggregates`. |
| `dbt_analytics` | Bash (dbt) | Builds and tests `models/marts/analytics`. |

Settings:

| Setting | Value | Why |
|---|---|---|
| `schedule` | `0 0 * * *` | Daily at midnight, as the assignment asks. |
| `start_date` | `2026-10-01`, `tz="Asia/Tbilisi"` | Midnight means local midnight, not UTC (Airflow's default). |
| `catchup` | `False` | Every run reloads the full file, so missed days don't need to be replayed. |
| `max_active_runs` | `1` | Two runs truncating and loading the same table at once would conflict. |
| `retries` / `retry_delay` | `2` / 5 minutes | Covers short failures: network, Kaggle, ClickHouse restarting. |
| `execution_timeout` | 30 minutes | A hung task fails instead of blocking every later run. A normal run takes under a minute. |

**One dbt task per layer.** Each dbt task runs:

```bash
dbt build --select 'path:models/<folder>/*.sql' --indirect-selection buildable
```

- `dbt build` builds each model and runs its tests right after it. A failing test stops the task, so bad data
  never reaches the next layer, and the Airflow UI shows which layer failed.
- `path:models/<folder>/*.sql` selects only that folder's models.
- `--indirect-selection buildable` decides which task runs a test that touches several models: the test runs in
  the task where all its models have been built. For example, the relationship test from `fact_sales` to
  `dim_product` runs in `dbt_analytics`. Every test runs in exactly one task.
- `dbt_project.yml` turns "nothing selected" into an error, so a typo in a folder name fails the task instead of
  passing with nothing built.

**Idempotent.** Every run rebuilds everything from the file: the raw table is truncated and reloaded, and every dbt
model is a full table rebuild. Running the DAG twice gives the same result as running it once.

**Light DAG file.** The scheduler re-parses the DAG file every 30 seconds, so the file imports only Airflow and
`pendulum`. The loader (pandas, clickhouse-connect) and `kaggle` are imported inside the tasks. `kaggle` in
particular logs in as soon as it is imported.

## Data model

```
                 ┌──────────────┐
                 │  dim_store   │ 62 rows
                 │──────────────│
                 │ store_key PK │◄────────────┐
                 │ store_id     │             │
                 │ store_name   │             │
                 └──────────────┘             │
                                              │
┌────────────────┐                  ┌─────────┴─────────┐
│  dim_product   │ 4 rows           │    fact_sales     │ 100,000 rows
│────────────────│                  │───────────────────│
│ product_key PK │◄─────────────────│ transaction_id PK │
│ product_id     │                  │ store_key FK      │
│ product_name   │                  │ product_key FK    │
│ abc_quantity   │                  │ sale_date, ...    │
│ abc_amount     │                  └───────────────────┘
└────────────────┘
```

A star schema: one fact table, two dimensions. The other models are built from these:

| Model | Grain (one row per) | Rows | Built from |
|---|---|---|---|
| `raw.raw_sales` | CSV row | 100,000 | the CSV file |
| `stg_sales` | transaction | 100,000 | `raw_sales` |
| `dim_store` | store | 62 | `stg_sales` |
| `fact_sales` | transaction | 100,000 | `stg_sales` |
| `agg_sales_daily` | day × store × product | 59,629 | `fact_sales` |
| `dim_store_lfl` | year × month × store | 248 | `agg_sales_daily`, `dim_store` |
| `product_abc` | product | 4 | `agg_sales_daily`, `stg_sales` |
| `dim_product` | product | 4 | `stg_sales`, `product_abc` |

All models are ClickHouse `MergeTree` tables, rebuilt in full on every run, each with an explicit `order_by`
(dbt-clickhouse sets none by default).

### Source data

Kaggle dataset [`fahadrehman07/retail-transaction-dataset`](https://www.kaggle.com/datasets/fahadrehman07/retail-transaction-dataset),
one file, `Retail_Transaction_Dataset.csv`: 100,000 rows, 10 columns, 12.8 MB.

| Column | Example | Notes |
|---|---|---|
| `CustomerID` | `109318` | |
| `ProductID` | `C` | Only 4 values: A, B, C, D. |
| `Quantity` | `7` | |
| `Price` | `80.07984415` | Up to 8 decimal places. |
| `TransactionDate` | `12/26/2023 12:32` | `M/d/yyyy H:mm`, not zero-padded, no time zone. 2023-04-29 to 2024-04-28. |
| `PaymentMethod` | `Cash` | |
| `StoreLocation` | `176 Andrew Cliffs`<br>`Baileyfort, HI 93354` | A full address with a line break inside the quoted field. Different in every row. |
| `ProductCategory` | `Books` | Every product appears in all 4 categories. |
| `DiscountApplied(%)` | `18.6770995` | One value is written as `4.60E-05`. |
| `TotalAmount` | `455.8627638` | `= Quantity × Price × (1 − Discount / 100)`, up to 9 decimal places. |

No nulls, no duplicate rows, no transaction id. The file has 200,001 physical lines for 100,000 rows because of the
line breaks in addresses, so it must be read with a real CSV parser.

### Raw layer

`raw.raw_sales` holds the file as it is: the 10 columns renamed to snake_case (`DiscountApplied(%)` →
`discount_pct`), **every column a `String`**, plus `loaded_at`. The loader reads the CSV with `dtype=str` and
`keep_default_na=False`, so nothing is converted on the way in: `007` keeps its leading zero, prices keep every
digit, and the text `NA` stays text. Python only moves data; all typing happens in one place, in dbt staging.

### Staging

`stg_sales` types and cleans the raw rows. It is a table, not a view: a view is only evaluated when queried, so a
bad cast would pass the staging tests and fail later, in another layer.

| Column | How |
|---|---|
| `transaction_id` | `cityHash64` of a tuple of all 10 raw text columns. The source has no id; a duplicate row would get a duplicate id and fail the `unique` test. |
| `store_id` | The 2-letter code before the ZIP code at the end of `StoreLocation` (`HI` in `..., HI 93354`). See [Business rules](#business-rules-and-assumptions). |
| `store_key`, `product_key` | `cityHash64(store_id)`, `cityHash64(product_id)`. Computed here so the fact never has to read `dim_product`. |
| `sold_at` | Parsed with an explicit format and time zone (`Asia/Tbilisi`), so the time stays exactly as written in the file. |
| `sale_date` | `toDate(sold_at)`. |
| `unit_price`, `discount_pct`, `revenue` | `Decimal(18, 9)`: the source has up to 9 decimal places; no rounding anywhere. `revenue = TotalAmount`. |
| `gross_amount` | `quantity × unit_price` |
| `discount_amount` | `gross_amount − revenue` |

### Core: dim_store, dim_product, fact_sales

- **`dim_store`**: one row per store code, with a generated name such as `Sunny Grocer HI`.
- **`dim_product`**: one row per product, with a generated name such as `Organic Blend C`, and the two ABC classes.
  It lives in the `analytics` folder because it needs `product_abc`.
- **`fact_sales`**: one row per transaction: the keys, `customer_id`, `sold_at`, `sale_date`, the measures
  (`quantity`, `unit_price`, `gross_amount`, `discount_amount`, `revenue`, `discount_pct`) and the
  transaction's own attributes (`category`, `payment_method`, `store_location`).

Names are built from word lists, picking words by the hash key, so a store or product gets the same name on every
run. `category` stays on the fact, not on `dim_product`, because every product is sold in every category: it
describes the sale, not the product.

### Daily aggregate: agg_sales_daily

One row per `sale_date` × `store_key` × `product_key` (59,629 rows), read from `fact_sales` only.

| Column | Definition |
|---|---|
| `quantity_total` | `sum(quantity)` |
| `revenue_total` | `sum(revenue)` |
| `gross_amount_total`, `discount_amount_total` | sums, kept so discounts can be recomputed at any level |
| `weighted_discount_pct` | `discount_amount_total / gross_amount_total × 100` |
| `transaction_count` | `count()` |
| `unique_customers` | `uniqExact(customer_id)` |

`weighted_discount_pct` is weighted by money, not an average of row percentages: a 10% discount on 100 and 0% on 300
is 2.5%, not 5%. `unique_customers` is exact, but it can't be summed across days: a customer who bought on two days
would be counted twice.

### Like-for-like: dim_store_lfl

One row per year × month × store for the current year: `year`, `month`, `store_id`, `lfl_status` (1 or 0).

A store is like-for-like in a month if it sold on **every day** of that month in the current year **and** in the
same month of the previous year.

- **Current year** = the year of the latest date in the data (2024), never today's date.
- **Months** = January up to the latest month in the data (April) → 62 stores × 4 months = 248 rows.
- **Required days, current year:** every day of the month. For the last month, only the days up to and including
  the latest date (April 1–28).
- **Required days, previous year:** every day of that month in the previous year, counted from its own calendar
  (February 2024 has 29 days, February 2023 has 28).
- **Days worked** = distinct `sale_date` per store and month, from `agg_sales_daily`.
- Every store gets a row for every month, even with no sales (the model starts from all stores × all months).

**With this dataset every status is 0.** The data starts on 2023-04-29, so there are no sales in January to March
2023, and only 2 days of April 2023. No store can meet the previous-year condition. The logic itself is proven by a
dbt unit test on hand-made data (see [Testing](#testing)).

| Month 2024 | Days required | Stores that sold every day in 2024 | Days in the same month of 2023 |
|---|---|---|---|
| January | 31 | 40 of 62 | 0 |
| February | 29 | 46 of 62 | 0 |
| March | 31 | 40 of 62 | 0 |
| April | 28 | 45 of 62 | 2 (April 29–30) |

### ABC analysis: product_abc

One row per product with the totals, share, cumulative share and class, separately by quantity and by amount
(revenue). `dim_product` takes `abc_quantity` and `abc_amount` from here.

1. Sum each product's quantity and revenue from `agg_sales_daily`.
2. Sort products by total, largest first; ties are broken by `product_id`.
3. Running total from the top, with an explicit window frame (`rows between unbounded preceding and current row`).
4. Class by cumulative share: **A** up to 50%, **B** up to 70%, **C** above.

| Product | Quantity | Share | Cumulative | Class | Revenue | Share | Cumulative | Class |
|---|---|---|---|---|---|---|---|---|
| C | 125,978 | 25.15% | 25.15% | A | 6,243,648.53 | 25.14% | 25.14% | A |
| D | 125,786 | 25.11% | 50.26% | B | 6,242,924.93 | 25.14% | 50.28% | B |
| B | 125,070 | 24.97% | 75.23% | C | 6,208,129.58 | 25.00% | 75.28% | C |
| A | 124,095 | 24.77% | 100.00% | C | 6,138,792.47 | 24.72% | 100.00% | C |

The rule is applied literally: the product that crosses 50% is B, the one that crosses 70% is C. The four products
sell almost equally (synthetic data), so the classes are decided by small differences; with real data the
spread would be much larger.

## Business rules and assumptions

**A store is the 2-letter code at the end of the address.** The dataset has no store column. `StoreLocation` is a
full street address that is different in every row, so it can't be a store as it is. Every address ends with a
2-letter US postal code and a ZIP code (`..., HI 93354`), and this parses for all 100,000 rows into 62 codes:
50 states, DC, 8 territories (AS, FM, GU, MH, MP, PR, PW, VI) and 3 military mail codes (AA, AE, AP). The model
treats each code as one store. The original address stays on `fact_sales` (`store_location`), so the assumption can
be checked or changed later.

**A product is `ProductID`.** There are only 4 (A–D). Each one appears equally in all 4 categories, so
`category` describes the sale, not the product, and stays on the fact. The alternative, product × category as
16 products, was considered and rejected: the assignment defines a product by its id and name only.

**One CSV row is one transaction.** The file has no transaction id; `transaction_id` is a hash of the whole row.

**Amount means net revenue.** `revenue = TotalAmount`, the amount after discount. ABC "by amount" uses revenue.

**Time is local time.** `TransactionDate` has no time zone. It is read as written, in `Asia/Tbilisi`, so
`sale_date` is the date in the file and nothing moves across midnight.

**"Current" comes from the data, not the clock.** The current year and month for LFL are taken from the latest
date in the data (2024-04-28). The dataset is static, so using today's date (2026) would give an empty result.

**LFL: the previous year uses the full month.** The assignment asks for every day of the month in both years. For
the last, unfinished month of the current year (April 1–28) the model still requires all of April in the previous
year. This follows the assignment literally; common retail practice would compare the same days (April 1–28 in both
years).

**ABC is literal.** A ≤ 50% < B ≤ 70% < C, by cumulative share. The product that crosses 50% is B. If one product
alone is over 50%, there is no A. ABC covers the whole data window (one year), not a single month.

**The daily schedule is a simulation.** The Kaggle dataset doesn't change, so every run downloads the same file and
produces the same tables. The pipeline is still built as if the file changed daily: full download, full reload,
full rebuild.

## Edge cases

| Case | How it is handled |
|---|---|
| Line breaks inside addresses | pandas' CSV parser keeps a quoted field with a line break as one value. Tested in `test_raw_sales.py`. |
| Leading zeros, long decimals, the text `NA` | Read as text (`dtype=str`, `keep_default_na=False`); nothing is converted in Python. Tested. |
| Scientific notation (`4.60E-05` in `DiscountApplied(%)`) | `toDecimal64` reads it correctly as 0.000046. |
| Dates not zero-padded (`8/5/2023 0:00`) | Parsed with an explicit format (`M/d/yyyy H:mm`), so a wrong format fails instead of guessing. Unit tested. |
| Missing or renamed column, empty file | `validate` fails the task **before** the raw table is truncated, so yesterday's data stays. Tested. |
| Rows lost during insert | The loader compares the table's row count with the file's and fails if they differ. Tested. |
| Failed download | The old CSV is deleted first; if the new one is missing after download, the task fails. |
| Duplicate rows | Two identical rows get the same `transaction_id` and fail its `unique` test. |
| Address without a store code | `store_id` would be empty; a singular test fails if any row has no code. |
| Leap year | LFL counts each year's days from its own calendar (Feb 2024 = 29, Feb 2023 = 28). Unit tested. |
| Last month only partly in the data | Only the days up to the latest date are required in the current year. Unit tested. |
| Store with no sales in a month | Still gets a row, with status 0: the model starts from all stores × all months. Unit tested. |
| Several rows for one store and day | Days are counted as distinct dates, not rows. Unit tested. |
| ABC exactly at 50% or 70% | Compared exactly (`running × 100 <= grand × 50`), not as floats, so 50% stays A. Unit tested. |
| ABC ties | Equal totals are ordered by `product_id`, and each product gets its own step in the running total (`rows` frame), so tied products can get different classes and the result is the same on every run. Unit tested. |
| Top product alone over 50% | No product gets A; the rule is followed as written. |
| Zero gross amount in a day | `weighted_discount_pct` is 0 instead of dividing by zero. |
| Pipeline run twice | Same result: full truncate and reload, full rebuild of every model. |

## Design decisions

| Decision | Why | Alternative considered |
|---|---|---|
| ClickHouse | Columnar analytics database; fast `group by` on large fact tables; one container. | Postgres for everything: simpler, but row-based and not built for analytics. |
| Every raw column as `String` | Python only moves data; all typing in one place (staging), visible in SQL. A bad value fails a dbt test with a clear message instead of being silently changed by pandas. | Typing in pandas: floats would lose digits, `NA` would become null. |
| Full refresh every run | The source is one small static file; a full reload is simple and idempotent. | Incremental loading: needed for growing data (see [Production](#production)). |
| Validate, then truncate and insert | A bad file never empties the table. | `EXCHANGE TABLES` (load into a new table, then swap): also hides the short empty window, but only dbt reads raw, and only after loading, so it was not needed. |
| `Decimal(18, 9)` for money | The source has 9 decimal places; sums reconcile exactly with the raw file. | `Float64`: sums drift in the last digits and the reconciliation test can't be exact. |
| `cityHash64` surrogate keys, computed in staging | Stable across runs; the fact doesn't need to read the dimensions, which avoids the cycle `fact → dim_product → product_abc → fact`. | Sequential ids from the dimension: change between runs and need a lookup. |
| No date dimension | `sale_date` (a `Date`) is enough; ClickHouse has functions for year, month, weekday. | A `dim_date` table: useful with holidays or fiscal calendars, which this data doesn't have. |
| Staging as a table | Errors appear in the staging task, where they belong. | A view: a bad cast passes staging and fails later. |
| `agg_sales_daily` from `fact_sales` only | One path for numbers; the reconciliation test proves raw, fact and aggregate agree. | |
| `dim_product` rebuilt with the ABC classes | The assignment's "update" is done as a rebuild: no mutations in ClickHouse, the table is always consistent. | `ALTER TABLE ... UPDATE`: an asynchronous mutation in ClickHouse, slow and hard to test. |
| Airflow 2.10 | Mature, well documented, fewer services than Airflow 3 (no separate API server or DAG processor). | Airflow 3: current version; upgrade listed in [Production](#production). |
| `LocalExecutor` | Tasks run on the scheduler; no Redis or workers needed for six sequential tasks. | `CeleryExecutor`: for many parallel tasks across machines. |
| dbt inside the Airflow image, own virtualenv | One image, no extra service; the virtualenv keeps dbt's dependencies apart from Airflow's. | A separate dbt container: cleaner isolation, more moving parts (see [Production](#production)). |
| One dbt task per layer | A failure shows which layer broke; each layer is tested before the next one starts. | One `dbt build` task: simpler, but one red box for everything. |
| Postgres for Airflow metadata only | Airflow needs a database for its own state; business data stays in ClickHouse. | SQLite: only works with `SequentialExecutor`. |
| Pinned versions | Images and packages are pinned, so a fresh clone builds the same stack. | |

## Testing

Tests run in two places: dbt tests inside the pipeline on every run, and pytest for the Python code.

### dbt (54 tests, run by `dbt build` in every DAG run)

| Kind | Count | What they check |
|---|---|---|
| Generic tests | 44 | `unique` and `not_null` on every key and id; `relationships` from `fact_sales` to both dimensions and from `dim_store_lfl` to `dim_store`; `accepted_values` for ABC classes (A, B, C), LFL status (0, 1) and payment methods. |
| Singular tests (`dbt/tests/`) | 4 | **Reconciliation**: row count, total quantity and total revenue are equal in `raw_sales`, `fact_sales` and `agg_sales_daily`, and raw is not empty. **Grain**: no duplicate (day, store, product) in `agg_sales_daily`, no duplicate (year, month, store) in `dim_store_lfl`. **Store code**: every address gave a code. |
| Unit tests | 6 | Model logic on small hand-made inputs, listed below. |

The unit tests matter most for LFL and ABC, because the real data can't show the logic working: every LFL status
is 0, and the four products are almost equal.

| Unit test | Proves |
|---|---|
| `stg_sales_parses_store_date_and_amounts` | Multi-line addresses give the store code, non-padded dates parse, gross − discount = revenue. |
| `agg_sales_daily_counts_and_weighted_discount` | Counts and distinct customers per day; discount weighted by money (2.5%, not 5%). |
| `dim_store_lfl_full_months_missing_days_and_leap_year` | A store with every day in both years gets 1, including February 2024 (29 days) against February 2023 (28) and a partial last month; one missing day, a missing previous-year day or no sales at all gives 0; several rows on one day count as one day. |
| `product_abc_boundaries_and_ties` | Exactly 50% is A, exactly 70% is B. Two products with equal revenue each get their own step in the running total: the first crosses 50% and is B, the second is C. |
| `product_abc_amount_exact_boundaries` | The same boundaries by amount, with decimals where a float comparison would give the wrong class. |
| `dim_product_takes_each_class_from_its_own_measure` | `abc_quantity` and `abc_amount` are not swapped. |

Run all dbt tests by hand:

```bash
docker compose exec airflow-scheduler /opt/dbt-venv/bin/dbt build --project-dir /opt/airflow/dbt
```

### pytest (13 tests)

| File | Tests | Where it runs |
|---|---|---|
| `tests/test_raw_sales.py` | 10: the CSV is read as exact text (multi-line addresses, leading zeros, `NA`); validation rejects a missing column and an empty file; columns match the raw table; the table is truncated only after validation; a bad file never touches the table; a row-count mismatch fails the load. ClickHouse is replaced by a fake client. | Locally and in the container |
| `tests/test_dags.py` | 3: task order, schedule and time zone, retries and timeouts. | Container only (needs Airflow) |

```bash
docker compose exec airflow-scheduler pytest tests    # all 13
uv run pytest                                          # locally: 10 pass, test_dags.py is skipped
```

`uv run pytest` and ruff also run before every commit through pre-commit (`uv run pre-commit install` once).

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `download_dataset` is "up for retry" after a few seconds | `KAGGLE_API_TOKEN` is missing or wrong, or on Linux `AIRFLOW_UID` doesn't match your user, so the container can't write to `data/`. | Open the task's log in the UI. Fix `.env`, run `docker compose up -d`, then clear the task. |
| `no such service` / `no configuration file provided` | The command was run outside the repository folder. | `cd` into the repository root. |
| A change in `.env` has no effect | Containers read `.env` only when they are created; `restart` doesn't recreate them. | `docker compose up -d`. |
| `airflow-init` shows "Exited" | Normal: it runs once and stops. | Nothing. |
| Airflow can't connect to Postgres | `POSTGRES_PASSWORD` contains a character like `@`, `/` or `:` that breaks the connection URL. | Use only letters and digits, then `docker compose down -v` and `up -d` (Postgres keeps the first password). |
| dbt says `No dbt_project.yml found` | The `dbt/` folder was recreated on the host (for example by a git operation), and the container still sees the old, empty one. | `docker compose restart airflow-scheduler airflow-webserver`. |
| Containers restart or are killed | Not enough memory for Docker. | Give Docker at least 4 GB. |
| Port 8080 or 8123 already in use | Another program uses the port. | Stop it, or change the left side of the port mapping in `docker-compose.yml`. |

## Limitations

- **The data is static and small.** The daily schedule reloads the same file every day; full refresh is right for
  100,000 rows but wouldn't scale.
- **The store is an assumption.** A store is a 2-letter postal code, not a real store; 3 of the 62 are military mail
  codes. With a real store id in the source, only `stg_sales` would change.
- **LFL is 0 everywhere and ABC is close to a tie** with this data. The logic is proven by unit tests, not by the
  real results.
- **`unique_customers` can't be summed** across days, stores or products. Distinct customers for a longer period
  must be counted from `fact_sales`.
- **Two choices can't be proven by a test.** Removing only the `product_id` tie-breaker from the ABC sort still
  passes, because ClickHouse happens to keep tied rows in input order; the tie-breaker is there so the order is
  guaranteed, not lucky. Exact vs approximate distinct counting (`uniqExact` vs `uniq`) gives the same result on
  small test data.
- **Airflow 2.10 is past its end of support.**
- **The raw table is briefly empty** between truncate and insert. Nothing else reads it at that time.
- **One ClickHouse node, no backups, no CI.**
- **Airflow isn't installed locally**, so the editor can't resolve Airflow imports, and the DAG test runs only in
  the container.

## Production

What would change for a production deployment:

- **Incremental loading.** Load only new data (by date or by file), partition the fact table by month, and build
  dbt models incrementally instead of full rebuilds.
- **Atomic raw load.** Load into a new table and swap it with `EXCHANGE TABLES`, so readers never see an empty table.
- **Separate images.** dbt in its own image, run as a Kubernetes pod (`KubernetesPodOperator`) instead of inside the
  Airflow image; Airflow on Kubernetes with the official Helm chart. Code built into images instead of mounted.
- **Airflow 3.** Upgrade from 2.10, which is past end of support.
- **Secrets manager.** Credentials from Vault or a cloud secrets manager instead of a `.env` file.
- **CI.** On every pull request: ruff, pytest, the DAG test, and `dbt build` against a test database, all in a test
  image.
- **Monitoring and alerting.** Alerts on task failures, run duration and data freshness; dbt source freshness checks;
  ClickHouse metrics.
- **Data quality.** Row-count anomaly checks (today vs yesterday), and keeping rejected rows instead of failing the
  whole load.
- **ClickHouse.** Replicated tables, backups, separate users for loading, dbt and readers.
- **Documentation.** `dbt docs` published, so analysts can browse models, columns and lineage.
