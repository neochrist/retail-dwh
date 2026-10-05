"""Download the Kaggle retail dataset and full-refresh load it into raw.raw_sales.

Python only to move data: every source column is loaded as text as in the file.
Typing and business logic happen in dbt.
"""

import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import clickhouse_connect
import pandas as pd
from clickhouse_connect.driver.client import Client

logger = logging.getLogger(__name__)

DATASET = "fahadrehman07/retail-transaction-dataset"
CSV_NAME = "Retail_Transaction_Dataset.csv"

# Source header -> raw column name. Also the list of columns the file must contain.
COLUMNS = {
    "CustomerID": "customer_id",
    "ProductID": "product_id",
    "Quantity": "quantity",
    "Price": "price",
    "TransactionDate": "transaction_date",
    "PaymentMethod": "payment_method",
    "StoreLocation": "store_location",
    "ProductCategory": "product_category",
    "DiscountApplied(%)": "discount_pct",
    "TotalAmount": "total_amount",
}

# dbt suggests writing sql in lowercase:
# docs: https://docs.getdbt.com/best-practices/how-we-style/2-how-we-style-our-sql
CREATE_DATABASE = "create database if not exists raw"

CREATE_TABLE = """
create table if not exists raw.raw_sales
(
    customer_id       String,
    product_id        String,
    quantity          String,
    price             String,
    transaction_date  String,
    payment_method    String,
    store_location    String,
    product_category  String,
    discount_pct      String,
    total_amount      String,
    loaded_at         DateTime('UTC')
)
engine = MergeTree
order by tuple()
"""


def download_dataset(data_dir: Path) -> Path:
    """Download and unzip the dataset into data_dir; return the CSV path."""
    # Imported here, not at the top: importing anything from `kaggle` tries to log in immediately.
    # docs: https://github.com/Kaggle/kaggle-cli/blob/main/src/kaggle/__init__.py
    from kaggle.api.kaggle_api_extended import KaggleApi

    data_dir.mkdir(parents=True, exist_ok=True)
    api = KaggleApi()
    api.authenticate()

    logger.info("Downloading %s into %s", DATASET, data_dir)
    api.dataset_download_files(DATASET, path=data_dir, unzip=True, force=True)

    csv_path = data_dir / CSV_NAME
    if not csv_path.exists():
        raise FileNotFoundError(f"Expected {csv_path} after download")
    logger.info("Downloaded %s (%d bytes)", csv_path, csv_path.stat().st_size)
    return csv_path


def read_sales_csv(csv_path: Path) -> pd.DataFrame:
    """Read the CSV with every value kept as text."""

    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    logger.info("Read %d rows from %s", len(df), csv_path)
    return df


def validate(df: pd.DataFrame) -> None:
    """Fail before touching the database if the file is not what we expect."""
    missing = sorted(set(COLUMNS) - set(df.columns))
    if missing:
        raise ValueError(f"CSV is missing columns: {missing}")
    if df.empty:
        raise ValueError("CSV has no rows")


def to_raw(df: pd.DataFrame) -> pd.DataFrame:
    """Keep the expected columns, rename them to snake_case, add loaded_at."""
    raw = df.filter(items=list(COLUMNS)).rename(columns=COLUMNS)
    raw["loaded_at"] = datetime.now(UTC)
    return raw


def get_client() -> Client:
    """Connect to ClickHouse using settings from the environment."""
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ["CLICKHOUSE_PORT"]),
        username=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
    )


def load_raw_sales(raw: pd.DataFrame, client: Client) -> None:
    """Full refresh: create the table if needed, empty it, insert all rows, check the count."""
    client.command(CREATE_DATABASE)
    client.command(CREATE_TABLE)
    client.command("truncate table raw.raw_sales")
    client.insert_df("raw.raw_sales", raw)

    loaded = client.command("select count() from raw.raw_sales")
    if loaded != len(raw):
        raise RuntimeError(f"Inserted {len(raw)} rows but raw.raw_sales has {loaded}")
    logger.info("Loaded %d rows into raw.raw_sales", loaded)


def load_from_csv(csv_path: Path) -> None:
    """Read, validate, and load one CSV file. Validation runs before the table is truncated."""
    df = read_sales_csv(csv_path)
    validate(df)
    load_raw_sales(to_raw(df), get_client())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    data_dir = Path(os.environ.get("DATA_DIR", "data"))
    load_from_csv(download_dataset(data_dir))
