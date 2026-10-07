from pathlib import Path

import pandas as pd
import pytest

from ingestion.raw_sales import COLUMNS, read_sales_csv, to_raw, validate

HEADER = ",".join(COLUMNS)

# Two rows shaped like the real file: the address has a line break inside quotes,
# the customer id has a leading zero, and the payment method is the text "NA".
TWO_ROWS = f"""{HEADER}
007,C,7,80.07984415,12/26/2023 12:32,Cash,"176 Andrew Cliffs
Baileyfort, HI 93354",Books,18.6770995,455.8627638
42,A,1,1.10,8/5/2023 0:00,NA,"1 Main St
Springfield, VA 51707",Electronics,0,1.10
"""


def write_csv(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "sales.csv"
    path.write_text(text)
    return path


def test_read_keeps_multiline_address_in_one_row(tmp_path: Path) -> None:
    df = read_sales_csv(write_csv(tmp_path, TWO_ROWS))

    assert len(df) == 2
    assert df.loc[0, "StoreLocation"] == "176 Andrew Cliffs\nBaileyfort, HI 93354"


def test_read_keeps_every_value_as_exact_text(tmp_path: Path) -> None:
    df = read_sales_csv(write_csv(tmp_path, TWO_ROWS))

    assert df.loc[0, "CustomerID"] == "007"  # leading zero kept
    assert df.loc[0, "Price"] == "80.07984415"  # no float approximation
    assert df.loc[1, "PaymentMethod"] == "NA"  # not turned into NaN
    assert all(isinstance(value, str) for value in df.to_numpy().ravel())


def test_validate_accepts_expected_file(tmp_path: Path) -> None:
    validate(read_sales_csv(write_csv(tmp_path, TWO_ROWS)))  # no exception = pass


def test_validate_rejects_missing_column(tmp_path: Path) -> None:
    df = read_sales_csv(write_csv(tmp_path, TWO_ROWS)).drop(columns=["TotalAmount"])

    with pytest.raises(ValueError, match="TotalAmount"):
        validate(df)


def test_validate_rejects_header_only_file(tmp_path: Path) -> None:
    df = read_sales_csv(write_csv(tmp_path, HEADER + "\n"))

    with pytest.raises(ValueError, match="no rows"):
        validate(df)


def test_to_raw_renames_columns_and_adds_loaded_at(tmp_path: Path) -> None:
    raw = to_raw(read_sales_csv(write_csv(tmp_path, TWO_ROWS)))

    assert list(raw.columns) == [*COLUMNS.values(), "loaded_at"]
    assert raw["loaded_at"].count() == len(raw)  # count() skips missing values
    assert isinstance(raw, pd.DataFrame)
