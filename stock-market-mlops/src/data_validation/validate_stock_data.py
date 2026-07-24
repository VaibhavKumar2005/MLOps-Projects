import logging
from typing import Final

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS: Final[list[str]] = [
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
]

PRICE_COLUMNS = [
    "Open",
    "High",
    "Low",
    "Close",
]

MIN_PRICE = 0
MIN_VOLUME = 0
MAX_PRICE = 1_000_000

class DataValidationError(Exception):
    """Raised when stock data validation fails."""


def _validate_dataset(df: pd.DataFrame) -> None:
    if df.empty:
        raise DataValidationError("Dataset is empty.")

    if not isinstance(df.index, pd.DatetimeIndex):
        raise DataValidationError("Index must be DatetimeIndex.")

    if not df.index.is_monotonic_increasing:
        raise DataValidationError("Index is not sorted.")

    if df.index.has_duplicates:
        raise DataValidationError("Duplicate timestamps detected.")

    if df.duplicated().any():
        raise DataValidationError("Duplicate rows detected.")


def _validate_columns(df: pd.DataFrame) -> None:
    missing = [
        c for c in REQUIRED_COLUMNS
        if c not in df.columns
    ]

    if missing:
        raise DataValidationError(
            f"Missing columns: {missing}"
        )


def _validate_missing_values(df: pd.DataFrame) -> None:
    if df[REQUIRED_COLUMNS].isnull().any().any():
        raise DataValidationError(
            "Missing values detected."
        )


def _validate_numeric(df: pd.DataFrame) -> None:
    if np.isinf(df[REQUIRED_COLUMNS].to_numpy()).any():
        raise DataValidationError(
            "Infinite values detected."
        )

    for col in PRICE_COLUMNS:
        if (df[col] <= 0).any():
            raise DataValidationError(
                f"{col} contains invalid prices."
            )

    if (df["Volume"] < 0).any():
        raise DataValidationError(
            "Negative volume detected."
        )


def _validate_ohlc(df: pd.DataFrame) -> None:
    checks = [
        (df["High"] >= df["Low"], "High < Low"),
        (df["High"] >= df["Open"], "High < Open"),
        (df["High"] >= df["Close"], "High < Close"),
        (df["Low"] <= df["Open"], "Low > Open"),
        (df["Low"] <= df["Close"], "Low > Close"),
    ]

    for passed, message in checks:
        if not passed.all():
            raise DataValidationError(message)

class DataValidationError(Exception):
    """Raised when stock data validation fails."""
    
def validate_stock_data(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Validate stock market OHLCV dataset.

    Returns
    -------
    pd.DataFrame
        Validated dataframe.

    Raises
    ------
    DataValidationError
    """

    logger.info("Starting stock data validation...")

    _validate_dataset(df)
    _validate_columns(df)
    _validate_missing_values(df)
    _validate_numeric(df)
    _validate_ohlc(df)

    logger.info("Validation successful.")

    return df