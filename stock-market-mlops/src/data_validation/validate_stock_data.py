import logging

import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
]


def validate_stock_data(df: pd.DataFrame) -> bool:
    """
    Validate downloaded stock data.
    Returns True if valid, False otherwise.
    """

    # Dataset exists
    if df.empty:
        logger.error("Dataset is empty.")
        return False

    # Required columns
    missing = [
        col
        for col in REQUIRED_COLUMNS
        if col not in df.columns
    ]

    if missing:
        logger.error(f"Missing columns: {missing}")
        return False

    # Duplicate timestamps
    if df.index.has_duplicates:
        logger.error("Duplicate timestamps detected.")
        return False

    # Missing values
    if df[REQUIRED_COLUMNS].isnull().any().any():
        logger.error("Missing values detected.")
        return False

    # Numeric checks
    if (df["Open"] <= 0).any():
        logger.error("Invalid Open prices.")
        return False

    if (df["High"] <= 0).any():
        logger.error("Invalid High prices.")
        return False

    if (df["Low"] <= 0).any():
        logger.error("Invalid Low prices.")
        return False

    if (df["Close"] <= 0).any():
        logger.error("Invalid Close prices.")
        return False

    if (df["Volume"] < 0).any():
        logger.error("Negative volume detected.")
        return False

    # Logical OHLC relationships
    if (df["High"] < df["Low"]).any():
        logger.error("High price lower than Low price.")
        return False

    if (df["High"] < df["Open"]).any():
        logger.error("High lower than Open.")
        return False

    if (df["High"] < df["Close"]).any():
        logger.error("High lower than Close.")
        return False

    if (df["Low"] > df["Open"]).any():
        logger.error("Low higher than Open.")
        return False

    if (df["Low"] > df["Close"]).any():
        logger.error("Low higher than Close.")
        return False

    # Sorted timestamps
    if not df.index.is_monotonic_increasing:
        logger.error("Dates are not sorted.")
        return False

    logger.info("Stock data validation passed.")
    return True