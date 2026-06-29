import pandas as pd


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
    """

    if df.empty:
        print("Dataset is empty.")
        return False

    missing = [
        col
        for col in REQUIRED_COLUMNS
        if col not in df.columns
    ]

    if missing:
        print(f"Missing columns: {missing}")
        return False

    if df.index.has_duplicates:
        print("Duplicate timestamps detected.")
        return False

    if df.isnull().sum().sum() > 0:
        print("Dataset contains missing values.")
        return False

    if (df["Volume"] < 0).any():
        print("Negative volume detected.")
        return False

    return True