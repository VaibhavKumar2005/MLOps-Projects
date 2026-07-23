from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
LIVE_DATA_DIR = PROJECT_ROOT / "data" / "live"
FEATURES_DIR = PROJECT_ROOT / "data" / "features"


def create_features(df, include_target=True):
    df = df.copy()

    # Moving averages
    df['MA_10'] = df['Close'].rolling(window=10).mean()
    df['MA_50'] = df['Close'].rolling(window=50).mean()

    # New moving averages
    df['SMA_20'] = df['Close'].rolling(window=20).mean()
    df['EMA_20'] = df['Close'].ewm(span=20, adjust=False).mean()

    # Returns
    df['Return'] = df['Close'].pct_change()

    # Volume change
    df['Volume_Change'] = df['Volume'].pct_change()

    # Lag features
    df['Lag_1'] = df['Close'].shift(1)
    df['Lag_2'] = df['Close'].shift(2)

    # Volatility
    df['Volatility'] = df['Return'].rolling(window=10).std()

    if include_target:
        # Target (next day price)
        df['Target'] = df['Close'].shift(-1)

    # Drop NaN
    df = df.dropna()

    return df


def build_feature_files(source_dir: Path = LIVE_DATA_DIR, target_dir: Path = FEATURES_DIR):
    """Create feature snapshots from the latest live raw data files."""

    target_dir.mkdir(parents=True, exist_ok=True)
    generated_files = []

    for source_file in sorted(source_dir.glob("*_live_data.csv")):
        df = pd.read_csv(source_file, index_col=0, parse_dates=True)
        df_features = create_features(df)
        output_file = target_dir / source_file.name.replace("_live_data.csv", "_features.csv")
        df_features.to_csv(output_file)
        generated_files.append(output_file)

    return generated_files


if __name__ == "__main__":
    files = build_feature_files()
    print(f"Generated {len(files)} feature file(s).")