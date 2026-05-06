import pandas as pd


def create_features(df, include_target=True):
    df = df.copy()

    # Moving averages
    df['MA_10'] = df['Close'].rolling(window=10).mean()
    df['MA_50'] = df['Close'].rolling(window=50).mean()

    # Returns
    df['Return'] = df['Close'].pct_change()

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


if __name__ == "__main__":
    df = pd.read_csv("data/AAPL_stock_data.csv", index_col=0, parse_dates=True)

    df_features = create_features(df)

    print(df_features.head())
    print(df_features.columns)