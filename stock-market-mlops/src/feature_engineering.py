import pandas as pd

def create_features(data):
    df['MA_10'] = df['Close'].rolling(window= 10).mean()
    df['MA_50'] = df['Close'].rolling(window= 50).mean()

    df['Return'] = df['Close'].pct_change()

    # Target (next day price)
    df['Target'] = df['Close'].shift(-1)

    # Drop NaN values
    df = df.dropna()

    return df

if __name__ == "__main__":
    df = pd.read_csv("data/AAPL_stock_data.csv")

    df_features = create_features(df)

    print(df_features.head())
    print(df_features.columns)