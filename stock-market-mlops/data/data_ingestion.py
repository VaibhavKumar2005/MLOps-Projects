import yfinance as yf

tickers = ["AAPL", "MSFT", "TSLA"]

for ticker in tickers:
    df = yf.download(
        ticker,
        period="2y",
        auto_adjust=True
    )

    df.to_csv(
        f"data/{ticker}_stock_data.csv"
    )