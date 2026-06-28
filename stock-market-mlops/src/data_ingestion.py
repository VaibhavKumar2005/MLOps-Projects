from pathlib import Path

import yfinance as yf

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

DATA_DIR.mkdir(exist_ok=True)

TICKERS = ["AAPL", "MSFT", "TSLA"]

for ticker in TICKERS:
    print(f"Downloading {ticker}...")

    df = yf.download(
        ticker,
        period="2y",
        auto_adjust=True,
        progress=False,
    )

    output_file = DATA_DIR / f"{ticker}_stock_data.csv"

    df.to_csv(output_file)

    print(f"Saved -> {output_file}")