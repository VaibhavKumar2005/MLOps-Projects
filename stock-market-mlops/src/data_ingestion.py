from pathlib import Path
import logging

import yfinance as yf

from data_validation.validate_stock_data import validate_stock_data


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

DATA_DIR.mkdir(exist_ok=True)

TICKERS = [
    "AAPL",
    "MSFT",
    "TSLA",
]


def download_stock_data(
        tickers=TICKERS,
        period="2y",
        interval="1d",
):
    """
    Download historical stock data from Yahoo Finance.

    Args:
        tickers (list): List of ticker symbols.
        period (str): Time period (1y, 2y, 5y, max, etc.).
        interval (str): Data interval (1d, 1h, etc.).
    """

    downloaded_files = []

    for ticker in tickers:
        try:
            logging.info(f"Downloading {ticker}...")

            df = yf.download(
                ticker,
                period=period,
                interval=interval,
                auto_adjust=True,
                progress=False,
            )

            if df.empty:
                logging.warning(f"No data returned for {ticker}")
                continue

            # Validate downloaded data
            if not validate_stock_data(df):
                logging.error(f"Validation failed for {ticker}")
                continue

            output_file = DATA_DIR / f"{ticker}_stock_data.csv"

            df.to_csv(output_file)

            downloaded_files.append(output_file)

            logging.info(f"Saved {ticker} -> {output_file}")

        except Exception as e:
            logging.exception(f"Failed downloading {ticker}: {e}")

    logging.info(f"Downloaded {len(downloaded_files)} dataset(s).")

    return downloaded_files


if __name__ == "__main__":
    download_stock_data()