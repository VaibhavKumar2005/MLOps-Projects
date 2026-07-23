from pathlib import Path
import logging

import yfinance as yf

from data_validation.validate_stock_data import validate_stock_data


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LIVE_DATA_DIR = PROJECT_ROOT / "data" / "live"

LIVE_DATA_DIR.mkdir(parents=True, exist_ok=True)

TICKERS = [
    "AAPL",
    "MSFT",
    "TSLA",
]


def collect_live_stock_data(
        tickers=TICKERS,
        period="5d",
        interval="15m",
):
    """Collect a fresh live market snapshot from Yahoo Finance.

    The pipeline uses this runtime snapshot instead of committed historical CSVs.
    """

    collected_files = []

    for ticker in tickers:
        try:
            logging.info("Collecting live snapshot for %s...", ticker)

            df = yf.download(
                ticker,
                period=period,
                interval=interval,
                auto_adjust=True,
                progress=False,
            )

            if df.empty:
                logging.warning("No data returned for %s", ticker)
                continue

            if not validate_stock_data(df):
                logging.error("Validation failed for %s", ticker)
                continue

            output_file = LIVE_DATA_DIR / f"{ticker}_live_data.csv"
            df.to_csv(output_file)

            collected_files.append(output_file)
            logging.info("Saved live snapshot for %s -> %s", ticker, output_file)

        except Exception as e:
            logging.exception("Failed collecting live data for %s: %s", ticker, e)

    logging.info("Collected %s live dataset(s).", len(collected_files))

    return collected_files


if __name__ == "__main__":
    collect_live_stock_data()