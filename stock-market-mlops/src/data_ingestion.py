import yfinance as yf
import pandas as pd
from datetime import datetime

def fetch_stock_data(ticker= "AAPL"):
    data = yf.download(
        ticker,
        start = "2010-01-01",
        end = datetime.today().strftime("%Y-%m-%d")
        )
    return data 


def save_data(data, ticker):
    file_path = f"data/{ticker}_stock_data.csv"
    data.to_csv(file_path)
    print(f"Data saved to {file_path}")

if __name__ == "__main__":
    tickers = ["AAPL", "TSLA", "MSFT"]
    for ticker in tickers:
        data = fetch_stock_data(ticker)
        
        if data.empty:
            print(f"No data found for {ticker}")
            continue

        print(data.tail())   # 👈 move inside loop
        save_data(data, ticker)