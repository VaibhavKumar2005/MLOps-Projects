import argparse
import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_CLIENT_ID, KAFKA_RAW_TOPIC


def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id=KAFKA_CLIENT_ID,
        key_serializer=lambda v: v.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def stream_csv(ticker, file_path, sleep_seconds=0.0):
    df = pd.read_csv(file_path, header=0, index_col=0, skiprows=[1, 2], parse_dates=True)

    producer = build_producer()
    try:
        for index, row in df.iterrows():
            payload = {
                "symbol": ticker,
                "timestamp": index.isoformat(),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row["Volume"]),
            }
            producer.send(KAFKA_RAW_TOPIC, key=ticker, value=payload)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
    finally:
        producer.flush()
        producer.close()


def main():
    parser = argparse.ArgumentParser(description="Stream CSV stock bars to Kafka.")
    parser.add_argument("--ticker", required=True, help="Stock ticker symbol.")
    parser.add_argument("--file", required=True, help="Path to CSV data file.")
    parser.add_argument("--sleep", type=float, default=0.0, help="Seconds between messages.")

    args = parser.parse_args()
    csv_path = Path(args.file)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    stream_csv(args.ticker, csv_path, args.sleep)


if __name__ == "__main__":
    main()
