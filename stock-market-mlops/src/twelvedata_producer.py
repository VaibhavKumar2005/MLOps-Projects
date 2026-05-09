import argparse
import json
import os
from datetime import datetime

from kafka import KafkaProducer
from websocket import WebSocketApp

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CLIENT_ID,
    KAFKA_RAW_TOPIC,
    TWELVEDATA_API_KEY_ENV,
    TWELVEDATA_WS_URL,
)


def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id=KAFKA_CLIENT_ID,
        key_serializer=lambda v: v.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def parse_timestamp(value):
    if isinstance(value, (int, float)):
        return datetime.utcfromtimestamp(value).isoformat()
    return str(value)


def run_stream(symbols, api_key):
    producer = build_producer()

    def on_message(_, message):
        data = json.loads(message)
        if "price" not in data or "symbol" not in data:
            return

        price = float(data["price"])
        timestamp = parse_timestamp(data.get("timestamp"))
        payload = {
            "symbol": data["symbol"],
            "timestamp": timestamp,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": 0.0,
        }
        producer.send(KAFKA_RAW_TOPIC, key=data["symbol"], value=payload)

    def on_error(_, error):
        raise RuntimeError(f"WebSocket error: {error}")

    def on_open(ws):
        subscribe_message = {
            "action": "subscribe",
            "params": {"symbols": ",".join(symbols)},
        }
        ws.send(json.dumps(subscribe_message))

    ws_url = f"{TWELVEDATA_WS_URL}?apikey={api_key}"
    ws = WebSocketApp(ws_url, on_open=on_open, on_message=on_message, on_error=on_error)
    try:
        ws.run_forever()
    finally:
        producer.flush()
        producer.close()


def main():
    parser = argparse.ArgumentParser(
        description="Stream Twelve Data prices to Kafka (stock.raw)."
    )
    parser.add_argument(
        "--symbols",
        default="AAPL,MSFT,TSLA",
        help="Comma-separated symbols to subscribe to.",
    )
    args = parser.parse_args()

    api_key = os.getenv(TWELVEDATA_API_KEY_ENV)
    if not api_key:
        raise RuntimeError(
            f"Missing API key. Set {TWELVEDATA_API_KEY_ENV} in your environment."
        )

    symbols = [symbol.strip().upper() for symbol in args.symbols.split(",") if symbol]
    if not symbols:
        raise ValueError("Provide at least one symbol.")

    run_stream(symbols, api_key)


if __name__ == "__main__":
    main()
