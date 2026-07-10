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


def run_stream(symbols, api_key):
    """Stream real-time prices from Twelve Data WebSocket to Kafka."""
    producer = build_producer()
    
    def on_message(_, message):
        try:
            data = json.loads(message)
            if "price" not in data or "symbol" not in data:
                return
            
            price = float(data["price"])
            timestamp = data.get("timestamp", datetime.utcnow().isoformat())
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
        except Exception as e:
            print(f"Error processing message: {e}")
    
    def on_error(_, error):
        print(f"WebSocket error: {error}")
    
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
        description="Stream real-time stock prices from Twelve Data to Kafka (production)."
    )
    parser.add_argument(
        "--symbols",
        default=os.getenv("TWELVEDATA_SYMBOLS", "AAPL,MSFT,TSLA"),
        help="Comma-separated symbols (e.g., AAPL,MSFT,TSLA).",
    )
    args = parser.parse_args()
    
    api_key = os.getenv(TWELVEDATA_API_KEY_ENV)
    if not api_key:
        raise RuntimeError(f"Set {TWELVEDATA_API_KEY_ENV} environment variable.")
    
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        raise ValueError("Provide at least one symbol.")
    
    print(f"Streaming live data for: {', '.join(symbols)}")
    run_stream(symbols, api_key)


if __name__ == "__main__":
    main()
