import json
from collections import defaultdict, deque

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CLIENT_ID,
    KAFKA_FEATURES_GROUP_ID,
    KAFKA_FEATURES_TOPIC,
    KAFKA_RAW_TOPIC,
)
from feature_engineering import create_features


def build_consumer():
    return KafkaConsumer(
        KAFKA_RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_FEATURES_GROUP_ID,
        client_id=KAFKA_CLIENT_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
    )


def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id=KAFKA_CLIENT_ID,
        key_serializer=lambda v: v.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def to_frame(records):
    df = pd.DataFrame.from_records(records)
    df["Date"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("Date")
    df = df.rename(
        columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
    )
    return df[["Open", "High", "Low", "Close", "Volume"]]


def main():
    consumer = build_consumer()
    producer = build_producer()

    buffers = defaultdict(lambda: deque(maxlen=120))

    try:
        for message in consumer:
            payload = message.value
            symbol = payload.get("symbol")
            if not symbol:
                continue

            buffers[symbol].append(payload)

            if len(buffers[symbol]) < 60:
                continue

            df = to_frame(list(buffers[symbol]))
            features = create_features(df, include_target=False)
            if features.empty:
                continue

            latest = features.tail(1).iloc[0].to_dict()
            feature_payload = {
                "symbol": symbol,
                "timestamp": features.tail(1).index[0].isoformat(),
                "features": latest,
            }
            producer.send(KAFKA_FEATURES_TOPIC, key=symbol, value=feature_payload)
    finally:
        producer.flush()
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
