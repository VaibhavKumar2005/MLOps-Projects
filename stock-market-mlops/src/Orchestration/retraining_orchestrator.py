from kafka import KafkaConsumer
import json
from training.train_model import train_model

consumer = KafkaConsumer(
    "drift.alerts",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode())
)

for message in consumer:
    alert = message.value

    if alert["severity"] == "HIGH":
        print("Retraining triggered")
        train_model()