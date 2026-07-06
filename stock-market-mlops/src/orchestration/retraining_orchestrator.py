from kafka import KafkaConsumer
import json


def train_model():
    from src.training.train_model import train_model as _train_model
    return _train_model()

def build_consumer():
    return KafkaConsumer(
        "drift.alerts",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda x: json.loads(x.decode())
    )

def run_retraining_orchestrator():
    consumer = build_consumer()

    for message in consumer:
        alert = message.value

        if alert["severity"] == "HIGH":
            print("Retraining triggered")
            train_model()


if __name__ == "__main__":
    run_retraining_orchestrator()