import json
import logging

from kafka import KafkaConsumer

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_DRIFT_ALERTS_TOPIC,
    KAFKA_RETRAIN_GROUP_ID,
)
from src.training.train_model import train_model

logger = logging.getLogger(__name__)


def build_consumer():
    return KafkaConsumer(
        KAFKA_DRIFT_ALERTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_RETRAIN_GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )


def run_retraining_orchestrator():
    consumer = build_consumer()

    logger.info("Retraining orchestrator started.")

    for message in consumer:
        alert = message.value

        severity = alert.get("severity", "").upper()
        symbol = alert.get("symbol", "UNKNOWN")

        if severity != "HIGH":
            logger.info(
                "Ignoring %s drift alert for %s",
                severity,
                symbol,
            )
            continue

        logger.info("High drift detected for %s", symbol)

        try:
            train_model()
            logger.info("Retraining completed successfully.")

        except Exception:
            logger.exception(
                "Retraining failed for %s",
                symbol,
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_retraining_orchestrator()