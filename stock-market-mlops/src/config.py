import os

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
).split(",")

KAFKA_CLIENT_ID = "stock-market-mlops"

KAFKA_RAW_TOPIC = "stock.raw"
KAFKA_FEATURES_TOPIC = "stock.features"
KAFKA_DRIFT_ALERTS_TOPIC = "drift.alerts"
KAFKA_RETRAIN_TRIGGER_TOPIC = "retrain.trigger"

KAFKA_FEATURES_GROUP_ID = "stock-feature-engineering"
KAFKA_DRIFT_GROUP_ID = "stock-drift-monitor"
KAFKA_RETRAIN_GROUP_ID = "stock-retraining"

# Databricks
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG", "stock_market_mlops")
DATABRICKS_RAW_SCHEMA = os.getenv("DATABRICKS_RAW_SCHEMA", "raw_data")
DATABRICKS_PREDICTIONS_SCHEMA = os.getenv("DATABRICKS_PREDICTIONS_SCHEMA", "predictions")

# Twelve Data
TWELVEDATA_WS_URL = "wss://ws.twelvedata.com/v1/quotes/price"
TWELVEDATA_API_KEY_ENV = "TWELVEDATA_API_KEY"
