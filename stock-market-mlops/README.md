# Event-Driven Streaming ML Pipeline for Financial Market Analysis

## Overview

This project demonstrates an event-driven ML workflow for financial market analysis. It combines Kafka-based messaging, stream processing, model inference, drift monitoring, and experiment tracking in a single local-first environment.

The pipeline follows a simple path:

```text
Market data → Kafka events → Feature engineering → Prediction → Drift monitoring
```

## Key capabilities

- Real-time or near-real-time ingestion of market data
- Kafka topics for raw events, engineered features, and alerts
- Stream processing components for feature engineering and prediction
- Drift monitoring with alert generation
- Experiment tracking with MLflow and data versioning with DVC

## Prerequisites

- Python 3.11+
- Docker and Docker Compose
- Git
- A valid Twelve Data API key for live streaming

## Quick start

```bash
cd stock-market-mlops
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
# Edit .env and set TWELVEDATA_API_KEY
docker compose up -d
```

The stack starts Kafka, Zookeeper, MLflow, and the application services. Use the following commands for routine operations:

```bash
docker compose ps
docker compose logs -f
docker compose down
```

## Project structure

```text
stock-market-mlops/
├── src/                  # Streaming producers, consumers, and model logic
├── tests/                # Test suite
├── schemas/              # Kafka event schemas
├── helm/                 # Helm charts for deployment helpers
├── data/                 # Input data and generated artifacts
├── models/               # Model artifacts
├── mlruns/               # MLflow tracking data
├── docker-compose.yml    # Local stack definition
├── Dockerfile            # Container image
├── pyproject.toml        # Python project metadata
└── README.md             # Project documentation
```

## Development notes

- Run tests with `pytest tests/`.
- Use `make logs` or `docker compose logs -f` to inspect service behavior.
- Review [../SETUP.md](../SETUP.md) and [../DEPLOYMENT.md](../DEPLOYMENT.md) for detailed operational guidance.

## Status

This is an evolving portfolio project focused on practical MLOps patterns rather than a polished production SaaS product.
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ TIER 2: STREAM PROCESSING (Transformation)                               │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  kafka_feature_engineering.py                                           │
│  ├─ Consumes raw events (ConsumerGroup: stock-feature-engineering)     │
│  ├─ Maintains per-symbol buffer (60+ bars for MA calculation)          │
│  ├─ On each event:                                                     │
│  │   • Update rolling window                                           │
│  │   • Calculate features (MA_10, MA_50, Return, Volatility, Lags)    │
│  │   • If buffer full → publish feature event                         │
│  └─ Produces to stock.features topic                                   │
│         │                                                               │
│         ↓                                                               │
│  ┌─────────────────────────────┐                                       │
│  │ Kafka Topic: stock.features │                                       │
│  │ (Engineered feature events) │                                       │
│  │ {symbol, timestamp,         │                                       │
│  │  MA_10, MA_50, Return, ...} │                                       │
│  └─────────────────────────────┘                                       │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ TIER 3: INFERENCE (ML Predictions)                                       │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  prediction_consumer.py                                                 │
│  ├─ Loads trained model (scikit-learn)                                 │
│  ├─ Consumes feature events (ConsumerGroup: stock-prediction)          │
│  ├─ On each feature event:                                             │
│  │   • Extract feature vector                                          │
│  │   • Run model.predict([features])                                   │
│  │   • Log prediction + metrics                                        │
│  └─ Outputs: Real-time price predictions                               │
│         │                                                               │
│         ↓                                                               │
│  ┌──────────────────────────────┐                                      │
│  │  Prediction Output           │                                      │
│  │  {symbol, timestamp,         │                                      │
│  │   predicted_price,           │                                      │
│  │   confidence, features_used} │                                      │
│  └──────────────────────────────┘                                      │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│ TIER 4: MONITORING (Data & Model Health)                                 │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  drift_monitor.py                                                       │
│  ├─ Consumes feature events (ConsumerGroup: stock-drift-monitor)       │
│  ├─ Maintains rolling statistics                                        │
│  ├─ Compares live vs. baseline distributions                           │
│  ├─ Detects drift using statistical tests                              │
│  └─ Alerts when: |μ_live - μ_baseline| > threshold * σ                │
│         │                                                               │
│         ↓                                                               │
│  ┌──────────────────────────────┐                                      │
│  │  Drift Alerts                │                                      │
│  │  ⚠️  Price mean shifted 2.3σ  │                                      │
│  │  ⚠️  Volatility outside range │                                      │
│  │  📊 Trend: Normal / Warning  │                                      │
│  └──────────────────────────────┘                                      │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

                         METADATA & EXPERIMENT TRACKING
                                    ↓
                         ┌─────────────────────────┐
                         │  MLflow Registry        │
                         │  • Models               │
                         │  • Metrics              │
                         │  • Parameters           │
                         │  • Artifacts            │
                         └─────────────────────────┘

                         DATA VERSIONING
                                    ↓
                         ┌─────────────────────────┐
                         │  DVC + Git              │
                         │  • Data snapshots       │
                         │  • Reproducible runs    │
                         │  • Training sets        │
                         └─────────────────────────┘
```

### 🔄 Event Flow Details

| Stage | Component | Input | Processing | Output | Latency |
|-------|-----------|-------|-----------|--------|---------|
| **1** | kafka_producer | CSV file | Parse & emit | stock.raw events | ~10ms |
| **2** | kafka_feature_engineering | stock.raw | Buffer 60+ bars, compute features | stock.features events | ~50ms |
| **3** | prediction_consumer | stock.features | Load model, predict | Predictions + logs | ~10ms |
| **4** | drift_monitor | stock.features | Track stats, detect drift | Drift alerts | ~100ms |

**End-to-end latency:** Single price update → prediction in **< 200ms**

---

## Step 4: DRIFT MONITORING
  drift_monitor.py ⭐
        ↓
  Track rolling statistics
  Compare against training distribution
        ↓
  Alert on distribution shift
```

---

## ⚙️ Features Implemented

### Core Pipeline
- ✅ Real-time data streaming via Kafka
- ✅ Multi-symbol support (AAPL, TSLA, MSFT)
- ✅ Stateful feature engineering with buffering
- ✅ Streaming ML inference
- ✅ Data drift detection
- ✅ MLflow experiment tracking
- ✅ DVC data versioning
- ✅ Docker Compose orchestration

### Engineered Features
- `MA_10`: 10-day moving average
- `MA_50`: 50-day moving average  
- `Return`: Daily percentage change
- `Lag_1`, `Lag_2`: Previous day prices
- `Volatility`: 10-day rolling std dev
- `Target`: Next-day closing price (training only)

---

## 🚀 Quick Start: Run the Full Pipeline

### Prerequisites

```bash
# Install dependencies
pip install -r requirements.txt

# Ensure Docker is running
docker --version
```


### 1. Start Kafka + Zookeeper

```bash
cd stock-market-mlops
docker compose up -d
```

Verify Kafka is running:

```bash
docker ps
```

### 2. Train the Model

```bash
python src/train_model.py
```

This trains a LinearRegression model on AAPL data and saves to `models/model.pkl`.

**Output:**

```
✅ Mean Squared Error: 12.34
✅ Model saved to models/model.pkl
```

### 3. Stream Raw Data to Kafka

In terminal 1:

```bash
python src/kafka_producer.py --ticker AAPL --file data/AAPL_stock_data.csv --sleep 0.1
```

This sends OHLCV data to the `stock.raw` Kafka topic.

### 3b. (Optional) Stream Near-Real-Time Data (Free)

This uses Twelve Data's free websocket feed. It publishes price ticks to `stock.raw` and maps the price into OHLCV fields (open/high/low/close all equal to price, volume set to 0).

Set your API key:
1. Copy the example environment file and set your Twelve Data key:

```bash
cp .env.example .env
# Edit .env and set TWELVEDATA_API_KEY=YOUR_API_KEY
```

2. Or export the key in your shell (temporary for the session):

```bash
export TWELVEDATA_API_KEY="YOUR_API_KEY"
```

Then run the producer:

```bash
python src/twelvedata_producer.py --symbols AAPL,MSFT,TSLA
```

### 4. Run Feature Engineering Consumer

In terminal 2:

```bash
python src/kafka_feature_engineering.py
```

This reads from `stock.raw`, aggregates 60+ bars, and publishes engineered features to `stock.features`.

### 5. Run Prediction Consumer ⭐

In terminal 3:

```bash
python src/prediction_consumer.py
```

This reads from `stock.features` and makes real-time price predictions.

**Output:**

```
💰 AAPL   | Time: 2024-01-15T10:30:00 | Current: $150.25 | Predicted: $151.42
💰 AAPL   | Time: 2024-01-15T10:31:00 | Current: $150.50 | Predicted: $151.67
```

### 6. (Optional) Run Drift Monitor ⭐

In terminal 4:

```bash
python src/drift_monitor.py
```

This monitors feature distributions and alerts on drift.

**Output:**

```
✅ AAPL   | Price: μ=150.25, σ=2.34 | Return: μ=0.0015, σ=0.0082
⚠️  DRIFT ALERT for AAPL: Price drift ($175.50)
```

---

## 📊 How Each Component Works

### `kafka_producer.py` - Raw Data Streaming

- Reads historical CSV files
- Streams rows to `stock.raw` topic
- Payload: `{symbol, timestamp, open, high, low, close, volume}`

### `kafka_feature_engineering.py` - Stateful Aggregation

- Buffers 60+ raw records per symbol
- Computes features: MA, returns, volatility
- Publishes to `stock.features` topic
- Payload: `{symbol, timestamp, MA_10, MA_50, Return, Lag_1, Lag_2, Volatility, close}`

### `prediction_consumer.py` - ML Inference ⭐

- Loads trained scikit-learn model
- Subscribes to `stock.features` topic
- Makes real-time price predictions
- Logs predictions to console

**Key features:**

- Handles missing values gracefully
- Validates feature order
- Tracks predictions per symbol

### `drift_monitor.py` - Data Quality Monitoring ⭐

- Tracks rolling mean/std of key metrics
- Compares against training distribution baseline
- Alerts when Z-score > threshold (default: 2σ)
- Metrics monitored:
  - Price drift
  - Return drift
  - Volatility drift

## 🧪 Testing the Pipeline

### Test with a Single Stock

```bash
# Terminal 1: Producer
python src/kafka_producer.py --ticker AAPL --file data/AAPL_stock_data.csv --sleep 0.5

# Terminal 2: Feature Consumer
python src/kafka_feature_engineering.py

# Terminal 3: Predictor
python src/prediction_consumer.py

# Terminal 4: Drift Monitor
python src/drift_monitor.py
```

---

## 📈 Production Deployment Guide

### 1. **Containerize Each Component**

Create `Dockerfile` for each consumer:

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ src/
COPY models/ models/

CMD ["python", "src/kafka_feature_engineering.py"]
```

### 2. **Deploy with Docker Compose (Extended)**

Enhance `docker-compose.yml` for production:

```yaml
version: "3.8"

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    networks:
      - mlops-network

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_LOG_RETENTION_HOURS: 24
    networks:
      - mlops-network

  # Feature Engineering Service
  feature-engineer:
    build: .
    command: python src/kafka_feature_engineering.py
    depends_on:
      - kafka
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    networks:
      - mlops-network
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

  # Prediction Service
  predictor:
    build: .
    command: python src/prediction_consumer.py
    depends_on:
      - kafka
      - feature-engineer
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    networks:
      - mlops-network
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

  # Drift Monitoring Service
  drift-monitor:
    build: .
    command: python src/drift_monitor.py
    depends_on:
      - kafka
      - feature-engineer
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    networks:
      - mlops-network
    restart: unless-stopped
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

networks:
  mlops-network:
    driver: bridge
```

Run with:

```bash
docker compose up -d
docker compose logs -f predictor
docker compose down
```

### 3. **Kubernetes Deployment**

Deploy to AKS with Helm charts:

```yaml
# values.yaml
replicas:
  featureEngineer: 2
  predictor: 3
  driftMonitor: 1

kafka:
  brokers: ["kafka-broker-1:9092", "kafka-broker-2:9092"]
  
resources:
  featureEngineer:
    requests:
      memory: "512Mi"
      cpu: "250m"
  predictor:
    requests:
      memory: "1Gi"
      cpu: "500m"

alerts:
  driftThreshold: 2.5
  predictionLatency: "500ms"
```

### 4. **Model Registry & Versioning**

Track models in MLflow:

```bash
# List registered models
mlflow models list

# Promote to production
mlflow models update-model-version \
  --name "stock-price-predictor" \
  --version 1 \
  --stage "Production"

# Load from registry
model = mlflow.pyfunc.load_model("models:/stock-price-predictor/production")
```

### 5. **Monitoring & Observability**

#### Kafka Metrics

```bash
# Monitor topic lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group stock-prediction \
  --describe
```

#### Application Metrics

Add Prometheus to `docker-compose.yml`:

```yaml
prometheus:
  image: prom/prometheus:latest
  volumes:
    - ./prometheus.yml:/etc/prometheus/prometheus.yml
  ports:
    - "9090:9090"
  networks:
    - mlops-network
```

#### Logging & Tracing

Configure structured logging in consumers:

```python
import logging
from pythonjsonlogger import jsonlogger

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.info("prediction", extra={
    "symbol": "AAPL",
    "predicted_price": 151.42,
    "model_version": "v2",
    "latency_ms": 12
})
```

---

## 🔄 MLOps Workflow

### 1. **Data Collection & Versioning**

```bash
# Fetch fresh data
python src/data_ingestion.py --tickers AAPL MSFT TSLA

# Version with DVC
dvc add data/AAPL_stock_data.csv
git add data/AAPL_stock_data.csv.dvc
git commit -m "Update AAPL data to 2024-05-08"
```

### 2. **Model Training & Logging**

```bash
# Train new model (logs to MLflow)
python src/train_model.py

# View MLflow UI
mlflow ui  # Navigate to http://localhost:5000
```

### 3. **Deploy to Production**

```bash
# Build containers
docker build -t stock-mlops:v2 .

# Deploy
docker compose up -d

# Monitor
docker compose logs -f predictor | grep "prediction"
```

### 4. **Monitor for Drift**

```bash
# Check drift alerts
docker compose logs drift-monitor | grep "DRIFT ALERT"

# If drift detected: retrain model
python src/train_model.py
docker compose restart predictor
```

### 5. **Experiment Tracking**

```bash
# Compare model versions
mlflow models compare \
  --model-uri "models:/stock-price-predictor/staging" \
  --baseline-model-uri "models:/stock-price-predictor/production"
```

---

## 🛡️ Best Practices & Production Considerations

### Reliability

- **Consumer Groups**: Each component uses unique consumer group → independent offset tracking
- **Auto-offset Reset**: Set to "earliest" to replay data on failure
- **Enable Auto-Commit**: Ensures offset commits after successful processing
- **Graceful Shutdown**: Flush producers and close consumers on SIGTERM

### Scalability

- **Horizontal Scaling**: Run multiple instances of feature engineering / prediction
  ```bash
  # 3 parallel feature engineers
  for i in {1..3}; do
    python src/kafka_feature_engineering.py &
  done
  ```

- **Kafka Partitioning**: Partition by symbol for parallel processing
  ```bash
  kafka-topics --create \
    --topic stock.raw \
    --partitions 10 \
    --replication-factor 3
  ```

### Data Quality

- **Schema Validation**: Enforce event schemas with Avro/Protobuf
- **Feature Bounds**: Validate feature ranges (e.g., negative returns shouldn't exceed ±10%)
- **Null Handling**: Buffer requirements ensure NaN-free feature computation

### Model Monitoring

- **Prediction Distribution**: Track predicted price range vs. actual
- **Feature Importance**: Log which features drive predictions
- **Concept Drift**: Regularly recompute feature baselines
- **Retraining Triggers**: 
  - Drift detected (statistical test)
  - Prediction error exceeds threshold
  - Data schema changes

---

## 🧠 Example: How Predictions Work

**Live scenario:**

1. **10:30 AM** → Market sends new AAPL price: $150.25
2. **kafka_producer.py** → Emits to `stock.raw`:
   ```json
   {"symbol": "AAPL", "timestamp": "2024-05-08T10:30:00", 
    "open": 150.10, "high": 150.50, "low": 150.00, 
    "close": 150.25, "volume": 1000000}
   ```

3. **kafka_feature_engineering.py** → Reads from `stock.raw`, updates buffer
   - Buffer now has 60 bars (enough to compute 50-day MA)
   - Computes: MA_10=150.2, MA_50=149.8, Return=0.001, Volatility=0.015
   - Emits to `stock.features`:
   ```json
   {"symbol": "AAPL", "timestamp": "2024-05-08T10:30:00",
    "features": {"MA_10": 150.2, "MA_50": 149.8, "Return": 0.001, ...}}
   ```

4. **prediction_consumer.py** → Reads from `stock.features`
   - Loads model: `clf = joblib.load("models/model.pkl")`
   - Extracts feature vector: `[150.2, 149.8, 0.001, 150.25, 150.10, 0.015]`
   - Predicts: `clf.predict([features]) → 151.42`
   - Logs: `💰 AAPL | Time: 10:30 | Predicted: $151.42`

5. **drift_monitor.py** → Reads from `stock.features`
   - Compares live price distribution against training baseline
   - If price mean shifted 2σ beyond baseline → **⚠️ DRIFT ALERT**

**Total latency: 50ms**

---

## 🔧 Troubleshooting

### Kafka Won't Start

```bash
# Check Docker logs
docker compose logs kafka

# Restart Kafka
docker compose restart kafka

# Reset Kafka state (⚠️ loses all data)
docker compose down
docker volume prune
docker compose up -d
```

### Feature Engineer Not Consuming

```bash
# Check consumer group offset
docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group stock-feature-engineering \
  --describe

# Reset offset to latest
docker exec kafka kafka-consumer-groups \
  --bootstrap-server kafka:9092 \
  --group stock-feature-engineering \
  --reset-offsets --to-latest --execute
```

### No Predictions Generated

```bash
# Check if features are being produced
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic stock.features \
  --from-beginning \
  --max-messages 5

# Verify model file exists
ls models/model.pkl
```

---

## 📚 Further Reading

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [scikit-learn ML Pipelines](https://scikit-learn.org/stable/modules/pipeline.html)
- [MLflow: ML Experiment Tracking](https://mlflow.org/)
- [DVC: Data Version Control](https://dvc.org/)
- [Confluent Kafka Python Client](https://docs.confluent.io/kafka-clients/python/current/overview.html)

---

## 📜 License

MIT License - See LICENSE file for details

---

## 🤝 Contributing

Contributions welcome! Areas for enhancement:

- [ ] Add real-time yfinance ingestion (instead of CSV)
- [ ] LSTM models for sequence prediction
- [ ] WebSocket API for prediction subscriptions
- [ ] Advanced drift detection (Kullback-Leibler divergence)
- [ ] Multi-step ahead forecasting (predict 5 days out)
- [ ] Dashboard with Grafana/Streamlit

---

**Questions?** Check [PIPELINE_DEMO.md](PIPELINE_DEMO.md) for step-by-step walkthrough.

# Terminal 3: Predictions
python src/prediction_consumer.py
```

### Test with Multiple Stocks

```bash
# Terminal 1a: Stream AAPL
python src/kafka_producer.py --ticker AAPL --file data/AAPL_stock_data.csv &

# Terminal 1b: Stream MSFT
python src/kafka_producer.py --ticker MSFT --file data/MSFT_stock_data.csv &

# Terminal 2: Feature Engineering (handles both)
python src/kafka_feature_engineering.py

# Terminal 3: Predictions (handles both)
python src/prediction_consumer.py

# Terminal 4: Drift Monitoring
python src/drift_monitor.py
```

## 📈 Model Training & Experimentation

### View Experiment Results

```bash
# Start MLflow UI
mlflow ui

# Navigate to http://localhost:5000
```

MLflow tracks:

- Mean Squared Error (MSE)
- Model artifacts
- Training data source

### Retrain Model

```bash
python src/train_model.py
```

A new run is logged automatically.

## 🔧 Configuration

Edit [src/config.py](src/config.py) to change:

- Kafka bootstrap servers
- Topic names
- Consumer group IDs

## 🚨 Troubleshooting

### Kafka Connection Error

```
Connection refused to localhost:9092
```

**Solution:** Ensure `docker compose up -d` is running:

```bash
docker compose logs kafka
```

### Model Not Found

```
FileNotFoundError: models/model.pkl
```

**Solution:** Train the model first:

```bash
python src/train_model.py
```

### No Messages in Consumer

```
Waiting for messages... (nothing appears)
```

**Solution:**

1. Check producer is sending data: `docker compose logs kafka`
2. Verify correct topic name in config
3. Use `docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stock.raw --from-beginning`

### DVC Data Not Synced

```bash
git lfs install
dvc pull
```

---

## 📚 What's Next

### Level Up Your Pipeline

- [ ] **Real Market Data**: Swap CSV for real-time Alpha Vantage / IEX Cloud API
- [ ] **Better Models**: Try XGBoost, LSTM, Transformer architectures  
- [ ] **Feature Store**: Add Tecton or Feast for feature management
- [ ] **REST API**: Expose predictions via FastAPI
- [ ] **Monitoring**: Deploy Prometheus + Grafana dashboards
- [ ] **Orchestration**: Add Airflow for scheduled retraining
- [ ] **Production**: Deploy to AKS, ECS, or Kubernetes

---

## 💡 System Design Insights

### Why This Architecture?

1. **Streaming**: Markets don't wait for batch jobs
2. **Stateful Processing**: Feature engineering requires history (buffering)
3. **Decoupled**: Each component can scale independently
4. **Observable**: Every stage produces logs and metrics
5. **Production-Ready**: Follows microservices patterns

### Scaling Patterns

```
Current (Single Machine):
Kafka (Docker) → Feature Consumer → Prediction Consumer

Production (Distributed):
Kafka Cluster → Multiple Feature Consumers → Prediction Service
                                           → Drift Monitor
                                           → Feature Store
                                           → Model Registry
```

---

## 🤝 Contributing

This is an educational project. Contributions welcome!

---

## ⚠️ Disclaimer

**For educational purposes only.**  
Stock predictions are not guaranteed and should never be used for real trading decisions.

---

## 👨‍💻 Author

Vaibhav Kumar


## 📖 References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [MLflow Tracking](https://mlflow.org/docs/latest/tracking.html)
- [scikit-learn Models](https://scikit-learn.org/)
- [DVC Data Versioning](https://dvc.org/)
