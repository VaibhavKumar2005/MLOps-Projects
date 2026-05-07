# 📈 Real-Time Stock Market ML Pipeline

## 🚀 Overview

A **production-grade streaming ML pipeline** for financial data. This system ingests market data, engineers features in real-time, trains predictive models, and monitors for data drift—all while streaming through Kafka.

This is not a toy project. This is how modern ML systems work:

```
Market Data → Kafka → Feature Engineering → ML Inference → Drift Monitoring
```

---

## 🧠 Problem Statement

Stock market data is:
- **Dynamic**: Distributions shift constantly
- **Streaming**: New prices arrive continuously
- **Production-critical**: Models must adapt in real-time

Traditional batch ML doesn't work. You need streaming architecture.

---

## 🛠️ Tech Stack

| Category | Tech |
|----------|------|
| **Streaming** | Apache Kafka |
| **ML Framework** | scikit-learn, MLflow |
| **Data Management** | DVC, Pandas |
| **Orchestration** | Docker Compose |
| **Language** | Python |

---

## 📂 Project Structure

```text
stock-market-mlops/
│
├── data/                              # Versioned datasets (managed by DVC)
│   ├── AAPL_stock_data.csv
│   ├── MSFT_stock_data.csv
│   └── TSLA_stock_data.csv
│
├── models/                            # Trained models
│   └── model.pkl                      # Scikit-learn LinearRegression
│
├── src/
│   ├── config.py                      # Kafka configuration
│   ├── data_ingestion.py              # Fetch data from yFinance
│   ├── feature_engineering.py         # Feature creation logic
│   ├── kafka_producer.py              # Stream CSV → stock.raw topic
│   ├── kafka_feature_engineering.py   # Consume raw → produce features
│   ├── train_model.py                 # Train and log with MLflow
│   ├── prediction_consumer.py         # ⭐ NEW: Predict from features
│   └── drift_monitor.py               # ⭐ NEW: Monitor data drift
│
├── docker-compose.yml                 # Kafka + Zookeeper setup
├── requirements.txt
└── README.md
```

---

## ⚙️ Architecture: Real-Time ML Pipeline

```
┌──────────────────────────────────────────────────────────────┐
│                   STREAMING ML PIPELINE                      │
└──────────────────────────────────────────────────────────────┘

Step 1: DATA INGESTION
  Historical CSV files
        ↓
  kafka_producer.py
        ↓
  stock.raw topic (raw OHLCV)

Step 2: FEATURE ENGINEERING  
  kafka_feature_engineering.py
        ↓
  Buffered aggregation (60+ records)
  Moving averages, returns, volatility
        ↓
  stock.features topic (engineered features)

Step 3: ML INFERENCE
  prediction_consumer.py ⭐
        ↓
  Load trained model
  Real-time predictions
        ↓
  Next-day price prediction

Step 4: DRIFT MONITORING
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

---

## 🧪 Testing the Pipeline

### Test with a Single Stock

```bash
# Terminal 1: Producer
python src/kafka_producer.py --ticker AAPL --file data/AAPL_stock_data.csv --sleep 0.5

# Terminal 2: Feature Consumer
python src/kafka_feature_engineering.py

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

---

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

---

## 🔧 Configuration

Edit [src/config.py](src/config.py) to change:
- Kafka bootstrap servers
- Topic names
- Consumer group IDs

---

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

---

## 📖 References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [MLflow Tracking](https://mlflow.org/docs/latest/tracking.html)
- [scikit-learn Models](https://scikit-learn.org/)
- [DVC Data Versioning](https://dvc.org/)
