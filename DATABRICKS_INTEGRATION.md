# Databricks Integration Guide for Stock Market MLOps Pipeline

## 📋 Overview

This guide provides a comprehensive strategy to integrate Databricks into your event-driven streaming ML pipeline. Databricks will enhance your system with:
- **Unified data platform** (storage + compute)
- **Managed MLflow** for model tracking
- **Delta Lake** for data versioning (replaces DVC)
- **Databricks Feature Store** for feature management
- **Databricks Workflows** for orchestration
- **SQL AI/Predictive** capabilities

---

## 🎯 Integration Strategy

### Phase 1: Setup & Authentication
### Phase 2: Data Migration & Streaming
### Phase 3: Model Training & Experimentation
### Phase 4: Production Deployment
### Phase 5: Monitoring & Governance

---

## 📝 Phase 1: Setup & Authentication

### 1.1 Prerequisites
```bash
# Install Databricks CLI and SDK
pip install databricks-cli==0.18.0
pip install databricks-sql-connector==3.0.0
pip install mlflow>=2.0.0  # Already in your requirements.txt
```

### 1.2 Configure Databricks Authentication

Create `.databricks/config` in your home directory:
```ini
[DEFAULT]
host = https://your-workspace-id.cloud.databricks.com
token = dapi12345...
```

Or set environment variables:
```bash
export DATABRICKS_HOST=https://your-workspace-id.cloud.databricks.com
export DATABRICKS_TOKEN=dapi12345...
```

### 1.3 Create Workspace Structure

Create these folders in Databricks workspace:
```
/Users/your-email@company.com/
├── stock-market-mlops/
│   ├── notebooks/
│   │   ├── 01_data_ingestion
│   │   ├── 02_feature_engineering
│   │   ├── 03_model_training
│   │   ├── 04_model_evaluation
│   │   └── 05_inference
│   ├── jobs/
│   └── pipelines/
```

---

## 📚 Phase 2: Data Migration & Streaming

### 2.1 Create Databricks SQL Warehouse & Catalog

```python
# Create via Databricks SQL UI or via API
from databricks.sql import sql

# Create catalog
CREATE CATALOG IF NOT EXISTS stock_market_mlops;
CREATE SCHEMA IF NOT EXISTS stock_market_mlops.raw_data;
CREATE SCHEMA IF NOT EXISTS stock_market_mlops.features;
CREATE SCHEMA IF NOT EXISTS stock_market_mlops.predictions;
```

### 2.2 Migrate Data from DVC to Delta Lake

Create a notebook: `01_data_migration.py`

```python
# Databricks Notebook
import pandas as pd
from pathlib import Path

# 1. Read historical data from local CSV
df_aapl = pd.read_csv("/Workspace/Repos/your-repo/data/AAPL_stock_data.csv")
df_msft = pd.read_csv("/Workspace/Repos/your-repo/data/MSFT_stock_data.csv")
df_tsla = pd.read_csv("/Workspace/Repos/your-repo/data/TSLA_stock_data.csv")

# 2. Convert to Delta Tables (Databricks-managed versioning replaces DVC)
aapl_delta = spark.createDataFrame(df_aapl)
aapl_delta.write.format("delta").mode("overwrite") \
    .saveAsTable("stock_market_mlops.raw_data.aapl_historical")

msft_delta = spark.createDataFrame(df_msft)
msft_delta.write.format("delta").mode("overwrite") \
    .saveAsTable("stock_market_mlops.raw_data.msft_historical")

tsla_delta = spark.createDataFrame(df_tsla)
tsla_delta.write.format("delta").mode("overwrite") \
    .saveAsTable("stock_market_mlops.raw_data.tsla_historical")

print("✅ Data migration complete. Tables in Unity Catalog:")
spark.sql("SHOW TABLES IN stock_market_mlops.raw_data").show()

# 3. Create table versions (replaces DVC versioning)
spark.sql("""
    CREATE TABLE IF NOT EXISTS stock_market_mlops.raw_data.aapl_historical_v1 AS
    SELECT *, current_timestamp() as ingestion_time FROM stock_market_mlops.raw_data.aapl_historical
""")
```

### 2.3 Setup Kafka Streaming with Databricks

Create notebook: `02_kafka_streaming_setup.py`

```python
# Databricks Notebook - PySpark Structured Streaming
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configure Kafka source
kafka_servers = "localhost:9092"  # or your Kafka broker
checkpoint_location = "/Workspace/dbfs:/stock-market-mlops/checkpoints/raw_data"

# Define schema for Kafka events
schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("symbol", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", LongType()),
])

# Read from Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", "stock.raw") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON and extract fields
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Write to Delta Lake (automatically versioned)
query = df_parsed.writeStream \
    .format("delta") \
    .option("checkpointLocation", checkpoint_location) \
    .mode("append") \
    .table("stock_market_mlops.raw_data.kafka_stream_raw")

print("✅ Streaming ingestion started")
```

### 2.4 Alternative: Direct Kafka to Delta (No Databricks Structured Streaming)

Update your Python producer to write to Databricks:

```python
# src/kafka_producer.py - Enhanced version
from databricks.sql import sql
from kafka import KafkaProducer
import json

# Initialize Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Initialize Databricks SQL connection
connection = sql.connect(
    server_hostname="your-workspace.cloud.databricks.com",
    http_path="/sql/1.0/warehouses/your-warehouse-id",
    auth_type="pat",
    personal_access_token="dapi..."
)

def send_market_data(symbol, data):
    """Send to both Kafka (streaming) and Databricks (backup)"""
    
    # Send to Kafka for real-time processing
    producer.send('stock.raw', value=data)
    
    # Also write to Databricks Delta for audit trail
    cursor = connection.cursor()
    cursor.execute(f"""
        INSERT INTO stock_market_mlops.raw_data.kafka_events
        VALUES ('{data['timestamp']}', '{symbol}', {data['open']}, 
                {data['high']}, {data['low']}, {data['close']}, {data['volume']})
    """)
```

---

## 🤖 Phase 3: Model Training & Experimentation

### 3.1 Create Training Notebook

Create notebook: `03_model_training.py` in Databricks

```python
# Databricks Notebook
import pandas as pd
import mlflow
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error

# Enable Databricks MLflow
mlflow.set_tracking_uri("databricks")
mlflow.set_experiment("/Users/your-email@company.com/stock-market-mlops/experiments")

# Load features from Delta
df_features = spark.table("stock_market_mlops.features.engineered_features") \
    .toPandas()

# Prepare data
X = df_features.drop(columns=["target", "symbol"])
y = df_features["target"]

split_idx = int(0.8 * len(df_features))
X_train, X_test = X[:split_idx], X[split_idx:]
y_train, y_test = y[:split_idx], y[split_idx:]

# Track with Databricks MLflow
with mlflow.start_run() as run:
    # Train model
    model = XGBRegressor(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        random_state=42
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
    
    # Evaluate
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    rmse = mse ** 0.5
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    
    # Log metrics to Databricks MLflow
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)
    mlflow.log_metric("mae", mae)
    
    # Log parameters
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 6)
    mlflow.log_param("learning_rate", 0.1)
    
    # Log model
    mlflow.xgboost.log_model(
        model,
        artifact_path="models/xgboost",
        registered_model_name="stock_market_xgboost"
    )
    
    print(f"✅ Model trained and logged. Run ID: {run.info.run_id}")
    print(f"   RMSE: {rmse:.4f}")
    print(f"   R²: {r2:.4f}")
```

### 3.2 Databricks Feature Store Integration

Create notebook: `03_feature_store.py`

```python
# Databricks Notebook
from databricks.feature_engineering import FeatureEngineeringClient

fs = FeatureEngineeringClient()

# Define feature table
feature_table_name = "stock_market_mlops.features.stock_technical_features"

# Create feature table from Delta
fs.create_table(
    name=feature_table_name,
    primary_keys=["symbol", "date"],
    df=spark.table("stock_market_mlops.features.engineered_features"),
    description="Technical analysis features for stock prediction"
)

# Later, use in training
fs_client = FeatureEngineeringClient()
training_data = fs_client.create_training_set(
    name="stock_training_set_v1",
    exclude_columns=["symbol"],
    df=training_labels_df
)

# Automatically links features with labels for reproducible training
```

---

## 🚀 Phase 4: Production Deployment

### 4.1 Register Model in Unity Catalog

```python
# In your training notebook
import mlflow

# Register model in Databricks Unity Catalog
mlflow.register_model(
    model_uri="runs:/run_id/models/xgboost",
    name="stock_market_mlops.models.stock_predictor"
)

# Set model aliases for easy reference
client = mlflow.MlflowClient()
client.set_registered_model_alias(
    name="stock_market_mlops.models.stock_predictor",
    alias="champion",
    version=2
)
```

### 4.2 Deploy Model as Databricks Model Serving Endpoint

```python
# Deploy via Databricks REST API
import requests
import json

DATABRICKS_HOST = "https://your-workspace.cloud.databricks.com"
TOKEN = "dapi..."

# Create serving endpoint
payload = {
    "name": "stock-predictor-endpoint",
    "config": {
        "served_models": [
            {
                "model_name": "stock_market_mlops.models.stock_predictor",
                "model_version": "2",
                "workload_size": "Small",
                "scale_to_zero_enabled": False
            }
        ]
    }
}

response = requests.post(
    f"{DATABRICKS_HOST}/api/2.0/serving-endpoints",
    headers={"Authorization": f"Bearer {TOKEN}"},
    json=payload
)

print(f"✅ Model serving endpoint created: {response.json()['id']}")

# Invoke endpoint
invoke_payload = {
    "dataframe_records": [
        {
            "feature1": 0.5,
            "feature2": 1.2,
            "feature3": 0.8
        }
    ]
}

response = requests.post(
    f"{DATABRICKS_HOST}/serving-endpoints/stock-predictor-endpoint/invocations",
    headers={"Authorization": f"Bearer {TOKEN}"},
    json=invoke_payload
)

print(f"Prediction: {response.json()}")
```

### 4.3 Databricks Workflows Orchestration

Create workflow in Databricks UI or via API:

```python
# workflow_config.py
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task,
    JobCluster,
    NotebookTask,
    SparkPythonTask,
    EmailNotificationConfig,
    CronSchedule,
)
from datetime import time

ws = WorkspaceClient()

# Create workflow
workflow = ws.jobs.create(
    name="stock-market-mlops-pipeline",
    tasks=[
        Task(
            task_key="data_ingestion",
            notebook_task=NotebookTask(
                notebook_path="/Users/your-email@company.com/stock-market-mlops/notebooks/01_data_ingestion"
            ),
            new_cluster={
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2,
            }
        ),
        Task(
            task_key="feature_engineering",
            depends_on=["data_ingestion"],
            notebook_task=NotebookTask(
                notebook_path="/Users/your-email@company.com/stock-market-mlops/notebooks/02_feature_engineering"
            ),
            new_cluster={
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2,
            }
        ),
        Task(
            task_key="model_training",
            depends_on=["feature_engineering"],
            notebook_task=NotebookTask(
                notebook_path="/Users/your-email@company.com/stock-market-mlops/notebooks/03_model_training"
            ),
            new_cluster={
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2,
            }
        ),
        Task(
            task_key="drift_monitoring",
            notebook_task=NotebookTask(
                notebook_path="/Users/your-email@company.com/stock-market-mlops/notebooks/05_drift_monitoring"
            ),
            new_cluster={
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 1,
            }
        ),
    ],
    job_clusters=[],
    email_notifications=EmailNotificationConfig(
        on_failure=["your-email@company.com"]
    ),
    schedule=CronSchedule(quartz_cron_expression="0 0 * * * ?", timezone_id="UTC"),
)

print(f"✅ Workflow created: {workflow.job_id}")
```

---

## 📊 Phase 5: Monitoring & Governance

### 5.1 Drift Monitoring with Databricks

Create notebook: `05_drift_monitoring.py`

```python
# Databricks Notebook
import pandas as pd
from scipy import stats

# Load current features
current_features = spark.table("stock_market_mlops.features.kafka_stream_features") \
    .toPandas()

# Load baseline (training data distribution)
baseline_features = spark.table("stock_market_mlops.features.training_baseline") \
    .toPandas()

# Calculate drift (Kolmogorov-Smirnov test)
drift_results = {}
for col in ["volatility", "momentum", "rsi"]:
    statistic, p_value = stats.ks_2samp(
        baseline_features[col],
        current_features[col]
    )
    drift_results[col] = {
        "statistic": statistic,
        "p_value": p_value,
        "is_drift": p_value < 0.05
    }

# Log drift metrics to MLflow
mlflow.log_dict(drift_results, "drift_analysis.json")

# Write alert to Delta if drift detected
alerts = []
for metric, result in drift_results.items():
    if result["is_drift"]:
        alerts.append({
            "metric": metric,
            "p_value": result["p_value"],
            "timestamp": pd.Timestamp.now(),
            "status": "DRIFT_DETECTED"
        })

if alerts:
    alerts_df = spark.createDataFrame(alerts)
    alerts_df.write.format("delta").mode("append") \
        .table("stock_market_mlops.predictions.drift_alerts")
    
    print(f"⚠️  {len(alerts)} drift alerts recorded")
```

### 5.2 Model Performance Monitoring

```python
# Databricks SQL Query
SELECT 
    model_name,
    model_version,
    DATE(inference_timestamp) as date,
    COUNT(*) as prediction_count,
    AVG(prediction_error) as avg_error,
    MAX(prediction_error) as max_error,
    PERCENTILE(prediction_error, 0.95) as p95_error
FROM stock_market_mlops.predictions.inference_logs
WHERE inference_timestamp > current_date() - INTERVAL '30 DAYS'
GROUP BY model_name, model_version, DATE(inference_timestamp)
ORDER BY date DESC;
```

### 5.3 Enable Unity Catalog Governance

```sql
-- Enable lineage and audit logging
ALTER SCHEMA stock_market_mlops.features SET OWNER TO `your-group@company.com`;

-- Set data classification
COMMENT ON TABLE stock_market_mlops.raw_data.kafka_stream_raw IS 
'Raw market data from Kafka. Sensitive: PII=none. Retention: 90 days';

-- Enable column-level masking
ALTER TABLE stock_market_mlops.features.engineered_features 
MODIFY COLUMN open SET MASK stock_market_mlops.mask_numeric();
```

---

## 🔄 Integration Flow Diagram

```
┌─────────────────────────────────────────────────────┐
│         Market Data Sources                         │
│   (TwelveData WebSocket, yFinance)                  │
└────────────────┬────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────┐
│    Kafka (stock.raw)                                │
│    Optional: Keep for real-time use                 │
└────────────────┬────────────────────────────────────┘
                 │
        ┌────────┴────────┐
        ▼                 ▼
    ┌────────────┐   ┌──────────────────┐
    │Kafka Flow  │   │Databricks Stream │
    │(existing)  │   │ (new)            │
    └────────────┘   └────────┬─────────┘
                              ▼
                    ┌─────────────────────┐
                    │Delta Lake Raw Data  │
                    │(replaces DVC)       │
                    └────────────┬────────┘
                                 ▼
                    ┌─────────────────────┐
                    │Feature Engineering  │
                    │(Spark/Databricks)   │
                    └────────────┬────────┘
                                 ▼
                    ┌─────────────────────┐
                    │Feature Store Table  │
                    └────────────┬────────┘
                                 ▼
                    ┌─────────────────────┐
                    │MLflow Training      │
                    │(Databricks MLflow)  │
                    └────────────┬────────┘
                                 ▼
                    ┌─────────────────────┐
                    │Model Registry       │
                    │(Unity Catalog)      │
                    └────────────┬────────┘
                                 ▼
                    ┌─────────────────────┐
                    │Model Serving        │
                    │(Databricks Endpoint)│
                    └─────────────────────┘
```

---

## 📦 Python Package Updates

Add to your `pyproject.toml`:

```toml
[project.optional-dependencies]
databricks = [
    "databricks-sql-connector>=3.0.0",
    "databricks-sdk>=0.20.0",
    "databricks-feature-engineering>=0.3.0",
]

ml-tracking = [
    "mlflow>=2.0.0",  # Already present
    "mlflow[databricks]>=2.0.0",  # For Databricks MLflow
]
```

---

## 🚀 Migration Path

### Option 1: Gradual Migration (Recommended)
1. **Week 1-2**: Set up Databricks workspace, catalog, schemas
2. **Week 3**: Migrate historical data to Delta Lake
3. **Week 4**: Move feature engineering to Databricks
4. **Week 5**: Transition model training to Databricks
5. **Week 6**: Deploy models via Databricks serving
6. **Week 7**: Shutdown Kafka+Docker, fully migrate to Databricks

### Option 2: Hybrid Mode (Keep Both)
- Keep Kafka for real-time streaming
- Use Databricks for batch processing, ML, and serving
- Kafka consumers write to both:
  - Kafka topics (for your existing consumer pipeline)
  - Delta tables (for Databricks processing)

### Option 3: Full Databricks Streaming
- Replace Kafka consumers with Databricks Structured Streaming
- Keep Kafka producers (or use Databricks API)
- All processing happens in Databricks notebooks/jobs

---

## ✅ Implementation Checklist

- [ ] Install Databricks SDK and CLI
- [ ] Set up Databricks workspace access
- [ ] Create Unity Catalog and schemas
- [ ] Migrate data from CSV/DVC to Delta
- [ ] Set up Kafka→Delta streaming
- [ ] Create feature engineering notebook
- [ ] Migrate model training to notebook
- [ ] Register model in Unity Catalog
- [ ] Deploy serving endpoint
- [ ] Create Databricks workflow
- [ ] Set up drift monitoring
- [ ] Enable governance/lineage
- [ ] Test end-to-end pipeline
- [ ] Decommission local DVC/Docker (optional)

---

## 🔗 Useful Resources

- [Databricks Unity Catalog Docs](https://docs.databricks.com/en/data-governance/unity-catalog/)
- [Databricks MLflow Integration](https://docs.databricks.com/en/mlflow/index.html)
- [Databricks Feature Store](https://docs.databricks.com/en/machine-learning/feature-store/index.html)
- [Databricks Workflows](https://docs.databricks.com/en/workflows/)
- [Databricks Model Serving](https://docs.databricks.com/en/machine-learning/model-serving/index.html)
- [Databricks SQL Connector for Python](https://docs.databricks.com/en/dev-tools/python-sql-connector.html)

---

## 💡 Next Steps

1. **Start with Phase 1**: Get Databricks access and set up authentication
2. **Choose migration strategy**: Gradual, hybrid, or full
3. **Test locally first**: Use Databricks Community Edition (free tier)
4. **Create test notebook**: Migrate one component (e.g., data ingestion)
5. **Validate results**: Ensure parity with existing pipeline
6. **Scale gradually**: Add more components week by week

