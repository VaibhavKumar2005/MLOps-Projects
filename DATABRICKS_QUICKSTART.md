# Quick Start: Databricks Integration for Stock Market MLOps

## 🚀 5-Minute Setup

### Step 1: Install Databricks SDK

```bash
# Install required packages
pip install databricks-cli databricks-sdk databricks-sql-connector

# Or update your pyproject.toml (see PYPROJECT_UPDATES.md)
```

### Step 2: Authenticate with Databricks

```bash
# Option A: Set environment variables
export DATABRICKS_HOST=https://your-workspace-id.cloud.databricks.com
export DATABRICKS_TOKEN=dapi123456789...

# Option B: Create ~/.databricks/config
[DEFAULT]
host = https://your-workspace-id.cloud.databricks.com
token = dapi123456789...

# Test connection
databricks workspace list
```

### Step 3: Create Unity Catalog Structure

```bash
# Run this in Databricks SQL editor or use the provided notebook
CREATE CATALOG stock_market_mlops;
CREATE SCHEMA stock_market_mlops.raw_data;
CREATE SCHEMA stock_market_mlops.features;
CREATE SCHEMA stock_market_mlops.predictions;
CREATE SCHEMA stock_market_mlops.models;
```

### Step 4: Migrate Your Data

```python
from src.databricks_utils import DatabricksConnector, setup_databricks_tables

# Initialize
db = DatabricksConnector()

# Create tables
setup_databricks_tables(db)

# Migrate your CSV data
import pandas as pd
df = pd.read_csv("stock-market-mlops/data/AAPL_stock_data.csv")
spark_df = spark.createDataFrame(df)
spark_df.write.format("delta").mode("overwrite") \
    .saveAsTable("stock_market_mlops.raw_data.aapl_historical")
```

### Step 5: Create Databricks Notebooks

Upload the provided notebooks to your workspace:
- `notebooks/01_data_migration.py` - Data setup
- `notebooks/02_feature_engineering.py` - Feature creation
- `notebooks/03_model_training.py` - Model training

### Step 6: Run Your First Notebook

1. Open notebook `01_data_migration.py` in Databricks
2. Update the CSV paths to point to your repo
3. Run all cells
4. Verify data appears in `stock_market_mlops.raw_data`

---

## 📚 Integration Patterns

### Pattern 1: Kafka + Databricks Hybrid (Recommended)

Keep existing Kafka infrastructure and add Databricks:

```python
# In your kafka_producer.py
from kafka import KafkaProducer
from src.databricks_utils import DatabricksConnector, KafkaEventsToDelta
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
db = DatabricksConnector()
bridge = KafkaEventsToDelta(db)

def send_event(symbol, data):
    # Send to Kafka (keep existing pipeline)
    producer.send('stock.raw', value=json.dumps(data).encode())
    
    # Also write to Databricks (new)
    bridge.write_market_event({
        'timestamp': data['timestamp'],
        'symbol': symbol,
        'open': data['open'],
        'high': data['high'],
        'low': data['low'],
        'close': data['close'],
        'volume': data['volume']
    })
```

### Pattern 2: Direct Inference with Databricks

Deploy your model to Databricks Model Serving:

```python
# Get predictions from Databricks endpoint
import requests

endpoint_url = "https://your-workspace.cloud.databricks.com/serving-endpoints/stock-predictor/invocations"
headers = {"Authorization": f"Bearer {token}"}

features = {
    "dataframe_records": [
        {
            "close": 150.2,
            "sma_20": 149.5,
            "momentum_5": 0.012,
            "volatility_20": 0.025
        }
    ]
}

response = requests.post(endpoint_url, json=features, headers=headers)
prediction = response.json()["predictions"]
```

### Pattern 3: Scheduled Batch Retraining

Create Databricks Workflow job:

```python
# Via API
import requests

workflow = {
    "name": "stock-market-retrain",
    "tasks": [
        {
            "task_key": "feature_engineering",
            "notebook_task": {
                "notebook_path": "/Users/email/stock-market-mlops/02_feature_engineering"
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2
            }
        },
        {
            "task_key": "model_training",
            "depends_on": [{"task_key": "feature_engineering"}],
            "notebook_task": {
                "notebook_path": "/Users/email/stock-market-mlops/03_model_training"
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2
            }
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 * * * ?",  # Daily at midnight
        "timezone_id": "UTC"
    }
}

requests.post(
    f"{host}/api/2.0/jobs/create",
    headers={"Authorization": f"Bearer {token}"},
    json=workflow
)
```

---

## 🔧 Troubleshooting

### Issue: "No module named 'databricks'"

```bash
pip install --upgrade databricks-sdk databricks-sql-connector
```

### Issue: "Authentication failed"

```bash
# Verify token is correct
databricks workspace list

# Set PAT token properly
export DATABRICKS_TOKEN=dapi...
```

### Issue: "Table not found in catalog"

```bash
# Verify catalog exists
databricks sql execute "SHOW CATALOGS" --warehouse-id abc123

# Create catalog if needed
databricks sql execute "CREATE CATALOG stock_market_mlops"
```

### Issue: "Notebook not found in workspace"

```bash
# List workspace paths
databricks workspace list /Users/your-email/

# Import notebook
databricks workspace import notebooks/01_data_migration.py \
    /Users/your-email/stock-market-mlops/01_data_migration.py
```

---

## 📊 Monitoring & Validation

### Verify Data Migration

```sql
-- Check if data made it to Databricks
SELECT COUNT(*) as total_records, 
       COUNT(DISTINCT symbol) as unique_symbols,
       MIN(timestamp) as earliest_date,
       MAX(timestamp) as latest_date
FROM stock_market_mlops.raw_data.kafka_events;
```

### Monitor MLflow Experiments

```sql
-- View all training runs
SELECT run_id, 
       metrics.f1_score,
       metrics.accuracy,
       start_time
FROM mlflow.runs
WHERE experiment_id = 123456
ORDER BY start_time DESC
LIMIT 10;
```

### Check Model Registry

```sql
-- List all registered models
SELECT * FROM mlflow.registered_models
WHERE name LIKE '%stock%';
```

---

## 🎯 Next Steps

1. ✅ Complete 5-minute setup above
2. ✅ Run data migration notebook
3. ✅ Test feature engineering
4. ✅ Train your first model on Databricks
5. ⬜ Deploy model serving endpoint
6. ⬜ Set up monitoring and alerts
7. ⬜ Configure automated retraining workflow

---

## 📖 Full Documentation

See [DATABRICKS_INTEGRATION.md](./DATABRICKS_INTEGRATION.md) for comprehensive guide.

## 🤝 Support

- Databricks Docs: https://docs.databricks.com
- MLflow Docs: https://mlflow.org/docs
- Community: https://community.databricks.com
