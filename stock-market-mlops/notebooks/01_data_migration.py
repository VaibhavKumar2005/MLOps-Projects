# Databricks notebook source

# MAGIC %md
# MAGIC # Stock Market MLOps - Phase 1: Data Migration
# MAGIC 
# MAGIC This notebook demonstrates how to migrate your historical stock data from CSV/DVC to Databricks Delta Lake.
# MAGIC 
# MAGIC **Key Steps:**
# MAGIC 1. Load historical CSV data from your repo
# MAGIC 2. Convert to Spark DataFrames
# MAGIC 3. Write to Delta Lake tables in Unity Catalog
# MAGIC 4. Verify data and schema

# COMMAND ----------

# MAGIC %python
# MAGIC import pandas as pd
# MAGIC from datetime import datetime
# MAGIC import logging
# MAGIC 
# MAGIC logging.basicConfig(level=logging.INFO)
# MAGIC logger = logging.getLogger(__name__)
# MAGIC 
# MAGIC print("✅ Libraries loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Historical CSV Data
# MAGIC 
# MAGIC Load the stock data files from your project repository

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Define paths to CSV files in workspace
# MAGIC csv_paths = {
# MAGIC     "AAPL": "/Workspace/Repos/your-username/MLOps/stock-market-mlops/data/AAPL_stock_data.csv",
# MAGIC     "MSFT": "/Workspace/Repos/your-username/MLOps/stock-market-mlops/data/MSFT_stock_data.csv",
# MAGIC     "TSLA": "/Workspace/Repos/your-username/MLOps/stock-market-mlops/data/TSLA_stock_data.csv",
# MAGIC }
# MAGIC 
# MAGIC # Load with pandas
# MAGIC dfs = {}
# MAGIC for symbol, path in csv_paths.items():
# MAGIC     try:
# MAGIC         df = pd.read_csv(path, header=0, index_col=0, skiprows=[1, 2], parse_dates=True)
# MAGIC         dfs[symbol] = df
# MAGIC         print(f"✅ Loaded {symbol}: {len(df)} rows, {df.shape[1]} columns")
# MAGIC         print(f"   Columns: {list(df.columns)}")
# MAGIC         print(f"   Date range: {df.index.min()} to {df.index.max()}")
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Failed to load {symbol}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Unity Catalog & Schemas
# MAGIC 
# MAGIC Set up the catalog structure in Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog
# MAGIC CREATE CATALOG IF NOT EXISTS stock_market_mlops;
# MAGIC 
# MAGIC -- Create schemas
# MAGIC CREATE SCHEMA IF NOT EXISTS stock_market_mlops.raw_data
# MAGIC   COMMENT = "Raw stock market data from external sources";
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS stock_market_mlops.features
# MAGIC   COMMENT = "Engineered features for ML training";
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS stock_market_mlops.predictions
# MAGIC   COMMENT = "Model predictions and inference logs";
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS stock_market_mlops.models
# MAGIC   COMMENT = "ML model artifacts and metadata";

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write Historical Data to Delta Tables
# MAGIC 
# MAGIC Convert CSV data to Delta Lake for versioning and ACID compliance

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Convert Pandas DataFrames to Spark and write to Delta
# MAGIC for symbol, df_pandas in dfs.items():
# MAGIC     try:
# MAGIC         # Convert to Spark DataFrame
# MAGIC         df_spark = spark.createDataFrame(df_pandas.reset_index())
# MAGIC         
# MAGIC         # Define table name
# MAGIC         table_name = f"stock_market_mlops.raw_data.{symbol.lower()}_historical"
# MAGIC         
# MAGIC         # Write to Delta (creates or overwrites)
# MAGIC         df_spark.write \
# MAGIC             .format("delta") \
# MAGIC             .mode("overwrite") \
# MAGIC             .option("mergeSchema", "true") \
# MAGIC             .saveAsTable(table_name)
# MAGIC         
# MAGIC         print(f"✅ Written {symbol} data to {table_name}")
# MAGIC         
# MAGIC         # Show sample
# MAGIC         spark.sql(f"SELECT * FROM {table_name} LIMIT 5").show()
# MAGIC         
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Failed to write {symbol}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Kafka Events Table (For Streaming)
# MAGIC 
# MAGIC Create table to receive real-time Kafka events

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS stock_market_mlops.raw_data.kafka_events (
# MAGIC     timestamp TIMESTAMP,
# MAGIC     symbol STRING,
# MAGIC     open DOUBLE,
# MAGIC     high DOUBLE,
# MAGIC     low DOUBLE,
# MAGIC     close DOUBLE,
# MAGIC     volume LONG,
# MAGIC     ingestion_time STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT = "Real-time market events from Kafka producers";
# MAGIC 
# MAGIC -- Create table for predictions
# MAGIC CREATE TABLE IF NOT EXISTS stock_market_mlops.predictions.inference_logs (
# MAGIC     timestamp STRING,
# MAGIC     symbol STRING,
# MAGIC     prediction DOUBLE,
# MAGIC     confidence DOUBLE,
# MAGIC     model_version STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT = "Model inference predictions and confidence scores";
# MAGIC 
# MAGIC -- Create table for drift alerts
# MAGIC CREATE TABLE IF NOT EXISTS stock_market_mlops.predictions.drift_alerts (
# MAGIC     timestamp STRING,
# MAGIC     metric STRING,
# MAGIC     z_score DOUBLE,
# MAGIC     value DOUBLE,
# MAGIC     threshold DOUBLE,
# MAGIC     symbol STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT = "Data drift detection alerts";

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Data in Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show all tables in catalog
# MAGIC SELECT 
# MAGIC     table_name,
# MAGIC     table_type,
# MAGIC     owner
# MAGIC FROM stock_market_mlops.information_schema.tables
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Verify row counts
# MAGIC tables = [
# MAGIC     "stock_market_mlops.raw_data.aapl_historical",
# MAGIC     "stock_market_mlops.raw_data.msft_historical",
# MAGIC     "stock_market_mlops.raw_data.tsla_historical",
# MAGIC ]
# MAGIC 
# MAGIC for table_name in tables:
# MAGIC     try:
# MAGIC         count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]["cnt"]
# MAGIC         print(f"✅ {table_name}: {count:,} rows")
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ {table_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Bronze/Silver/Gold Tables (Medallion Architecture)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- BRONZE: Raw data (as-is from source)
# MAGIC -- Already created above (raw_data.kafka_events)
# MAGIC 
# MAGIC -- SILVER: Cleaned and enriched data
# MAGIC CREATE TABLE IF NOT EXISTS stock_market_mlops.raw_data.stock_data_silver (
# MAGIC     timestamp TIMESTAMP,
# MAGIC     symbol STRING,
# MAGIC     open DOUBLE,
# MAGIC     high DOUBLE,
# MAGIC     low DOUBLE,
# MAGIC     close DOUBLE,
# MAGIC     volume LONG,
# MAGIC     daily_return DOUBLE,
# MAGIC     ingestion_date DATE,
# MAGIC     _data_quality_score DOUBLE,
# MAGIC     _processed_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT = "Cleaned stock market data with validation"
# MAGIC TBLPROPERTIES ("delta.autoOptimize.optimizeWrite" = "true");
# MAGIC 
# MAGIC -- GOLD: Analysis-ready data for ML
# MAGIC CREATE TABLE IF NOT EXISTS stock_market_mlops.features.stock_features_gold (
# MAGIC     timestamp TIMESTAMP,
# MAGIC     symbol STRING,
# MAGIC     -- Technical indicators
# MAGIC     sma_20 DOUBLE,
# MAGIC     sma_50 DOUBLE,
# MAGIC     rsi DOUBLE,
# MAGIC     macd DOUBLE,
# MAGIC     bollinger_upper DOUBLE,
# MAGIC     bollinger_lower DOUBLE,
# MAGIC     -- Volatility metrics
# MAGIC     volatility DOUBLE,
# MAGIC     atr DOUBLE,
# MAGIC     -- Target variable
# MAGIC     target DOUBLE,
# MAGIC     -- Metadata
# MAGIC     feature_version STRING,
# MAGIC     _generated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (symbol)
# MAGIC COMMENT = "ML-ready features for stock prediction models"
# MAGIC TBLPROPERTIES ("delta.autoOptimize.optimizeWrite" = "true");

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Data Quality Checks

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.functions import col, when, count, isnan, isnull
# MAGIC 
# MAGIC # Data quality checks for AAPL data
# MAGIC df = spark.table("stock_market_mlops.raw_data.aapl_historical")
# MAGIC 
# MAGIC print("📊 Data Quality Report for AAPL")
# MAGIC print("=" * 50)
# MAGIC 
# MAGIC # Check for nulls
# MAGIC null_counts = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
# MAGIC print("\nNull counts per column:")
# MAGIC null_counts.show()
# MAGIC 
# MAGIC # Check for invalid values
# MAGIC price_cols = ["Open", "High", "Low", "Close"]
# MAGIC invalid_prices = df.filter(
# MAGIC     (col("Open") <= 0) | (col("High") < col("Low")) | 
# MAGIC     (col("Close") <= 0) | (col("Volume") < 0)
# MAGIC ).count()
# MAGIC 
# MAGIC print(f"\n⚠️  Invalid price records: {invalid_prices}")
# MAGIC 
# MAGIC # Statistics
# MAGIC print("\nStatistics:")
# MAGIC df.select([c for c in df.columns if c not in ["Date", "Index"]]).describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC - **Notebook 02**: Set up Kafka streaming to Delta
# MAGIC - **Notebook 03**: Feature engineering pipeline
# MAGIC - **Notebook 04**: Model training with MLflow
# MAGIC - **Notebook 05**: Model serving and inference
# MAGIC - **Notebook 06**: Drift monitoring and alerts
