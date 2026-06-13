# Databricks notebook source

# MAGIC %md
# MAGIC # Stock Market MLOps - Phase 2: Feature Engineering with Databricks Feature Store
# MAGIC 
# MAGIC This notebook demonstrates:
# MAGIC 1. Loading raw data from Delta tables
# MAGIC 2. Computing technical indicators (moving averages, RSI, MACD, etc.)
# MAGIC 3. Creating feature tables in Databricks Feature Store
# MAGIC 4. Publishing features for model training

# COMMAND ----------

# MAGIC %python
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC from pyspark.sql.functions import col, when, lag, avg, stddev_pop, lit, current_timestamp
# MAGIC from pyspark.sql.window import Window
# MAGIC from datetime import datetime, timedelta
# MAGIC import logging
# MAGIC 
# MAGIC logger = logging.getLogger(__name__)
# MAGIC print("✅ Libraries loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Raw Data from Delta

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Load historical data
# MAGIC df_aapl = spark.table("stock_market_mlops.raw_data.aapl_historical")
# MAGIC df_msft = spark.table("stock_market_mlops.raw_data.msft_historical")
# MAGIC df_tsla = spark.table("stock_market_mlops.raw_data.tsla_historical")
# MAGIC 
# MAGIC print(f"AAPL: {df_aapl.count()} rows")
# MAGIC print(f"MSFT: {df_msft.count()} rows")
# MAGIC print(f"TSLA: {df_tsla.count()} rows")
# MAGIC 
# MAGIC # Combine all stocks with symbol column
# MAGIC df_combined = df_aapl.withColumn("symbol", lit("AAPL")) \
# MAGIC     .unionByName(df_msft.withColumn("symbol", lit("MSFT"))) \
# MAGIC     .unionByName(df_tsla.withColumn("symbol", lit("TSLA")))
# MAGIC 
# MAGIC df_combined = df_combined.orderBy("symbol", "Date")
# MAGIC df_combined.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Feature Engineering - Technical Indicators

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Window specification for time series calculations
# MAGIC window_spec = Window.partitionBy("symbol").orderBy("Date")
# MAGIC 
# MAGIC # Calculate features
# MAGIC df_features = df_combined \
# MAGIC     .withColumn("daily_return", (col("Close") - lag("Close").over(window_spec)) / lag("Close").over(window_spec)) \
# MAGIC     .withColumn("daily_volatility", col("daily_return").cast("double")) \
# MAGIC     .withColumn("high_low_ratio", col("High") / col("Low")) \
# MAGIC     .withColumn("close_open_ratio", col("Close") / col("Open")) \
# MAGIC     .withColumn("volume_change", (col("Volume") - lag("Volume").over(window_spec)) / lag("Volume").over(window_spec))
# MAGIC 
# MAGIC print("✅ Basic features computed")
# MAGIC df_features.select("Date", "symbol", "Close", "daily_return", "high_low_ratio").display()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Calculate moving averages (SMA)
# MAGIC window_20 = Window.partitionBy("symbol").orderBy("Date").rangeBetween(-20*86400, 0)
# MAGIC window_50 = Window.partitionBy("symbol").orderBy("Date").rangeBetween(-50*86400, 0)
# MAGIC 
# MAGIC df_features = df_features \
# MAGIC     .withColumn("sma_20", avg("Close").over(window_20)) \
# MAGIC     .withColumn("sma_50", avg("Close").over(window_50)) \
# MAGIC     .withColumn("sma_ratio", col("Close") / col("sma_20"))
# MAGIC 
# MAGIC print("✅ Moving averages computed")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Calculate momentum indicators
# MAGIC df_features = df_features \
# MAGIC     .withColumn("momentum_5", (col("Close") - lag("Close", 5).over(window_spec)) / lag("Close", 5).over(window_spec)) \
# MAGIC     .withColumn("momentum_10", (col("Close") - lag("Close", 10).over(window_spec)) / lag("Close", 10).over(window_spec)) \
# MAGIC     .withColumn("roc_10", (col("Close") / lag("Close", 10).over(window_spec) - 1) * 100)  # Rate of Change
# MAGIC 
# MAGIC print("✅ Momentum indicators computed")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Calculate volatility (rolling standard deviation)
# MAGIC window_20_vol = Window.partitionBy("symbol").orderBy("Date").rowsBetween(-20, 0)
# MAGIC window_10_vol = Window.partitionBy("symbol").orderBy("Date").rowsBetween(-10, 0)
# MAGIC 
# MAGIC df_features = df_features \
# MAGIC     .withColumn("volatility_20", stddev_pop("daily_return").over(window_20_vol)) \
# MAGIC     .withColumn("volatility_10", stddev_pop("daily_return").over(window_10_vol))
# MAGIC 
# MAGIC print("✅ Volatility indicators computed")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Remove rows with null values (from lag/window calculations)
# MAGIC df_features_clean = df_features.dropna()
# MAGIC 
# MAGIC print(f"✅ Cleaned features: {df_features_clean.count()} rows (removed nulls from window calculations)")
# MAGIC 
# MAGIC # Show sample features
# MAGIC cols_to_show = ["Date", "symbol", "Close", "sma_20", "sma_ratio", "momentum_5", "volatility_20"]
# MAGIC df_features_clean.select(cols_to_show).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Target Variable for ML

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Target: Predict next day's price movement (direction)
# MAGIC df_features_with_target = df_features_clean \
# MAGIC     .withColumn("next_close", lead("Close").over(window_spec)) \
# MAGIC     .withColumn("price_change", col("next_close") - col("Close")) \
# MAGIC     .withColumn("target", when(col("price_change") > 0, 1).otherwise(0))  # Binary classification
# MAGIC 
# MAGIC # Drop last row (no target available)
# MAGIC df_features_with_target = df_features_with_target.filter(col("next_close").isNotNull())
# MAGIC 
# MAGIC print(f"✅ Target variable created: {df_features_with_target.count()} training samples")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify no target values are null
# MAGIC SELECT COUNT(*) as total_rows, 
# MAGIC        SUM(CASE WHEN target IS NULL THEN 1 ELSE 0 END) as null_targets
# MAGIC FROM stock_market_mlops.features.stock_features_gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write Features to Delta Lake

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Select relevant columns for feature table
# MAGIC feature_columns = [
# MAGIC     "Date", "symbol",
# MAGIC     "Close", "Open", "High", "Low", "Volume",
# MAGIC     "daily_return", "sma_20", "sma_50", "sma_ratio",
# MAGIC     "momentum_5", "momentum_10", "roc_10",
# MAGIC     "volatility_20", "volatility_10",
# MAGIC     "target"
# MAGIC ]
# MAGIC 
# MAGIC df_features_final = df_features_with_target.select(feature_columns) \
# MAGIC     .withColumn("feature_version", lit("v1")) \
# MAGIC     .withColumn("_generated_at", current_timestamp())
# MAGIC 
# MAGIC # Write to feature store
# MAGIC df_features_final.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .option("mergeSchema", "true") \
# MAGIC     .saveAsTable("stock_market_mlops.features.engineered_features")
# MAGIC 
# MAGIC print("✅ Features written to: stock_market_mlops.features.engineered_features")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Verify features by symbol
# MAGIC spark.sql("""
# MAGIC     SELECT symbol, COUNT(*) as sample_count, 
# MAGIC            MIN(Date) as first_date, 
# MAGIC            MAX(Date) as last_date,
# MAGIC            ROUND(AVG(target), 2) as avg_target
# MAGIC     FROM stock_market_mlops.features.engineered_features
# MAGIC     GROUP BY symbol
# MAGIC     ORDER BY symbol
# MAGIC """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Databricks Feature Store Table (Optional)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC try:
# MAGIC     from databricks.feature_engineering import FeatureEngineeringClient
# MAGIC     
# MAGIC     fs = FeatureEngineeringClient()
# MAGIC     
# MAGIC     # Create feature table
# MAGIC     fs.create_table(
# MAGIC         name="stock_market_mlops.features.stock_market_features",
# MAGIC         primary_keys=["symbol", "Date"],
# MAGIC         df=df_features_final,
# MAGIC         description="Technical analysis features for stock price prediction",
# MAGIC         tags={"team": "mlops", "project": "stock-market"},
# MAGIC     )
# MAGIC     
# MAGIC     print("✅ Databricks Feature Store table created")
# MAGIC     
# MAGIC except Exception as e:
# MAGIC     print(f"⚠️  Feature Store not available (may require premium tier): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Data Quality Report

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC     symbol,
# MAGIC     COUNT(*) as total_samples,
# MAGIC     SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) as up_days,
# MAGIC     SUM(CASE WHEN target = 0 THEN 1 ELSE 0 END) as down_days,
# MAGIC     ROUND(100.0 * SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) as pct_up,
# MAGIC     ROUND(AVG(close), 2) as avg_close,
# MAGIC     ROUND(STDDEV_POP(volatility_20), 4) as volatility_mean,
# MAGIC     ROUND(AVG(volume), 0) as avg_volume
# MAGIC FROM stock_market_mlops.features.engineered_features
# MAGIC GROUP BY symbol
# MAGIC ORDER BY symbol;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ✅ Features created:
# MAGIC - **Price-based**: Close, Open, High, Low, Volume
# MAGIC - **Trend**: SMA-20, SMA-50, SMA ratio
# MAGIC - **Momentum**: Momentum-5, Momentum-10, Rate of Change
# MAGIC - **Volatility**: Volatility-20, Volatility-10
# MAGIC - **Target**: Binary classification (up/down)
# MAGIC 
# MAGIC **Next**: Use these features in `03_model_training.py` notebook
