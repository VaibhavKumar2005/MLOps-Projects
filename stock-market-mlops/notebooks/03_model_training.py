# Databricks notebook source

# MAGIC %md
# MAGIC # Stock Market MLOps - Phase 3: Model Training with Databricks MLflow
# MAGIC 
# MAGIC This notebook demonstrates:
# MAGIC 1. Loading features from Delta
# MAGIC 2. Training XGBoost model
# MAGIC 3. Logging to Databricks MLflow (managed tracking)
# MAGIC 4. Registering model in Unity Catalog
# MAGIC 5. Setting model aliases for serving

# COMMAND ----------

# MAGIC %python
# MAGIC import pandas as pd
# MAGIC import numpy as np
# MAGIC from sklearn.model_selection import train_test_split
# MAGIC from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, confusion_matrix
# MAGIC from xgboost import XGBClassifier
# MAGIC import mlflow
# MAGIC from datetime import datetime
# MAGIC import logging
# MAGIC 
# MAGIC mlflow.set_tracking_uri("databricks")
# MAGIC 
# MAGIC logger = logging.getLogger(__name__)
# MAGIC print("✅ Libraries loaded and MLflow tracking configured to Databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Features from Delta

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Load engineered features
# MAGIC df_features = spark.table("stock_market_mlops.features.engineered_features").toPandas()
# MAGIC 
# MAGIC print(f"✅ Loaded {len(df_features)} samples from Delta Lake")
# MAGIC print(f"Shape: {df_features.shape}")
# MAGIC print(f"Columns: {list(df_features.columns)}")
# MAGIC print(f"\nData Info:")
# MAGIC print(df_features.info())

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Prepare features and target
# MAGIC # Exclude Date, symbol, feature_version, and metadata columns
# MAGIC exclude_cols = ["Date", "symbol", "feature_version", "_generated_at", "target"]
# MAGIC X_columns = [col for col in df_features.columns if col not in exclude_cols]
# MAGIC 
# MAGIC X = df_features[X_columns].fillna(0)
# MAGIC y = df_features["target"]
# MAGIC 
# MAGIC print(f"Features: {len(X_columns)} dimensions")
# MAGIC print(f"Feature columns: {X_columns}")
# MAGIC print(f"\nTarget distribution:")
# MAGIC print(y.value_counts(normalize=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Train/Test Split

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Time-based split (respects temporal order)
# MAGIC split_idx = int(0.8 * len(df_features))
# MAGIC 
# MAGIC X_train = X[:split_idx]
# MAGIC X_test = X[split_idx:]
# MAGIC y_train = y[:split_idx]
# MAGIC y_test = y[split_idx:]
# MAGIC 
# MAGIC print(f"✅ Train/Test Split (80/20):")
# MAGIC print(f"   Train: {len(X_train)} samples")
# MAGIC print(f"   Test:  {len(X_test)} samples")
# MAGIC print(f"\nClass distribution (Train):")
# MAGIC print(f"   Down (0): {(y_train == 0).sum()} ({100*(y_train == 0).sum()/len(y_train):.1f}%)")
# MAGIC print(f"   Up (1):   {(y_train == 1).sum()} ({100*(y_train == 1).sum()/len(y_train):.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create MLflow Experiment

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Set experiment path (Databricks auto-creates if doesn't exist)
# MAGIC experiment_path = "/Users/{current_user}/stock-market-mlops/experiments"
# MAGIC 
# MAGIC try:
# MAGIC     mlflow.set_experiment(experiment_path)
# MAGIC     print(f"✅ Experiment set to: {experiment_path}")
# MAGIC except:
# MAGIC     # Fallback if path format doesn't work
# MAGIC     mlflow.set_experiment("stock-market-mlops")
# MAGIC     print(f"✅ Experiment set to: stock-market-mlops")
# MAGIC 
# MAGIC experiment = mlflow.get_experiment_by_name(mlflow.active_run() and mlflow.active_run().info.experiment_id or "stock-market-mlops")
# MAGIC print(f"Experiment ID: {experiment.experiment_id if experiment else 'N/A'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Train XGBoost Model with MLflow Tracking

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Model hyperparameters
# MAGIC model_params = {
# MAGIC     "n_estimators": 100,
# MAGIC     "max_depth": 6,
# MAGIC     "learning_rate": 0.1,
# MAGIC     "subsample": 0.8,
# MAGIC     "colsample_bytree": 0.8,
# MAGIC     "gamma": 1,
# MAGIC     "random_state": 42,
# MAGIC     "early_stopping_rounds": 10,
# MAGIC     "eval_metric": "logloss"
# MAGIC }
# MAGIC 
# MAGIC print("🚀 Starting model training with MLflow tracking...")
# MAGIC print(f"Parameters: {model_params}")

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC with mlflow.start_run() as run:
# MAGIC     run_id = run.info.run_id
# MAGIC     print(f"📝 MLflow Run ID: {run_id}")
# MAGIC     
# MAGIC     # Log parameters to MLflow
# MAGIC     mlflow.log_params(model_params)
# MAGIC     
# MAGIC     # Create and train model
# MAGIC     model = XGBClassifier(**model_params)
# MAGIC     model.fit(
# MAGIC         X_train, y_train,
# MAGIC         eval_set=[(X_test, y_test)],
# MAGIC         verbose=False
# MAGIC     )
# MAGIC     
# MAGIC     print("✅ Model training complete")
# MAGIC     
# MAGIC     # Make predictions
# MAGIC     y_pred = model.predict(X_test)
# MAGIC     y_pred_proba = model.predict_proba(X_test)[:, 1]
# MAGIC     
# MAGIC     # Calculate metrics
# MAGIC     accuracy = accuracy_score(y_test, y_pred)
# MAGIC     precision = precision_score(y_test, y_pred)
# MAGIC     recall = recall_score(y_test, y_pred)
# MAGIC     f1 = f1_score(y_test, y_pred)
# MAGIC     auc_roc = roc_auc_score(y_test, y_pred_proba)
# MAGIC     
# MAGIC     # Log metrics to MLflow
# MAGIC     mlflow.log_metric("accuracy", accuracy)
# MAGIC     mlflow.log_metric("precision", precision)
# MAGIC     mlflow.log_metric("recall", recall)
# MAGIC     mlflow.log_metric("f1_score", f1)
# MAGIC     mlflow.log_metric("auc_roc", auc_roc)
# MAGIC     
# MAGIC     print("\n📊 Model Performance Metrics:")
# MAGIC     print(f"   Accuracy:  {accuracy:.4f}")
# MAGIC     print(f"   Precision: {precision:.4f}")
# MAGIC     print(f"   Recall:    {recall:.4f}")
# MAGIC     print(f"   F1-Score:  {f1:.4f}")
# MAGIC     print(f"   AUC-ROC:   {auc_roc:.4f}")
# MAGIC     
# MAGIC     # Log confusion matrix
# MAGIC     cm = confusion_matrix(y_test, y_pred)
# MAGIC     print(f"\nConfusion Matrix:")
# MAGIC     print(cm)
# MAGIC     
# MAGIC     # Feature importance
# MAGIC     feature_importance = pd.DataFrame({
# MAGIC         "feature": X_columns,
# MAGIC         "importance": model.feature_importances_
# MAGIC     }).sort_values("importance", ascending=False)
# MAGIC     
# MAGIC     print(f"\n🔝 Top 5 Feature Importance:")
# MAGIC     print(feature_importance.head())
# MAGIC     
# MAGIC     # Log feature importance as artifact
# MAGIC     feature_importance.to_csv("/tmp/feature_importance.csv", index=False)
# MAGIC     mlflow.log_artifact("/tmp/feature_importance.csv")
# MAGIC     
# MAGIC     # Log model to MLflow
# MAGIC     mlflow.xgboost.log_model(
# MAGIC         model,
# MAGIC         artifact_path="xgboost_model",
# MAGIC         registered_model_name="stock_market_mlops.models.stock_price_predictor"
# MAGIC     )
# MAGIC     
# MAGIC     print(f"\n✅ Model logged to MLflow")
# MAGIC     print(f"   Model URI: runs:/{run_id}/xgboost_model")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Check Registered Models

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all models in the registry
# MAGIC SELECT name, 
# MAGIC        latest_version,
# MAGIC        creator_user_id,
# MAGIC        last_updated_timestamp
# MAGIC FROM mlflow.registered_models
# MAGIC WHERE name LIKE '%stock%'
# MAGIC ORDER BY last_updated_timestamp DESC;

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Get model info
# MAGIC client = mlflow.MlflowClient()
# MAGIC 
# MAGIC try:
# MAGIC     model_name = "stock_market_mlops.models.stock_price_predictor"
# MAGIC     model_info = client.get_registered_model(model_name)
# MAGIC     print(f"✅ Model: {model_name}")
# MAGIC     print(f"   Latest Version: {model_info.latest_versions[-1].version}")
# MAGIC     print(f"   Latest Stage: {model_info.latest_versions[-1].current_stage}")
# MAGIC     print(f"   Created: {model_info.creation_timestamp}")
# MAGIC     
# MAGIC except Exception as e:
# MAGIC     print(f"Model info retrieval result: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Set Model Aliases for Easy Reference

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC try:
# MAGIC     client = mlflow.MlflowClient()
# MAGIC     model_name = "stock_market_mlops.models.stock_price_predictor"
# MAGIC     latest_version = client.get_registered_model(model_name).latest_versions[-1].version
# MAGIC     
# MAGIC     # Set alias for production use
# MAGIC     client.set_registered_model_alias(
# MAGIC         name=model_name,
# MAGIC         alias="champion",
# MAGIC         version=int(latest_version)
# MAGIC     )
# MAGIC     
# MAGIC     print(f"✅ Model alias 'champion' set to version {latest_version}")
# MAGIC     
# MAGIC     # Later, models can be loaded by alias:
# MAGIC     # model = mlflow.xgboost.load_model(f\"models:/{model_name}@champion\")\
# MAGIC     
# MAGIC except Exception as e:
# MAGIC     print(f"Alias setting: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Model Comparison (Optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare model performance across runs
# MAGIC SELECT 
# MAGIC     m.run_id,
# MAGIC     m.metrics.accuracy,
# MAGIC     m.metrics.f1_score,
# MAGIC     m.metrics.auc_roc,
# MAGIC     m.params.max_depth,
# MAGIC     m.params.learning_rate,
# MAGIC     m.start_time,
# MAGIC     m.end_time,
# MAGIC     (m.end_time - m.start_time) / 1000 as duration_seconds
# MAGIC FROM mlflow.runs m
# MAGIC WHERE m.experiment_id IN (
# MAGIC     SELECT experiment_id FROM mlflow.experiments WHERE name LIKE '%stock%'
# MAGIC )
# MAGIC ORDER BY m.metrics.f1_score DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ✅ Model Training Complete:
# MAGIC - **Model Type**: XGBoost Classifier
# MAGIC - **Training Samples**: ~{len(X_train)}
# MAGIC - **Test Samples**: ~{len(X_test)}
# MAGIC - **Features Used**: {len(X_columns)}
# MAGIC - **Registry**: `stock_market_mlops.models.stock_price_predictor`
# MAGIC - **Alias**: `champion` (latest version)
# MAGIC 
# MAGIC **Next Steps**:
# MAGIC - Move model to `Production` stage when validated
# MAGIC - Deploy via Databricks Model Serving endpoint
# MAGIC - Monitor model performance with inference logs
# MAGIC - Track data drift and trigger retraining if needed
