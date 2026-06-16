import pandas as pd
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import joblib
import mlflow
import os
import json
from pathlib import Path
from feature_engineering import create_features
from evaluate_model import evaluate_models
from promote_model import promote_if_better

def train_model():
    """Train a linear regression model on stock data."""
    
    # Ensure output directory exists
    os.makedirs("models", exist_ok=True)
    
    # Load and process data
    # Keep header row 0, skip metadata rows 1-2 (Ticker and Date), use first column as index
    df = pd.read_csv("data/AAPL_stock_data.csv", header=0, index_col=0, skiprows=[1, 2], parse_dates=True)
    df = create_features(df)

    # Prepare features and target
    X = df.drop(columns=["Target"])
    y = df["Target"]

    # Train-test split (80-20)
    split_index = int(len(df) * 0.8)
    X_train, X_test = X[:split_index], X[split_index:]
    y_train, y_test = y[:split_index], y[split_index:]

    # Train model with MLflow tracking
    mlflow.set_experiment("stock-market-experiment")
    
    with mlflow.start_run() as run:
        # Train XGBoost model
        model = XGBRegressor(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            random_state=42,
            early_stopping_rounds=10
        )
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False
        )

        # Make predictions and evaluate
        preds = model.predict(X_test)
        mse = mean_squared_error(y_test, preds)
        mae = mean_absolute_error(y_test, preds)
        r2 = r2_score(y_test, preds)
        
        print(f"Mean Squared Error: {mse:.4f}")
        print(f"Mean Absolute Error: {mae:.4f}")
        print(f"R² Score: {r2:.4f}")

        # Log metrics
        mlflow.log_metric("mse", mse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2_score", r2)
        
        # Log feature importance
        feature_importance = dict(zip(
            X_train.columns,
            model.feature_importances_
        ))
        # Sort by importance
        feature_importance = dict(
            sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
        )
        mlflow.log_dict(feature_importance, "feature_importance.json")
        
        print("\nFeature Importance:")
        for feature, importance in feature_importance.items():
            print(f"  {feature:15s} {importance:6.4f}")
        
        # Log model with registry
        model_name = "stock_predictor"
        mlflow.xgboost.log_model(
            xgb_model=model, 
            artifact_path="model",
            registered_model_name=model_name
        )
        
        # Promote to Staging automatically
        client = mlflow.tracking.MlflowClient()
        model_version = client.get_latest_versions(model_name, stages=["None"])[0].version
        client.transition_model_version_stage(
            name=model_name,
            version=model_version,
            stage="Staging"
        )
        print(f"✅ Model registered as '{model_name}' version {model_version} and promoted to 'Staging'")

        # Save model locally (legacy support)
        joblib.dump(model, "models/model.pkl")
        print("Model saved to models/model.pkl")
        
    return model, mse


if __name__ == "__main__":
    train_model()