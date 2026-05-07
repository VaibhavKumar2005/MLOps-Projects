import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import joblib
import mlflow
import os
from pathlib import Path
from feature_engineering import create_features


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
    
    with mlflow.start_run():
        # Train model
        model = LinearRegression()
        model.fit(X_train, y_train)

        # Make predictions and evaluate
        preds = model.predict(X_test)
        mse = mean_squared_error(y_test, preds)
        
        print(f"Mean Squared Error: {mse}")

        # Log metrics
        mlflow.log_metric("mse", mse)
        
        # Log model
        mlflow.sklearn.log_model(model, "model")
        
        # Save model locally
        joblib.dump(model, "models/model.pkl")
        print("Model saved to models/model.pkl")
        
    return model, mse


if __name__ == "__main__":
    train_model()