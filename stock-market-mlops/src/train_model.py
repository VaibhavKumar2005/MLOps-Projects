import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import joblib

def train_model():
    df = pd.read_csv("data/AAPL_stock_data.csv", index_col=0)

    from feature_engineering import create_features
    df = create_features(df)

    X = df.drop(columns=["Target"])
    y = df["Target"]

    split_index = int(len(df) * 0.8)

    X_train, X_test = X[:split_index], X[split_index:]
    y_train, y_test = y[:split_index], y[split_index:]

    model = LinearRegression()
    model.fit(X_train, y_train)

    preds = model.predict(X_test)

    mse = mean_squared_error(y_test, preds)
    print(f"Mean Squared Error: {mse}")

    joblib.dump(model, "models/model.pkl")
    print("Model saved!")

if __name__ == "__main__":
    train_model()