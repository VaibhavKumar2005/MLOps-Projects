from fastapi import FastAPI
import mlflow.pyfunc
import pandas as pd

app = FastAPI()

model = mlflow.pyfunc.load_model(
    "models:/stock_model/Production"
)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/predict")
def predict(features: dict):

    df = pd.DataFrame([features])

    prediction = model.predict(df)

    return {
        "prediction": float(
            prediction[0]
        )
    }