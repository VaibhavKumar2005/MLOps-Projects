from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path

import joblib
import mlflow
import mlflow.sklearn
import mlflow.xgboost
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor

from src.feature_engineering import create_features


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_PATH = BASE_DIR / "data" / "AAPL_stock_data.csv"
MODELS_DIR = BASE_DIR / "models"
EXPERIMENT_NAME = "stock-market-experiment"
REGISTERED_MODEL_NAME = "stock_predictor"
TARGET_COLUMN = "Target"
TEST_SIZE = 0.2
RANDOM_STATE = 42


def _load_raw_dataset(data_path: Path) -> pd.DataFrame:
    """Load the historical stock CSV using the repo's current file layout."""

    if not data_path.exists():
        raise FileNotFoundError(f"Dataset not found: {data_path}")

    logger.info("Loading dataset from %s", data_path)
    return pd.read_csv(data_path, header=0, index_col=0, skiprows=[1, 2], parse_dates=True)


def _build_models() -> dict[str, tuple[object, dict[str, object]]]:
    """Return the candidate models and the parameters to log for each one."""

    return {
        "linear_regression": (
            LinearRegression(),
            {},
        ),
        "random_forest": (
            RandomForestRegressor(
                n_estimators=300,
                max_depth=None,
                min_samples_split=2,
                min_samples_leaf=1,
                random_state=RANDOM_STATE,
                n_jobs=-1,
            ),
            {
                "n_estimators": 300,
                "max_depth": None,
                "min_samples_split": 2,
                "min_samples_leaf": 1,
                "random_state": RANDOM_STATE,
                "n_jobs": -1,
            },
        ),
        "xgboost": (
            XGBRegressor(
                n_estimators=300,
                max_depth=5,
                learning_rate=0.05,
                subsample=0.9,
                colsample_bytree=0.9,
                reg_lambda=1.0,
                random_state=RANDOM_STATE,
                objective="reg:squarederror",
                eval_metric="rmse",
                verbosity=0,
            ),
            {
                "n_estimators": 300,
                "max_depth": 5,
                "learning_rate": 0.05,
                "subsample": 0.9,
                "colsample_bytree": 0.9,
                "reg_lambda": 1.0,
                "random_state": RANDOM_STATE,
                "objective": "reg:squarederror",
                "eval_metric": "rmse",
            },
        ),
    }


def _evaluate_model(model, X_test: pd.DataFrame, y_test: pd.Series) -> dict[str, float]:
    predictions = model.predict(X_test)
    return {
        "rmse": float(mean_squared_error(y_test, predictions, squared=False)),
        "mae": float(mean_absolute_error(y_test, predictions)),
        "r2": float(r2_score(y_test, predictions)),
    }


def _feature_importance_artifact(model, feature_names: list[str]) -> dict[str, float] | None:
    if not hasattr(model, "feature_importances_"):
        return None

    importance_pairs = zip(feature_names, model.feature_importances_)
    return dict(sorted(((name, float(score)) for name, score in importance_pairs), key=lambda item: item[1], reverse=True))


def _log_model_artifact(model_name: str, model) -> None:
    if model_name == "xgboost":
        mlflow.xgboost.log_model(model, artifact_path="model")
    else:
        mlflow.sklearn.log_model(model, artifact_path="model")


def train_model():
    """Train, compare, and register candidate stock price models."""

    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    mlflow.set_experiment(EXPERIMENT_NAME)

    raw_df = _load_raw_dataset(DATA_PATH)
    featured_df = create_features(raw_df)

    if TARGET_COLUMN not in featured_df.columns:
        raise KeyError(f"Expected target column '{TARGET_COLUMN}' after feature creation.")

    X = featured_df.drop(columns=[TARGET_COLUMN])
    y = featured_df[TARGET_COLUMN]

    if len(featured_df) < 10:
        raise ValueError("Not enough rows after feature engineering to train models.")

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=TEST_SIZE,
        shuffle=False,
    )

    logger.info(
        "Training split prepared: train_rows=%s, test_rows=%s, features=%s",
        len(X_train),
        len(X_test),
        list(X.columns),
    )

    results: list[dict[str, object]] = []

    with mlflow.start_run(run_name="train_model"):
        mlflow.log_params(
            {
                "dataset_path": str(DATA_PATH),
                "target_column": TARGET_COLUMN,
                "test_size": TEST_SIZE,
                "random_state": RANDOM_STATE,
                "train_rows": len(X_train),
                "test_rows": len(X_test),
                "feature_count": X.shape[1],
                "source_rows": len(raw_df),
                "featured_rows": len(featured_df),
            }
        )

        for model_name, (model, model_params) in _build_models().items():
            logger.info("Training %s", model_name)

            with mlflow.start_run(run_name=model_name, nested=True):
                mlflow.log_params(model_params)
                model.fit(X_train, y_train)

                metrics = _evaluate_model(model, X_test, y_test)
                mlflow.log_metrics(metrics)

                feature_importance = _feature_importance_artifact(model, list(X.columns))
                if feature_importance is not None:
                    with tempfile.TemporaryDirectory() as tmpdir:
                        artifact_path = Path(tmpdir) / f"{model_name}_feature_importance.json"
                        artifact_path.write_text(json.dumps(feature_importance, indent=2), encoding="utf-8")
                        mlflow.log_artifact(str(artifact_path), artifact_path="feature_importance")

                _log_model_artifact(model_name, model)

                results.append(
                    {
                        "model_name": model_name,
                        "model": model,
                        "params": model_params,
                        "metrics": metrics,
                        "feature_importance": feature_importance,
                    }
                )

                logger.info(
                    "%s -> RMSE: %.4f | MAE: %.4f | R2: %.4f",
                    model_name,
                    metrics["rmse"],
                    metrics["mae"],
                    metrics["r2"],
                )

        comparison_df = pd.DataFrame(
            [
                {
                    "model_name": item["model_name"],
                    "rmse": item["metrics"]["rmse"],
                    "mae": item["metrics"]["mae"],
                    "r2": item["metrics"]["r2"],
                }
                for item in results
            ]
        ).sort_values(["rmse", "mae"], ascending=[True, True], ignore_index=True)

        with tempfile.TemporaryDirectory() as tmpdir:
            comparison_path = Path(tmpdir) / "model_comparison.csv"
            comparison_df.to_csv(comparison_path, index=False)
            mlflow.log_artifact(str(comparison_path), artifact_path="comparison")

        best_row = comparison_df.iloc[0]
        best_result = next(item for item in results if item["model_name"] == best_row["model_name"])
        best_model = best_result["model"]

        mlflow.log_dict(best_row.to_dict(), "best_model_summary.json")
        mlflow.log_metric("best_rmse", float(best_row["rmse"]))
        mlflow.log_metric("best_mae", float(best_row["mae"]))
        mlflow.log_metric("best_r2", float(best_row["r2"]))
        mlflow.set_tag("best_model_name", best_row["model_name"])

        logger.info(
            "Best model selected: %s (RMSE %.4f, MAE %.4f, R2 %.4f)",
            best_row["model_name"],
            float(best_row["rmse"]),
            float(best_row["mae"]),
            float(best_row["r2"]),
        )

        if best_row["model_name"] == "xgboost":
            mlflow.xgboost.log_model(
                best_model,
                artifact_path="best_model",
                registered_model_name=REGISTERED_MODEL_NAME,
            )
        else:
            mlflow.sklearn.log_model(
                best_model,
                artifact_path="best_model",
                registered_model_name=REGISTERED_MODEL_NAME,
            )

        client = mlflow.tracking.MlflowClient()
        registered_versions = client.search_model_versions(f"name='{REGISTERED_MODEL_NAME}'")
        if registered_versions:
            latest_version = max(registered_versions, key=lambda item: int(item.version))
            try:
                client.transition_model_version_stage(
                    name=REGISTERED_MODEL_NAME,
                    version=latest_version.version,
                    stage="Production",
                )
                logger.info(
                    "Registered model %s version %s transitioned to Production",
                    REGISTERED_MODEL_NAME,
                    latest_version.version,
                )
            except Exception as exc:  # pragma: no cover - registry stage promotion is environment dependent
                logger.warning("Could not transition model to Production: %s", exc)

        fallback_path = MODELS_DIR / "model.pkl"
        joblib.dump(best_model, fallback_path)
        logger.info("Saved fallback model to %s", fallback_path)

    return best_model, comparison_df


if __name__ == "__main__":
    train_model()