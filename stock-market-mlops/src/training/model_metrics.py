from sklearn.metrics import (
    mean_squared_error,
    mean_absolute_error,
    r2_score
)

def calculate_metrics(
        y_true,
        y_pred
):
    return {
        "rmse": mean_squared_error(
            y_true,
            y_pred,
            squared=False
        ),
        "mae": mean_absolute_error(
            y_true,
            y_pred
        ),
        "r2": r2_score(
            y_true,
            y_pred
        )
    }