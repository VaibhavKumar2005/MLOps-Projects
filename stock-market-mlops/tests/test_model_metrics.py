from src.training.model_metrics import calculate_metrics

def test_metrics():

    y_true = [1, 2, 3]
    y_pred = [1, 2, 3]

    metrics = calculate_metrics(
        y_true,
        y_pred
    )

    assert metrics["r2"] == 1.0