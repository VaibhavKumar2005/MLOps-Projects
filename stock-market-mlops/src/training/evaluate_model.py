from sklearn.metrics import r2_score

def evaluate_models(prod_preds,
                    new_preds,
                    y_true):

    prod_score = r2_score(
        y_true,
        prod_preds
    )

    new_score = r2_score(
        y_true,
        new_preds
    )

    return {
        "production": prod_score,
        "candidate": new_score
    }