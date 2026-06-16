import mlflow

THRESHOLD = 0.01

def promote_if_better(
        candidate_score,
        production_score,
        model_name,
        version
):

    if candidate_score > (
            production_score + THRESHOLD
    ):

        client = mlflow.MlflowClient()

        client.transition_model_version_stage(
            name=model_name,
            version=version,
            stage="Production"
        )

        print("Promoted")