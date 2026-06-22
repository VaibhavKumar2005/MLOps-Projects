from unittest.mock import patch

@patch(
    "src.orchestration.retraining_orchestrator.train_model"
)
def test_retraining(mock_train):

    mock_train()

    mock_train.assert_called_once()