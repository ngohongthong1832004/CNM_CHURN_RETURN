import pytest
from unittest.mock import Mock, patch
from src.mlflow_utils.experiment_tracker import ExperimentTracker


@patch("src.mlflow_utils.experiment_tracker.MlflowClient")
@patch("src.mlflow_utils.experiment_tracker.mlflow")
def test_init_creates_new_experiment(mock_mlflow, mock_client_class):
    mock_mlflow.get_experiment_by_name.return_value = None
    mock_mlflow.create_experiment.return_value = "new_exp_id"

    tracker = ExperimentTracker("http://localhost:5000", "new_experiment")

    mock_mlflow.create_experiment.assert_called_once_with(
        name="new_experiment", artifact_location=None
    )
    assert tracker.experiment_id == "new_exp_id"


@patch("src.mlflow_utils.experiment_tracker.MlflowClient")
@patch("src.mlflow_utils.experiment_tracker.mlflow")
def test_init_uses_existing_experiment(mock_mlflow, mock_client_class):
    mock_exp = Mock()
    mock_exp.experiment_id = "existing_id"
    mock_mlflow.get_experiment_by_name.return_value = mock_exp

    tracker = ExperimentTracker("http://localhost:5000", "existing_exp")

    mock_mlflow.create_experiment.assert_not_called()
    assert tracker.experiment_id == "existing_id"


@patch("src.mlflow_utils.experiment_tracker.MlflowClient")
@patch("src.mlflow_utils.experiment_tracker.mlflow")
def test_log_params_delegates_to_mlflow(mock_mlflow, mock_client_class):
    mock_mlflow.get_experiment_by_name.return_value = None
    mock_mlflow.create_experiment.return_value = "exp_id"

    tracker = ExperimentTracker("http://localhost:5000", "test")
    params = {"lr": 0.01, "n_estimators": 100}
    tracker.log_params(params)

    mock_mlflow.log_params.assert_called_once_with(params)


@patch("src.mlflow_utils.experiment_tracker.MlflowClient")
@patch("src.mlflow_utils.experiment_tracker.mlflow")
def test_log_metrics_delegates_to_mlflow(mock_mlflow, mock_client_class):
    mock_mlflow.get_experiment_by_name.return_value = None
    mock_mlflow.create_experiment.return_value = "exp_id"

    tracker = ExperimentTracker("http://localhost:5000", "test")
    metrics = {"accuracy": 0.95, "auc": 0.98}
    tracker.log_metrics(metrics)

    mock_mlflow.log_metrics.assert_called_once_with(metrics, step=None)


@patch("src.mlflow_utils.experiment_tracker.MlflowClient")
@patch("src.mlflow_utils.experiment_tracker.mlflow")
def test_get_best_run_returns_none_when_no_runs(mock_mlflow, mock_client_class):
    mock_mlflow.get_experiment_by_name.return_value = None
    mock_mlflow.create_experiment.return_value = "exp_id"
    mock_client = Mock()
    mock_client.search_runs.return_value = []
    mock_client_class.return_value = mock_client

    tracker = ExperimentTracker("http://localhost:5000", "test")
    result = tracker.get_best_run("accuracy")

    assert result is None


@patch("src.mlflow_utils.experiment_tracker.MlflowClient")
@patch("src.mlflow_utils.experiment_tracker.mlflow")
def test_get_best_run_returns_top_result(mock_mlflow, mock_client_class):
    mock_mlflow.get_experiment_by_name.return_value = None
    mock_mlflow.create_experiment.return_value = "exp_id"
    mock_run = Mock()
    mock_run.data.metrics = {"accuracy": 0.95}
    mock_client = Mock()
    mock_client.search_runs.return_value = [mock_run]
    mock_client_class.return_value = mock_client

    tracker = ExperimentTracker("http://localhost:5000", "test")
    result = tracker.get_best_run("accuracy")

    assert result == mock_run
    mock_client.search_runs.assert_called_once_with(
        experiment_ids=["exp_id"],
        filter_string="",
        max_results=1,
        order_by=["metrics.accuracy DESC"],
    )
