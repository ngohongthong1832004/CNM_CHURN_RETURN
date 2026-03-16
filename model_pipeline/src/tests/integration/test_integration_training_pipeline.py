"""
Docstring for model_pipeline.src.tests.integration.test_integration_training_pipeline
Integration tests for the complete training pipeline
Tests the workflow: Data preparation -> Training -> Logging -> Model saving
"""
import pytest
import pandas as pd
import numpy as np  
import tempfile
import os   
from unittest.mock import Mock, patch, MagicMock
import mlflow

from src.mlflow_utils.experiment_tracker import ExperimentTracker
from src.model.xgboost_trainer import XGBoostTrainer



class TestCompleteTrainingPipeline:

    @patch('src.mlflow_utils.experiment_tracker.mlflow')
    @patch('src.model.xgboost_trainer.mlflow')
    def test_full_training_workflow(
        self,
        mock_xgb_mlflow,
        mock_tracker_mlflow,
        training_config,
        sample_training_data
    ):
        mock_exp = Mock()
        mock_exp.experiment_id = 'test_exp_id'
        mock_tracker_mlflow.get_experiment_by_name.return_value = mock_exp

        mock_run = Mock()
        mock_run.info.run_id = "test_run_id"
        mock_tracker_mlflow.start_run.return_value.__enter__.return_value = mock_run

        tracker = ExperimentTracker(
            tracking_uri=training_config["mlflow"]["tracking_uri"],
            experiment_name=training_config["mlflow"]["experiment_name"]
        )
        trainer = XGBoostTrainer(
            config=training_config["model"],
            experiment_tracker=tracker
        )

        dtrain, dtest, y_train, y_test = trainer.prepare_data(
            data=sample_training_data,
            target_col=training_config["features"]["target_column"],
            feature_cols=training_config["features"]["training_features"],
            test_size=training_config["model"]["train_test_split"]
        )

        assert trainer.feature_names is not None
        # assert len(trainer.feature_names) == 
        # assert len(y_train) ==   # 80% of
        # assert len(y_test) ==    # 20% of 

        with patch('src.model.xgboost_trainer.xgb.train') as mock_train:
            mock_model = Mock()
            mock_model.best_iteration = 15
            mock_model.best_score = 0.85
            mock_model.get_score.return_value = {
                'age': 100, 'income': 80, 'tenure': 60, 'balance': 40
            }
            mock_train.return_value = mock_model

            with tracker.start_run(run_name='integration_test_run'):
                model = trainer.train(
                    dtrain=dtrain,
                    dtest=dtest,
                    num_boost_round=training_config["model"]["xgboost"]["n_estimators"],
                    early_stopping_rounds=training_config["model"]["xgboost"]["early_stopping_rounds"]
                )
                assert model == mock_model
                assert trainer.model == mock_model

                mock_tracker_mlflow.log_params.assert_called()
                with patch.object(trainer.model, 'predict', return_value=np.array([0.2, 0.8])):
                    predictions = trainer.predict(sample_training_data.head(2))
                    assert len(predictions) == 2
        
    @patch('src.mlflow_utils.experiment_tracker.mlflow')
    