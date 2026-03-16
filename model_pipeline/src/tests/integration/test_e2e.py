"""
End-to-End Test: Full MLOps Cycle with Real Data and Real MLflow
This test runs the complete workflow without mocks on actual small dataset

Test Workflow:
1. Load real (small) dataset
2. Train baseline model
3. Train challenger models with different hyperparameters
4. Evaluate all models
5. Compare models
6. Register best model
7. Promote to staging
8. Compare staging vs champion
9. Promote to champion if better
10. Verify all artifacts and metrics are logged

NOTE: This test requires a running MLflow server
Run with: pytest tests/e2e/test_e2e_full_cycle.py -v -s
"""

import pytest
import pandas as pd 
import numpy as np
import mlflow 
import time
from pathlib import Path
import tempfile

from src.mlflow_utils.experiment_tracker import ExperimentTracker
from src.mlflow_utils.model_registry import ModelRegistry
from src.model.xgboost_trainer import XGBoostTrainer
from src.model.evaluator import ModelEvaluator

pytestmark = [pytest.mark.e2e, pytest.mark.slow, pytest.mark.requires_mlflow]

@pytest.fixture(scope="session")
def shared_state():
    return {}



@pytest.fixture(scope='module')
def mlflow_config():
    # load up mlflow config

    ...

class TestFullMlopsCycle:
    

    def test_02_train_baseline(
        self,
        mlflow_config,
        small_real_dataset,
        app_configs
    ):
        print("\n" + "="*60)
        print("TEST 2: Training baseline model")
        print("="*60)

        model_configs = app_configs['model_configs']

        tracker = ExperimentTracker(
            tracking_uri=mlflow_config["tracking_uri"],
            experiment_name=mlflow_config["experiment_name"]
        )
        config = {
            "xgboost": app_configs["baseline"],
            "train_test_split": 0.2,
            "random_state": 42
        }

        trainer = XGBoostTrainer(config=config, experiment_tracker=tracker)

        with tracker.start_run(
            run_name="baseline_model",
            tags={"model_type": "baseline"}
        ) as run:
            dtrain, dtest, y_train, y_test = trainer.prepare_data(
                data=small_real_dataset,
                target_col='churn',
                feature_cols=['age', 'tenure_months', 'monthly_charges', 'total_charges'],
                test_size=0.2,
                random_state=42
            )
            
            print(f"Data prepared: {len(y_train)} train, {len(y_test)} test sample")
            model = trainer.train(
                dtrain=dtrain,
                dtest=dtest,
                num_boost_round=,
                early_stopping_rounds=
            )

            print(f"Model trained: best_iteration={model.best_iteration}, best_score={model.best_score:.4f}")

            input_example = small_real_dataset[['age', 'tenure_months', 'monthly_charges', 'total_charges']].head(3)
            trainer.save_model(model_name="model", input_example=input_example)

            print(f"Model saved with run_id: {run.info.run_id=}")
            pytest.baseline_run_id = run.info.run_id
        
        runs = tracker




