import pytest
import pandas as pd
import numpy as np


@pytest.fixture
def training_config():
    return {
        "mlflow": {
            "tracking_uri": "http://localhost:5000",
            "experiment_name": "test_integration_experiment",
        },
        "model": {
            "train_test_split": 0.2,
            "xgboost": {
                "n_estimators": 10,
                "early_stopping_rounds": 5,
                "max_depth": 3,
                "learning_rate": 0.1,
                "objective": "binary:logistic",
                "eval_metric": "logloss",
            },
        },
        "features": {
            "target_column": "churn",
            "training_features": ["age", "income", "tenure", "balance"],
        },
    }


@pytest.fixture
def sample_training_data():
    np.random.seed(42)
    n = 100
    return pd.DataFrame(
        {
            "age": np.random.randint(18, 70, n).astype(float),
            "income": np.random.uniform(20000, 100000, n),
            "tenure": np.random.randint(0, 120, n).astype(float),
            "balance": np.random.uniform(0, 50000, n),
            "churn": np.random.randint(0, 2, n),
        }
    )


@pytest.fixture
def small_real_dataset():
    np.random.seed(42)
    n = 50
    return pd.DataFrame(
        {
            "age": np.random.randint(18, 70, n).astype(float),
            "tenure_months": np.random.randint(0, 120, n).astype(float),
            "monthly_charges": np.random.uniform(20, 100, n),
            "total_charges": np.random.uniform(0, 5000, n),
            "churn": np.random.randint(0, 2, n),
        }
    )


@pytest.fixture
def app_configs():
    return {
        "baseline": {
            "max_depth": 3,
            "learning_rate": 0.1,
            "n_estimators": 10,
            "early_stopping_rounds": 5,
            "objective": "binary:logistic",
            "eval_metric": "logloss",
        },
        "model_configs": {
            "xgboost": {
                "max_depth": 3,
                "learning_rate": 0.1,
            }
        },
    }
