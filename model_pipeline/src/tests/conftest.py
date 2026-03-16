"""
Docstring for model_pipeline.src.tests.conftest
Shared pytest Fixtures and configuration for all tests
"""

import pytest
import pandas as pd
import numpy as np
import tempfile
from pathlib import Path
import yaml
import os


@pytest.fixture(scope='session')
def test_data_dir():
    """
    Create temp folder for test data
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)

@pytest.fixture
def sample_config_file():
    sample_config_path = Path(__file__).parent / "fixtures" / "sample_config.yaml"
    with open(sample_config_path) as f:
        return yaml.safe_load(f)


@pytest.fixture
def mock_mlflow_client():
    from unittest.mock import Mock

    client = Mock()
    


    return client


@pytest.fixture(autouse=True)
def reset_mlflow():
    """
    Docstring for reset_mlflow
    Reset mlflow state between run
    """
    import mlflow
    try:
        mlflow.end_run()
    except:
        pass

    yield
    try:
        mlflow.end_run()
    except:
        pass

@pytest.fixture(scope='session', autouse=True)
def setup_test_env():
    original_env = os.environ.copy()
    
    # Set test environment variables
    os.environ["AWS_ACCESS_KEY_ID"] = "test_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test_secret"
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"
    os.environ["MLFLOW_TRACKING_URI"] = "http://localhost:5000"
    
    yield
    
    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)
    



