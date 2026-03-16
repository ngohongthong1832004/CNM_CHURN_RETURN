"""
Docstring for model_pipeline.src.utility.helper
"""
import yaml

def load_config(config_path: str) -> dict:
    """Load configuration"""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)