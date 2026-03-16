import mlflow
import os


def load_model(model_uri: str = "runs:/c4b92406479d490993622563a35a47f7/xgboost_churn"):
    """
    Load model from MLflow
    
    Args:
        model_uri: MLflow model URI (default: latest model)
    
    Returns:
        Loaded MLflow model
    """
    MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI")

    # 1. Configure MLflow Tracking
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    print(MLFLOW_TRACKING_URI)
    
    
    # 3. Load model
    model = mlflow.pyfunc.load_model(model_uri)
    
    print("Model loaded successfully!")
    
    return model


if __name__ == "__main__":
    # Test loading model
    model = load_model("runs:/9cd85098d8fe463c963edae4c3a93280/xgboost_churn")
    
    # Get feature names
    custom_model_instance = model.unwrap_python_model()
    print("Feature names:", custom_model_instance.feature_names)
