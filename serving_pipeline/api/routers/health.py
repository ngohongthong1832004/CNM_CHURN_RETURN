"""
Health check endpoints
"""
from fastapi import APIRouter, Query
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from schemas import HealthResponse
from datetime import datetime
from .predict import get_model

router = APIRouter(prefix="/health", tags=["Health"])


@router.get("/", response_model=HealthResponse)
async def health_check():
    """
    Check API health status
    """
    try:
        model = get_model()
        model_loaded = model is not None
    except Exception:
        model_loaded = False
    
    return HealthResponse(
        status="healthy",
        model_loaded=model_loaded,
        timestamp=datetime.now().isoformat()
    )


@router.get("/ready")
async def readiness_check():
    """
    Kubernetes readiness probe
    """
    try:
        model = get_model()
        if model is None:
            return {"ready": False, "reason": "Model not loaded"}
        return {"ready": True}
    except Exception as e:
        return {"ready": False, "reason": f"Model loading failed: {str(e)}"}


@router.get("/live")
async def liveness_check():
    """
    Kubernetes liveness probe
    """
    return {"alive": True}


@router.get("/sample-customers")
async def sample_customers(n: int = Query(10, ge=1, le=100)):
    """
    Return n sample customer_ids from the feature store parquet.
    Use these IDs with POST /predict/by-customer-id.
    """
    import pandas as pd

    feast_repo = os.getenv(
        "FEAST_REPO_PATH",
        "/app/churn_feature_store/churn_features/feature_repo",
    )
    parquet_path = os.path.join(feast_repo, "data", "processed_churn_data.parquet")

    if not os.path.exists(parquet_path):
        return {"error": f"Parquet not found at {parquet_path}. Run lakehouse_etl DAG first.", "customer_ids": []}

    try:
        df = pd.read_parquet(parquet_path, columns=["customer_id"])
        ids = df["customer_id"].dropna().astype(str).unique().tolist()
        return {
            "total_in_parquet": len(ids),
            "sample_customer_ids": ids[:n],
        }
    except Exception as e:
        return {"error": str(e), "customer_ids": []}