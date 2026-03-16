"""
Health check endpoints
"""
from fastapi import APIRouter
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from api.schemas import HealthResponse
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