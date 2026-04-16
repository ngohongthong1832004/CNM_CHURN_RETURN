"""
FastAPI main application
"""
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from routers import predict, health, monitor
import logging
import time
from prometheus_client import make_asgi_app
from metrics import HTTP_REQUESTS, HTTP_LATENCY
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def prometheus_middleware(request: Request, call_next):
    """Track HTTP request count and latency for every endpoint."""
    start = time.perf_counter()
    response: Response = await call_next(request)
    duration = time.perf_counter() - start

    path = request.url.path
    HTTP_REQUESTS.labels(
        method=request.method,
        path=path,
        status_code=response.status_code,
    ).inc()
    HTTP_LATENCY.labels(method=request.method, path=path).observe(duration)
    return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan event handler for startup and shutdown"""
    # Startup
    logger.info("Starting Customer Churn Prediction API...")
    logger.info("Model loaded successfully")
    logger.info("API ready to serve predictions")
    yield
    # Shutdown
    logger.info("Shutting down API...")


# Create FastAPI app
app = FastAPI(
    title="Customer Churn Prediction API",
    description="ML API for predicting customer churn using RandomForest model",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus middleware (runs after CORS)
app.middleware("http")(prometheus_middleware)

# Expose /metrics endpoint (scraped by Prometheus)
app.mount("/metrics", make_asgi_app())

# Include routers
app.include_router(predict.router)
app.include_router(health.router)
app.include_router(monitor.router)


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Customer Churn Prediction API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }