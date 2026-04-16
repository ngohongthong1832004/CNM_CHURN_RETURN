"""
Prometheus metric definitions for the Churn Prediction API.

HTTP metrics  — tracked by PrometheusMiddleware in main.py
Business metrics — recorded directly in predict.py routers
"""
from prometheus_client import Counter, Histogram

# ── HTTP metrics ───────────────────────────────────────────────────────────────
HTTP_REQUESTS = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status_code"],
)

HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# ── Business metrics ───────────────────────────────────────────────────────────
PREDICTIONS = Counter(
    "churn_predictions_total",
    "Total churn predictions",
    # endpoint: single | batch | customer_id
    # result:   churn  | no_churn
    ["endpoint", "result"],
)

BATCH_SIZE = Histogram(
    "churn_batch_prediction_size",
    "Number of records per batch prediction request",
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000),
)

FEAST_LOOKUPS = Counter(
    "feast_feature_lookups_total",
    "Feast online feature store lookups",
    ["status"],  # success | error
)
