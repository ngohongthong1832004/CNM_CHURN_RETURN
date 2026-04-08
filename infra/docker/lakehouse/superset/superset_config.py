import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "aio2025-superset-secret-key-changeme")

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "sqlite:////app/superset_home/superset.db?check_same_thread=false",
)

if SQLALCHEMY_DATABASE_URI.startswith("sqlite:"):
    SQLALCHEMY_ENGINE_OPTIONS = {
        "connect_args": {"check_same_thread": False},
    }

# Tắt CSRF cho môi trường dev
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False

# Tăng timeout cho Trino query
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300

# Tắt Celery async — dùng sync execution
SQLLAB_ASYNC_TIME_LIMIT_SEC = 0
GLOBAL_ASYNC_QUERIES_TRANSPORT = "polling"
RESULTS_BACKEND = None

# Cho phép hiển thị iframe (optional)
HTTP_HEADERS = {}
