#!/bin/bash
set -e

echo "=== [1/3] Superset DB migration ==="
superset db upgrade

echo "=== [2/3] Create admin user ==="
superset fab create-admin \
    --username "${SUPERSET_ADMIN_USERNAME:-admin}" \
    --firstname "Admin" \
    --lastname "User" \
    --email "${SUPERSET_ADMIN_EMAIL:-admin@superset.com}" \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin}" 2>/dev/null || true

superset init
echo "=== [3/3] Init complete. Starting server... ==="

# ---------------------------------------------------------------------------
# Them Trino datasource sau khi Gunicorn da san sang (chay background)
# ---------------------------------------------------------------------------
(
    echo "[datasource] Waiting for Superset server..."
    for i in $(seq 1 40); do
        if curl -sf http://localhost:8088/health > /dev/null 2>&1; then
            echo "[datasource] Server ready."
            break
        fi
        sleep 3
    done

    python3 - <<'PYEOF'
import os, time, requests

BASE  = "http://localhost:8088"
USER  = os.environ.get("SUPERSET_ADMIN_USERNAME", "admin")
PWD   = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")

session = requests.Session()
resp = session.post(f"{BASE}/api/v1/security/login", json={
    "username": USER, "password": PWD,
    "provider": "db", "refresh": True,
})
token = resp.json().get("access_token")
if not token:
    print("[datasource] WARN: login failed, skip datasource setup.")
    exit(0)

headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Kiem tra da co chua
dbs = session.get(f"{BASE}/api/v1/database/?q=(filters:!((col:database_name,opr:DatabaseStartsWith,val:'Lakehouse')))",
                  headers=headers).json()
if dbs.get("count", 0) > 0:
    print("[datasource] Trino datasource already exists.")
    exit(0)

payload = {
    "database_name": "Lakehouse (Trino)",
    "sqlalchemy_uri": "trino://trino@trino:8080/iceberg",
    "expose_in_sqllab": True,
    "allow_run_async": False,
    "extra": '{"engine_params":{"connect_args":{"http_scheme":"http"}}}',
}
r = session.post(f"{BASE}/api/v1/database/", json=payload, headers=headers)
if r.status_code in (200, 201):
    print("[datasource] Trino datasource added OK.")
else:
    print(f"[datasource] WARN: {r.status_code} {r.text[:200]}")
PYEOF
) &

# ---------------------------------------------------------------------------
# Gunicorn chay foreground (PID 1 cua container)
# ---------------------------------------------------------------------------
exec gunicorn \
    --bind "0.0.0.0:8088" \
    --workers 2 \
    --worker-class gthread \
    --threads 2 \
    --timeout 120 \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    "superset.app:create_app()"
