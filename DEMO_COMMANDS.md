# 🎯 Demo Commands Quick Reference

Dán các lệnh này vào terminal/browser khi quay video.

---

## 🔍 Scene 2: Data Flow - Bronze/Silver/Gold Query

### Trino SQL (run from Superset SQL Lab or Trino CLI)

**Bronze count:**
```sql
SELECT COUNT(*) as raw_records, COUNT(DISTINCT customer_id) as unique_customers
FROM iceberg.bronze.customer_events;
```

**Silver count:**
```sql
SELECT COUNT(*) as cleaned_records, COUNT(DISTINCT customer_id) as unique_customers
FROM iceberg.silver.customers;
```

**Gold count:**
```sql
SELECT COUNT(*) as feature_ready_records, COUNT(DISTINCT customer_id) as unique_customers
FROM iceberg.gold.churn_features;
```

**Sample Gold record (with new dashboard features):**
```sql
SELECT 
  customer_id,
  age,
  gender,
  tenure_months,
  subscription_type,
  total_spend,
  support_calls,
  churned,
  -- New dashboard features:
  tenure_age_ratio,
  spend_per_usage,
  support_calls_per_tenure,
  spending_group,
  tenure_group
FROM iceberg.gold.churn_features
LIMIT 5;
```

---

## 🤖 Scene 4: Feature Views

### Check Feast features (Terminal)

```bash
cd data-pipeline/churn_feature_store/churn_features/feature_repo

# Show feature_views.py (first 50 lines to see all feature groups)
head -50 feature_views.py

# Or grep specific feature views
grep "name=" feature_views.py

# Show online store config
cat feature_store.yaml
```

---

## 🎯 Scene 6: Health Check

### Terminal (PowerShell)

```bash
curl -s http://localhost:8000/health/ | ConvertFrom-Json | ConvertTo-Json
```

**Expected output:**
```json
{
  "status": "healthy",
  "model_uri": "models:/customer_churn_model@champion",
  "predictions_count": 42,
  "feast_status": "connected"
}
```

---

## 🔮 Scene 7: API Predict by Customer ID

### Terminal (PowerShell)

#### Option 1: Simple curl
```bash
curl -X POST http://localhost:8000/predict/by-customer-id `
  -H "Content-Type: application/json" `
  -d '{"customer_id": "1776312653"}' | ConvertFrom-Json | ConvertTo-Json
```

#### Option 2: Store in variable, pretty print
```powershell
$result = curl -s -X POST http://localhost:8000/predict/by-customer-id `
  -H "Content-Type: application/json" `
  -d '{"customer_id": "1776312653"}' | ConvertFrom-Json

Write-Host "Prediction: $($result.churn)"
Write-Host "Confidence: $($result.probability)"
```

**Expected output:**
```json
{
  "customer_id": "1776312653",
  "churn": 0,
  "probability": 0.15
}
```

---

## 📊 Scene 9: Superset Dashboard Queries

### Query 1: Churn Rate by Spending Group

```sql
SELECT 
  spending_group,
  COUNT(*) as total_customers,
  SUM(CAST(churned AS INTEGER)) as churn_count,
  ROUND(100.0 * SUM(CAST(churned AS INTEGER)) / COUNT(*), 2) as churn_rate_pct
FROM iceberg.gold.churn_features
GROUP BY spending_group
ORDER BY spending_group;
```

**Insight:** High spending groups should have lower churn rate

### Query 2: Average Spend by Tenure Group

```sql
SELECT 
  tenure_group,
  COUNT(*) as customer_count,
  ROUND(AVG(total_spend), 2) as avg_total_spend,
  ROUND(AVG(usage_frequency), 2) as avg_usage_freq
FROM iceberg.gold.churn_features
GROUP BY tenure_group
ORDER BY tenure_group;
```

**Insight:** Longer tenure = higher spend + loyalty

### Query 3: Support Calls as Churn Indicator

```sql
SELECT 
  CASE WHEN churned = 1 THEN 'Churned' ELSE 'Retained' END as status,
  COUNT(*) as customer_count,
  ROUND(AVG(support_calls), 2) as avg_support_calls,
  ROUND(AVG(support_calls_per_tenure), 3) as avg_support_calls_per_tenure,
  ROUND(AVG(payment_delay_days), 2) as avg_payment_delay
FROM iceberg.gold.churn_features
GROUP BY churned
ORDER BY churned DESC;
```

**Insight:** Churned customers have fewer support calls (may indicate disengagement)

### Query 4: Feature Distribution by Churn

```sql
SELECT 
  CASE WHEN churned = 1 THEN 'Churned' ELSE 'Retained' END as status,
  subscription_type,
  contract_length,
  COUNT(*) as count,
  ROUND(AVG(total_spend), 2) as avg_spend
FROM iceberg.gold.churn_features
GROUP BY churned, subscription_type, contract_length
ORDER BY churned DESC, count DESC
LIMIT 10;
```

**Insight:** Identify high-risk segments (e.g., Basic subscription + Monthly contract)

---

## 🎚️ Scene 8: Gradio UI Manual Prediction

### Input values for manual prediction (copy one of these):

**Customer Profile 1: High Risk (likely to churn)**
```
Age: 45
Tenure (months): 12
Gender: Male
Usage Frequency: 4
Support Calls: 6
Payment Delay: 20
Subscription Type: Basic
Contract Length: Monthly
Total Spend: 300.50
Last Interaction: 25
```

**Customer Profile 2: Low Risk (likely to retain)**
```
Age: 28
Tenure (months): 36
Gender: Female
Usage Frequency: 18
Support Calls: 1
Payment Delay: 2
Subscription Type: Premium
Contract Length: Annual
Total Spend: 850.00
Last Interaction: 5
```

**Customer Profile 3: Medium Risk**
```
Age: 52
Tenure (months): 24
Gender: Male
Usage Frequency: 10
Support Calls: 3
Payment Delay: 8
Subscription Type: Standard
Contract Length: Semi-Annual
Total Spend: 600.00
Last Interaction: 15
```

---

## 📈 Scene 10: Grafana Metrics

### URLs to navigate to

1. **FastAPI Overview Dashboard**
   - http://localhost:3000/d/fastapi-overview

2. **Airflow Overview Dashboard**
   - http://localhost:3000/d/airflow-overview

3. **Infrastructure Overview**
   - http://localhost:3000/d/infrastructure

### Key metrics to highlight

- **FastAPI:**
  - Request rate (requests/sec)
  - Latency (p50, p95, p99)
  - Error rate (5xx, 4xx)
  
- **Airflow:**
  - DAG run count (success/failed)
  - Task duration histogram
  
- **Infrastructure:**
  - CPU usage
  - Memory usage
  - Disk I/O

---

## 📋 Scene 11: Airflow DAGs

### URLs

1. **All DAGs:** http://localhost:8080/dags
2. **data_simulator:** http://localhost:8080/dags/data_simulator
3. **lakehouse_etl:** http://localhost:8080/dags/lakehouse_etl
4. **churn_feature_pipeline:** http://localhost:8080/dags/churn_feature_pipeline
5. **churn_retraining_pipeline:** http://localhost:8080/dags/churn_retraining_pipeline

### Terminal: Check last DAG run status

```bash
# Using curl + Airflow API (if you have token)
$AIRFLOW_URL = "http://localhost:8080"
$USER = "airflow"
$PASS = "airflow"

# Get version (health check)
curl -s "$AIRFLOW_URL/api/v2/version" | ConvertFrom-Json | ConvertTo-Json

# List recent DAG runs (requires auth token, skip if not set up)
```

### Docker logs: Check DAG task execution

```bash
# Check lakehouse_etl last run
docker logs aio_airflow_scheduler --tail 100 | findstr "lakehouse_etl"

# Check serving API logs (to see predictions being made)
docker logs aio_serving_api --tail 50
```

---

## 🔐 Scene 5: MLflow Model Registry

### URLs

1. **Models list:** http://localhost:5000/#/models
2. **Customer Churn Model:** http://localhost:5000/#/models/customer_churn_model
3. **Champion version:** Click on version tag `champion`

### Key info to show

- **Model name:** `customer_churn_model`
- **Champion alias:** Shows latest champion version
- **Metrics:** F1-score, accuracy, precision, recall
- **Parameters:** Model hyperparameters (max_depth, learning_rate, etc.)
- **Artifacts:** Input features, model pickle, requirements.txt

---

## 🌐 Browser URLs Reference

| Service | URL | Login |
|---------|-----|-------|
| Airflow | http://localhost:8080 | airflow / airflow |
| MLflow | http://localhost:5000 | - |
| Gradio | http://localhost:7860 | - |
| Superset | http://localhost:8088 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| Trino | http://localhost:8090 | - |
| Nessie | http://localhost:19120 | - |
| MinIO | http://localhost:9001 | minio / minio123 |

---

## ⚡ Quick Copy-Paste Commands

### Health check all services
```bash
# Airflow
curl -s http://localhost:8080/api/v2/version | jq '.version'

# MLflow
curl -s http://localhost:5000/health | jq '.'

# Serving API
curl -s http://localhost:8000/health/ | jq '.'

# Superset
curl -s http://localhost:8088/api/v1/security/login -d '{"username":"admin","password":"admin"}' | jq '.access_token'
```

### Check data volumes
```bash
# From terminal, run these Trino queries one by one

# Total records in Gold table
docker exec aio_trino trino --server localhost:8080 -u trino -c iceberg `
  -e "SELECT COUNT(*) FROM iceberg.gold.churn_features;"

# Unique customers
docker exec aio_trino trino --server localhost:8080 -u trino -c iceberg `
  -e "SELECT COUNT(DISTINCT customer_id) FROM iceberg.gold.churn_features;"
```

### Check Feast online store (Redis)
```bash
# Connect to Redis
docker exec aio_redis redis-cli

# Inside redis-cli:
KEYS "*" | head -20
DBSIZE
QUIT
```

---

## 💡 Tips for Smooth Demo

1. **Pre-load all browser tabs** before recording
2. **Disable notifications** on your OS
3. **Zoom in on important numbers** (use zoom software or browser zoom)
4. **Pause 2 sec after each query result** to let viewers read
5. **Speak slowly** and explain each metric
6. **Have alternate data** if queries don't return expected results
7. **Record audio separate** if possible (better quality control)

---

## 🆘 Troubleshooting During Demo

| Issue | Fix |
|-------|-----|
| **API returning 404** | Check if serving container is running: `docker logs aio_serving_api` |
| **Trino query fails** | Verify tables exist: `docker exec aio_trino trino -c iceberg -e "SHOW TABLES FROM iceberg.gold;"` |
| **Gradio page blank** | Check if serving container loaded model: `curl http://localhost:8000/health/` |
| **Superset can't query Trino** | Re-add datasource: Superset UI → Settings → Database → Add Lakehouse (Trino) |
| **MLflow model not found** | Ensure churn_retraining_pipeline has run: Check Airflow DAG history |
| **Metrics not showing in Grafana** | Wait 1 min for Prometheus scrape interval |

---

**Good luck! 🎬 Let me know if you need any adjustments to the script.**
