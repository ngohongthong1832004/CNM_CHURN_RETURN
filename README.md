# MLOps End-to-End: Customer Churn Prediction System

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![MLflow](https://img.shields.io/badge/MLflow-3.x-orange.svg)](https://mlflow.org/)
[![Airflow](https://img.shields.io/badge/Airflow-3.1.1-017CEE.svg)](https://airflow.apache.org/)
[![Feast](https://img.shields.io/badge/Feast-0.58-red.svg)](https://feast.dev/)
[![Kafka](https://img.shields.io/badge/Kafka-KRaft-231F20.svg)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)

He thong MLOps end-to-end du doan customer churn, bao gom toan bo vong doi: sinh du lieu, lakehouse, feature store, huan luyen lai mo hinh va serving.

---

## Architecture

```
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                          DATA INGESTION                                     │
 │                                                                             │
 │   [DAG: data_simulator]  schedule: 23:45 UTC daily                         │
 │                                                                             │
 │   simulate.py                                                               │
 │   (sinh ~5 000 records/ngay)  ──►  Kafka                                   │
 │                                    topic: churn.raw.events                 │
 │                                    (3-node KRaft cluster)                  │
 └────────────────────────────────────┬────────────────────────────────────────┘
                                      │ kafka_to_bronze
                                      ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                          LAKEHOUSE  (Apache Iceberg + Nessie + MinIO)       │
 │                                                                             │
 │   [DAG: lakehouse_etl]  schedule: 00:00 UTC daily                          │
 │                                                                             │
 │   Bronze                  Silver                  Gold                     │
 │   bronze.customer_events  silver.customers         gold.churn_features      │
 │   (raw append-only)  ──►  (dedup + validate)  ──►  (feature engineering)   │
 │                                                         │                  │
 │                                              ┌──────────┴──────────┐       │
 │                                              ▼                     ▼       │
 │                                    export parquet           Trino query    │
 │                                    processed_churn_         (Superset      │
 │                                    data.parquet             dashboards)    │
 └──────────────────────────────────────────────┬──────────────────────────────┘
                                                │ TriggerDagRunOperator
          ┌─────────────────────────────────────┴─────────────────────────────┐
          │                                                                   │
          ▼                                                                   ▼
 ┌────────────────────────┐                               ┌───────────────────────────┐
 │  FEATURE STORE         │                               │  MODEL PIPELINE           │
 │                        │                               │                           │
 │  [DAG: churn_feature_  │                               │  [DAG: churn_retraining_  │
 │   pipeline]            │                               │   pipeline]               │
 │  schedule: 00:30 daily │                               │  schedule: Sunday 00:00   │
 │                        │                               │  (+ manual trigger)       │
 │  feast apply           │                               │                           │
 │  feast materialize ──► Redis                           │  train (parallel):        │
 │  (online store)        │                               │  ├─ logistic_regression   │
 │                        │                               │  ├─ decision_tree         │
 └────────────────────────┘                               │  ├─ random_forest         │
                                                          │  ├─ xgboost               │
                                                          │  ├─ lightgbm              │
                                                          │  └─ catboost              │
                                                          │        │                  │
                                                          │        ▼                  │
                                                          │  find_best_model (F1)     │
                                                          │        │                  │
                                                          │        ▼                  │
                                                          │  evaluate_best_model      │
                                                          │        │                  │
                                                          │        ▼                  │
                                                          │  register_champion ──► MLflow Registry
                                                          │  models:/customer_churn_model@champion
                                                          └───────────────────────────┘

 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                          SERVING PIPELINE                                   │
 │                                                                             │
 │   Gradio UI  (port 7860)                                                   │
 │       │                                                                     │
 │       ├──► POST /predict/          (full feature payload)                  │
 │       │         │                                                           │
 │       │         ▼                                                           │
 │       │      FastAPI (port 8000)                                            │
 │       │         │                                                           │
 │       │         ▼                                                           │
 │       │    Champion model  ──►  { "churn": 0 | 1 }                         │
 │       │   (MLflow Registry)                                                 │
 │       │                                                                     │
 │       ├──► POST /predict/by-customer-id   (customer_id only)               │
 │       │         │                                                           │
 │       │         ▼                                                           │
 │       │      FastAPI ──► Feast online store ──► Redis                      │
 │       │         │         (feature lookup by customer_id)                  │
 │       │         ▼                                                           │
 │       │    Champion model  ──►  { "churn": 0 | 1 }                         │
 │       │                                                                     │
 │       └──► POST /predict/batch    (up to 1000 records via CSV/JSON)        │
 │                 │                                                           │
 │                 ▼                                                           │
 │            FastAPI  ──►  [{ "churn": 0|1 }, ...]                           │
 │                                                                             │
 │   /metrics  ──► Prometheus ──► Grafana  (request count, latency, Feast     │
 │                                          lookup success/error rate)        │
 │   /monitor/drift  ──► Evidently drift report (production vs baseline)      │
 └─────────────────────────────────────────────────────────────────────────────┘

 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                          MONITORING STACK                                   │
 │                                                                             │
 │   node_exporter  ──► Prometheus ──► Grafana                                │
 │   statsd-exporter (Airflow metrics via UDP 8125)                           │
 │   Promtail ──► Loki ──► Grafana (log aggregation)                         │
 │                                                                             │
 │   Dashboards:                                                               │
 │   ├─ Infrastructure Overview  (CPU, RAM, disk, network)                    │
 │   ├─ Observability Overview   (FastAPI request rate, latency, error rate)  │
 │   ├─ Airflow Overview         (DAG run count, scheduler heartbeat)         │
 │   └─ FastAPI Overview         (per-endpoint metrics)                       │
 └─────────────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```text
aio2025-mlops-project01/
├── data-simulator/
│   └── simulate.py               # Sinh du lieu churn, publish len Kafka
│
├── data-pipeline/
│   └── churn_feature_store/
│       └── churn_features/
│           └── feature_repo/
│               ├── feature_store.yaml   # Feast config (Redis online store)
│               ├── churn_entities.py
│               ├── churn_features.py
│               └── data/
│                   └── processed_churn_data.parquet  # Output cua lakehouse_etl
│
├── model_pipeline/
│   └── src/
│       ├── config/               # YAML config cho tung model
│       │   ├── catboost.yaml
│       │   ├── lightgbm.yaml
│       │   ├── xgboost.yaml
│       │   ├── random_forest.yaml
│       │   ├── decision_tree.yaml
│       │   └── logistic_regression.yaml
│       ├── model/
│       │   └── xgboost_trainer.py  # GenericBinaryClassifierTrainer (6 models)
│       ├── mlflow_utils/
│       │   └── experiment_tracker.py
│       └── scripts/
│           ├── train.py
│           └── eval.py
│
├── serving_pipeline/
│   ├── api/
│   │   ├── main.py               # FastAPI app + Prometheus middleware
│   │   ├── routers/
│   │   │   ├── predict.py        # /predict/, /predict/by-customer-id, /predict/batch
│   │   │   ├── health.py         # /health/
│   │   │   └── monitor.py        # /monitor/drift
│   │   ├── schemas.py
│   │   └── metrics.py            # Prometheus counters/histograms
│   ├── ui.py                     # Gradio UI
│   ├── load_model.py
│   ├── sample_retrieval.py       # Feast online feature lookup
│   ├── pre_processing.py
│   ├── Dockerfile
│   ├── Dockerfile.ui
│   ├── docker-compose.yml
│   ├── .env.example
│   └── test_batch.csv            # 20-row CSV de test /predict/batch
│
├── infra/
│   └── docker/
│       ├── mlflow/               # MLflow server + MySQL + MinIO
│       ├── kafka/                # 3-node Kafka KRaft cluster
│       ├── lakehouse/            # Nessie + Trino + Superset
│       ├── airflow/              # Airflow 3.1.1 CeleryExecutor + DAGs
│       │   ├── dags/
│       │   │   ├── data_simulator_dag.py
│       │   │   ├── lakehouse_etl_dag.py
│       │   │   ├── churn_feature_pipeline_dag.py
│       │   │   └── churn_retraining_dag.py
│       │   └── requirements.txt  # feast==0.58.0, mlflow>=2.16.0, numpy>=2.0.0
│       └── monitor/              # Prometheus + Grafana + Loki + StatsD + Promtail
│
├── start.ps1                     # One-command full system startup
├── cleanup.ps1                   # Full teardown + volume/image cleanup
└── workflow.md                   # Chi tiet luong chay thuc te
```

---

## Prerequisites

| Requirement | Version |
|---|---|
| Docker Desktop | 20.10+ (Docker Compose v2) |
| Python | 3.11 |
| RAM | 16 GB khuyen nghi |
| Disk | 20 GB+ free |
| OS | Windows 10/11 (PowerShell 7+) |

Tao Docker network dung chung (chi can chay 1 lan):

```powershell
docker network create aio-network
```

---

## Quick Start

```powershell
git clone https://github.com/ThuanNaN/aio2025-mlops-project01.git
cd aio2025-mlops-project01

docker network create aio-network

.\start.ps1
```

`start.ps1` chay 4 phase:

| Phase | Mo ta | Thoi gian uoc tinh |
|---|---|---|
| **1 - Infrastructure** | Build Airflow image, start tat ca Docker stacks, cho services san sang | 3-5 phut |
| **2 - Data Pipeline** | `data_simulator` → `lakehouse_etl` → `churn_feature_pipeline` | 10-15 phut |
| **3 - Model Pipeline** | Train 6 models song song, chon best F1, register champion | 10-30 phut |
| **4 - Serving** | Start FastAPI + Gradio UI | 1-2 phut |

**Fresh restart (xoa het va chay lai):**

```powershell
.\cleanup.ps1 -RemoveImages -Force
docker network create aio-network
.\start.ps1
```

**Skip cac phase da hoan thanh:**

```powershell
# Chi restart serving voi model hien tai
.\start.ps1 -SkipInfra -SkipData -SkipModel

# Chi chay lai tu Data Pipeline tro di
.\start.ps1 -SkipInfra
```

---

## Airflow DAGs

### DAG 1: `data_simulator`

**Schedule:** 23:45 UTC hang ngay

```
simulate_to_kafka  ──►  kafka_to_bronze
```

| Task | Mo ta |
|---|---|
| `simulate_to_kafka` | Sinh ~5 000 records khach hang, publish JSON vao Kafka topic `churn.raw.events` |
| `kafka_to_bronze` | Consume tu Kafka → parse → append vao Iceberg table `bronze.customer_events` |

---

### DAG 2: `lakehouse_etl`

**Schedule:** 00:00 UTC hang ngay

```
init_namespaces  ──►  bronze_to_silver  ──►  silver_to_gold  ──►  export_gold_parquet
                                                                           │
                                                              TriggerDagRunOperator
                                                                           │
                                                                           ▼
                                                              churn_feature_pipeline
```

| Task | Mo ta |
|---|---|
| `init_namespaces` | Tao Iceberg namespaces (bronze, silver, gold) tren Nessie neu chua co |
| `bronze_to_silver` | Dedup, validate, chuan hoa kieu du lieu → `silver.customers` |
| `silver_to_gold` | Feature engineering (tinh age_group, tenure_group, ...) → `gold.churn_features` |
| `export_gold_parquet` | Export Gold table ra `processed_churn_data.parquet` |

---

### DAG 3: `churn_feature_pipeline`

**Schedule:** 00:30 UTC hang ngay (hoac tu dong trigger boi `lakehouse_etl`)

```
feast_apply  ──►  feast_materialize_incremental
```

| Task | Mo ta |
|---|---|
| `feast_apply` | Ap dung schema Feast (entities + feature views) |
| `feast_materialize_incremental` | Load parquet moi nhat vao Redis online store theo `customer_id` |

---

### DAG 4: `churn_retraining_pipeline`

**Schedule:** 00:00 UTC Chu Nhat hang tuan (hoac trigger thu cong)

```
train_logistic_regression ─┐
train_decision_tree        │
train_random_forest        ├──►  find_best_model  ──►  evaluate_best_model  ──►  register_champion
train_xgboost              │
train_lightgbm             │
train_catboost             ─┘
```

| Task | Mo ta |
|---|---|
| `train_*` (x6) | Train 6 models song song, log metrics vao MLflow |
| `find_best_model` | Query MLflow lay run co `training_f1_score` cao nhat trong 3 gio qua |
| `evaluate_best_model` | Chay `eval.py` voi threshold validation (accuracy ≥ 0.8, F1 ≥ 0.8) |
| `register_champion` | Register model vao MLflow Registry, gan alias `champion` |

**Models duoc train:**

| Model | Config |
|---|---|
| Logistic Regression | `src/config/logistic_regression.yaml` |
| Decision Tree | `src/config/decision_tree.yaml` |
| Random Forest | `src/config/random_forest.yaml` |
| XGBoost | `src/config/xgboost.yaml` |
| LightGBM | `src/config/lightgbm.yaml` |
| CatBoost | `src/config/catboost.yaml` |

Model champion duoc serving qua URI: `models:/customer_churn_model@champion`

---

## Serving Pipeline

### API Endpoints

| Method | Endpoint | Mo ta |
|---|---|---|
| `GET` | `/health/` | Health check + model status |
| `POST` | `/predict/` | Predict tu full feature payload |
| `POST` | `/predict/by-customer-id` | Predict tu customer_id (lookup Feast/Redis) |
| `POST` | `/predict/batch` | Batch predict (toi da 1000 records) |
| `GET` | `/monitor/drift` | Evidently drift report (production vs baseline) |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/docs` | Swagger UI |

### Predict voi full payload

```bash
curl -X POST http://localhost:8000/predict/ \
  -H "Content-Type: application/json" \
  -d '{
    "Age": 35,
    "Gender": "Male",
    "Tenure": 24,
    "Usage_Frequency": 15,
    "Support_Calls": 2,
    "Payment_Delay": 5,
    "Subscription_Type": "Premium",
    "Contract_Length": "Annual",
    "Total_Spend": 500.0,
    "Last_Interaction": 10
  }'
```

Response:
```json
{ "churn": 0 }
```

### Predict by customer_id

```bash
curl -X POST http://localhost:8000/predict/by-customer-id \
  -H "Content-Type: application/json" \
  -d '{ "customer_id": "1776312653" }'
```

Yeu cau: `churn_feature_pipeline` phai chay truoc de Feast da materialize features vao Redis.

### Batch Predict

```bash
curl -X POST http://localhost:8000/predict/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"Age":45,"Gender":"Male","Tenure":12,"Usage_Frequency":4,"Support_Calls":6,"Payment_Delay":20,"Subscription_Type":"Basic","Contract_Length":"Monthly","Total_Spend":300.5,"Last_Interaction":25},
    {"Age":28,"Gender":"Female","Tenure":36,"Usage_Frequency":18,"Support_Calls":1,"Payment_Delay":2,"Subscription_Type":"Premium","Contract_Length":"Annual","Total_Spend":850.0,"Last_Interaction":5}
  ]'
```

File CSV mau de test: `serving_pipeline/test_batch.csv` (20 records, da cover du cac profile khach hang).

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| **Serving API** | http://localhost:8000/docs | - |
| **Gradio UI** | http://localhost:7860 | - |
| **MLflow** | http://localhost:5000 | - |
| **Airflow** | http://localhost:8080 | airflow / airflow |
| **MinIO Console** | http://localhost:9001 | minio / minio123 |
| **Nessie** | http://localhost:19120 | - |
| **Trino** | http://localhost:8090 | - |
| **Superset** | http://localhost:8088 | admin / admin |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | - |

---

## Infrastructure Stacks

| Stack | Services | Ports |
|---|---|---|
| `mlflow/` | MLflow Server, MySQL, MinIO | 5000, 3306, 9000, 9001 |
| `kafka/` | broker-1, broker-2, broker-3 (KRaft) | 9092, 9192, 9292 |
| `lakehouse/` | Nessie (Iceberg catalog), Trino, Superset | 19120, 8090, 8088 |
| `airflow/` | Webserver, Scheduler, Worker, Redis, PostgreSQL | 8080 |
| `monitor/` | Prometheus, Grafana, Loki, Promtail, StatsD-exporter, node_exporter | 9090, 3000, 3100, 9102 |
| `serving_pipeline/` | FastAPI, Gradio UI | 8000, 7860 |

**Startup order:**

```
mlflow  →  lakehouse  →  kafka  →  monitor  →  airflow  →  serving
```

Tat ca services dung chung Docker network `aio-network`.

---

## Monitoring

### Grafana Dashboards

| Dashboard | Noi dung |
|---|---|
| **Infrastructure Overview** | CPU, RAM, disk I/O, network (node_exporter) |
| **Observability Overview** | FastAPI request rate, latency (p50/p95/p99), error rate, Feast lookup metrics |
| **Airflow Overview** | DAG run count, scheduler heartbeat, task success/failed |
| **FastAPI Overview** | Per-endpoint request count va latency |

### Evidently Drift Monitoring

```bash
curl http://localhost:8000/monitor/drift
```

So sanh phan phoi cua production data (ghi nhan khi co prediction) vs baseline training data.

### Superset

Ket noi truc tiep den Trino de query Iceberg tables:

- `bronze.customer_events` — du lieu raw tu Kafka
- `silver.customers` — du lieu da clean
- `gold.churn_features` — features cho model

---

## Key Design Decisions

- **Feast version alignment:** Airflow workers va API container deu dung `feast==0.58.0` voi `entity_key_serialization_version: 3` de dam bao Redis key format nhat quan.
- **Serving separation:** UI (Gradio) chi goi API, khong truy cap truc tiep Feast/Redis. Moi logic predict nam o FastAPI.
- **MLflow autolog:** `log_post_training_metrics=False, log_datasets=False` de tranh MLflow 3.x chay cross-validation ngam trong luc training (tiet kiem 30+ phut tren tap nho).
- **LightGBM early stopping:** Su dung `early_stopping(50)` callback kem voi `max_depth=6`, `num_leaves=31` de tranh overfitting (train 99% / test 58% neu khong co).
- **numpy 2.x compatibility:** Toan bo dependency chain (feast 0.58.0, pyiceberg>=0.8.0, mlflow>=2.16.0, pandas>=2.2.2, pyarrow>=16.0.0, scikit-learn>=1.5.0) da duoc align de su dung numpy>=2.0.0.
