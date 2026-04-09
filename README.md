# MLOps End-to-End: Customer Churn Prediction System

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/downloads/)
[![MLflow](https://img.shields.io/badge/MLflow-2.15-orange.svg)](https://mlflow.org/)
[![Airflow](https://img.shields.io/badge/Airflow-3.x-017CEE.svg)](https://airflow.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-KRaft-231F20.svg)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)

Hệ thống MLOps end-to-end dự đoán khách hàng rời bỏ (churn), triển khai đầy đủ vòng đời ML từ sinh dữ liệu đến serving và monitoring.

---

## 📖 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Pipelines](#pipelines)
- [Infrastructure](#infrastructure)
- [Monitoring](#monitoring)

---

## 🎯 Overview

- **Data Simulator**: Sinh dữ liệu churn hàng ngày → publish lên Kafka → ghi vào Iceberg Bronze table
- **Lakehouse**: ETL Bronze → Silver → Gold (Nessie + Trino + Superset), export parquet cho training
- **Feature Store**: Feast apply + materialize features mới nhất vào Redis online store
- **Model Pipeline**: Train 6 thuật toán song song, chọn best F1, register `champion` lên MLflow Registry
- **Serving Pipeline**: FastAPI prediction service + Gradio UI + Evidently drift monitoring

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Data Flow (Daily)                            │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  [data-simulator]                                                    │
│    simulate.py ──► Kafka (churn.raw.events)                          │
│                         │                                            │
│                         ▼                                            │
│  [lakehouse_etl]                                                     │
│    Bronze ──► Silver (dedup/validate) ──► Gold (feature eng.)        │
│                         │                                            │
│              ┌──────────┴──────────┐                                 │
│              ▼                     ▼                                 │
│    [Trino + Superset]    export parquet                               │
│       dashboard                    │                                 │
│                                    ▼                                 │
│  [churn_feature_pipeline]    [churn_retraining] (weekly)             │
│    feast apply                train 6 models in parallel             │
│    materialize ──► Redis      find best F1                           │
│                               register champion ──► MLflow Registry  │
│                                                          │            │
│                                                          ▼            │
│                                              [Serving Pipeline]       │
│                                               FastAPI + Gradio UI     │
│                                               Evidently monitoring    │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
aio2025-mlops-project01/
├── data-simulator/                  # Kafka producer
│   ├── simulate.py                 # Sinh records → publish JSON lên Kafka
│   ├── Dockerfile
│   └── requirements.txt
│
├── data-pipeline/                   # Feast feature store
│   ├── churn_feature_store/
│   │   └── churn_features/
│   │       └── feature_repo/       # entities, data_sources, feature_views
│   └── requirements.txt
│
├── model_pipeline/                  # Model training & evaluation
│   ├── src/
│   │   ├── config/                 # YAML per model type (6 models)
│   │   ├── model/                  # GenericBinaryClassifierTrainer
│   │   ├── mlflow_utils/           # ExperimentTracker, ModelRegistry
│   │   └── scripts/                # train.py, eval.py, register_model.py
│   └── train_all.ps1
│
├── serving_pipeline/                # Model serving
│   ├── api/
│   │   ├── main.py
│   │   ├── routers/                # predict, health, monitor
│   │   └── schemas.py
│   ├── ui.py                       # Gradio interface
│   ├── load_model.py
│   ├── monitoring.py               # Evidently drift
│   └── docker-compose.yml
│
├── infra/
│   └── docker/
│       ├── mlflow/                 # MLflow server + MySQL + MinIO
│       ├── kafka/                  # 3-node Kafka cluster (KRaft)
│       ├── lakehouse/              # Nessie + Trino + Superset
│       ├── airflow/                # Airflow 3.x + DAGs
│       ├── monitor/                # Prometheus + Grafana + Loki
│       ├── run.ps1                 # PowerShell control script
│       └── run.sh                  # Bash control script
│
├── start.ps1                        # One-click launcher (Windows)
└── workflow.md                      # Chi tiết luồng chạy
```

---

## 🔧 Prerequisites

- **OS**: Windows (PowerShell) hoặc Linux/macOS
- **RAM**: 16GB recommended
- **Storage**: 20GB+ free
- **Docker Desktop**: 20.10+ với Docker Compose v2
- **Python**: 3.11

Tạo Docker network dùng chung:
```powershell
docker network create aio-network
```

---

## 🚀 Quick Start

### Chạy toàn bộ hệ thống (one-click)

```powershell
# Clone
git clone https://github.com/ThuanNaN/aio2025-mlops-project01.git
cd aio2025-mlops-project01

# Tạo network
docker network create aio-network

# Chạy từ đầu đến cuối (Infra → Data → Model → Serving)
.\start.ps1
```

`start.ps1` tự động:
1. Start toàn bộ Docker services
2. Trigger DAG `data_simulator` → `lakehouse_etl` → `churn_feature_pipeline`
3. Trigger DAG `churn_retraining_pipeline` (train 6 models, register champion)
4. Start serving API + UI

### Các tùy chọn skip:

```powershell
# Bỏ qua infra (services đã chạy)
.\start.ps1 -SkipInfra

# Chỉ restart serving với model mới
.\start.ps1 -SkipInfra -SkipData

# Chỉ serving (model đã register sẵn)
.\start.ps1 -SkipInfra -SkipData -SkipModel
```

### Access URLs sau khi start:

| Service | URL | Credentials |
|---|---|---|
| Serving API | http://localhost:8000/docs | — |
| Gradio UI | http://localhost:7860 | — |
| MLflow | http://localhost:5000 | — |
| Airflow | http://localhost:8080 | airflow / airflow |
| MinIO | http://localhost:9001 | minio / minio123 |
| Superset | http://localhost:8088 | admin / admin |
| Trino | http://localhost:8090 | — |
| Nessie | http://localhost:19120 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |

---

## 🔄 Pipelines

### Data Simulator → Kafka → Lakehouse

**Location**: [`data-simulator/`](data-simulator/), [`infra/docker/airflow/dags/`](infra/docker/airflow/dags/)

DAG `data_simulator` (23:45 UTC hàng ngày):
1. `simulate_to_kafka` — sinh ~100 records, publish JSON lên Kafka topic `churn.raw.events`
2. `kafka_to_bronze` — consume từ Kafka, append vào Iceberg `bronze.customer_events`

DAG `lakehouse_etl` (00:00 UTC hàng ngày):
1. `bronze_to_silver` — dedup, validate
2. `silver_to_gold` — feature engineering (`tenure_age_ratio`, `spend_per_usage`, ...)
3. `export_gold_parquet` — export ra parquet cho Feast + training

DAG `churn_feature_pipeline` (00:30 UTC hàng ngày):
1. `feast_apply` — đăng ký features
2. `feast_materialize_incremental` — đẩy vào Redis online store

---

### Model Pipeline

**Location**: [`model_pipeline/`](model_pipeline/)

**Supported Models**: `xgboost`, `lightgbm`, `catboost`, `random_forest`, `decision_tree`, `logistic_regression`

DAG `churn_retraining_pipeline` (Chủ nhật 00:00 UTC):
1. Train 6 models **song song** từ parquet Gold
2. `find_best_model` — chọn run có `training_f1_score` cao nhất trong MLflow
3. `evaluate_best_model` — validate threshold
4. `register_champion` — register vào MLflow Registry, set alias `champion`

Train thủ công (ngoài Airflow):
```powershell
cd model_pipeline
$env:PYTHONPATH = (Get-Location).Path
.\train_all.ps1
```

---

### Serving Pipeline

**Location**: [`serving_pipeline/`](serving_pipeline/)

```powershell
cd serving_pipeline
# Sửa .env: set MODEL_URI=models:/customer_churn_model@champion
docker compose up -d
```

**Predict example**:
```bash
curl -X POST http://localhost:8000/predict/ \
  -H "Content-Type: application/json" \
  -d '{
    "Age": 35, "Gender": "Male", "Tenure": 24,
    "Usage_Frequency": 15, "Support_Calls": 2,
    "Payment_Delay": 5, "Subscription_Type": "Premium",
    "Contract_Length": "Annual", "Total_Spend": 500,
    "Last_Interaction": 10
  }'
```

---

## 🏗️ Infrastructure

**Location**: [`infra/docker/`](infra/docker/)

| Stack | Services | Ports |
|---|---|---|
| `mlflow/` | MLflow + MySQL + MinIO | 5000, 9000, 9001 |
| `kafka/` | 3-node Kafka (KRaft) | 9092, 9192, 9292 |
| `lakehouse/` | Nessie + Trino + Superset | 19120, 8090, 8088 |
| `monitor/` | Prometheus + Grafana + Loki | 9090, 3000 |
| `airflow/` | Airflow 3.x (Celery) | 8080 |

**PowerShell**:
```powershell
cd infra/docker
.\run.ps1 start all          # start toàn bộ
.\run.ps1 start mlflow       # start service riêng lẻ
.\run.ps1 stop all
.\run.ps1 status
```

**Bash**:
```bash
cd infra/docker
./run.sh up
./run.sh down
./run.sh status
```

---

## 📊 Monitoring

### Grafana (http://localhost:3000)
- FastAPI request latency, throughput
- Model prediction counts, error rates
- Dashboard: [FastAPI Observability 16110](https://grafana.com/grafana/dashboards/16110-fastapi-observability/)

### Evidently Drift
```bash
GET http://localhost:8000/monitor/drift?format=json
```

### Superset (http://localhost:8088)
- Dashboard trực tiếp từ Iceberg tables (Bronze / Silver / Gold) qua Trino
