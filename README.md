# MLOps End-to-End: Customer Churn Prediction System

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/downloads/)
[![MLflow](https://img.shields.io/badge/MLflow-2.15-orange.svg)](https://mlflow.org/)
[![Airflow](https://img.shields.io/badge/Airflow-3.x-017CEE.svg)](https://airflow.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-KRaft-231F20.svg)](https://kafka.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)

He thong MLOps end-to-end du doan customer churn, bao gom ingestion, lakehouse, feature store, retraining, serving va monitoring.

## Overview

- Data Simulator: sinh du lieu churn hang ngay, publish len Kafka.
- Lakehouse: ETL Bronze -> Silver -> Gold voi Iceberg + Nessie + MinIO.
- Feature Store: Feast apply + materialize Gold parquet vao Redis online store.
- Model Pipeline: train 6 model song song, chon best F1, register `champion` tren MLflow Registry.
- Serving Pipeline: FastAPI + Gradio + Evidently monitoring.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Data Preparation Flow                         │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  [data_simulator]  daily 23:45                                       │
│    simulate.py ──► Kafka (churn.raw.events)                          │
│                         │                                            │
│                         ▼  (kafka_to_bronze)                         │
│                   Iceberg Bronze                                     │
│                         │                                            │
│  [lakehouse_etl]  daily 00:00                                        │
│                         ▼                                            │
│    Bronze ──► Silver (dedup/validate) ──► Gold (feature eng.)        │
│                                               │          │           │
│                                               ▼          ▼           │
│                                  [Trino + Superset]  export parquet  │
│                                     dashboard              │         │
│                                                   ┌────────┘         │
│                                                   ▼                  │
│                          ┌────────────────────────┴──────────────┐  │
│                          ▼                                        ▼  │
│               [churn_feature_pipeline]            [churn_retraining] │
│                daily 00:30                         weekly (Sun)      │
│                 feast apply                        train 6 models    │
│                 materialize ──► Redis              find best F1      │
│                                                    register champion │
│                                                         ▼            │
│                                                   MLflow Registry    │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│                        Serving Request Flow                          │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Gradio UI                                                           │
│      │                                                               │
│      ├─► POST /predict/          (full feature payload)              │
│      │         │                                                     │
│      │         ▼                                                     │
│      │      FastAPI ──► XGBoost champion model ──► {"churn": 0|1}   │
│      │                   (from MLflow Registry)                      │
│      │                                                               │
│      └─► POST /predict/by-customer-id   (only customer_id)          │
│                │                                                     │
│                ▼                                                     │
│             FastAPI                                                  │
│                │                                                     │
│                ▼                                                     │
│          Feast online store ──► Redis (lookup features by id)        │
│                │                                                     │
│                ▼                                                     │
│         XGBoost champion model ──► {"churn": 0|1}                   │
│          (from MLflow Registry)                                      │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

## Canonical Serving Flow

Serving da duoc don lai theo mot luong ro rang:

- UI chi goi API.
- API la diem duy nhat xu ly prediction.
- `POST /predict/`: nhan full feature payload.
- `POST /predict/by-customer-id`: API tu lookup online features tu Feast/Redis theo `customer_id`, sau do predict.

Dieu nay giup:

- khong trung logic lookup giua UI va API
- de dong bo validation va logging
- de deploy Docker vi UI khong can truy cap Feast repo hay Redis truc tiep

## Project Structure

```text
aio2025-mlops-project01/
|-- data-simulator/
|   |-- simulate.py
|
|-- data-pipeline/
|   |-- churn_feature_store/
|       |-- churn_features/
|           |-- feature_repo/
|
|-- model_pipeline/
|   |-- src/
|       |-- config/
|       |-- model/
|       |-- mlflow_utils/
|       |-- scripts/
|
|-- serving_pipeline/
|   |-- api/
|   |   |-- main.py
|   |   |-- routers/
|   |   |-- schemas.py
|   |-- ui.py
|   |-- load_model.py
|   |-- monitoring.py
|   |-- Dockerfile
|   |-- Dockerfile.ui
|   |-- docker-compose.yml
|
|-- infra/
|   |-- docker/
|       |-- mlflow/
|       |-- kafka/
|       |-- lakehouse/
|       |-- airflow/
|       |-- monitor/
|
|-- start.ps1
|-- workflow.md
```

## Prerequisites

- Docker Desktop 20.10+ voi Docker Compose v2
- Python 3.11
- RAM khuyen nghi: 16 GB
- Disk free: 20 GB+

Tao Docker network dung chung:

```powershell
docker network create aio-network
```

## Quick Start

```powershell
git clone https://github.com/ThuanNaN/aio2025-mlops-project01.git
cd aio2025-mlops-project01

docker network create aio-network

.\start.ps1
```

`start.ps1` se:

1. Start cac Docker stack.
2. Trigger `data_simulator` -> `lakehouse_etl` -> `churn_feature_pipeline`.
3. Trigger `churn_retraining_pipeline`.
4. Start serving API + UI.

## Main Pipelines

### 1. Data Simulator

DAG `data_simulator`:

1. `simulate_to_kafka`
2. `kafka_to_bronze`

Output: `bronze.customer_events`

### 2. Lakehouse ETL

DAG `lakehouse_etl`:

1. `init_namespaces`
2. `bronze_to_silver`
3. `silver_to_gold`
4. `export_gold_parquet`

Outputs:

- `silver.customers`
- `gold.churn_features`
- `data-pipeline/churn_feature_store/churn_features/feature_repo/data/processed_churn_data.parquet`

### 3. Feature Store

DAG `churn_feature_pipeline`:

1. `feast_apply`
2. `feast_materialize_incremental`

Output: Redis online features theo `customer_id`

### 4. Retraining

DAG `churn_retraining_pipeline`:

1. train 6 model song song
2. `find_best_model`
3. `evaluate_best_model`
4. `register_champion`

Output: model alias `models:/customer_churn_model@champion`

## Serving Pipeline

Chay rieng serving:

```powershell
cd serving_pipeline
docker compose up -d --build
```

Luu y:

- `api` build tu `Dockerfile`
- `ui` build tu `Dockerfile.ui`
- UI local source se khop voi runtime container

### Predict with full payload

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
    "Total_Spend": 500,
    "Last_Interaction": 10
  }'
```

### Predict by customer_id

```bash
curl -X POST http://localhost:8000/predict/by-customer-id \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "123"
  }'
```

## Service URLs

| Service | URL |
|---|---|
| Serving API docs | http://localhost:8000/docs |
| Gradio UI | http://localhost:7860 |
| MLflow | http://localhost:5000 |
| Airflow | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| Superset | http://localhost:8088 |
| Trino | http://localhost:8090 |
| Nessie | http://localhost:19120 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |

## Infrastructure

| Stack | Services | Ports |
|---|---|---|
| `mlflow/` | MLflow + MySQL + MinIO | 5000, 9000, 9001 |
| `kafka/` | 3-node Kafka (KRaft) | 9092, 9192, 9292 |
| `lakehouse/` | Nessie + Trino + Superset | 19120, 8090, 8088 |
| `monitor/` | Prometheus + Grafana + Loki | 9090, 3000 |
| `airflow/` | Airflow 3.x (Celery) | 8080 |

Startup order:

```text
mlflow -> lakehouse -> kafka -> monitor -> airflow -> serving
```

## Monitoring

- Grafana: infra metrics
- Evidently: drift report qua API `/monitor/drift`
- Superset: dashboard truc tiep tren Iceberg tables qua Trino

## Notes

- Feast lookup cho `customer_id` nam o API, khong nam o UI.
- UI va API da tach dung vai tro: UI la client, API la backend xu ly nghiep vu.
- `workflow.md` mo ta chi tiet luong chay thuc te.
