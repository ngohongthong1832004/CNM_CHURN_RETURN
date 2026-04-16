# End-to-End Workflow

Tai lieu nay mo ta luong chay thuc te cua he thong, tu ingest du lieu den prediction va monitoring.

## 1. Data Simulation -> Kafka -> Bronze

DAG: `data_simulator`  
Schedule: `23:45 UTC` moi ngay

Thanh phan:

- `data-simulator/simulate.py`
- `infra/docker/airflow/dags/data_simulator_dag.py`
- Kafka topic `churn.raw.events`
- Iceberg table `bronze.customer_events`

Luong:

1. `simulate_to_kafka` sinh khoang 100 records moi.
2. Records duoc publish len Kafka topic `churn.raw.events`.
3. `kafka_to_bronze` consume Kafka, parse JSON, type-cast, append vao Iceberg Bronze qua Nessie catalog.

Output:

- `bronze.customer_events` tren MinIO `s3://lakehouse/`

## 2. Bronze -> Silver -> Gold -> Parquet

DAG: `lakehouse_etl`  
Schedule: `00:00 UTC` moi ngay

Thanh phan:

- `infra/docker/airflow/dags/lakehouse_etl_dag.py`
- Nessie REST catalog
- MinIO warehouse

Luong:

1. `init_namespaces`: tao `bronze`, `silver`, `gold` neu chua ton tai.
2. `bronze_to_silver`: doc Bronze, dedup theo `customer_id`, validate, overwrite `silver.customers`.
3. `silver_to_gold`: tao feature-ready dataset, overwrite `gold.churn_features`.
4. `export_gold_parquet`: export Gold table ra:
   `data-pipeline/churn_feature_store/churn_features/feature_repo/data/processed_churn_data.parquet`

Outputs:

- `silver.customers`
- `gold.churn_features`
- `processed_churn_data.parquet`

## 3. Feast Apply + Materialize

DAG: `churn_feature_pipeline`  
Schedule: `00:30 UTC` moi ngay

Thanh phan:

- `data-pipeline/churn_feature_store/churn_features/feature_repo/churn_entities.py`
- `data_sources.py`
- `feature_views.py`

Luong:

1. `feast_apply`: dang ky entity, source, feature views vao Feast registry.
2. `feast_materialize_incremental`: materialize feature moi nhat vao Redis online store.

Output:

- Redis online features, truy xuat theo `customer_id`

## 4. Weekly Retraining

DAG: `churn_retraining_pipeline`  
Schedule: `00:00 UTC` Chu nhat

Thanh phan:

- `model_pipeline/src/scripts/train.py`
- `model_pipeline/src/scripts/eval.py`
- `model_pipeline/src/scripts/register_model.py`

Luong:

1. Train 6 models song song tu `processed_churn_data.parquet`.
2. `find_best_model`: tim run co `training_f1_score` cao nhat.
3. `evaluate_best_model`: danh gia lai run tot nhat.
4. `register_champion`: register vao MLflow va set alias `champion`.

Output:

- MLflow model alias `models:/customer_churn_model@champion`

## 5. Serving Architecture

Thanh phan:

- `serving_pipeline/api/main.py`
- `serving_pipeline/api/routers/predict.py`
- `serving_pipeline/ui.py`
- `serving_pipeline/load_model.py`
- `serving_pipeline/monitoring.py`

Nguyen tac:

- UI chi goi API
- API la diem duy nhat xu ly prediction
- Feast lookup cho `customer_id` nam trong API, khong nam trong UI

### 5.1 Predict with full payload

Endpoint:

- `POST /predict/`

Luong:

1. Request gui day du feature payload.
2. API validate input.
3. API lazy-load model tu MLflow neu can.
4. Model predict churn.
5. Input + prediction duoc append vao production dataset phuc vu drift monitoring.

### 5.2 Predict by customer_id

Endpoint:

- `POST /predict/by-customer-id`

Luong:

1. UI hoac client gui `customer_id` len API.
2. API goi Feast online store de lay online features tu Redis.
3. API map feature ve schema model input.
4. API predict churn bang model dang load tu MLflow.
5. Production log duoc ghi lai cho monitoring.

Loi ich:

- khong lap logic lookup o UI
- de deploy Docker vi UI khong can truy cap Redis/Feast repo
- de dong bo validation, logging, va future auth

## 6. Gradio UI

Thanh phan:

- `serving_pipeline/ui.py`
- `serving_pipeline/Dockerfile.ui`

Tabs hien tai:

1. `Single Prediction`
   - predict tu form input
   - predict tu `customer_id`
2. `Batch Prediction`

Luu y:

- UI `customer_id` goi API `/predict/by-customer-id`
- UI khong truy cap Feast truc tiep nua
- `serving_pipeline/docker-compose.yml` build UI tu source local de runtime khop voi code

## 7. Monitoring

### Evidently

- `GET /monitor/drift`
- So sanh reference data voi `production.csv`

### Superset

- Query truc tiep Iceberg Bronze/Silver/Gold qua Trino

### Grafana

- Theo doi infrastructure metrics

## 8. Runtime Dependency Summary

```text
Serving API needs:
- MLflow
- MinIO
- Feast repo
- Redis online store

Gradio UI needs:
- FastAPI only

Airflow needs:
- Kafka
- Nessie
- MinIO
- MLflow
- Redis
```

## 9. Schedule Summary

```text
23:45  data_simulator
00:00  lakehouse_etl
00:30  churn_feature_pipeline
Sun    churn_retraining_pipeline
```
