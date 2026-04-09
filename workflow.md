# End-to-End Workflow (Data to Prediction Result)

Tài liệu này mô tả luồng chạy thực tế của toàn bộ hệ thống, từ sinh dữ liệu đến kết quả dự đoán.

---

## 1) Data Simulation → Kafka → Iceberg Bronze

DAG: `data_simulator` — Schedule: **23:45 UTC hàng ngày**

Main components:
- `data-simulator/simulate.py` — Kafka producer
- `infra/docker/airflow/dags/data_simulator_dag.py` — 2 tasks: `simulate_to_kafka` → `kafka_to_bronze`
- Kafka topic: `churn.raw.events` (brokers: `broker-1:9094`, `broker-2:9094`, `broker-3:9094`)
- Iceberg Bronze table: `bronze.customer_events` (Nessie REST Catalog / MinIO)

Flow:
1. `simulate_to_kafka`: `simulate.py` sinh ~100 records khách hàng mới với phân phối thực tế (age, tenure, spend, churn label...).
2. Mỗi record được serialize thành JSON và publish lên Kafka topic `churn.raw.events` (key = `customer_id`).
3. `kafka_to_bronze`: Airflow consumer group `airflow-bronze-consumer` poll Kafka, parse JSON, type-cast, và append vào Iceberg Bronze table qua Nessie REST catalog.

Output:
- Iceberg table `bronze.customer_events` trên MinIO (`s3://lakehouse/`)

---

## 2) Lakehouse ETL — Bronze → Silver → Gold → Parquet

DAG: `lakehouse_etl` — Schedule: **00:00 UTC hàng ngày**

Main components:
- `infra/docker/airflow/dags/lakehouse_etl_dag.py` — 4 tasks: `init_namespaces` → `bronze_to_silver` → `silver_to_gold` → `export_gold_parquet`
- PyIceberg catalog (Nessie), MinIO storage

Flow:
1. `init_namespaces`: tạo Iceberg namespaces `bronze`, `silver`, `gold` nếu chưa có.
2. `bronze_to_silver`: đọc toàn bộ Bronze → dedup theo `customer_id` (giữ bản `created_at` mới nhất) → validate (age, tenure, total_spend) → ghi đè Silver table.
3. `silver_to_gold`: tính derived features (`tenure_age_ratio`, `spend_per_usage`, `support_calls_per_tenure`, `spending_group`, `tenure_group`) → ghi đè Gold table với schema Feast-compatible.
4. `export_gold_parquet`: export Gold table → file parquet tại `feature_repo/data/processed_churn_data.parquet`.

Output:
- Iceberg tables: `silver.customers`, `gold.churn_features` trên MinIO
- Parquet file: `data-pipeline/churn_feature_store/churn_features/feature_repo/data/processed_churn_data.parquet`

Trino + Superset query trực tiếp các Iceberg tables (Bronze/Silver/Gold) để dashboard.

---

## 3) Feature Store — Feast Apply + Materialize

DAG: `churn_feature_pipeline` — Schedule: **00:30 UTC hàng ngày**

Main components:
- `data-pipeline/churn_feature_store/churn_features/feature_repo/churn_entities.py`
- `data-pipeline/churn_feature_store/churn_features/feature_repo/data_sources.py`
- `data-pipeline/churn_feature_store/churn_features/feature_repo/feature_views.py`

Flow:
1. `feast_apply`: đăng ký entity, data source, feature views vào Feast registry (đọc từ parquet do Lakehouse ETL export).
2. `feast_materialize_incremental`: đẩy features mới nhất từ parquet vào Redis online store.

Output:
- Redis online store: feature vectors có thể truy xuất real-time theo `customer_id`.

---

## 4) Model Training — churn_retraining_pipeline

DAG: `churn_retraining_pipeline` — Schedule: **00:00 UTC Chủ nhật (weekly)**

Main components:
- `model_pipeline/src/scripts/train.py`
- `model_pipeline/src/model/xgboost_trainer.py` — `GenericBinaryClassifierTrainer` + `BinaryClassifierWrapper`
- `model_pipeline/src/config/*.yaml` — config riêng cho từng model type
- `model_pipeline/src/mlflow_utils/experiment_tracker.py`

Flow:
1. Train 6 models **song song** từ parquet (`processed_churn_data.parquet`):
   - `logistic_regression`, `decision_tree`, `random_forest`, `xgboost`, `lightgbm`, `catboost`
2. Mỗi model: LabelEncoder cho categorical columns → `prepare_data()` split train/val → `train()` → log metrics + feature importance vào MLflow.
3. `save_model()`: log MLflow pyfunc artifact với `BinaryClassifierWrapper` (hỗ trợ `return_probs`, `return_both`).

Output:
- MLflow run per model: `run_id`, metrics (`training_f1_score`, `train_accuracy`, `test_accuracy`), artifact path.

---

## 5) Model Evaluation & Registry

Tiếp nối trong DAG `churn_retraining_pipeline`:

Main components:
- `model_pipeline/src/scripts/eval.py`
- `model_pipeline/src/scripts/register_model.py`
- `model_pipeline/src/mlflow_utils/model_registry.py`

Flow:
1. `find_best_model`: query MLflow tìm run có `training_f1_score` cao nhất trong các experiments vừa train.
2. `evaluate_best_model`: chạy `eval.py` với run_id của model tốt nhất — validate threshold, optional export predictions CSV.
3. `register_champion`: `mlflow.register_model()` → set alias `champion` → model sẵn sàng cho serving.

Output:
- Model version trong MLflow Registry với alias `champion`.
- Serving dùng URI: `models:/customer_churn_model@champion`

---

## 6) Serving API

Main components:
- `serving_pipeline/api/main.py`
- `serving_pipeline/api/routers/predict.py`
- `serving_pipeline/pre_processing.py`
- `serving_pipeline/load_model.py`
- `serving_pipeline/api/routers/monitor.py`
- `serving_pipeline/monitoring.py`

Prediction flow:
1. `POST /predict/` (single) hoặc `/predict/batch`
2. Input validated bởi `pre_processing.validate_input()`.
3. `get_model()` lazy-load MLflow pyfunc model một lần duy nhất (MODEL_URI + MLFLOW_TRACKING_URI).
4. Model predict churn (0/1), trả về prediction + probability.
5. Input + prediction async append vào `production.csv` cho drift monitoring.

Monitoring flow:
1. `GET /monitor/drift` → load reference data + production data.
2. `monitoring.generate_drift_report()` chạy Evidently metrics.
3. Trả về JSON metrics hoặc HTML report.

---

## 7) UI — Gradio

Main component:
- `serving_pipeline/ui.py`

Tabs:
1. **Single Prediction**: nhập fields → POST `/predict/`.
2. **Batch Prediction**: upload CSV → POST `/predict/batch`.
3. **Customer Data Check**: lookup Feast online features theo `customer_id` → POST `/predict/`.

Network (docker compose):
- UI internal port: `7823` → host port: `7860`
- UI → API: `http://api:8000` (internal `aio-network`)

---

## 8) Infrastructure

Docker stacks (tất cả dùng `aio-network`):

| Stack | Services | Ports |
|---|---|---|
| `infra/docker/mlflow` | MLflow server + MySQL + MinIO | MLflow:5000, MinIO:9000/9001 |
| `infra/docker/kafka` | 3-node Kafka (KRaft) | broker-1:9092, broker-2:9192, broker-3:9292 |
| `infra/docker/lakehouse` | Nessie + Trino + Superset | Nessie:19120, Trino:8090, Superset:8088 |
| `infra/docker/monitor` | Prometheus + Grafana + Loki | Grafana:3000, Prometheus:9090 |
| `infra/docker/airflow` | Airflow 3.x (CeleryExecutor) + PostgreSQL + Redis | Airflow:8080 |

Startup order: `mlflow` → `lakehouse` → `kafka` → `monitor` → `airflow`

Airflow DAG schedule order hàng ngày:
```
23:45  data_simulator          (simulate → Kafka → Bronze)
00:00  lakehouse_etl           (Bronze → Silver → Gold → parquet)
00:30  churn_feature_pipeline  (feast apply + materialize → Redis)
Sun    churn_retraining_pipeline (train → best → champion)
```

Runtime dependencies cho serving:
- API cần: MLflow server + MinIO (load model artifact)
- UI cần: API
- Customer lookup (Feast) cần: Feast registry + Redis online store

---

## 9) Kết quả cuối cùng

| Output | URL / Path |
|---|---|
| Serving API docs | http://localhost:8000/docs |
| Gradio UI | http://localhost:7860 |
| MLflow experiments/models | http://localhost:5000 |
| Airflow DAG monitor | http://localhost:8080 |
| Superset dashboard | http://localhost:8088 |
| Grafana dashboard | http://localhost:3000 |
| Prediction logs (drift) | `serving_pipeline/api/data/production/production.csv` |
| Model artifacts | MinIO `s3://mlflow/` via MLflow |
| Lakehouse data | MinIO `s3://lakehouse/` (Iceberg tables) |
