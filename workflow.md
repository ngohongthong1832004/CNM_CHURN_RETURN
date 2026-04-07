# End-to-End Workflow (Data to Prediction Result)

This document summarizes the runtime and code flow across the whole project, from dataset preparation to model output in API/UI.

## 1) Data Preparation and Versioning

Main components:
- data-pipeline/data/raw and data-pipeline/data/processed
- data-pipeline/scripts/processing+EDA.ipynb
- data-pipeline/churn_feature_store/churn_features/feature_repo/prepare_feast_data.py
- DVC metadata files (*.dvc)

Flow:
1. Raw CSV data is cleaned/processed in notebook/scripts.
2. Processed output (for example df_processed.csv) is transformed into Feast-compatible parquet in prepare_feast_data.py.
3. DVC tracks/pulls data artifacts so runs are reproducible.

Key output artifact:
- data-pipeline/churn_feature_store/churn_features/feature_repo/data/processed_churn_data.parquet

## 2) Feature Store (Feast) Registration and Online Serving

Main components:
- data-pipeline/churn_feature_store/churn_features/feature_repo/churn_entities.py
- data-pipeline/churn_feature_store/churn_features/feature_repo/data_sources.py
- data-pipeline/churn_feature_store/churn_features/feature_repo/feature_views.py
- data-pipeline/scripts/sample_retrieval.py

Flow:
1. Feast entity/customer key is defined in churn_entities.py.
2. Offline source points to parquet in data_sources.py.
3. Feature views define demographics and behavior feature groups in feature_views.py.
4. feast apply registers objects in Feast registry.
5. feast materialize-incremental pushes features to Redis online store.
6. sample_retrieval.py fetches online features by customer_id.

Result:
- Online feature vectors can be retrieved in real-time for serving.

## 3) Model Training with MLflow Tracking

Main components:
- model_pipeline/src/scripts/train.py
- model_pipeline/src/model/xgboost_trainer.py (GenericBinaryClassifierTrainer + BinaryClassifierWrapper)
- model_pipeline/src/config/*.yaml
- model_pipeline/src/mlflow_utils/experiment_tracker.py

Flow:
1. train.py loads config and training data (csv/parquet).
2. Categorical columns are encoded with LabelEncoder.
3. GenericBinaryClassifierTrainer.prepare_data() splits train/validation.
4. GenericBinaryClassifierTrainer.train() trains selected model type (logistic_regression / decision_tree / random_forest).
5. Metrics and feature importance are logged to MLflow.
6. save_model() logs an MLflow pyfunc model artifact with wrapper logic.

Result:
- MLflow run_id + model artifact path (runs:/<run_id>/<model_name>)

## 4) Evaluation and Registry Lifecycle

Main components:
- model_pipeline/src/scripts/eval.py
- model_pipeline/src/model/evaluator.py
- model_pipeline/src/scripts/register_model.py
- model_pipeline/src/mlflow_utils/model_registry.py

Flow:
1. eval.py loads model via run_id/model_uri and evaluation dataset.
2. evaluator.py runs mlflow.models.evaluate() and optionally threshold validation.
3. Optional prediction output CSV is generated and logged.
4. register_model.py registers model, sets alias (staging/champion/production), and promotes model.

Result:
- Versioned and aliased model in MLflow registry.

## 5) Serving API Runtime Flow

Main components:
- serving_pipeline/api/main.py
- serving_pipeline/api/routers/predict.py
- serving_pipeline/pre_processing.py
- serving_pipeline/load_model.py
- serving_pipeline/api/routers/health.py
- serving_pipeline/api/routers/monitor.py
- serving_pipeline/monitoring.py

Prediction flow:
1. Request hits POST /predict/ (single) or /predict/batch.
2. Input is validated in pre_processing.validate_input().
3. Schema fields are mapped to model fields via map_schema_to_preprocessing().
4. get_model() lazily loads MLflow model once using MODEL_URI + MLFLOW_TRACKING_URI.
5. Model predicts churn (0/1).
6. Input + prediction are asynchronously appended to production.csv for drift monitoring.

Monitoring flow:
1. /monitor/drift loads reference and current production data.
2. monitoring.generate_drift_report() runs Evidently metrics.
3. JSON drift metrics (or HTML report) are returned.

## 6) UI Runtime Flow (Gradio)

Main component:
- serving_pipeline/ui.py

Tabs and behavior:
1. Single Prediction tab: user enters fields -> UI sends payload to API /predict/.
2. Batch Prediction tab: upload CSV -> UI sends records to API /predict/batch.
3. Customer Data Check tab:
   - Calls get_customer_features() from sample_retrieval.py (Feast online lookup).
   - Maps retrieved feature columns to API schema.
   - Calls API /predict/ and renders result.

Networking details used in docker compose:
- UI internal app port: 7823
- Host exposed UI port: 7860 (7860:7823)
- UI to API URL inside compose network: http://api:8000

## 7) Infrastructure Dependencies

Main stacks:
- infra/docker/mlflow: MLflow + MySQL + MinIO
- infra/docker/kafka: Kafka cluster
- infra/docker/monitor: Prometheus + Grafana + Loki
- infra/docker/airflow: orchestration stack

Core runtime dependency for prediction:
- API requires MLflow + MinIO access (MODEL_URI artifact lookup)
- UI requires API
- Optional feature store retrieval requires Feast registry + Redis online store

## 8) Where Final Results Appear

User-facing outputs:
- API docs: http://localhost:8000/docs
- UI: http://localhost:7860
- MLflow runs/models: http://localhost:5000

Persisted artifacts:
- Online features: Redis (Feast materialization)
- Model artifacts: MinIO via MLflow
- Prediction logs for drift: serving_pipeline/data_model/production/production.csv
- Drift reports (optional save): serving_pipeline/reports/drift/*
