"""
DAG: data_simulator
Schedule : hàng ngày 23:45 UTC — chạy trước lakehouse_etl_dag (00:00)

Flow:
  simulate_new_data → ghi vào Iceberg Bronze table (lakehouse.bronze.customer_events)
  → lakehouse_etl_dag (00:00) sẽ đọc Bronze → Silver → Gold → parquet
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DATA_SIMULATOR = "/opt/data-simulator"

NESSIE_URI   = os.environ.get("NESSIE_URI",              "http://nessie:19120/iceberg")
MINIO_URL    = os.environ.get("MLFLOW_S3_ENDPOINT_URL",  "http://minio:9000")
MINIO_KEY    = os.environ.get("AWS_ACCESS_KEY_ID",        "minio")
MINIO_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY",    "minio123")

default_args = {
    "owner": "mlops",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="data_simulator",
    default_args=default_args,
    description=(
        "Sinh ~100 bản ghi khách hàng churn mới mỗi ngày, "
        "ghi vào Iceberg Bronze table trên MinIO/Nessie"
    ),
    schedule="45 23 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["data-simulator", "lakehouse"],
) as dag:

    simulate_new_data = BashOperator(
        task_id="simulate_new_data",
        bash_command=(
            f"python {DATA_SIMULATOR}/simulate.py "
            "--n-records 100 "
            f"--nessie-uri {NESSIE_URI} "
            f"--minio-url {MINIO_URL} "
            f"--minio-key {MINIO_KEY} "
            f"--minio-secret {MINIO_SECRET} "
            "--warehouse s3://lakehouse/"
        ),
        env={
            "PYTHONPATH": DATA_SIMULATOR,
        },
    )
