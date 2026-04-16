"""
DAG: churn_feature_pipeline
Schedule : hàng ngày 00:30 UTC — sau lakehouse_etl_dag (00:00) đã export parquet

Flow:
  feast_apply → feast_materialize_incremental

Lưu ý: bước prepare_feast_data đã được chuyển vào lakehouse_etl_dag
  (task export_gold_parquet) — Gold table export trực tiếp ra parquet
  tại feature_repo/data/processed_churn_data.parquet
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DATA_PIPELINE = "/opt/data-pipeline"
FEATURE_REPO  = f"{DATA_PIPELINE}/churn_feature_store/churn_features/feature_repo"

default_args = {
    "owner": "mlops",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="churn_feature_pipeline",
    default_args=default_args,
    description=(
        "Feast apply + materialize-incremental "
        "(parquet đã được lakehouse_etl_dag export trước đó)"
    ),
    schedule="30 0 * * *",   # 00:30 UTC — sau lakehouse_etl (00:00)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["feature-store", "feast", "data-pipeline"],
) as dag:

    feast_apply = BashOperator(
        task_id="feast_apply",
        # Delete stale registry.db first — a corrupt/outdated registry causes
        # EntityNotFoundException during materialize even when feast apply succeeds.
        # The registry is regenerated cleanly from the Python feature definitions.
        bash_command=(
            f"cd {FEATURE_REPO} && "
            "rm -f data/registry.db && "
            "feast apply"
        ),
    )

    feast_materialize = BashOperator(
        task_id="feast_materialize_incremental",
        bash_command=(
            f'cd {FEATURE_REPO} && '
            'feast materialize-incremental "$(date -u +%Y-%m-%dT%H:%M:%S)"'
        ),
    )

    feast_apply >> feast_materialize
