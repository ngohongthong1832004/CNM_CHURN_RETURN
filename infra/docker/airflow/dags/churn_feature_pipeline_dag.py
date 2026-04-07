"""
DAG: churn_feature_pipeline
Schedule : daily at midnight (00:00 UTC)
Tasks    : prepare_feast_data → feast_apply → feast_materialize_incremental

Requires volumes in docker-compose:
  - ../../../data-pipeline:/opt/data-pipeline
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ── Paths inside the Airflow worker container ─────────────────────────────────
DATA_PIPELINE = "/opt/data-pipeline"
FEATURE_REPO = f"{DATA_PIPELINE}/churn_feature_store/churn_features/feature_repo"

# ── DAG definition ─────────────────────────────────────────────────────────────
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
    description="Prepare Feast data and materialize features to online store (daily)",
    schedule="0 0 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["feature-store", "feast", "data-pipeline"],
) as dag:

    prepare_feast_data = BashOperator(
        task_id="prepare_feast_data",
        bash_command=(
            f"cd {DATA_PIPELINE} && "
            "python churn_feature_store/churn_features/feature_repo/prepare_feast_data.py"
        ),
    )

    feast_apply = BashOperator(
        task_id="feast_apply",
        bash_command=f"cd {FEATURE_REPO} && feast apply",
    )

    feast_materialize = BashOperator(
        task_id="feast_materialize_incremental",
        # Passes the current UTC timestamp to feast materialize-incremental
        bash_command=(
            f'cd {FEATURE_REPO} && '
            'feast materialize-incremental "$(date -u +%Y-%m-%dT%H:%M:%S)"'
        ),
    )

    prepare_feast_data >> feast_apply >> feast_materialize
