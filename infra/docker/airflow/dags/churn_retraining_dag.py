"""
DAG: churn_retraining_pipeline
Schedule : weekly, Sunday midnight (0 0 * * 0)
           (also supports manual trigger / TriggerDagRunOperator from churn_feature_pipeline)

Flow:
  train_logistic_regression ─┐
  train_decision_tree        ├─► find_best_model ─► evaluate_best_model ─► register_champion
  train_random_forest        ─┘

Requires volumes in docker-compose:
  - ../../../model_pipeline:/opt/model_pipeline
  - ../../../data-pipeline:/opt/data-pipeline
"""

import os
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# ── Paths inside the Airflow worker container ─────────────────────────────────
MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_PIPELINE = "/opt/model_pipeline"
DATA_PIPELINE = "/opt/data-pipeline"
TRAINING_DATA = (
    f"{DATA_PIPELINE}/churn_feature_store/churn_features/"
    "feature_repo/data/processed_churn_data.parquet"
)

# ── Model metadata ─────────────────────────────────────────────────────────────
MODELS = {
    "logistic_regression": {
        "config": "src/config/logistic_regression.yaml",
        "experiment": "test_churn_prediction_logistic_v1.1",
        "artifact": "logistic_regression_churn",
    },
    "decision_tree": {
        "config": "src/config/decision_tree.yaml",
        "experiment": "test_churn_prediction_decision_tree_v1.1",
        "artifact": "decision_tree_churn",
    },
    "random_forest": {
        "config": "src/config/random_forest.yaml",
        "experiment": "test_churn_prediction_random_forest_v1.1",
        "artifact": "random_forest_churn",
    },
    "xgboost": {
        "config": "src/config/xgboost.yaml",
        "experiment": "test_churn_prediction_xgboost_v1.1",
        "artifact": "xgboost_churn",
    },
    "lightgbm": {
        "config": "src/config/lightgbm.yaml",
        "experiment": "test_churn_prediction_lightgbm_v1.1",
        "artifact": "lightgbm_churn",
    },
    "catboost": {
        "config": "src/config/catboost.yaml",
        "experiment": "test_churn_prediction_catboost_v1.1",
        "artifact": "catboost_churn",
    },
}


# ── Python callables ───────────────────────────────────────────────────────────

def find_best_model(**context):
    """
    Query MLflow for the best run (highest training_f1_score) among the 3 training
    experiments created in the last 3 hours.  Push run_id + model_type to XCom.
    """
    import mlflow  # imported here so Airflow scheduler can parse DAG without mlflow installed

    mlflow.set_tracking_uri(MLFLOW_URI)

    dag_run = context.get("dag_run")
    logical_date = getattr(dag_run, "logical_date", None)
    if logical_date is None:
        from datetime import timezone

        logical_date = datetime.now(timezone.utc)

    cutoff_ms = int((logical_date - timedelta(hours=3)).timestamp() * 1000)

    best_run_id: str | None = None
    best_f1 = -1.0
    best_model_type = None

    for model_type, meta in MODELS.items():
        try:
            runs = mlflow.search_runs(
                experiment_names=[meta["experiment"]],
                filter_string=(
                    "tags.task = 'churn_prediction' and "
                    f"attributes.start_time >= {cutoff_ms} "
                    "and attributes.status = 'FINISHED'"
                ),
                order_by=["metrics.training_f1_score DESC"],
                max_results=1,
            )
        except Exception as exc:
            print(f"[WARN] Could not search experiment {meta['experiment']}: {exc}")
            continue

        if runs.empty:
            print(f"[INFO] No finished runs found for {model_type}")
            continue

        f1 = float(runs.iloc[0].get("metrics.training_f1_score", -1.0))
        print(f"[INFO] {model_type}: training_f1_score={f1:.4f}  run_id={runs.iloc[0]['run_id']}")

        if f1 > best_f1:
            best_f1 = f1
            best_run_id = runs.iloc[0]["run_id"]
            best_model_type = model_type

    if best_run_id is None:
        raise RuntimeError(
            "No finished training runs found in any experiment. "
            "Make sure training tasks completed successfully."
        )

    print(f"[INFO] Best model: {best_model_type}  f1={best_f1:.4f}  run_id={best_run_id}")
    context["ti"].xcom_push(key="best_run_id", value=best_run_id)
    context["ti"].xcom_push(key="best_f1", value=best_f1)
    context["ti"].xcom_push(key="best_model_type", value=best_model_type)
    return best_run_id


def evaluate_best_model(**context):
    """
    Run eval.py for the best model using the run_id from XCom.
    Picks the right config file based on best_model_type.
    """
    run_id = context["ti"].xcom_pull(task_ids="find_best_model", key="best_run_id")
    model_type = context["ti"].xcom_pull(task_ids="find_best_model", key="best_model_type")
    config_path = MODELS[model_type]["config"]

    env = {
        **os.environ,
        "PYTHONPATH": MODEL_PIPELINE,
        "MLFLOW_TRACKING_URI": MLFLOW_URI,
    }

    cmd = [
        sys.executable,
        "src/scripts/eval.py",
        "--config", config_path,
        "--run-id", run_id,
        "--eval-data-path", TRAINING_DATA,
        "--validate-thresholds",
    ]

    print(f"[INFO] Evaluating {model_type} run_id={run_id}")
    subprocess.run(cmd, cwd=MODEL_PIPELINE, check=True, env=env)


def register_champion(**context):
    """
    Register the best model to MLflow Model Registry and set 'champion' alias.
    """
    import mlflow

    mlflow.set_tracking_uri(MLFLOW_URI)

    run_id = context["ti"].xcom_pull(task_ids="find_best_model", key="best_run_id")
    model_type = context["ti"].xcom_pull(task_ids="find_best_model", key="best_model_type")
    artifact_name = MODELS[model_type]["artifact"]

    model_uri = f"runs:/{run_id}/{artifact_name}"
    print(f"[INFO] Registering {model_uri} as customer_churn_model")

    mv = mlflow.register_model(model_uri=model_uri, name="customer_churn_model")

    client = mlflow.MlflowClient()
    client.set_registered_model_alias("customer_churn_model", "champion", mv.version)

    print(f"[INFO] Registered v{mv.version} and set alias 'champion'")
    context["ti"].xcom_push(key="model_version", value=mv.version)
    return mv.version


# ── Helper to build training bash command ─────────────────────────────────────

def _train_cmd(model_type: str) -> str:
    meta = MODELS[model_type]
    return (
        f"cd {MODEL_PIPELINE} && "
        f"PYTHONPATH={MODEL_PIPELINE} "
        f"python src/scripts/train.py "
        f"--config {meta['config']} "
        f"--training-data-path {TRAINING_DATA} "
        "--run-name airflow_" + model_type + "_$(date -u +%Y%m%d_%H%M%S)"
    )


# ── DAG definition ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "mlops",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="churn_retraining_pipeline",
    default_args=default_args,
    description=(
        "Train logistic-regression / decision-tree / random-forest / "
        "xgboost / lightgbm / catboost in parallel, "
        "select best F1, evaluate, and register as champion"
    ),
    schedule="0 0 * * 0",   # weekly – Sunday midnight UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["training", "mlflow", "retraining"],
) as dag:

    # ── Train all 3 models in parallel ────────────────────────────────────────
    train_tasks = {
        model_type: BashOperator(
            task_id=f"train_{model_type}",
            bash_command=_train_cmd(model_type),
        )
        for model_type in MODELS
    }

    # ── Find run with highest f1_score ────────────────────────────────────────
    find_best = PythonOperator(
        task_id="find_best_model",
        python_callable=find_best_model,
    )

    # ── Evaluate best model + validate thresholds ─────────────────────────────
    evaluate_best = PythonOperator(
        task_id="evaluate_best_model",
        python_callable=evaluate_best_model,
    )

    # ── Register to Model Registry + set 'champion' alias ────────────────────
    register_best = PythonOperator(
        task_id="register_champion",
        python_callable=register_champion,
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    for train_task in train_tasks.values():
        train_task >> find_best

    find_best >> evaluate_best >> register_best
