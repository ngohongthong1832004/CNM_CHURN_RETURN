"""
DAG: lakehouse_etl
Schedule: hàng ngày 00:00 UTC

Flow:
  init_namespaces → bronze_to_silver → silver_to_gold → export_gold_parquet

Layers:
  Bronze  iceberg.bronze.customer_events  — raw từ simulator
  Silver  iceberg.silver.customers        — cleaned + deduplicated
  Gold    iceberg.gold.churn_features     — features ready cho training + Feast

Output:
  /opt/data-pipeline/churn_feature_store/churn_features/
    feature_repo/data/processed_churn_data.parquet   ← Feast + training dùng
"""

import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ── Config ─────────────────────────────────────────────────────────────────────
NESSIE_URI  = os.environ.get("NESSIE_URI", "http://nessie:19120/iceberg")
MINIO_URL   = os.environ.get("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
AWS_KEY     = os.environ.get("AWS_ACCESS_KEY_ID", "minio")
AWS_SECRET  = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio123")
AWS_REGION  = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
WAREHOUSE   = "s3://lakehouse/"

FEAST_PARQUET = (
    "/opt/data-pipeline/churn_feature_store/churn_features/"
    "feature_repo/data/processed_churn_data.parquet"
)

CATALOG_PROPS = {
    "type": "rest",
    "uri": NESSIE_URI,
    "warehouse": WAREHOUSE,
    "s3.endpoint": MINIO_URL,
    "s3.access-key-id": AWS_KEY,
    "s3.secret-access-key": AWS_SECRET,
    "s3.region": AWS_REGION,
    "s3.path-style-access": "true",
    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
}


def namespace_exists(catalog, namespace):
    """Compatibility helper for pyiceberg catalog APIs across versions."""
    if hasattr(catalog, "namespace_exists"):
        return catalog.namespace_exists(namespace)

    if hasattr(catalog, "list_namespaces"):
        try:
            return namespace in catalog.list_namespaces()
        except Exception:
            return False

    if hasattr(catalog, "load_namespace_properties"):
        try:
            catalog.load_namespace_properties(namespace)
            return True
        except Exception:
            return False

    return False


# ── Task functions ─────────────────────────────────────────────────────────────

def init_namespaces(**_):
    """Tạo namespace bronze/silver/gold nếu chưa có."""
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog("nessie", **CATALOG_PROPS)
    for ns in [("bronze",), ("silver",), ("gold",)]:
        if not namespace_exists(catalog, ns):
            catalog.create_namespace(ns)
            print(f"[OK] Created namespace {ns[0]}")
        else:
            print(f"[SKIP] Namespace {ns[0]} already exists")


def bronze_to_silver(**context):
    """
    Đọc toàn bộ Bronze → dedup theo customer_id (giữ bản mới nhất)
    → validate → ghi đè Silver.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        NestedField, StringType, LongType, FloatType, TimestamptzType, DateType
    )

    catalog = load_catalog("nessie", **CATALOG_PROPS)

    # ── Bronze schema ──────────────────────────────────────────────────────────
    bronze_schema = Schema(
        NestedField(1,  "customer_id",       LongType(),       required=True),
        NestedField(2,  "age",               LongType()),
        NestedField(3,  "gender",            StringType()),
        NestedField(4,  "tenure",            LongType()),
        NestedField(5,  "usage_frequency",   LongType()),
        NestedField(6,  "support_calls",     LongType()),
        NestedField(7,  "payment_delay",     LongType()),
        NestedField(8,  "subscription_type", StringType()),
        NestedField(9,  "contract_length",   StringType()),
        NestedField(10, "total_spend",       FloatType()),
        NestedField(11, "last_interaction",  LongType()),
        NestedField(12, "churn",             LongType()),
        NestedField(13, "ingest_date",       DateType()),
        NestedField(14, "created_at",        TimestamptzType()),
    )

    # ── Silver schema ─────────────────────────────────────────────────────────
    silver_schema = bronze_schema  # same, deduped

    # Đảm bảo bảng tồn tại
    for tbl_id, schema in [
        ("bronze.customer_events", bronze_schema),
        ("silver.customers",       silver_schema),
    ]:
        if not catalog.table_exists(tbl_id):
            catalog.create_table(tbl_id, schema=schema)
            print(f"[OK] Created table {tbl_id}")

    # Đọc Bronze
    bronze = catalog.load_table("bronze.customer_events")
    df = bronze.scan().to_pandas()
    print(f"[Bronze] rows={len(df)}")

    if df.empty:
        print("[SKIP] No bronze data yet.")
        return

    # Dedup: giữ bản mới nhất theo created_at
    df = df.sort_values("created_at").drop_duplicates("customer_id", keep="last")

    # Validate
    df = df[df["age"].between(18, 100)]
    df = df[df["tenure"].between(0, 120)]
    df = df[df["total_spend"] > 0]
    df = df.dropna(subset=["customer_id", "churn"])

    print(f"[Silver] rows after clean={len(df)}")

    # Ghi đè Silver (overwrite)
    silver = catalog.load_table("silver.customers")
    silver.overwrite(pa.Table.from_pandas(df, schema=silver.schema().as_arrow()))
    print("[OK] Silver table updated.")


def silver_to_gold(**context):
    """
    Đọc Silver → tính derived features → ghi đè Gold.
    Schema Gold khớp với Feast feature views.
    """
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    from datetime import timezone as tz
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        NestedField, StringType, LongType, FloatType, TimestamptzType
    )

    catalog = load_catalog("nessie", **CATALOG_PROPS)

    # ── Gold schema (Feast-compatible) ────────────────────────────────────────
    gold_schema = Schema(
        NestedField(1,  "customer_id",               StringType(),      required=True),
        NestedField(2,  "event_timestamp",            TimestamptzType(), required=True),
        NestedField(3,  "created_timestamp",          TimestamptzType()),
        NestedField(4,  "age",                        FloatType()),
        NestedField(5,  "gender",                     StringType()),
        NestedField(6,  "tenure_months",              FloatType()),
        NestedField(7,  "usage_frequency",            FloatType()),
        NestedField(8,  "support_calls",              FloatType()),
        NestedField(9,  "payment_delay_days",         FloatType()),
        NestedField(10, "subscription_type",          StringType()),
        NestedField(11, "contract_length",            StringType()),
        NestedField(12, "total_spend",                FloatType()),
        NestedField(13, "last_interaction_days",      FloatType()),
        NestedField(14, "churned",                    LongType()),
        NestedField(15, "tenure_age_ratio",           FloatType()),
        NestedField(16, "spend_per_usage",            FloatType()),
        NestedField(17, "support_calls_per_tenure",   FloatType()),
        NestedField(18, "spending_group",             StringType()),
        NestedField(19, "tenure_group",               StringType()),
    )

    if not catalog.table_exists("gold.churn_features"):
        catalog.create_table("gold.churn_features", schema=gold_schema)
        print("[OK] Created table gold.churn_features")

    silver = catalog.load_table("silver.customers")
    df = silver.scan().to_pandas()
    print(f"[Silver] rows={len(df)}")

    if df.empty:
        print("[SKIP] Silver is empty.")
        return

    now = datetime.now(tz.utc)

    # ── Rename + feature engineering ─────────────────────────────────────────
    gold = pd.DataFrame()
    gold["customer_id"]          = df["customer_id"].astype(str)
    gold["event_timestamp"]      = pd.to_datetime(df["created_at"], utc=True)
    gold["created_timestamp"]    = now
    gold["age"]                  = df["age"].astype(float)
    gold["gender"]               = df["gender"]
    gold["tenure_months"]        = df["tenure"].astype(float)
    gold["usage_frequency"]      = df["usage_frequency"].astype(float)
    gold["support_calls"]        = df["support_calls"].astype(float)
    gold["payment_delay_days"]   = df["payment_delay"].astype(float)
    gold["subscription_type"]    = df["subscription_type"]
    gold["contract_length"]      = df["contract_length"]
    gold["total_spend"]          = df["total_spend"].astype(float)
    gold["last_interaction_days"] = df["last_interaction"].astype(float)
    gold["churned"]              = df["churn"].astype("int64")

    gold["tenure_age_ratio"]       = gold["tenure_months"] / np.maximum(gold["age"], 1)
    gold["spend_per_usage"]        = gold["total_spend"] / np.maximum(gold["usage_frequency"], 1)
    gold["support_calls_per_tenure"] = gold["support_calls"] / np.maximum(gold["tenure_months"], 1)

    gold["spending_group"] = pd.cut(
        gold["total_spend"],
        bins=[0, 250, 500, 750, float("inf")],
        labels=["Low", "Medium", "High", "Very High"],
    ).astype(str)

    gold["tenure_group"] = pd.cut(
        gold["tenure_months"],
        bins=[0, 12, 24, 36, float("inf")],
        labels=["<1yr", "1-2yr", "2-3yr", "3+yr"],
    ).astype(str)

    print(f"[Gold] rows={len(gold)}")

    gold_tbl = catalog.load_table("gold.churn_features")
    gold_tbl.overwrite(pa.Table.from_pandas(gold, schema=gold_tbl.schema().as_arrow()))
    print("[OK] Gold table updated.")


def export_gold_parquet(**context):
    """
    Export Gold table → parquet file cho Feast + model training.
    """
    import os
    from pathlib import Path
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog("nessie", **CATALOG_PROPS)

    if not catalog.table_exists("gold.churn_features"):
        raise RuntimeError("Gold table does not exist. Run silver_to_gold first.")

    gold = catalog.load_table("gold.churn_features")
    df = gold.scan().to_pandas()
    print(f"[Export] Gold rows={len(df)}")

    out = Path(FEAST_PARQUET)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"[OK] Exported Gold → {out}  ({out.stat().st_size // 1024} KB)")


# ── DAG ────────────────────────────────────────────────────────────────────────

default_args = {
    "owner": "mlops",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="lakehouse_etl",
    default_args=default_args,
    description="Bronze→Silver→Gold ETL + export parquet cho Feast/training",
    schedule="0 0 * * *",       # 00:00 UTC hàng ngày (sau simulator 23:45)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["lakehouse", "etl", "iceberg"],
) as dag:

    t_init = PythonOperator(
        task_id="init_namespaces",
        python_callable=init_namespaces,
    )

    t_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=bronze_to_silver,
    )

    t_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=silver_to_gold,
    )

    t_export = PythonOperator(
        task_id="export_gold_parquet",
        python_callable=export_gold_parquet,
    )

    t_init >> t_silver >> t_gold >> t_export
