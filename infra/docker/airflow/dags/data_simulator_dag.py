"""
DAG: data_simulator
Schedule: hàng ngày 23:45 UTC

Flow:
  simulate_to_kafka → kafka_to_bronze

  simulate_to_kafka : sinh ~100 records, publish JSON vào Kafka topic churn.raw.events
  kafka_to_bronze   : consume từ Kafka → parse → append vào Iceberg Bronze table

Sau đó lakehouse_etl_dag (00:00) đọc Bronze → Silver → Gold → parquet
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# ── Config ─────────────────────────────────────────────────────────────────────
DATA_SIMULATOR = "/opt/data-simulator"

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "broker-1:9094,broker-2:9094,broker-3:9094")
KAFKA_TOPIC   = os.environ.get("KAFKA_TOPIC",   "churn.raw.events")

NESSIE_URI   = os.environ.get("NESSIE_URI",             "http://nessie:19120/iceberg")
MINIO_URL    = os.environ.get("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
MINIO_KEY    = os.environ.get("AWS_ACCESS_KEY_ID",       "minio")
MINIO_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY",   "minio123")

CATALOG_PROPS = {
    "type":                 "rest",
    "uri":                  NESSIE_URI,
    "warehouse":            "s3://lakehouse/",
    "s3.endpoint":          MINIO_URL,
    "s3.access-key-id":     MINIO_KEY,
    "s3.secret-access-key": MINIO_SECRET,
    "s3.region":            "us-east-1",
    "s3.path-style-access": "true",
    "py-io-impl":           "pyiceberg.io.pyarrow.PyArrowFileIO",
}


# ── Helper ─────────────────────────────────────────────────────────────────────
def _namespace_exists(catalog, namespace: tuple) -> bool:
    """Compatibility helper cho pyiceberg catalog APIs."""
    if hasattr(catalog, "namespace_exists"):
        return catalog.namespace_exists(namespace)
    if hasattr(catalog, "load_namespace_properties"):
        try:
            catalog.load_namespace_properties(namespace)
            return True
        except Exception:
            return False
    return False


def _build_bronze_schema():
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        DateType, FloatType, LongType,
        NestedField, StringType, TimestamptzType,
    )
    return Schema(
        NestedField(1,  "customer_id",       LongType(),        required=True),
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


# ── Task: consume Kafka → Iceberg Bronze ───────────────────────────────────────
def kafka_to_bronze(**_):
    """
    Consume tất cả records mới từ Kafka topic churn.raw.events,
    parse JSON, type-cast, và append vào Iceberg Bronze table.
    """
    import json

    import pandas as pd
    import pyarrow as pa
    from confluent_kafka import Consumer, KafkaError
    from pyiceberg.catalog import load_catalog

    # ── Iceberg setup ──────────────────────────────────────────────────────────
    catalog = load_catalog("nessie", **CATALOG_PROPS)

    if not _namespace_exists(catalog, ("bronze",)):
        catalog.create_namespace(("bronze",))
        print("[OK] Created namespace bronze")

    if not catalog.table_exists("bronze.customer_events"):
        catalog.create_table("bronze.customer_events", schema=_build_bronze_schema())
        print("[OK] Created table bronze.customer_events")

    # ── Kafka consumer ─────────────────────────────────────────────────────────
    consumer = Consumer({
        "bootstrap.servers":   KAFKA_BROKERS,
        "group.id":            "airflow-bronze-consumer",
        "auto.offset.reset":   "earliest",
        "enable.auto.commit":  False,
        "session.timeout.ms":  30000,
        "max.poll.interval.ms": 300000,
    })
    consumer.subscribe([KAFKA_TOPIC])

    records    = []
    empty_cnt  = 0
    MAX_EMPTY  = 10   # 10 × 1 s timeout liên tiếp → dừng

    try:
        while empty_cnt < MAX_EMPTY:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                empty_cnt += 1
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    empty_cnt += 1
                else:
                    raise RuntimeError(f"Kafka error: {msg.error()}")
                continue

            empty_cnt = 0
            records.append(json.loads(msg.value().decode("utf-8")))

        consumer.commit()
        print(f"[Kafka] Consumed {len(records)} records from '{KAFKA_TOPIC}'")
    finally:
        consumer.close()

    if not records:
        print("[SKIP] Không có records mới trong Kafka topic.")
        return

    # ── Type-cast ──────────────────────────────────────────────────────────────
    df = pd.DataFrame(records)

    df["ingest_date"] = pd.to_datetime(df["ingest_date"]).dt.date
    df["created_at"]  = pd.to_datetime(df["created_at"], utc=True)

    for col in ["customer_id", "age", "tenure", "usage_frequency",
                "support_calls", "payment_delay", "last_interaction", "churn"]:
        df[col] = df[col].astype("int64")
    df["total_spend"] = df["total_spend"].astype("float32")

    # ── Ghi vào Bronze ─────────────────────────────────────────────────────────
    bronze = catalog.load_table("bronze.customer_events")
    arrow_batch = pa.Table.from_pandas(df, schema=bronze.schema().as_arrow())
    bronze.append(arrow_batch)

    print(f"[OK] Appended {len(df)} records → bronze.customer_events")


# ── DAG ────────────────────────────────────────────────────────────────────────
default_args = {
    "owner":            "mlops",
    "depends_on_past":  False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="data_simulator",
    default_args=default_args,
    description=(
        "Sinh ~100 bản ghi khách hàng churn mới mỗi ngày, "
        "publish vào Kafka topic churn.raw.events, "
        "sau đó consume và ghi vào Iceberg Bronze table"
    ),
    schedule="45 23 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["data-simulator", "kafka", "lakehouse"],
) as dag:

    simulate_to_kafka = BashOperator(
        task_id="simulate_to_kafka",
        bash_command=(
            f"python {DATA_SIMULATOR}/simulate.py "
            "--n-records 100 "
            f"--kafka-brokers '{KAFKA_BROKERS}' "
            f"--kafka-topic {KAFKA_TOPIC}"
        ),
        env={"PYTHONPATH": DATA_SIMULATOR},
    )

    to_bronze = PythonOperator(
        task_id="kafka_to_bronze",
        python_callable=kafka_to_bronze,
    )

    simulate_to_kafka >> to_bronze
