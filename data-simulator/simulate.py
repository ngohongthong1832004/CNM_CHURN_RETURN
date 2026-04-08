"""
Data Simulator — sinh dữ liệu khách hàng churn mới hàng ngày.
Ghi vào Iceberg Bronze table (lakehouse.bronze.customer_events) trên MinIO/Nessie.

Usage:
  python simulate.py --n-records 100
  python simulate.py --n-records 50 --nessie-uri http://localhost:19120/iceberg
"""

import argparse
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
from loguru import logger
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DateType,
    FloatType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)


def namespace_exists(catalog, namespace: tuple[str, ...]) -> bool:
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

# ── Phân phối dựa trên dataset gốc ──────────────────────────────────────────
GENDER_CHOICES   = ["Male", "Female"]
GENDER_PROBS     = [0.50, 0.50]
SUBSCRIPTION_TYPES = ["Basic", "Standard", "Premium"]
SUBSCRIPTION_PROBS = [0.40, 0.35, 0.25]
CONTRACT_LENGTHS = ["Monthly", "Quarterly", "Annual"]
CONTRACT_PROBS   = [0.50, 0.30, 0.20]
SPEND_PARAMS = {
    "Basic":    (100,  600,  320, 100),
    "Standard": (250,  900,  550, 150),
    "Premium":  (450, 1200,  780, 180),
}

# ── Bronze Iceberg schema ────────────────────────────────────────────────────
BRONZE_SCHEMA = Schema(
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


# ── Churn probability ────────────────────────────────────────────────────────
def _churn_prob(support_calls, payment_delay, usage_freq, contract, tenure):
    score = 0.18
    score += (support_calls / 10) * 0.30
    score += (payment_delay / 30) * 0.20
    score += (1 - usage_freq / 30) * 0.15
    score += {"Monthly": +0.10, "Quarterly": 0.0, "Annual": -0.10}.get(contract, 0.0)
    score += (1 - min(tenure, 36) / 36) * 0.10
    return float(np.clip(score, 0.05, 0.95))


# ── Sinh dữ liệu ────────────────────────────────────────────────────────────
def generate_customers(n: int, start_id: int, seed: int | None = None) -> pd.DataFrame:
    rng  = np.random.default_rng(seed)
    now  = datetime.now(timezone.utc)
    today = date.today()

    ages           = rng.integers(18, 71, size=n)
    genders        = rng.choice(GENDER_CHOICES,   size=n, p=GENDER_PROBS)
    tenures        = rng.integers(1, 61, size=n)
    usage_freqs    = rng.integers(1, 31, size=n)
    support_calls  = rng.integers(0, 11, size=n)
    payment_delays = rng.integers(0, 31, size=n)
    subscriptions  = rng.choice(SUBSCRIPTION_TYPES, size=n, p=SUBSCRIPTION_PROBS)
    contracts      = rng.choice(CONTRACT_LENGTHS,   size=n, p=CONTRACT_PROBS)
    last_ints      = rng.integers(1, 31, size=n)

    total_spends = np.zeros(n)
    for sub, (lo, hi, mu, sigma) in SPEND_PARAMS.items():
        mask = subscriptions == sub
        total_spends[mask] = np.clip(rng.normal(mu, sigma, mask.sum()), lo, hi).round(2)

    churns = np.array([
        int(rng.random() < _churn_prob(
            support_calls[i], payment_delays[i],
            usage_freqs[i], contracts[i], tenures[i]
        ))
        for i in range(n)
    ])

    return pd.DataFrame({
        "customer_id":       np.arange(start_id, start_id + n, dtype="int64"),
        "age":               ages.astype("int64"),
        "gender":            genders,
        "tenure":            tenures.astype("int64"),
        "usage_frequency":   usage_freqs.astype("int64"),
        "support_calls":     support_calls.astype("int64"),
        "payment_delay":     payment_delays.astype("int64"),
        "subscription_type": subscriptions,
        "contract_length":   contracts,
        "total_spend":       total_spends.astype("float32"),
        "last_interaction":  last_ints.astype("int64"),
        "churn":             churns.astype("int64"),
        "ingest_date":       today,
        "created_at":        now,
    })


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Sinh data churn vào Iceberg Bronze")
    parser.add_argument("--n-records",   type=int, default=100)
    parser.add_argument("--nessie-uri",  default="http://nessie:19120/iceberg")
    parser.add_argument("--minio-url",   default="http://minio:9000")
    parser.add_argument("--minio-key",   default="minio")
    parser.add_argument("--minio-secret",default="minio123")
    parser.add_argument("--warehouse",   default="s3://lakehouse/")
    parser.add_argument("--seed",        type=int, default=None)
    args = parser.parse_args()

    catalog_props = {
        "type": "rest",
        "uri": args.nessie_uri,
        "warehouse": args.warehouse,
        "s3.endpoint": args.minio_url,
        "s3.access-key-id": args.minio_key,
        "s3.secret-access-key": args.minio_secret,
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    }

    catalog = load_catalog("nessie", **catalog_props)

    # Tạo namespace + table nếu chưa có
    if not namespace_exists(catalog, ("bronze",)):
        catalog.create_namespace(("bronze",))

    if not catalog.table_exists("bronze.customer_events"):
        catalog.create_table("bronze.customer_events", schema=BRONZE_SCHEMA)
        logger.info("Created table bronze.customer_events")

    table = catalog.load_table("bronze.customer_events")

    # Lấy max customer_id hiện tại
    df_existing = table.scan(selected_fields=("customer_id",)).to_pandas()
    start_id = int(df_existing["customer_id"].max()) + 1 if len(df_existing) else 1
    logger.info(f"Records hiện tại: {len(df_existing):,} | Bắt đầu từ ID: {start_id}")

    # Sinh và ghi
    new_df = generate_customers(args.n_records, start_id, seed=args.seed)
    arrow_table = pa.Table.from_pandas(new_df, schema=table.schema().as_arrow())
    table.append(arrow_table)

    churn_rate = new_df["churn"].mean()
    logger.info(
        f"Đã ghi {args.n_records} records vào Bronze | "
        f"Churn rate: {churn_rate:.1%} | "
        f"Ngày: {date.today()}"
    )


if __name__ == "__main__":
    main()
