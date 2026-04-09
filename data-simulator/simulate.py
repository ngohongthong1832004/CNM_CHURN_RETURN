"""
Data Simulator — sinh dữ liệu khách hàng churn mới hàng ngày.
Publish JSON records vào Kafka topic (default: churn.raw.events).

Usage:
  python simulate.py --n-records 100
  python simulate.py --n-records 100 --kafka-brokers broker-1:9094
"""

import argparse
import json
from datetime import date, datetime, timezone

import numpy as np
import pandas as pd
from confluent_kafka import Producer
from loguru import logger

# ── Phân phối dựa trên dataset gốc ──────────────────────────────────────────
GENDER_CHOICES     = ["Male", "Female"]
GENDER_PROBS       = [0.50, 0.50]
SUBSCRIPTION_TYPES = ["Basic", "Standard", "Premium"]
SUBSCRIPTION_PROBS = [0.40, 0.35, 0.25]
CONTRACT_LENGTHS   = ["Monthly", "Quarterly", "Annual"]
CONTRACT_PROBS     = [0.50, 0.30, 0.20]
SPEND_PARAMS = {
    "Basic":    (100,  600,  320, 100),
    "Standard": (250,  900,  550, 150),
    "Premium":  (450, 1200,  780, 180),
}


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
def generate_customers(n: int, base_id: int, seed: int | None = None) -> pd.DataFrame:
    rng   = np.random.default_rng(seed)
    now   = datetime.now(timezone.utc)
    today = date.today()

    ages           = rng.integers(18, 71, size=n)
    genders        = rng.choice(GENDER_CHOICES,    size=n, p=GENDER_PROBS)
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
        "customer_id":       np.arange(base_id, base_id + n, dtype="int64"),
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


def _to_json_safe(record: dict) -> dict:
    """Convert numpy/date/datetime types to JSON-serializable Python types."""
    safe = {}
    for k, v in record.items():
        if isinstance(v, datetime):          # datetime trước date (datetime là subclass của date)
            safe[k] = v.isoformat()
        elif isinstance(v, date):
            safe[k] = v.isoformat()
        elif isinstance(v, np.integer):
            safe[k] = int(v)
        elif isinstance(v, np.floating):
            safe[k] = float(v)
        else:
            safe[k] = v
    return safe


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed for key {msg.key()}: {err}")


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Sinh data churn và publish vào Kafka")
    parser.add_argument("--n-records",     type=int,  default=100)
    parser.add_argument("--kafka-brokers", default="broker-1:9094,broker-2:9094,broker-3:9094")
    parser.add_argument("--kafka-topic",   default="churn.raw.events")
    parser.add_argument("--seed",          type=int,  default=None)
    args = parser.parse_args()

    # base_id từ UTC timestamp → unique mỗi lần chạy, tăng đơn điệu
    base_id = int(datetime.now(timezone.utc).timestamp())
    logger.info(f"Sinh {args.n_records} records, base_id={base_id}")

    df = generate_customers(args.n_records, base_id, seed=args.seed)

    producer = Producer({
        "bootstrap.servers": args.kafka_brokers,
        "acks":              "all",
        "retries":           3,
        "retry.backoff.ms":  500,
    })

    published = 0
    for record in df.to_dict(orient="records"):
        safe = _to_json_safe(record)
        producer.produce(
            topic=args.kafka_topic,
            key=str(safe["customer_id"]).encode("utf-8"),
            value=json.dumps(safe).encode("utf-8"),
            on_delivery=delivery_report,
        )
        producer.poll(0)
        published += 1

    producer.flush()

    churn_rate = df["churn"].mean()
    logger.info(
        f"Published {published} records → topic '{args.kafka_topic}' | "
        f"Churn rate: {churn_rate:.1%}"
    )


if __name__ == "__main__":
    main()
