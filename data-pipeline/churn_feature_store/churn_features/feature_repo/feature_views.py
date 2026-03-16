from feast import FeatureView, Field
from feast.types import Float32, Int64, String
from datetime import timedelta
from churn_entities import customer
from data_sources import customer_stats_source

# Customer demographics feature view
customer_demographics = FeatureView(
    name="customer_demographics",
    entities=[customer],
    ttl=None,
    schema=[
        Field(name="age", dtype=Float32),
        Field(name="gender", dtype=String),
        Field(name="tenure_months", dtype=Float32),
        Field(name="subscription_type", dtype=String),
        Field(name="contract_length", dtype=String),
    ],
    source=customer_stats_source,
    online=True
)

# Customer behavior feature view
customer_behavior = FeatureView(
    name="customer_behavior",
    entities=[customer],
    ttl=None,
    schema=[
        Field(name="usage_frequency", dtype=Float32),
        Field(name="support_calls", dtype=Float32),
        Field(name="payment_delay_days", dtype=Float32),
        Field(name="total_spend", dtype=Float32),
        Field(name="last_interaction_days", dtype=Float32),
    ],
    source=customer_stats_source,
    online=True
)

# Churn target feature view
churn_target = FeatureView(
    name="churn_target",
    entities=[customer],
    ttl=None,
    schema=[
        Field(name="churned", dtype=Int64),
    ],
    source=customer_stats_source,
    online=True
)