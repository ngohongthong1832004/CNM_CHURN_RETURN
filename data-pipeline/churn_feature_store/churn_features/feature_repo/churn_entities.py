from feast import Entity
from feast.value_type import ValueType

# Define the customer entity
customer = Entity(
    name="customer",
    description="A customer entity with unique ID",
    join_keys=["customer_id"],
    value_type=ValueType.INT64,
    tags={
        "owner": "data_team",
        "domain": "customer_analytics",
        "team": "AIO_mlops"
    }
)