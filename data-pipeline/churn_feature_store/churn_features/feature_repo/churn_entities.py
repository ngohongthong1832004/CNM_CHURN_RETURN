from feast import Entity

# customer_id is stored as STRING in the Gold parquet (silver_to_gold does .astype(str))
# Do NOT add value_type — newer Feast versions infer the type from the data source,
# and importing feast.value_type.ValueType can silently fail on some installs,
# causing feast apply to produce an empty registry.
customer = Entity(
    name="customer",
    description="A customer entity with unique ID",
    join_keys=["customer_id"],
    tags={
        "owner": "data_team",
        "domain": "customer_analytics",
        "team": "AIO_mlops"
    }
)