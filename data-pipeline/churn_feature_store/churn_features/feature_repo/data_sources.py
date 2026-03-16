from feast import FileSource
from datetime import datetime

# Define your data source
customer_stats_source = FileSource(
    name="customer_stats_source",
    path="data/processed_churn_data.parquet",  # Your processed data
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)