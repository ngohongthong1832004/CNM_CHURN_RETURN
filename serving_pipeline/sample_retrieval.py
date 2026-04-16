from feast import FeatureStore
import pandas as pd
import os
from typing import Union, List

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
repo_path = os.getenv(
    "FEAST_REPO_PATH",
    os.path.join(project_root, "data-pipeline", "churn_feature_store", "churn_features", "feature_repo"),
)

# Validate required env vars at import time so errors are immediately obvious
_redis_conn = os.getenv("FEAST_REDIS_CONNECTION_STRING")
if not _redis_conn:
    raise EnvironmentError(
        "FEAST_REDIS_CONNECTION_STRING is not set. "
        "Add it to serving_pipeline/.env — example: FEAST_REDIS_CONNECTION_STRING=redis:6379"
    )

FEATURES = [
    # ---------- customer_demographics ----------
    "customer_demographics:age",
    "customer_demographics:gender",
    "customer_demographics:tenure_months",
    "customer_demographics:subscription_type",
    "customer_demographics:contract_length",
    # ---------- customer_behavior ----------
    "customer_behavior:usage_frequency",
    "customer_behavior:support_calls",
    "customer_behavior:payment_delay_days",
    "customer_behavior:total_spend",
    "customer_behavior:last_interaction_days",
]


def get_customer_features(customer_id: Union[int, str, List[Union[int, str]]]) -> pd.DataFrame:
    """
    Get features from feature store for customer_id or list of customer_ids
    
    Args:
        customer_id: Customer ID (int, str) or list of customer IDs
        
    Returns:
        DataFrame containing features
    """
    store = FeatureStore(repo_path=repo_path)
    
    # Convert customer_id to list if single value
    if not isinstance(customer_id, list):
        customer_ids = [customer_id]
    else:
        customer_ids = customer_id
    
    # Prepare entity rows — always use string to match Gold table customer_id type (StringType)
    entity_rows = [{"customer_id": str(cid)} for cid in customer_ids]
    
    # Get features from feature store
    df = store.get_online_features(
        entity_rows=entity_rows,
        features=FEATURES,
    ).to_df()
    print(f"Features for customer_id: {customer_id} - Shape: {df.shape}")
    return df


# Main execution when run as script
if __name__ == "__main__":
    entity_rows = [{"customer_id": i} for i in range(2, 40)]
    store = FeatureStore(repo_path=repo_path)
    df = store.get_online_features(
        entity_rows=entity_rows,
        features=FEATURES,
    ).to_df()
    print(df)

