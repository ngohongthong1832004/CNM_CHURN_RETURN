import pandas as pd

df = pd.read_parquet("churn_feature_store/churn_features/feature_repo/data/processed_churn_data.parquet")

customer_ids = df["customer_id"].unique()
print("Total unique customers:", len(customer_ids))
print(df.head())