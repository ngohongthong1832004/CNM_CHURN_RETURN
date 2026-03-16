import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import os

def prepare_data_for_feast(input_path, output_path="data/processed_churn_data.parquet"):
    """
    Convert processed churn data to Feast-compatible format
    """
    # Load your processed data
    df = pd.read_csv(input_path)
    
    # Add timestamp columns required by Feast
    current_time = datetime.now()
    
    # Create event timestamp (simulate data from last 90 days)
    df['event_timestamp'] = current_time - pd.to_timedelta(
        np.random.randint(0, 90 * 24 * 60 * 60, size=len(df)),
        unit='s'
    )
    
    # Create created timestamp (when feature was computed)
    df['created_timestamp'] = current_time
    
    # Ensure customer_id is string
    df['customer_id'] = df['CustomerID'].astype(str)
    
    # Map column names to Feast-compatible names
    column_mapping = {
        'Age': 'age',
        'Gender': 'gender',
        'Tenure': 'tenure_months',
        'Usage Frequency': 'usage_frequency',
        'Support Calls': 'support_calls',
        'Payment Delay': 'payment_delay_days',
        'Subscription Type': 'subscription_type',
        'Contract Length': 'contract_length',
        'Total Spend': 'total_spend',
        'Last Interaction': 'last_interaction_days',
        'Churn': 'churned',
        'Tenure_Age_Ratio': 'tenure_age_ratio',
        'Spend_per_Usage': 'spend_per_usage',
        'Support_Calls_per_Tenure': 'support_calls_per_tenure',
    }
    
    # Rename columns
    df = df.rename(columns=column_mapping)
    
    # Add any missing engineered features
    if 'avg_monthly_spend' not in df.columns:
        df['avg_monthly_spend'] = df['total_spend'] / np.maximum(df['tenure_months'], 1)
    
    if 'churn_risk_score' not in df.columns:
        # Simple risk score calculation
        df['churn_risk_score'] = (
            df['payment_delay_days'] * 0.3 +
            (df['support_calls'] / np.maximum(df['tenure_months'], 1)) * 0.2 +
            (1 - (df['last_interaction_days'] / 30)) * 0.5
        ).clip(0, 1)
    
    # Select and order columns for Feast
    feast_columns = [
        'customer_id',
        'event_timestamp',
        'created_timestamp',
        'age',
        'gender',
        'tenure_months',
        'usage_frequency',
        'support_calls',
        'payment_delay_days',
        'subscription_type',
        'contract_length',
        'total_spend',
        'last_interaction_days',
        'tenure_age_ratio',
        'spend_per_usage',
        'support_calls_per_tenure',
        'avg_monthly_spend',
        'churn_risk_score',
        'churned'
    ]
    
    df_feast = df[feast_columns].copy()
    
    # Save as Parquet (Feast recommended format)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_feast.to_parquet(output_path, index=False)
    
    print(f"Data prepared for Feast. Shape: {df_feast.shape}")
    print(f"Saved to: {output_path}")
    
    return df_feast

if __name__ == "__main__":
    # Update this path to your processed data
    input_file = "../../../data/processed/df_processed.csv"
    prepare_data_for_feast(input_file)