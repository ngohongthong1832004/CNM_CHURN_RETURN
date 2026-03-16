import pandas as pd
import numpy as np
from typing import Dict, Any

# Mapping dictionaries
GENDER_MAPPING = {'Male': 1, 'Female': 0}
SUBSCRIPTION_MAPPING = {'Basic': 0, 'Standard': 1, 'Premium': 2}
CONTRACT_MAPPING = {'Monthly': 0, 'Quarterly': 1, 'Annual': 2}

# Feature groups with NEW column names
NUMERICAL_FEATURES = [
    'age', 'tenure_months', 'usage_frequency', 'support_calls',
    'payment_delay_days', 'total_spend', 'last_interaction_days'
]

CATEGORICAL_FEATURES = ['gender', 'subscription_type', 'contract_length']

# Engineered features (matching model requirements)
ENGINEERED_FEATURES = [
    'spend_per_usage', 
    'support_calls_per_tenure',
    'avg_monthly_spend'
]


def map_schema_to_preprocessing(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map schema field names to preprocessing field names
    
    Args:
        data: Dictionary with schema field names (Age, Tenure, etc.)
        
    Returns:
        Dictionary with preprocessing field names (age, tenure_months, etc.)
    """
    mapping = {
        'Age': 'age',
        'Tenure': 'tenure_months',
        'Usage_Frequency': 'usage_frequency',
        'Support_Calls': 'support_calls',
        'Payment_Delay': 'payment_delay_days',
        'Total_Spend': 'total_spend',
        'Last_Interaction': 'last_interaction_days',
        'Gender': 'gender',
        'Subscription_Type': 'subscription_type',
        'Contract_Length': 'contract_length'
    }
    
    # Convert to lowercase keys for case-insensitive matching
    data_lower = {k.lower(): v for k, v in data.items()}
    mapped_data = {}
    
    for schema_key, preprocess_key in mapping.items():
        # Try exact match first, then case-insensitive
        if schema_key in data:
            mapped_data[preprocess_key] = data[schema_key]
        elif schema_key.lower() in data_lower:
            mapped_data[preprocess_key] = data_lower[schema_key.lower()]
        elif preprocess_key in data:
            # Already in correct format
            mapped_data[preprocess_key] = data[preprocess_key]
    
    return mapped_data
def validate_input(data: Dict[str, Any]) -> tuple[bool, str]:
    """
    Validate input data (supports both schema and preprocessing formats)
    
    Args:
        data: Dictionary with customer features (schema or preprocessing format)
        
    Returns:
        (is_valid, error_message)
    """
    # Map schema to preprocessing format for validation
    if any(key[0].isupper() for key in data.keys()):  # Schema format detected
        data = map_schema_to_preprocessing(data)
    
    # Required fields (using preprocessing column names)
    required_fields = [
        'age', 'gender', 'tenure_months', 'usage_frequency',
        'support_calls', 'payment_delay_days', 'subscription_type',
        'contract_length', 'total_spend', 'last_interaction_days'
    ]
    
    # Check missing fields
    missing_fields = [f for f in required_fields if f not in data]
    if missing_fields:
        return False, f"Missing required fields: {', '.join(missing_fields)}"
    
    # Validate ranges
    validations = [
        ('age', 18, 100),
        ('tenure_months', 0, 72),
        ('usage_frequency', 0, 30),
        ('support_calls', 0, 20),
        ('payment_delay_days', 0, 60),
        ('total_spend', 0, 10000),
        ('last_interaction_days', 0, 365)
    ]
    
    for field, min_val, max_val in validations:
        value = data.get(field)
        if value is None:
            continue
        if not isinstance(value, (int, float)):
            return False, f"{field} must be a number"
        if not (min_val <= value <= max_val):
            return False, f"{field} must be between {min_val} and {max_val}"
    
    # Validate categorical fields
    gender = data.get('gender')
    if gender not in ['Male', 'Female', 'male', 'female']:
        return False, "gender must be 'Male' or 'Female'"
    
    subscription_type = data.get('subscription_type')
    if subscription_type not in ['Basic', 'Standard', 'Premium', 'basic', 'standard', 'premium']:
        return False, "subscription_type must be 'Basic', 'Standard', or 'Premium'"
    
    contract_length = data.get('contract_length')
    if contract_length not in ['Monthly', 'Quarterly', 'Annual', 'monthly', 'quarterly', 'annual']:
        return False, "contract_length must be 'Monthly', 'Quarterly', or 'Annual'"
    
    return True, ""

def save_production_data(data: Dict[str, Any], prediction: int, 
                        production_file: str = None):
    """
    Save prediction to production dataset for drift monitoring
    
    Args:
        data: Input data dictionary
        prediction: Model prediction (0 or 1)
        production_file: Path to save production data 
    """
    import os
    
    # Set default path relative to serving_pipeline directory
    if production_file is None:
        # Get the directory where this file is located
        current_dir = os.path.dirname(os.path.abspath(__file__))
        production_file = os.path.join(current_dir, "data_model", "production", "production.csv")
    
    # Add metadata
    record = data.copy()
    record['prediction'] = prediction
    
    # Create DataFrame
    df_new = pd.DataFrame([record])
    
    # Append to file
    if os.path.exists(production_file):
        df_existing = pd.read_csv(production_file)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new
    
    # Save
    os.makedirs(os.path.dirname(production_file), exist_ok=True)
    df_combined.to_csv(production_file, index=False)
    
    # Log for debugging
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"Saved production data to: {production_file} (Total records: {len(df_combined)})")
    
    return len(df_combined)


def get_feature_names() -> list:
    """
    Get final feature names for model training
    
    Returns:
        List of feature names
    """
    return (
        NUMERICAL_FEATURES + 
        ['gender_male', 'subscription_type_encoded', 'contract_length_encoded'] +
        ENGINEERED_FEATURES
    )


# Example usage and testing
if __name__ == "__main__":
    # Test with sample data
    sample_data = {
        'age': 35,
        'gender': 'Male',
        'tenure_months': 24,
        'usage_frequency': 15,
        'support_calls': 3,
        'payment_delay_days': 5,
        'subscription_type': 'Premium',
        'contract_length': 'Annual',
        'total_spend': 500,
        'last_interaction_days': 7
    }
    
    # Validate
    is_valid, error_msg = validate_input(sample_data)
    print(f"Validation: {is_valid}")
    if not is_valid:
        print(f"Error: {error_msg}")
    
    # Preprocess
    df_processed = preprocess_input(sample_data)
    print(f"\nProcessed features: {df_processed.columns.tolist()}")
    print(f"\nProcessed data:\n{df_processed}")
    
    # Get feature names
    print(f"\nExpected features for model: {get_feature_names()}")