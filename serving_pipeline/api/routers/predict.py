"""
Prediction endpoints with batch support
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from schemas import ChurnInput, ChurnPrediction, CustomerIdInput
from metrics import PREDICTIONS, BATCH_SIZE, FEAST_LOOKUPS
from pre_processing import validate_input, save_production_data, map_schema_to_preprocessing
from load_model import load_model
from sample_retrieval import get_customer_features
import logging
import pandas as pd
from typing import List
from dotenv import load_dotenv
load_dotenv()

router = APIRouter(prefix="/predict", tags=["Prediction"])
logger = logging.getLogger(__name__)

# Model will be loaded lazily on first use
_model = None
MODEL_URI = os.getenv("MODEL_URI")
if MODEL_URI is None:
    raise ValueError("MODEL_URI environment variable not set")


def get_model():
    """Lazy load model - only load when needed"""
    global _model
    if _model is None:
        logger.info("Loading model from MLflow...")
        _model = load_model(model_uri=MODEL_URI)
        logger.info("Model loaded successfully")
    return _model


def _build_model_input(mapped_data: dict) -> pd.DataFrame:
    """Build a model-ready DataFrame from normalized feature names."""
    is_valid, error_msg = validate_input(mapped_data)
    if not is_valid:
        raise HTTPException(status_code=422, detail=error_msg)

    df_input = pd.DataFrame([mapped_data])
    for col in df_input.select_dtypes(include="number").columns:
        df_input[col] = df_input[col].astype("float32")
    return df_input


def _predict_from_mapped_data(mapped_data: dict) -> int:
    """Predict churn from normalized feature names."""
    df_input = _build_model_input(mapped_data)
    logger.info(f"Input DataFrame shape: {df_input.shape}, columns: {df_input.columns.tolist()}")
    logger.info(f"DataFrame dtypes: {df_input.dtypes.to_dict()}")

    model = get_model()
    prediction = model.predict(df_input)[0]
    return int(prediction)


def _normalize_gender(value) -> str:
    """Normalize gender to 'Male' or 'Female' regardless of Feast encoding."""
    if value in ("Male", "male"):
        return "Male"
    if value in ("Female", "female"):
        return "Female"
    # Encoded as integer/float: GENDER_MAPPING = {'Male': 1, 'Female': 0}
    try:
        if int(float(value)) == 1:
            return "Male"
        if int(float(value)) == 0:
            return "Female"
    except (TypeError, ValueError):
        pass
    return str(value) if value is not None else ""


def _normalize_categorical(value, title_case: bool = True) -> str:
    """Strip whitespace and optionally title-case a categorical string from Feast."""
    if value is None:
        return ""
    s = str(value).strip()
    return s.title() if title_case else s


def _fetch_mapped_features_from_feast(customer_id: str) -> dict:
    """Fetch online features from Feast and normalize them for model input."""
    df = get_customer_features(customer_id)
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No online features found for customer_id={customer_id}")

    feature_row = df.iloc[0].to_dict()
    entity_value = feature_row.get("customer_id")
    if entity_value is None or pd.isna(entity_value):
        raise HTTPException(status_code=404, detail=f"Feast returned no entity row for customer_id={customer_id}")

    logger.info(f"Feast raw features for {customer_id}: {feature_row}")

    # Feast returns a row with all-None features when the entity has never been
    # materialized — detect this early and return 404 instead of letting
    # downstream validation produce a confusing 422.
    feature_keys = [
        "age", "gender", "tenure_months", "usage_frequency", "support_calls",
        "payment_delay_days", "subscription_type", "contract_length",
        "total_spend", "last_interaction_days",
    ]
    if all(feature_row.get(k) is None for k in feature_keys):
        raise HTTPException(
            status_code=404,
            detail=(
                f"No materialized features found for customer_id={customer_id}. "
                "Ensure the churn_feature_pipeline DAG has been run in Airflow "
                "(feast apply + feast materialize-incremental)."
            ),
        )

    mapped_data = {
        "age": feature_row.get("age"),
        "gender": _normalize_gender(feature_row.get("gender")),
        "tenure_months": feature_row.get("tenure_months"),
        "usage_frequency": feature_row.get("usage_frequency"),
        "support_calls": feature_row.get("support_calls"),
        "payment_delay_days": feature_row.get("payment_delay_days"),
        "subscription_type": _normalize_categorical(feature_row.get("subscription_type")),
        "contract_length": _normalize_categorical(feature_row.get("contract_length")),
        "total_spend": feature_row.get("total_spend"),
        "last_interaction_days": feature_row.get("last_interaction_days"),
    }
    logger.info(f"Normalized mapped_data for {customer_id}: {mapped_data}")
    return mapped_data


@router.post("/", response_model=ChurnPrediction)
async def predict_churn(data: ChurnInput, background_tasks: BackgroundTasks):
    """
    Predict customer churn probability for a single customer
    """
    try:
        input_data = data.model_dump()
        logger.info(f"Received input data: {input_data}")
        mapped_data = map_schema_to_preprocessing(input_data)
        prediction_int = _predict_from_mapped_data(mapped_data)
        
        logger.info(f"Single prediction: {prediction_int}")
        PREDICTIONS.labels(
            endpoint="single",
            result="churn" if prediction_int == 1 else "no_churn",
        ).inc()

        # Save to production data (background) - without probability
        background_tasks.add_task(
            save_production_data, 
            input_data, 
            prediction_int
        )
        
        return ChurnPrediction(
            churn=prediction_int
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Prediction error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")


@router.post("/by-customer-id", response_model=ChurnPrediction)
async def predict_churn_by_customer_id(data: CustomerIdInput, background_tasks: BackgroundTasks):
    """
    Predict churn using online features fetched from Feast by customer_id.
    """
    try:
        customer_id = data.customer_id.strip()
        if not customer_id:
            raise HTTPException(status_code=422, detail="customer_id must not be empty")

        logger.info(f"Fetching online features from Feast for customer_id={customer_id}")
        try:
            mapped_data = _fetch_mapped_features_from_feast(customer_id)
            FEAST_LOOKUPS.labels(status="success").inc()
        except HTTPException:
            FEAST_LOOKUPS.labels(status="error").inc()
            raise
        prediction_int = _predict_from_mapped_data(mapped_data)

        logger.info(f"Feast-backed prediction for customer_id={customer_id}: {prediction_int}")
        PREDICTIONS.labels(
            endpoint="customer_id",
            result="churn" if prediction_int == 1 else "no_churn",
        ).inc()

        production_record = {"customer_id": customer_id, **mapped_data}
        background_tasks.add_task(save_production_data, production_record, prediction_int)

        return ChurnPrediction(churn=prediction_int)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Feast-backed prediction error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")


@router.post("/batch", response_model=List[ChurnPrediction])
async def predict_batch(data_list: List[ChurnInput], background_tasks: BackgroundTasks):
    """
    Batch prediction for multiple customers
    
    **Limitations:**
    - Maximum 1000 customers per request
    - Timeout: 60 seconds
    """
    try:
        # Validate batch size
        if len(data_list) > 1000:
            raise HTTPException(
                status_code=400, 
                detail="Batch size exceeds limit of 1000 customers"
            )
        
        if len(data_list) == 0:
            raise HTTPException(status_code=400, detail="Empty batch")
        
        logger.info(f"Processing batch of {len(data_list)} customers")
        BATCH_SIZE.observe(len(data_list))
        
        results = []
        all_inputs = []
        
        # Process all customers
        for idx, data in enumerate(data_list):
            try:
                input_data = data.model_dump()
                mapped_data = map_schema_to_preprocessing(input_data)
                prediction_int = _predict_from_mapped_data(mapped_data)
                
                results.append(ChurnPrediction(
                    churn=prediction_int
                ))
                
                all_inputs.append((input_data, prediction_int))
                
            except HTTPException as e:
                logger.warning(f"Validation failed for customer {idx}: {e.detail}")
                results.append(ChurnPrediction(
                    churn=0
                ))
            except Exception as e:
                logger.error(f"Error processing customer {idx}: {str(e)}")
                # Return neutral prediction on error
                results.append(ChurnPrediction(
                    churn=0
                ))
        
        # Save all to production data (background) - without probability
        for input_data, pred in all_inputs:
            background_tasks.add_task(save_production_data, input_data, pred)
        
        churn_count = sum(r.churn for r in results)
        PREDICTIONS.labels(endpoint="batch", result="churn").inc(churn_count)
        PREDICTIONS.labels(endpoint="batch", result="no_churn").inc(len(results) - churn_count)
        logger.info(f"Batch prediction completed: {len(results)} results")
        
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Batch prediction error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Batch prediction failed: {str(e)}")
