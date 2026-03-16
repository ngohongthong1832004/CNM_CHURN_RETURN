"""
Prediction endpoints with batch support
"""
from fastapi import APIRouter, HTTPException, BackgroundTasks
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from api.schemas import ChurnInput, ChurnPrediction
from pre_processing import validate_input, save_production_data, map_schema_to_preprocessing
from load_model import load_model
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


@router.post("/", response_model=ChurnPrediction)
async def predict_churn(data: ChurnInput, background_tasks: BackgroundTasks):
    """
    Predict customer churn probability for a single customer
    """
    try:
        input_data = data.model_dump()
        logger.info(f"Received input data: {input_data}")
        
        # Validate
        is_valid, error_msg = validate_input(input_data)
        if not is_valid:
            logger.error(f"Validation failed: {error_msg}")
            raise HTTPException(status_code=422, detail=error_msg)
        
        # Map schema to model input format (model has built-in preprocessing)
        mapped_data = map_schema_to_preprocessing(input_data)
        
        # Convert to DataFrame - model will handle preprocessing internally
        df_input = pd.DataFrame([mapped_data])
        
        # Convert numeric columns to float as required by model schema
        float_columns = ['usage_frequency', 'payment_delay_days', 'total_spend']
        for col in float_columns:
            if col in df_input.columns:
                df_input[col] = df_input[col].astype(float)
        
        logger.info(f"Input DataFrame shape: {df_input.shape}, columns: {df_input.columns.tolist()}")
        logger.info(f"DataFrame dtypes: {df_input.dtypes.to_dict()}")
        
        # Predict - model has built-in preprocessing
        model = get_model()
        prediction = model.predict(df_input)[0]
        prediction_int = int(prediction)
        
        logger.info(f"Single prediction: {prediction_int}")
        
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
        
        results = []
        all_inputs = []
        
        # Process all customers
        for idx, data in enumerate(data_list):
            try:
                input_data = data.model_dump()
                
                # Validate
                is_valid, error_msg = validate_input(input_data)
                if not is_valid:
                    logger.warning(f"Validation failed for customer {idx}: {error_msg}")
                    # Return neutral prediction for invalid data
                    results.append(ChurnPrediction(
                        churn=0
                    ))
                    continue
                
                # Map schema to model input format (model has built-in preprocessing)
                mapped_data = map_schema_to_preprocessing(input_data)
                
                # Convert to DataFrame - model will handle preprocessing internally
                df_input = pd.DataFrame([mapped_data])
                
                # Convert numeric columns to float as required by model schema
                float_columns = ['usage_frequency', 'payment_delay_days', 'total_spend']
                for col in float_columns:
                    if col in df_input.columns:
                        df_input[col] = df_input[col].astype(float)
                
                # Predict - model has built-in preprocessing
                model = get_model()
                prediction = model.predict(df_input)[0]
                prediction_int = int(prediction)
                
                results.append(ChurnPrediction(
                    churn=prediction_int
                ))
                
                all_inputs.append((input_data, prediction_int))
                
            except Exception as e:
                logger.error(f"Error processing customer {idx}: {str(e)}")
                # Return neutral prediction on error
                results.append(ChurnPrediction(
                    churn=0
                ))
        
        # Save all to production data (background) - without probability
        for input_data, pred in all_inputs:
            background_tasks.add_task(save_production_data, input_data, pred)
        
        logger.info(f"Batch prediction completed: {len(results)} results")
        
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Batch prediction error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Batch prediction failed: {str(e)}")
