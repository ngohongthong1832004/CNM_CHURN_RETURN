from pydantic import BaseModel, Field
from typing import Literal
from typing import Optional

class ChurnInput(BaseModel):
    """Input schema for churn prediction"""
    
    Age: int = Field(..., ge=18, le=65, description="Customer age")
    Gender: Literal["Male", "Female"] = Field(..., description="Customer gender")
    Tenure: int = Field(..., ge=1, le=60, description="Months with company")
    Usage_Frequency: int = Field(..., ge=1, le=30, description="Monthly usage frequency")
    Support_Calls: int = Field(..., ge=0, le=10, description="Number of support calls")
    Payment_Delay: int = Field(..., ge=0, le=30, description="Payment delay in days")
    Subscription_Type: Literal["Basic", "Standard", "Premium"] = Field(..., description="Subscription tier")
    Contract_Length: Literal["Monthly", "Quarterly", "Annual"] = Field(..., description="Contract duration")
    Total_Spend: float = Field(..., ge=100, le=1000, description="Total amount spent")
    Last_Interaction: int = Field(..., ge=1, le=30, description="Days since last interaction")
    
    class Config:
        json_schema_extra = {
            "example": {
                "Age": 30,
                "Gender": "Female",
                "Tenure": 39,
                "Usage_Frequency": 14,
                "Support_Calls": 5,
                "Payment_Delay": 18,
                "Subscription_Type": "Standard",
                "Contract_Length": "Annual",
                "Total_Spend": 932.0,
                "Last_Interaction": 17
            }
        }


class ChurnPrediction(BaseModel):
    """Prediction response"""
    churn: int = Field(..., description="Predicted churn (0=Active, 1=Churn)")


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    model_loaded: bool
    timestamp: str

class DriftMetricsResponse(BaseModel):
    """Response schema for drift metrics"""
    overall_drift_score: float = Field(..., description="Overall drift score (0-1)")
    drift_status: str = Field(..., description="Drift status: LOW/MEDIUM/HIGH")
    dataset_drift: Optional[bool] = Field(None, description="Whether dataset drift detected")
    number_of_drifted_features: Optional[int] = Field(None, description="Number of features with drift")
    total_features: Optional[int] = Field(None, description="Total number of features")
    drift_by_feature: Optional[dict] = Field(None, description="Drift details by feature")
    target_drift: Optional[bool] = Field(None, description="Whether target drift detected")
    prediction_drift: Optional[bool] = Field(None, description="Whether prediction drift detected")
    performance: Optional[dict] = Field(None, description="Performance metrics comparison")
    reference_data_size: int = Field(..., description="Size of reference dataset")
    current_data_size: int = Field(..., description="Size of current dataset")
    timestamp: str = Field(..., description="Timestamp of drift check")