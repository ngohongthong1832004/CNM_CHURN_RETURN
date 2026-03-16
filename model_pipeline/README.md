# MLflow Model System - Customer Churn Prediction

A production-ready MLOps system for training, evaluating, and managing XGBoost models for customer churn prediction using MLflow.

## Overview

This project implements a complete MLOps pipeline for customer churn prediction using:

- **MLflow** for experiment tracking, model registry, and artifact management
- **XGBoost** for high-performance gradient boosting classification
- **MinIO** for S3-compatible artifact storage
- **Docker** for containerized MLflow server
- **SHAP** for model explainability

The system provides end-to-end functionality from data preprocessing to model deployment with full tracking and versioning capabilities.

## Features
### Experiment Tracking
- Automatic logging of parameters, metrics, and artifacts
- Support for nested runs and experiment organization
- Integration with MLflow UI for visualization

### Model Training
- XGBoost binary classification with GPU support
- Configurable hyperparameters via YAML
- Automatic feature importance logging
- Early stopping and validation monitoring

### Model Evaluation
- Comprehensive metrics (AUC, log loss, accuracy)
- SHAP explainability integration
- Model comparison against baseline
- Threshold validation for deployment gates

### Model Registry
- Version control for trained models
- Alias management (staging, champion, production)
- Model promotion workflows
- Complete model lifecycle tracking

### Infrastructure
- Dockerized MLflow tracking server
- MinIO for artifact storage (S3-compatible)
- PostgreSQL backend for metadata
- Production-ready configuration

## 📁 Project Structure

```
model_pipeline/
├── docker-compose.yaml          # MLflow infrastructure setup
├── README.md                    # This file
├── notebook/
│   └── test_end_to_end.ipynb   # End-to-end testing notebook
└── src/
    ├── config/
    │   └── config.yaml          # Model and MLflow configuration
    ├── data/
    │   ├── train.csv            # Training dataset
    │   ├── test.csv             # Testing dataset
    │   └── *.ipynb              # Data preprocessing notebooks
    ├── mlflow_utils/
    │   ├── experiment_tracker.py    # MLflow experiment management
    │   └── model_registry.py        # Model registry operations
    ├── model/
    │   ├── xgboost_trainer.py      # XGBoost training pipeline
    │   └── evaluator.py            # Model evaluation logic
    ├── scripts/
    │   ├── train.py                # Training script
    │   ├── eval.py                 # Evaluation script
    │   └── register_model.py       # Model registry CLI
    ├── run_sh/
    │   ├── train.sh                # Training execution script
    │   ├── eval.sh                 # Evaluation execution script
    │   ├── register_model.sh       # Model registration script
    │   ├── set_model_alias.sh      # Set model alias
    │   ├── promote_model.sh        # Promote model to production
    │   ├── list_models.sh          # List registered models
    │   └── model_info.sh           # Get model information
    └── utility/
        └── helper.py               # Utility functions
```

## Prerequisites

- **Python**: 3.10 or higher
- **Docker & Docker Compose**: For MLflow infrastructure
- **CUDA** (optional): For GPU-accelerated training
- **Git**: For version control



## ⚙️ Configuration

### Main Configuration File: `src/config/config.yaml`

```yaml
mlflow:
  tracking_uri: "http://localhost:5000"
  experiment_name: "test_churn_prediction_v0.1"
  artifact_location: "s3://mlflow/"
  registry_uri: "http://localhost:5000"
  tags:
    task: "churn_prediction"
    purpose: "test"

model:
  name: "xgboost_churn"
  version: "0.1.0"
  type: "classifier"
  train_test_split: 0.2
  random_state: 42
  
  xgboost:
    booster: "gbtree"
    device: "cuda"  # or "cpu"
    max_depth: 6
    eta: 0.1
    objective: "binary:logistic"
    eval_metric: ["auc", "logloss", "error@0.5"]
    # ... more parameters

evaluation:
  shap:
    enabled: true
    explainer_type: "exact"
    max_samples: 100
  
  thresholds:
    accuracy: 0.85
    auc: 0.80

features:
  target_column: "Churn"
  training_features:
    - Age
    - Tenure
    - Usage Frequency
    # ... more features
```

### Environment Variables

The system uses these environment variables (set in training/eval scripts):

```bash
export AWS_ACCESS_KEY_ID="minio"
export AWS_SECRET_ACCESS_KEY="minio123"
export AWS_DEFAULT_REGION="us-east-1"
export MLFLOW_S3_ENDPOINT_URL="http://localhost:9000"
```

## 🎬 Quick Start

### 1. Train a Model

```bash
cd src/run_sh
chmod +x *.sh

# Run training
./train.sh
```

**Output**: You'll get a `run_id` (e.g., `20c0e794e88f41ab9fe3685a06c54874`)

### 2. Evaluate the Model

```bash
# Update RUN_ID in eval.sh with your run_id
./eval.sh
```

### 3. Register the Model

```bash
# Update RUN_ID in register_model.sh
./register_model.sh
```

### 4. Promote to Production

```bash
# Update VERSION in set_model_alias.sh
./set_model_alias.sh

# Promote to champion
./promote_model.sh
```

### 5. Use the Model

```python
import mlflow

# Load production model
model = mlflow.pyfunc.load_model("models:/xgboost_churn_model@champion")

# Make predictions
predictions = model.predict(test_data)
```

## 🔄 Model Registry Workflow

### Complete Lifecycle

```
┌─────────────┐
│   Training  │  ./train.sh
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Evaluation │  ./eval.sh
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ Registration│  ./register_model.sh
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Staging   │  ./set_model_alias.sh (alias=staging)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Validation │  Additional testing
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Production │  ./promote_model.sh
└─────────────┘
```

### Best Practices

1. **Development Cycle**
   ```bash
   # Train multiple models
   ./train.sh  # experiment-1
   ./train.sh  # experiment-2
   ./train.sh  # experiment-3
   
   # Evaluate all
   ./eval.sh  # Update RUN_ID each time
   
   # Register best model
   ./register_model.sh
   ```

2. **Staging Validation**
   ```bash
   # Register and set to staging
   ./register_model.sh
   ./set_model_alias.sh  # alias=staging
   
   # Test in staging environment
   # Run A/B tests
   # Validate performance
   
   # Promote when ready
   ./promote_model.sh
   ```

3. **Production Deployment**
   ```bash
   # Always compare against current champion
   python src/scripts/eval.py \
       --model-uri "models:/xgboost_churn_model@staging" \
       --compare-baseline "models:/xgboost_churn_model@champion"
   
   # If improvement confirmed, promote
   ./promote_model.sh
   ```

## 🏗️ Architecture

### System Components

```
┌─────────────────────────────────────────────────────────┐
│                     MLflow UI (Port 5000)                │
│                   Experiment Tracking & Registry         │
└───────────────────────┬─────────────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
        ▼               ▼               ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  PostgreSQL  │ │    MinIO     │ │Training/Eval │
│   Metadata   │ │  Artifacts   │ │   Scripts    │
│   Storage    │ │  (S3-like)   │ │              │
└──────────────┘ └──────────────┘ └──────────────┘
```

### Data Flow

```
Input Data → Preprocessing → Training → Evaluation → Registration → Production
    ↓            ↓              ↓           ↓            ↓             ↓
  Raw CSV    Cleaned Data   XGBoost    Metrics     Model URI    Deployed Model
                                       SHAP         Version
```

### Module Architecture

```
mlflow_utils/
├── experiment_tracker.py   → Handles run lifecycle, logging
└── model_registry.py       → Model versioning, aliases

model/
├── xgboost_trainer.py     → Training pipeline, feature engineering
└── evaluator.py          → Evaluation metrics, SHAP

scripts/
├── train.py              → CLI for training
├── eval.py               → CLI for evaluation
└── register_model.py     → CLI for registry operations
```