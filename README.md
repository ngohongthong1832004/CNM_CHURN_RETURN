# MLOps End-to-End: Customer Churn Prediction System

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![MLflow](https://img.shields.io/badge/MLflow-Latest-orange.svg)](https://mlflow.org/)
[![Feast](https://img.shields.io/badge/Feast-Feature_Store-red.svg)](https://feast.dev/)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-Ready-326CE5.svg)](https://kubernetes.io/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)

A **production-ready MLOps system** for customer churn prediction, demonstrating best practices in machine learning operations including data versioning, feature stores, experiment tracking, model serving, and infrastructure automation.

## 📖 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Pipelines](#pipelines)
- [Infrastructure](#infrastructure)
- [Monitoring & Observability](#monitoring--observability)
- [Development](#development)
- [Data Source](#data-source)
- [References](#references)

---

## 🎯 Overview

This project implements a complete MLOps pipeline for **customer churn prediction**, covering the entire machine learning lifecycle:

- **Data Pipeline**: Version-controlled data with DVC, feature engineering with Feast, and Redis-backed online feature serving
- **Model Pipeline**: XGBoost model training with MLflow experiment tracking, model registry, and automated evaluation
- **Serving Pipeline**: FastAPI-based prediction service with Gradio UI and monitoring integration
- **Infrastructure**: Kubernetes and Docker orchestration for PostgreSQL, MinIO, MLflow, Kafka, Airflow, and monitoring stack

The system is designed for scalability, reproducibility, and production deployment.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      MLOps Infrastructure                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────┐      ┌──────────────────┐                │
│  │  Data Pipeline    │      │  Model Pipeline  │                │
│  ├───────────────────┤      ├──────────────────┤                │
│  │ • DVC (S3/MinIO)  │      │ • MLflow Track   │                │
│  │ • Feast Features  │─────▶│ • XGBoost Model  │                │
│  │ • Redis Online    │      │ • Model Registry │                │
│  └───────────────────┘      └────────┬─────────┘                │
│           │                          │                          │
│           │                          ▼                          │
│           │                  ┌──────────────────┐               │
│           └─────────────────▶│ Serving Pipeline │               │
│                              ├──────────────────┤               │
│                              │ • FastAPI        │               │
│                              │ • Gradio UI      │               │
│                              │ • Monitoring     │               │
│                              └──────────────────┘               │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │              Infrastructure Services                       │ │
│  ├────────────────────────────────────────────────────────────┤ │
│  │ PostgreSQL | MinIO | MLflow | Kafka | Airflow | Monitoring │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
aio2025-mlops-project01/
├── data-pipeline/                  # Data versioning and feature store
│   ├── churn_feature_store/       # Feast feature definitions
│   ├── data/                      # Raw and processed datasets
│   ├── scripts/                   # Data processing scripts
│   └── requirements.txt           # DVC, Feast, Redis dependencies
│
├── model_pipeline/                 # Model training and evaluation
│   ├── src/
│   │   ├── config/               # Model configuration (YAML)
│   │   ├── data/                 # Training and test datasets
│   │   ├── mlflow_utils/         # Experiment & registry utilities
│   │   ├── model/                # XGBoost trainer and evaluator
│   │   ├── scripts/              # train.py, eval.py, register_model.py
│   │   └── run_sh/               # Bash automation scripts
│   ├── notebook/                 # Jupyter notebooks for testing
│   └── mlruns/                   # Local MLflow artifacts (optional)
│
├── serving_pipeline/               # Model serving and UI
│   ├── api/                       # FastAPI application
│   │   ├── main.py               # API entry point
│   │   ├── routers/              # Predict, health, monitor endpoints
│   │   └── schemas.py            # Pydantic models
│   ├── ui.py                     # Gradio web interface
│   ├── load_model.py             # MLflow model loader
│   ├── monitoring.py             # Metrics collection
│   ├── docker-compose.yml        # API and UI containers
│   └── requirements.txt          # FastAPI, MLflow, Gradio dependencies
│
├── infra/                          # Infrastructure as Code
│   ├── docker/                    # Docker Compose stacks
│   │   ├── mlflow/               # MLflow + PostgreSQL + MinIO
│   │   ├── kafka/                # 3-node Kafka cluster + UI
│   │   ├── airflow/              # Airflow 3.x + PostgreSQL
│   │   ├── monitor/              # Prometheus + Grafana + Loki
│   │   └── run.sh                # Unified service control script
│   └── k8s/                       # Kubernetes manifests
│       ├── namespace.yaml        # MLOps namespace
│       ├── postgres/             # Database deployment
│       ├── minio/                # Object storage
│       ├── mlflow/               # Tracking server
│       ├── kafka/                # Kafka cluster
│       ├── airflow/              # Workflow orchestration
│       ├── dashboard/            # Kubernetes Dashboard
│       └── deploy.sh             # K8s deployment automation
│
└── README.md                       # This file
```

---

## 🔧 Prerequisites

### System Requirements
- **OS**: Linux, macOS, or Windows with WSL2
- **RAM**: 8GB minimum, 16GB recommended
- **Storage**: 20GB+ free disk space
- **Python**: 3.10 or higher

### Software Dependencies
- **Docker**: 20.10+
- **Docker Compose**: v2.0+
- **Kubernetes** (optional): kubectl + cluster (minikube/kind/cloud)
- **Git**: For repository management
- **CUDA** (optional): For GPU-accelerated training

### Create Docker Network
```bash
docker network create aio-network
```

---

## 🚀 Quick Start

### 1. Clone Repository
```bash
git clone https://github.com/ThuanNaN/aio2025-mlops-project01.git
cd aio2025-mlops-project01
```

### 2. Start Infrastructure (Docker)
```bash
cd infra/docker
./run.sh start all
```

This starts MLflow, Kafka, Airflow, and monitoring services.

**Access URLs:**
- MLflow UI: http://localhost:5000
- Airflow UI: http://localhost:8080 (admin/admin)
- Grafana: http://localhost:3000 (admin/admin)
- Kafka UI: http://localhost:9021
- Prometheus: http://localhost:9090

### 3. Setup Data Pipeline
```bash
cd data-pipeline

# Create virtual environment
conda create -n churn_mlops python=3.10 -y
conda activate churn_mlops

# Install dependencies
pip install -r requirements.txt

# Pull versioned data
dvc pull

# Initialize Feast
cd churn_feature_store
feast apply
```

### 4. Train Model
```bash
cd model_pipeline/src

# Configure environment (edit config/config.yaml)
# Set MLflow tracking URI: http://localhost:5000

# Run training
bash run_sh/train.sh

# Evaluate model
bash run_sh/eval.sh

# Register model
bash run_sh/register_model.sh --model_name customer_churn_model
```

### 5. Serve Model
```bash
cd serving_pipeline

# Set environment variables
export MODEL_URI="models:/customer_churn_model/champion"
export MLFLOW_TRACKING_URI="http://localhost:5000"
export MLFLOW_S3_ENDPOINT_URL="http://localhost:9000"
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"

# Start API and UI
docker-compose up -d

# Access services
# API: http://localhost:8000/docs
# UI: http://localhost:7860
```

---

## 🔄 Pipelines

### Data Pipeline

**Location**: [`data-pipeline/`](data-pipeline/)

**Workflow**:
1. **Data Versioning**: DVC tracks datasets in S3/MinIO
2. **Feature Engineering**: Transform raw customer data
3. **Feature Store**: Register features with Feast
4. **Online Serving**: Materialize features to Redis

**Key Commands**:
```bash
# Pull latest data
dvc pull

# Apply Feast features
feast apply

# Materialize to Redis
feast materialize-incremental $(date +%Y-%m-%d)
```

📖 [Detailed Documentation](data-pipeline/README.md)

---

### Model Pipeline

**Location**: [`model_pipeline/`](model_pipeline/)

**Workflow**:
1. **Configuration**: Edit `src/config/config.yaml`
2. **Training**: `bash run_sh/train.sh` → logs to MLflow
3. **Evaluation**: `bash run_sh/eval.sh` → metrics + SHAP plots
4. **Registration**: `bash run_sh/register_model.sh`
5. **Promotion**: `bash run_sh/set_model_alias.sh --alias champion`

**Key Scripts**:
- `train.py`: XGBoost training with MLflow tracking
- `eval.py`: Model evaluation and comparison
- `register_model.py`: Register model to MLflow Registry

📖 [Detailed Documentation](model_pipeline/README.md)

---

### Serving Pipeline

**Location**: [`serving_pipeline/`](serving_pipeline/)

**Components**:
- **FastAPI**: REST API with `/predict`, `/health`, `/metrics`
- **Gradio**: Interactive web UI for predictions
- **Monitoring**: Prometheus metrics export

**API Example**:
```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "age": 35,
    "gender": "Male",
    "tenure": 24,
    "usage_frequency": 15,
    "support_calls": 2,
    "payment_delay": 5,
    "subscription_type": "Premium",
    "contract_length": "Annual",
    "total_spend": 500,
    "last_interaction": 10
  }'
```

**Response**:
```json
{
  "churn_probability": 0.23,
  "prediction": "Not Churn",
  "model_version": "champion"
}
```

---

## 🏗️ Infrastructure

### Docker Deployment

**Location**: [`infra/docker/`](infra/docker/)

**Services**:
- **MLflow**: Tracking server + PostgreSQL + MinIO
- **Kafka**: 3-node cluster (KRaft mode) + Kafka UI
- **Airflow**: Airflow 3.x with scheduler, webserver, PostgreSQL
- **Monitoring**: Prometheus + Grafana + Loki

**Control Script**:
```bash
cd infra/docker

# Start all services
./run.sh start all

# Start specific service
./run.sh start mlflow

# Stop services
./run.sh stop all

# Check status
./run.sh status
```

📖 [Docker Documentation](infra/docker/README.md)

---

### Kubernetes Deployment

**Location**: [`infra/k8s/`](infra/k8s/)

**Architecture**:
- Namespace: `mlops`
- Services: PostgreSQL, MinIO, MLflow, Kafka (3-node), Airflow 3.x
- Storage: PersistentVolumeClaims for data persistence
- Dashboard: Kubernetes Dashboard for cluster management

**Deploy**:
```bash
cd infra/k8s

# Deploy all resources
./deploy.sh

# Check status
kubectl get all -n mlops

# Get dashboard token
./get-dashboard-token.sh
```

**Access Services** (with port-forward):
```bash
kubectl port-forward -n mlops svc/mlflow 5000:5000
kubectl port-forward -n mlops svc/airflow-webserver 8080:8080
kubectl port-forward -n mlops svc/minio-console 9001:9001
```

📖 [Kubernetes Documentation](infra/k8s/README.md)

---

## 📊 Monitoring & Observability

### Prometheus Metrics
- API request latency and throughput
- Model prediction counts
- Error rates
- System resource usage

### Grafana Dashboards
- FastAPI observability: [Dashboard 16110](https://grafana.com/grafana/dashboards/16110-fastapi-observability/)
- Custom churn prediction metrics
- Infrastructure monitoring

### Loki Logs
- Centralized log aggregation
- Query logs from all services

**Access**: http://localhost:3000 (admin/admin)

**References**:
- [FastAPI Observability Guide](https://github.com/Blueswen/fastapi-observability)

---
#   C N M _ C H U R N _ R E T U R N  
 