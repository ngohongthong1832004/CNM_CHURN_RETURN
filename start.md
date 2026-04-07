# Runbook lệnh từ đầu đến cuối (PowerShell)

## 0) Vào thư mục project

```powershell
cd C:\Users\bapho\workspace\IUH\CongNgheMoi\aio2025-mlops-project01-main
```

## 1) Tạo Docker network dùng chung

```powershell
docker network create aio-network
```

## 2) Tạo file env cho MLflow stack

```powershell
@"
MYSQL_DATABASE=mlflow
MYSQL_USER=mlflow
MYSQL_PASSWORD=mlflow
MYSQL_ROOT_PASSWORD=root
AWS_ACCESS_KEY_ID=minio
AWS_SECRET_ACCESS_KEY=minio123
"@ | Set-Content -Path .\infra\docker\mlflow\.env
```

## 3) Khởi động hạ tầng Docker (MLflow, Kafka, Monitor, Airflow)

```powershell
docker compose -f .\infra\docker\mlflow\docker-compose.yaml up -d
docker compose -f .\infra\docker\kafka\docker-compose.yaml up -d
docker compose -f .\infra\docker\monitor\docker-compose.yaml up -d
docker compose -f .\infra\docker\airflow\docker-compose.yaml up -d
```

## 4) Tạo/activate Python env và cài dependencies

```powershell
if (-not (Test-Path .\.venv\Scripts\python.exe)) {
		python -m venv .\.venv
}

.\.venv\Scripts\Activate.ps1

pip install -r .\data-pipeline\requirements.txt
pip install -r .\serving_pipeline\requirements.txt
pip install mlflow pandas pyarrow scikit-learn loguru pyyaml evidently
```

## 5) Data pipeline + Feast

```powershell
cd .\data-pipeline

dvc pull

python .\churn_feature_store\churn_features\feature_repo\prepare_feast_data.py

docker rm -f redis-feast 2>$null
docker run -d --name redis-feast -p 6379:6379 redis:7

$env:FEAST_REDIS_CONNECTION_STRING = "localhost:6379"

cd .\churn_feature_store\churn_features\feature_repo
feast apply

$TS = Get-Date -Format "yyyy-MM-ddTHH:mm:ss"
feast materialize-incremental $TS

cd ..\..\..\..
```

## 6) Train model

### 6.1 Train all models (khuyến nghị)

```powershell
cd .\model_pipeline
$env:PYTHONPATH = (Get-Location).Path

.\train_all.ps1
```

### 6.2 Hoặc train 1 model logistic

```powershell
cd .\model_pipeline
$env:PYTHONPATH = (Get-Location).Path

$RUN_NAME = "logreg_" + (Get-Date -Format "yyyyMMdd_HHmmss")
python .\src\scripts\train.py `
	--config .\src\config\logistic_regression.yaml `
	--training-data-path ..\data-pipeline\churn_feature_store\churn_features\feature_repo\data\processed_churn_data.parquet `
	--run-name $RUN_NAME
```

Lưu lại RUN_ID từ log train để dùng ở bước sau.

## 7) Evaluate và register model

```powershell
cd .\model_pipeline
$env:PYTHONPATH = (Get-Location).Path

$RUN_ID = "<PASTE_RUN_ID_HERE>"

python .\src\scripts\eval.py `
	--config .\src\config\logistic_regression.yaml `
	--run-id $RUN_ID `
	--eval-data-path ..\data-pipeline\churn_feature_store\churn_features\feature_repo\data\processed_churn_data.parquet `
	--validate-thresholds `
	--output-path-prediction .\outputs\predictions_logreg.csv

python .\src\scripts\register_model.py `
	--config .\src\config\logistic_regression.yaml `
	register --run-id $RUN_ID --model-name customer_churn_model

# Sau khi register, điền version thực tế từ MLflow
$VERSION = "<PASTE_VERSION_HERE>"

python .\src\scripts\register_model.py `
	--config .\src\config\logistic_regression.yaml `
	set-alias --model-name customer_churn_model --version $VERSION --alias champion
```

## 8) Cấu hình serving

```powershell
cd ..\serving_pipeline

Copy-Item .\.env.example .\.env -Force
```

Sửa file .env, set MODEL_URI về run vừa train, ví dụ:

```text
MODEL_URI="runs:/<RUN_ID>/logistic_regression_churn"
```

Các biến còn lại giữ như mặc định docker nội bộ:

```text
MLFLOW_TRACKING_URI=http://mlflow_server:5000
MLFLOW_S3_ENDPOINT_URL=http://minio:9000
API_BASE_URL=http://api:8000
```

## 9) Chạy API + UI

```powershell
docker compose up -d
```

## 10) Kiểm tra kết quả

```powershell
# Health API
Invoke-RestMethod -Uri http://localhost:8000/health -Method Get

# Test predict
$body = @{ Age=30; Gender='Female'; Tenure=39; Usage_Frequency=14; Support_Calls=5; Payment_Delay=18; Subscription_Type='Standard'; Contract_Length='Annual'; Total_Spend=932.0; Last_Interaction=17 } | ConvertTo-Json
Invoke-RestMethod -Uri 'http://localhost:8000/predict/' -Method Post -ContentType 'application/json' -Body $body

# Drift metrics
Invoke-RestMethod -Uri 'http://localhost:8000/monitor/drift?format=json' -Method Get
```

Mở UI/Docs:

```powershell
Start-Process http://localhost:5000
Start-Process http://localhost:8000/docs
Start-Process http://localhost:7860
```

## 11) Dừng toàn bộ

```powershell
# Dừng serving
cd .\serving_pipeline
docker compose down

# Dừng infra stacks
cd ..\infra\docker
docker compose -f .\airflow\docker-compose.yaml down
docker compose -f .\monitor\docker-compose.yaml down
docker compose -f .\kafka\docker-compose.yaml down
docker compose -f .\mlflow\docker-compose.yaml down

# Dừng Redis Feast
docker rm -f redis-feast
```