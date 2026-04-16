<#
.SYNOPSIS
    MLOps Platform -- Chay toan bo he thong tu A den Z

.DESCRIPTION
    Phase 1  INFRA   : Khoi dong Docker services (mlflow/MinIO -> lakehouse/Nessie -> kafka -> monitor -> airflow)
    Phase 2  DATA    : Trigger Airflow DAGs: data_simulator -> lakehouse_etl -> churn_feature_pipeline
    Phase 3  MODEL   : Trigger Airflow DAG: churn_retraining_pipeline (train 6 models -> chon best -> register)
    Phase 4  SERVING : Khoi dong serving pipeline (FastAPI + Gradio UI)

.PARAMETER SkipInfra
    Bo qua Phase 1 (services da chay roi)
.PARAMETER SkipData
    Bo qua Phase 2 (data da co san)
.PARAMETER SkipModel
    Bo qua Phase 3 (model champion da duoc register)
.PARAMETER SkipServing
    Bo qua Phase 4
.PARAMETER AirflowUser
    Username Airflow (default: airflow)
.PARAMETER AirflowPassword
    Password Airflow (default: airflow)
.PARAMETER ModelName
    Ten model trong MLflow Registry (default: customer_churn_model)
.PARAMETER ModelAlias
    Alias cua model champion (default: champion)

.EXAMPLE
    # Chay toan bo tu dau
    .\start.ps1

    # Chi chay data + model + serving (infra da up)
    .\start.ps1 -SkipInfra

    # Chi restart serving voi model moi
    .\start.ps1 -SkipInfra -SkipData -SkipModel
#>
param(
    [switch]$SkipInfra,
    [switch]$SkipData,
    [switch]$SkipModel,
    [switch]$SkipServing,
    [string]$AirflowUser     = "airflow",
    [string]$AirflowPassword = "airflow",
    [string]$ModelName       = "customer_churn_model",
    [string]$ModelAlias      = "champion"
)

$ErrorActionPreference = "Stop"

# ---- Paths ------------------------------------------------------------------
$ROOT         = Split-Path -Parent $MyInvocation.MyCommand.Path
$INFRA_SCRIPT = Join-Path $ROOT "infra\docker\run.ps1"
$SERVING_DIR  = Join-Path $ROOT "serving_pipeline"
$AIRFLOW_URL  = "http://localhost:8080"
$MLFLOW_URL   = "http://localhost:5000"

# ---- Console helpers --------------------------------------------------------
function Write-Step([string]$msg) {
    Write-Host ""
    Write-Host "======================================================" -ForegroundColor Cyan
    Write-Host "  $msg" -ForegroundColor Cyan
    Write-Host "======================================================" -ForegroundColor Cyan
}
function Write-Ok([string]$msg)   { Write-Host "  [OK]  $msg" -ForegroundColor Green }
function Write-Info([string]$msg) { Write-Host "  [..]  $msg" -ForegroundColor DarkCyan }
function Write-Warn([string]$msg) { Write-Host "  [!!]  $msg" -ForegroundColor Yellow }

# ---- Wait-Until: poll dieu kien den khi true hoac timeout ------------------
function Wait-Until {
    param(
        [scriptblock]$Condition,
        [string]$Label,
        [int]$TimeoutSec  = 300,
        [int]$IntervalSec = 10
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    Write-Info "Waiting for: $Label (timeout ${TimeoutSec}s)"
    while ((Get-Date) -lt $deadline) {
        try { if (& $Condition) { Write-Ok "$Label -- ready"; return } } catch { }
        Write-Host "    ..." -NoNewline
        Start-Sleep -Seconds $IntervalSec
    }
    throw "TIMEOUT waiting for: $Label"
}

# ---- Airflow REST helpers ---------------------------------------------------
$script:AirflowToken = $null

function Get-AirflowToken {
    $body = @{ username = $AirflowUser; password = $AirflowPassword } | ConvertTo-Json
    $resp = Invoke-RestMethod -Method POST -Uri "$AIRFLOW_URL/auth/token" `
        -Body $body -ContentType "application/json" -UseBasicParsing
    return $resp.access_token
}

function Invoke-Airflow {
    param([string]$Method, [string]$Path, [hashtable]$Body = $null)

    if (-not $script:AirflowToken) {
        $script:AirflowToken = Get-AirflowToken
    }

    $uri     = "$AIRFLOW_URL/api/v2$Path"
    $headers = @{
        "Content-Type"  = "application/json"
        "Authorization" = "Bearer $script:AirflowToken"
    }
    $params = @{
        Method          = $Method
        Uri             = $uri
        Headers         = $headers
        UseBasicParsing = $true
    }
    if ($Body) { $params.Body = ($Body | ConvertTo-Json -Depth 5) }
    return Invoke-RestMethod @params
}

function Unpause-Dag([string]$DagId) {
    Invoke-Airflow -Method PATCH -Path "/dags/$DagId" -Body @{ is_paused = $false } | Out-Null
    Write-Ok "DAG unpaused: $DagId"
}

function Trigger-Dag([string]$DagId) {
    $logicalDate = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    $run = Invoke-Airflow -Method POST -Path "/dags/$DagId/dagRuns" -Body @{
        logical_date = $logicalDate
        conf         = @{}
    }
    Write-Ok "DAG triggered: $DagId  (run_id=$($run.dag_run_id))"
    return $run.dag_run_id
}

function Get-DagRunState([string]$DagId, [string]$RunId) {
    $run = Invoke-Airflow -Method GET -Path "/dags/$DagId/dagRuns/$RunId"
    return $run.state
}

# Trigger DAG roi poll cho den khi success/failed
function Run-DagAndWait {
    param(
        [string]$DagId,
        [int]$TimeoutSec  = 1800,
        [int]$IntervalSec = 15
    )
    Write-Info "Running DAG: $DagId"
    Unpause-Dag $DagId
    $runId   = Trigger-Dag $DagId
    $deadline = (Get-Date).AddSeconds($TimeoutSec)

    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds $IntervalSec
        $state = Get-DagRunState -DagId $DagId -RunId $runId
        Write-Host "    $DagId => $state" -ForegroundColor DarkGray
        if ($state -eq "success") { Write-Ok "DAG completed: $DagId"; return }
        if ($state -in @("failed", "upstream_failed")) {
            throw "DAG FAILED: $DagId (run_id=$runId, state=$state)"
        }
    }
    throw "TIMEOUT waiting for DAG: $DagId (run_id=$runId)"
}

# ---- MLflow helpers ---------------------------------------------------------
function Get-ChampionModelUri([string]$Name, [string]$Alias) {
    $uri = "$MLFLOW_URL/api/2.0/mlflow/registered-models/alias?name=$([Uri]::EscapeDataString($Name))&alias=$Alias"
    try {
        $resp    = Invoke-RestMethod -Uri $uri -UseBasicParsing
        $version = $resp.model_version.version
        Write-Ok "Champion model: $Name v$version (@$Alias)"
    } catch {
        Write-Warn "Model '$Name@$Alias' chua co trong registry -- dung URI mac dinh"
    }
    return "models:/$Name@$Alias"
}

# =============================================================================
# PHASE 1 -- INFRASTRUCTURE
# =============================================================================
if (-not $SkipInfra) {
    Write-Step "Phase 1 / 4 -- Infrastructure (Docker services)"

    if (-not (Test-Path $INFRA_SCRIPT)) { throw "Khong tim thay: $INFRA_SCRIPT" }

    # Rebuild Airflow image truoc khi start de dam bao requirements.txt moi nhat duoc ap dung
    # (feast version, pyiceberg, etc.)
    Write-Info "Rebuild Airflow image (dam bao requirements.txt cap nhat)..."
    & $INFRA_SCRIPT build airflow

    Write-Info "Goi run.ps1 start all..."
    & $INFRA_SCRIPT start all

    Wait-Until -Label "Airflow API" -TimeoutSec 300 -IntervalSec 10 -Condition {
        try {
            $r = Invoke-RestMethod -Uri "$AIRFLOW_URL/api/v2/version" -UseBasicParsing
            return ($null -ne $r.version)
        } catch { return $false }
    }

    Wait-Until -Label "MLflow" -TimeoutSec 120 -IntervalSec 5 -Condition {
        try { Invoke-RestMethod -Uri "$MLFLOW_URL/health" -UseBasicParsing | Out-Null; return $true }
        catch { return $false }
    }

    Wait-Until -Label "Nessie" -TimeoutSec 120 -IntervalSec 5 -Condition {
        try { Invoke-RestMethod -Uri "http://localhost:19120/api/v2/config" -UseBasicParsing | Out-Null; return $true }
        catch { return $false }
    }

    Wait-Until -Label "Kafka broker-1 (port 9092)" -TimeoutSec 120 -IntervalSec 5 -Condition {
        try {
            $t = Test-NetConnection -ComputerName localhost -Port 9092 -InformationLevel Quiet -WarningAction SilentlyContinue
            return $t
        } catch { return $false }
    }

    Write-Ok "Tat ca services da san sang"
} else {
    Write-Warn "Phase 1 bo qua (-SkipInfra)"
}

# =============================================================================
# PHASE 2 -- DATA PIPELINE
# =============================================================================
if (-not $SkipData) {
    Write-Step "Phase 2 / 4 -- Data Pipeline (Airflow DAGs)"

    # Dam bao Airflow API da san sang (quan trong khi dung -SkipInfra)
    Wait-Until -Label "Airflow API" -TimeoutSec 180 -IntervalSec 5 -Condition {
        try {
            Invoke-Airflow -Method GET -Path "/version" | Out-Null
            return $true
        } catch { return $false }
    }

    # 2a. Sinh du lieu moi: simulate -> Kafka topic -> Iceberg Bronze
    Write-Info "2a. data_simulator -- simulate_to_kafka >> kafka_to_bronze"
    Run-DagAndWait -DagId "data_simulator" -TimeoutSec 600

    # 2b. Bronze -> Silver -> Gold -> export parquet
    Write-Info "2b. lakehouse_etl -- Bronze->Silver->Gold->Parquet"
    Run-DagAndWait -DagId "lakehouse_etl" -TimeoutSec 900

    # 2c. Feast apply + materialize
    Write-Info "2c. churn_feature_pipeline -- Feast materialize"
    Run-DagAndWait -DagId "churn_feature_pipeline" -TimeoutSec 600

    Write-Ok "Data pipeline hoan thanh"
} else {
    Write-Warn "Phase 2 bo qua (-SkipData)"
}

# =============================================================================
# PHASE 3 -- MODEL PIPELINE
# =============================================================================
if (-not $SkipModel) {
    Write-Step "Phase 3 / 4 -- Model Pipeline (churn_retraining_pipeline)"

    Write-Info "Train 6 models song song (logreg, dtree, rf, xgboost, lightgbm, catboost)"
    Write-Info "Chon model tot nhat theo F1, evaluate, register lam champion"
    Write-Warn "Buoc nay co the mat 10-30 phut..."

    Run-DagAndWait -DagId "churn_retraining_pipeline" -TimeoutSec 3600

    Write-Ok "Model pipeline hoan thanh -- champion model da duoc register"
} else {
    Write-Warn "Phase 3 bo qua (-SkipModel)"
}

# =============================================================================
# PHASE 4 -- SERVING PIPELINE
# =============================================================================
if (-not $SkipServing) {
    Write-Step "Phase 4 / 4 -- Serving Pipeline (API + UI)"

    $servingCompose = Join-Path $SERVING_DIR "docker-compose.yml"
    if (-not (Test-Path $servingCompose)) { throw "Khong tim thay: $servingCompose" }

    $modelUri = Get-ChampionModelUri -Name $ModelName -Alias $ModelAlias

    # Tao .env cho serving_pipeline
    $envFile  = Join-Path $SERVING_DIR ".env"
    $envLines = @(
        "MODEL_URI=$modelUri",
        "MLFLOW_TRACKING_URI=http://mlflow_server:5000",
        "MLFLOW_S3_ENDPOINT_URL=http://minio:9000",
        "AWS_ACCESS_KEY_ID=minio",
        "AWS_SECRET_ACCESS_KEY=minio123",
        "AWS_DEFAULT_REGION=us-east-1",
        "FEAST_REDIS_CONNECTION_STRING=redis:6379",
        "API_BASE_URL=http://api:8000"
    )
    $envLines | Set-Content -Path $envFile -Encoding ASCII
    Write-Ok "Tao $envFile  (MODEL_URI=$modelUri)"

    Write-Info "Khoi dong serving pipeline..."
    docker compose -f $servingCompose up -d --pull missing
    if ($LASTEXITCODE -ne 0) { throw "docker compose up that bai" }

    Wait-Until -Label "Serving API /health" -TimeoutSec 120 -IntervalSec 5 -Condition {
        try {
            Invoke-RestMethod -Uri "http://localhost:8000/health" -UseBasicParsing | Out-Null
            return $true
        } catch { return $false }
    }

    Write-Ok "Serving pipeline da san sang"
} else {
    Write-Warn "Phase 4 bo qua (-SkipServing)"
}

# =============================================================================
# DONE
# =============================================================================
Write-Host ""
Write-Host "======================================================" -ForegroundColor Green
Write-Host "  HE THONG DA SAN SANG" -ForegroundColor Green
Write-Host "======================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Airflow UI    ->  http://localhost:8080   (airflow / airflow)"
Write-Host "  MLflow UI     ->  http://localhost:5000"
Write-Host "  MinIO UI      ->  http://localhost:9001   (minio / minio123)"
Write-Host "  Nessie        ->  http://localhost:19120"
Write-Host "  Trino         ->  http://localhost:8090"
Write-Host "  Superset      ->  http://localhost:8088   (admin / admin)"
Write-Host "  Grafana       ->  http://localhost:3000"
Write-Host "  Prometheus    ->  http://localhost:9090"
Write-Host "  Serving API   ->  http://localhost:8000/docs"
Write-Host "  Serving UI    ->  http://localhost:7860"
Write-Host ""
