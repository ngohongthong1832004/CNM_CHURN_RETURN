<#
.SYNOPSIS
    Xoa toan bo Docker resources cua du an MLOps (containers, named volumes, bind-mount dirs, images, network)

.PARAMETER RemoveImages
    Xoa luon cac image da build/pull boi du an (mac dinh: khong xoa)
.PARAMETER Force
    Bo qua xac nhan truoc khi xoa

.EXAMPLE
    # Xoa containers + volumes (giu lai images)
    .\cleanup.ps1

    # Xoa ca images
    .\cleanup.ps1 -RemoveImages

    # Khong hoi xac nhan
    .\cleanup.ps1 -Force
#>
param(
    [switch]$RemoveImages,
    [switch]$Force
)

$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path

function Write-Step([string]$msg) {
    Write-Host ""
    Write-Host "======================================================" -ForegroundColor Red
    Write-Host "  $msg" -ForegroundColor Red
    Write-Host "======================================================" -ForegroundColor Red
}
function Write-Ok([string]$msg)   { Write-Host "  [OK]  $msg" -ForegroundColor Green }
function Write-Info([string]$msg) { Write-Host "  [..]  $msg" -ForegroundColor DarkCyan }
function Write-Warn([string]$msg) { Write-Host "  [!!]  $msg" -ForegroundColor Yellow }

# ---- Xac nhan ---------------------------------------------------------------
if (-not $Force) {
    Write-Host ""
    Write-Host "  CANH BAO: Script nay se xoa TOAN BO:" -ForegroundColor Yellow
    Write-Host "    - Tat ca containers cua du an" -ForegroundColor Yellow
    Write-Host "    - Tat ca named volumes (postgres-db-volume, broker-*-data, superset-home, grafana-storage)" -ForegroundColor Yellow
    Write-Host "    - Tat ca bind-mount data dirs (mysql_data, minio_data, airflow/logs, serving data)" -ForegroundColor Yellow
    if ($RemoveImages) {
        Write-Host "    - Cac Docker images da build (mlflow_server, aio_superset, airflow custom)" -ForegroundColor Yellow
    }
    Write-Host "    - Docker network: aio-network" -ForegroundColor Yellow
    Write-Host ""
    $confirm = Read-Host "  Nhap 'yes' de xac nhan"
    if ($confirm -ne "yes") {
        Write-Warn "Da huy."
        exit 0
    }
}

# =============================================================================
# COMPOSE FILES -- thu tu nguoc lai voi thu tu khoi dong
# =============================================================================
$composeFiles = @(
    # @{ Path = "$ROOT\cloudflare_tunnel\docker-compose.yml";      Label = "Cloudflare Tunnel" },
    @{ Path = "$ROOT\serving_pipeline\docker-compose.yml";       Label = "Serving Pipeline" },
    @{ Path = "$ROOT\data-simulator\docker-compose.yml";         Label = "Data Simulator" },
    @{ Path = "$ROOT\infra\docker\monitor\docker-compose.yaml";  Label = "Monitor (Grafana/Prometheus/Loki)" },
    @{ Path = "$ROOT\infra\docker\airflow\docker-compose.yaml";  Label = "Airflow" },
    @{ Path = "$ROOT\infra\docker\kafka\docker-compose.yaml";    Label = "Kafka" },
    @{ Path = "$ROOT\infra\docker\lakehouse\docker-compose.yml"; Label = "Lakehouse (Nessie/Trino/Superset)" },
    @{ Path = "$ROOT\infra\docker\mlflow\docker-compose.yaml";   Label = "MLflow / MinIO / MySQL" }
)

Write-Step "Buoc 1/4 -- Docker Compose down (containers + named volumes)"

foreach ($c in $composeFiles) {
    if (Test-Path $c.Path) {
        Write-Info "Down: $($c.Label)"
        $dir  = Split-Path -Parent $c.Path
        $file = Split-Path -Leaf  $c.Path
        Push-Location $dir
        docker compose -f $file down --volumes --remove-orphans 2>&1 | ForEach-Object { "    $_" }
        Pop-Location
        Write-Ok "$($c.Label) -- done"
    } else {
        Write-Warn "Khong tim thay: $($c.Path) -- bo qua"
    }
}

# =============================================================================
# BIND-MOUNT DATA DIRECTORIES
# =============================================================================
Write-Step "Buoc 2/4 -- Xoa bind-mount data directories"

$bindDirs = @(
    "$ROOT\infra\docker\mlflow\mysql_data",
    "$ROOT\infra\docker\mlflow\minio_data",
    "$ROOT\infra\docker\airflow\logs",
    "$ROOT\serving_pipeline\api\data",
    "$ROOT\serving_pipeline\api\reports"
)

foreach ($d in $bindDirs) {
    if (Test-Path $d) {
        Write-Info "Xoa: $d"
        Remove-Item -Recurse -Force $d
        Write-Ok "Da xoa: $d"
    } else {
        Write-Info "Khong ton tai (bo qua): $d"
    }
}

# =============================================================================
# DOCKER IMAGES (optional)
# =============================================================================
if ($RemoveImages) {
    Write-Step "Buoc 3/4 -- Xoa Docker images da build"

    $images = @(
        "mlflow_server",
        "aio_superset"
    )

    foreach ($img in $images) {
        $exists = docker images -q $img 2>$null
        if ($exists) {
            Write-Info "Xoa image: $img"
            docker rmi -f $img 2>&1 | ForEach-Object { "    $_" }
            Write-Ok "Da xoa: $img"
        } else {
            Write-Info "Image khong ton tai (bo qua): $img"
        }
    }

    # Xoa dangling images (<none>) sinh ra tu qua trinh build
    Write-Info "Xoa dangling images..."
    docker image prune -f 2>&1 | ForEach-Object { "    $_" }
    Write-Ok "Dangling images da duoc don dep"
} else {
    Write-Info "Buoc 3/4 -- Bo qua xoa images (them -RemoveImages de xoa)"
}

# =============================================================================
# NETWORK
# =============================================================================
Write-Step "Buoc 4/4 -- Xoa Docker network: aio-network"

$netExists = docker network ls --filter name=aio-network --format "{{.Name}}" 2>$null
if ($netExists -eq "aio-network") {
    Write-Info "Xoa network: aio-network"
    docker network rm aio-network 2>&1 | ForEach-Object { "    $_" }
    Write-Ok "Da xoa network: aio-network"
} else {
    Write-Info "Network aio-network khong ton tai (bo qua)"
}

# =============================================================================
# DONE
# =============================================================================
Write-Host ""
Write-Host "======================================================" -ForegroundColor Green
Write-Host "  DON DEP HOAN THANH" -ForegroundColor Green
Write-Host "======================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  De khoi dong lai du an: .\start.ps1" -ForegroundColor DarkCyan
Write-Host ""
