param(
    [Parameter(Position = 0)]
    [ValidateSet("start", "stop", "restart", "status", "build", "logs", "help")]
    [string]$Command = "help",

    [Parameter(Position = 1)]
    [ValidateSet("all", "mlflow", "kafka", "monitor", "airflow", "lakehouse")]
    [string]$Service = "all"
)

$ErrorActionPreference = "Stop"

$script:ScriptDir     = Split-Path -Parent $MyInvocation.MyCommand.Path
$script:NetworkName   = "aio-network"
$script:IsWindowsHost = $env:OS -eq "Windows_NT"

# Startup order: mlflow (MinIO) -> lakehouse (Nessie needs MinIO) -> airflow (DAGs need Nessie)
$script:Services = @("mlflow", "kafka", "monitor", "lakehouse", "airflow")

$script:ServiceConfig = @{
    mlflow    = @{
        ComposeFile = Join-Path $script:ScriptDir "mlflow\docker-compose.yaml"
        Setup       = { Ensure-MlflowEnvFile }
        Ports       = @(
            "MLflow UI  -> http://localhost:5000"
            "MinIO UI   -> http://localhost:9001  (minio / minio123)"
        )
    }
    kafka     = @{
        ComposeFile = Join-Path $script:ScriptDir "kafka\docker-compose.yaml"
        Setup       = { }
        Ports       = @(
            "Kafka Broker 1 -> localhost:9092"
            "Kafka Broker 2 -> localhost:9192"
            "Kafka Broker 3 -> localhost:9292"
        )
    }
    monitor   = @{
        ComposeFile = Join-Path $script:ScriptDir "monitor\docker-compose.yaml"
        Setup       = { }
        Ports       = @(
            "Grafana    -> http://localhost:3000"
            "Prometheus -> http://localhost:9090"
        )
    }
    lakehouse = @{
        ComposeFile = Join-Path $script:ScriptDir "lakehouse\docker-compose.yml"
        Setup       = { }
        Ports       = @(
            "Nessie   -> http://localhost:19120"
            "Trino    -> http://localhost:8090"
            "Superset -> http://localhost:8088  (admin / admin)"
        )
    }
    airflow   = @{
        ComposeFile = Join-Path $script:ScriptDir "airflow\docker-compose.yaml"
        Setup       = { Ensure-AirflowDirectories }
        Ports       = @(
            "Airflow UI -> http://localhost:8080  (airflow / airflow)"
        )
    }
}

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

function Write-Info([string]$msg)    { Write-Host $msg -ForegroundColor Cyan }
function Write-Success([string]$msg) { Write-Host $msg -ForegroundColor Green }
function Write-Warn([string]$msg)    { Write-Host $msg -ForegroundColor Yellow }
function Write-Err([string]$msg)     { Write-Host $msg -ForegroundColor Red }

function Write-ErrorAndExit([string]$msg, [int]$code = 1) {
    Write-Err $msg
    exit $code
}

function Test-CommandExists([string]$name) {
    return $null -ne (Get-Command $name -ErrorAction SilentlyContinue)
}

function Invoke-DockerCompose([string[]]$ComposeArgs) {
    & docker compose @ComposeArgs
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed (exit $LASTEXITCODE)"
    }
}

# ---------------------------------------------------------------------------
# Prerequisites
# ---------------------------------------------------------------------------

function Ensure-Prerequisites {
    if (-not (Test-CommandExists "docker")) {
        Write-ErrorAndExit "Docker CLI not found. Install Docker Desktop first."
    }
    & docker version | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-ErrorAndExit "Docker not responding. Start Docker Desktop first."
    }
}

function Ensure-Network {
    $exists = (& docker network ls --filter "name=^$script:NetworkName$" --format "{{.Name}}") 2>$null
    if ($exists -notcontains $script:NetworkName) {
        Write-Info "Creating Docker network '$script:NetworkName'..."
        & docker network create $script:NetworkName | Out-Null
    }
}

function Ensure-MlflowEnvFile {
    $envPath = Join-Path $script:ScriptDir "mlflow\.env"
    if (Test-Path $envPath) { return }
    Write-Warn "mlflow/.env not found -- creating with defaults..."
    $content = @(
        "MYSQL_DATABASE=mlflow",
        "MYSQL_USER=mlflow",
        "MYSQL_PASSWORD=mlflow",
        "MYSQL_ROOT_PASSWORD=root",
        "AWS_ACCESS_KEY_ID=minio",
        "AWS_SECRET_ACCESS_KEY=minio123"
    )
    $content | Set-Content -Path $envPath -Encoding ASCII
}

function Ensure-MinioBuckets {
    # Buckets (mlflow, lakehouse) are created automatically by the 'mc' and 'mc-lakehouse'
    # init containers defined in each compose file. We just wait for them to complete.
    $deadline = (Get-Date).AddSeconds(90)
    Write-Info "  Waiting for MinIO mc init container to finish..."
    while ((Get-Date) -lt $deadline) {
        $state = (& docker inspect --format "{{.State.Status}}" aio_minio_mc 2>$null)
        if ($state -eq "exited") {
            $exit = (& docker inspect --format "{{.State.ExitCode}}" aio_minio_mc 2>$null)
            if ($exit -eq "0") { Write-Success "  MinIO bucket 'mlflow' is ready."; return }
        }
        Start-Sleep -Seconds 3
    }
    Write-Warn "  WARNING: mc init container did not exit cleanly -- check MinIO manually."
}

function Ensure-AirflowDirectories {
    $airflowDir = Join-Path $script:ScriptDir "airflow"
    foreach ($folder in @("logs", "plugins", "config")) {
        $path = Join-Path $airflowDir $folder
        if (-not (Test-Path $path)) {
            New-Item -ItemType Directory -Path $path | Out-Null
        }
    }
}

# ---------------------------------------------------------------------------
# Service helpers
# ---------------------------------------------------------------------------

function Get-SelectedServices {
    if ($Service -eq "all") { return $script:Services }
    return @($Service)
}

function Get-ComposeFile([string]$name) {
    $f = $script:ServiceConfig[$name].ComposeFile
    if (-not (Test-Path $f)) {
        throw "Compose file not found for '$name': $f"
    }
    return $f
}

function Invoke-ServiceSetup([string]$name) {
    $block = $script:ServiceConfig[$name].Setup
    if ($null -ne $block) { & $block }
}

function Show-ServicePorts([string]$name) {
    $ports = $script:ServiceConfig[$name].Ports
    if ($ports) {
        foreach ($p in $ports) {
            Write-Host "    $p" -ForegroundColor DarkCyan
        }
    }
}

function Get-StartArguments([string]$name, [string]$composeFile) {
    $composeArgs = @("-f", $composeFile, "up", "-d")
    return $composeArgs
}

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

function Start-Services {
    Ensure-Network

    foreach ($name in Get-SelectedServices) {
        $composeFile = Get-ComposeFile $name
        Invoke-ServiceSetup $name
        Write-Info "Starting $name..."
        Invoke-DockerCompose (Get-StartArguments -Name $name -ComposeFile $composeFile)

        # Wait for critical services before starting dependents
        if ($name -eq "mlflow") {
            Write-Warn "  Waiting 10s for MinIO to be ready..."
            Start-Sleep -Seconds 10
            Ensure-MinioBuckets
        }
        if ($name -eq "lakehouse") {
            Write-Warn "  Waiting 15s for Nessie + Trino to be ready..."
            Start-Sleep -Seconds 15
        }
    }

    Write-Success "`n=== All services started ==="
    foreach ($name in Get-SelectedServices) {
        Write-Host "`n  [$name]" -ForegroundColor Yellow
        Show-ServicePorts $name
    }
    Write-Host ""
}

function Stop-Services {
    # Stop in reverse order
    $selected = [System.Collections.ArrayList]@(Get-SelectedServices)
    $selected.Reverse()

    foreach ($name in $selected) {
        $composeFile = Get-ComposeFile $name
        Write-Info "Stopping $name..."
        Invoke-DockerCompose @("-f", $composeFile, "down")
    }
    Write-Success "Services stopped."
}

function Restart-Services {
    Stop-Services
    Start-Sleep -Seconds 3
    Start-Services
}

function Invoke-BuildServices {
    foreach ($name in Get-SelectedServices) {
        $composeFile = Get-ComposeFile $name
        Write-Info "Building $name..."
        Invoke-DockerCompose @("-f", $composeFile, "build", "--no-cache")
    }
    Write-Success "Build complete."
}

function Show-Logs {
    if ($Service -eq "all") {
        Write-Warn "Specify a service for logs. Example: .\run.ps1 logs airflow"
        return
    }
    $composeFile = Get-ComposeFile $Service
    & docker compose -f $composeFile logs --tail=100 -f
}

function Show-Status {
    foreach ($name in Get-SelectedServices) {
        $composeFile = Get-ComposeFile $name
        Write-Info "[$name]"
        Invoke-DockerCompose @("-f", $composeFile, "ps")
        Write-Host ""
    }
}

function Show-Help {
    Write-Host ""
    Write-Host "MLOps Platform -- Docker Stack Runner (Windows)" -ForegroundColor White
    Write-Host ""
    Write-Host "USAGE"
    Write-Host "  .\run.ps1 <command> [service]"
    Write-Host ""
    Write-Host "COMMANDS"
    Write-Host "  start    Start service(s)       .\run.ps1 start all"
    Write-Host "  stop     Stop service(s)        .\run.ps1 stop all"
    Write-Host "  restart  Restart service(s)     .\run.ps1 restart airflow"
    Write-Host "  build    Rebuild image(s)        .\run.ps1 build airflow"
    Write-Host "  status   Show container status  .\run.ps1 status all"
    Write-Host "  logs     Tail logs (1 service)  .\run.ps1 logs airflow"
    Write-Host "  help     Show this message"
    Write-Host ""
    Write-Host "SERVICES"
    Write-Host "  all        All services (in startup order)"
    Write-Host "  mlflow     MLflow tracking + MinIO + MySQL"
    Write-Host "  kafka      Kafka cluster (3 brokers)"
    Write-Host "  monitor    Loki + Prometheus + Grafana"
    Write-Host "  lakehouse  Nessie (Iceberg catalog) + Trino + Superset"
    Write-Host "  airflow    Airflow (scheduler, worker, webserver...)"
    Write-Host ""
    Write-Host "STARTUP ORDER"
    Write-Host "  mlflow -> lakehouse -> kafka -> monitor -> airflow"
    Write-Host "  (MinIO must run before Nessie; Nessie must run before Airflow)"
    Write-Host ""
    Write-Host "PORTS"
    Write-Host "  MLflow UI   http://localhost:5000"
    Write-Host "  MinIO UI    http://localhost:9001   (minio / minio123)"
    Write-Host "  Nessie      http://localhost:19120"
    Write-Host "  Trino       http://localhost:8090"
    Write-Host "  Superset    http://localhost:8088   (admin / admin)"
    Write-Host "  Airflow UI  http://localhost:8080   (airflow / airflow)"
    Write-Host "  Grafana     http://localhost:3000"
    Write-Host "  Prometheus  http://localhost:9090"
    Write-Host ""
    Write-Host "NOTE"
    Write-Host "  - Network '$script:NetworkName' is created automatically."
    Write-Host "  - mlflow/.env is created automatically if missing."
    Write-Host "  - After changing Airflow requirements.txt: .\run.ps1 build airflow"
    Write-Host ""
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

try {
    switch ($Command) {
        "help"    { Show-Help }
        default {
            Ensure-Prerequisites
            switch ($Command) {
                "start"   { Start-Services }
                "stop"    { Stop-Services }
                "restart" { Restart-Services }
                "build"   { Invoke-BuildServices }
                "logs"    { Show-Logs }
                "status"  { Show-Status }
                default   { Show-Help }
            }
        }
    }
} catch {
    Write-ErrorAndExit $_.Exception.Message
}
