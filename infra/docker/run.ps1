param(
    [Parameter(Position = 0)]
    [ValidateSet("start", "stop", "restart", "status", "help")]
    [string]$Command = "help",

    [Parameter(Position = 1)]
    [ValidateSet("all", "mlflow", "kafka", "monitor", "airflow")]
    [string]$Service = "all"
)

$ErrorActionPreference = "Stop"

$script:ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$script:NetworkName = "aio-network"
$script:IsWindowsHost = $env:OS -eq "Windows_NT"
$script:Services = @("mlflow", "kafka", "monitor", "airflow")
$script:ServiceConfig = @{
    mlflow = @{
        ComposeFile = Join-Path $script:ScriptDir "mlflow\docker-compose.yaml"
        Setup = { Ensure-MlflowEnvFile }
    }
    kafka = @{
        ComposeFile = Join-Path $script:ScriptDir "kafka\docker-compose.yaml"
        Setup = { }
    }
    monitor = @{
        ComposeFile = Join-Path $script:ScriptDir "monitor\docker-compose.yaml"
        Setup = { }
    }
    airflow = @{
        ComposeFile = Join-Path $script:ScriptDir "airflow\docker-compose.yaml"
        Setup = { Ensure-AirflowDirectories }
    }
}

function Write-Info([string]$Message) {
    Write-Host $Message -ForegroundColor Cyan
}

function Write-Success([string]$Message) {
    Write-Host $Message -ForegroundColor Green
}

function Write-Warn([string]$Message) {
    Write-Host $Message -ForegroundColor Yellow
}

function Write-ErrorAndExit([string]$Message, [int]$Code = 1) {
    Write-Host $Message -ForegroundColor Red
    exit $Code
}

function Test-CommandExists([string]$Name) {
    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Invoke-DockerCompose([string[]]$ComposeArgs) {
    & docker compose @ComposeArgs
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed with exit code $LASTEXITCODE"
    }
}

function Ensure-Prerequisites {
    if (-not (Test-CommandExists "docker")) {
        Write-ErrorAndExit "Docker CLI was not found. Please install Docker Desktop and try again."
    }

    & docker version | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-ErrorAndExit "Docker is installed but not responding. Please start Docker Desktop and try again."
    }
}

function Ensure-Network {
    $networkId = (& docker network ls --filter "name=^$script:NetworkName$" --format "{{.Name}}") 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to check Docker networks."
    }

    if ($networkId -contains $script:NetworkName) {
        return
    }

    Write-Info "Creating Docker network '$script:NetworkName'..."
    & docker network create $script:NetworkName | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create Docker network '$script:NetworkName'."
    }
}

function Ensure-MlflowEnvFile {
    $envPath = Join-Path $script:ScriptDir "mlflow\.env"
    if (Test-Path $envPath) {
        return
    }

    Write-Warn "MLflow .env not found. Creating a default file at $envPath"
    @"
MYSQL_DATABASE=mlflow
MYSQL_USER=mlflow
MYSQL_PASSWORD=mlflow
MYSQL_ROOT_PASSWORD=root
AWS_ACCESS_KEY_ID=minio
AWS_SECRET_ACCESS_KEY=minio123
"@ | Set-Content -Path $envPath -Encoding ASCII
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

function Get-SelectedServices {
    if ($Service -eq "all") {
        return $script:Services
    }

    return @($Service)
}

function Get-ComposeFile([string]$Name) {
    $composeFile = $script:ServiceConfig[$Name].ComposeFile
    if (-not (Test-Path $composeFile)) {
        throw "Compose file not found for service '$Name': $composeFile"
    }

    return $composeFile
}

function Invoke-ServiceSetup([string]$Name) {
    $setupBlock = $script:ServiceConfig[$Name].Setup
    if ($null -ne $setupBlock) {
        & $setupBlock
    }
}

function Get-StartArguments([string]$Name, [string]$ComposeFile) {
    $args = @("-f", $ComposeFile, "up", "-d")

    if ($Name -eq "monitor" -and $script:IsWindowsHost) {
        # node_exporter and dcgm-exporter use Linux-specific host mounts/runtime settings.
        $args += @("loki", "prometheus", "grafana")
    }

    return $args
}

function Start-Services {
    Ensure-Network

    foreach ($name in Get-SelectedServices) {
        $composeFile = Get-ComposeFile $name
        Invoke-ServiceSetup $name
        Write-Info "Starting $name..."
        Invoke-DockerCompose (Get-StartArguments -Name $name -ComposeFile $composeFile)
    }

    Write-Success "Requested services started."
}

function Stop-Services {
    foreach ($name in Get-SelectedServices) {
        $composeFile = Get-ComposeFile $name
        Write-Info "Stopping $name..."
        Invoke-DockerCompose @("-f", $composeFile, "down")
    }

    Write-Success "Requested services stopped."
}

function Restart-Services {
    Stop-Services
    Start-Services
}

function Show-Status {
    foreach ($name in Get-SelectedServices) {
        $composeFile = Get-ComposeFile $name
        Write-Info ("Status for {0}:" -f $name)
        Invoke-DockerCompose @("-f", $composeFile, "ps")
        Write-Host ""
    }
}

function Show-Help {
    @"
Windows Docker stack runner for this repo

Usage:
  .\run.ps1 <start|stop|restart|status|help> [all|mlflow|kafka|monitor|airflow]

Examples:
  .\run.ps1 start all
  .\run.ps1 start mlflow
  .\run.ps1 status airflow
  .\run.ps1 stop all

Notes:
  - Automatically creates the external Docker network '$script:NetworkName' if needed.
  - Automatically creates 'infra\docker\mlflow\.env' with default values if missing.
"@ | Write-Host
}

try {
    switch ($Command) {
        "help" { Show-Help }
        default {
            Ensure-Prerequisites

            switch ($Command) {
                "start" { Start-Services }
                "stop" { Stop-Services }
                "restart" { Restart-Services }
                "status" { Show-Status }
                default { Show-Help }
            }
        }
    }
} catch {
    Write-ErrorAndExit $_.Exception.Message
}
