param(
    [Parameter(Position = 0)]
    [ValidateSet("deploy", "teardown", "status", "token", "help")]
    [string]$Command = "help",

    [Parameter(Position = 1)]
    [ValidateSet("all", "core", "dashboard", "airflow")]
    [string]$Target = "all"
)

$ErrorActionPreference = "Stop"

$script:ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$script:Namespace = "mlops"
$script:DashboardNamespace = "kubernetes-dashboard"

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

function Invoke-WithKubectlNoProxy([scriptblock]$ScriptBlock) {
    $proxyVars = @("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")
    $originalValues = @{}

    foreach ($name in $proxyVars) {
        $originalValues[$name] = [Environment]::GetEnvironmentVariable($name)
        [Environment]::SetEnvironmentVariable($name, "", "Process")
    }

    try {
        & $ScriptBlock
    } finally {
        foreach ($name in $proxyVars) {
            [Environment]::SetEnvironmentVariable($name, $originalValues[$name], "Process")
        }
    }
}

function Invoke-Kubectl([string[]]$CommandArgs) {
    Invoke-WithKubectlNoProxy {
        & kubectl @CommandArgs
    }
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl failed with exit code $LASTEXITCODE"
    }
}

function Ensure-Prerequisites {
    if (-not (Test-CommandExists "kubectl")) {
        Write-ErrorAndExit "kubectl was not found. Please install kubectl and try again."
    }

    $context = Invoke-WithKubectlNoProxy {
        (& kubectl config current-context) 2>$null
    }
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($context)) {
        Write-ErrorAndExit "kubectl is installed but no active cluster context is configured."
    }
}

function Apply-File([string]$RelativePath) {
    $path = Join-Path $script:ScriptDir $RelativePath
    Write-Info "Applying $RelativePath"
    Invoke-Kubectl @("apply", "-f", $path)
}

function Delete-File([string]$RelativePath) {
    $path = Join-Path $script:ScriptDir $RelativePath
    Write-Info "Deleting $RelativePath"
    Invoke-Kubectl @("delete", "-f", $path, "--ignore-not-found=true")
}

function Wait-ForPod([string]$Namespace, [string]$Label, [int]$TimeoutSeconds = 180) {
    Write-Info "Waiting for pods with label '$Label' in namespace '$Namespace'..."
    try {
        Invoke-Kubectl @("wait", "--for=condition=ready", "pod", "-l", $Label, "-n", $Namespace, "--timeout=$($TimeoutSeconds)s")
    } catch {
        Write-Warn "Pod readiness wait failed. Collecting diagnostics for label '$Label'..."

        Invoke-WithKubectlNoProxy {
            & kubectl get pods -l $Label -n $Namespace -o wide
            if ($LASTEXITCODE -eq 0) {
                $podListJson = & kubectl get pods -l $Label -n $Namespace -o json
                if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($podListJson)) {
                    $podNames = (($podListJson | ConvertFrom-Json).items | ForEach-Object { $_.metadata.name }) |
                        Where-Object { -not [string]::IsNullOrWhiteSpace($_) }

                    foreach ($podName in $podNames) {
                        Write-Host ""
                        Write-Host "Describe for pod/$podName" -ForegroundColor Yellow
                        & kubectl describe pod $podName -n $Namespace
                    }
                }
            }

            Write-Host ""
            Write-Host "Recent events in namespace '$Namespace'" -ForegroundColor Yellow
            & kubectl get events -n $Namespace --sort-by=.lastTimestamp
        }

        throw
    }
}

function Wait-ForJob([string]$Namespace, [string]$JobName, [int]$TimeoutSeconds = 120) {
    Write-Info "Waiting for job '$JobName' in namespace '$Namespace'..."
    Invoke-Kubectl @("wait", "--for=condition=complete", "job/$JobName", "-n", $Namespace, "--timeout=$($TimeoutSeconds)s")
}

function Wait-ForResourceDeletion([string]$Namespace, [string]$ResourceType, [string]$ResourceName, [int]$TimeoutSeconds = 180) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    Write-Info "Waiting for $ResourceType/$ResourceName to be deleted from namespace '$Namespace'..."

    do {
        $resourceOutput = Invoke-WithKubectlNoProxy {
            (& kubectl get $ResourceType $ResourceName -n $Namespace --ignore-not-found -o name) 2>$null
        }

        if ($LASTEXITCODE -eq 0 -and [string]::IsNullOrWhiteSpace($resourceOutput)) {
            return
        }

        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)

    throw "Timed out waiting for $ResourceType/$ResourceName to be deleted in namespace '$Namespace'."
}

function Get-DashboardToken {
    $tokenFile = Join-Path $script:ScriptDir "dashboard-token.txt"
    if (Test-Path $tokenFile) {
        return (Get-Content $tokenFile -Raw).Trim()
    }

    $token = Invoke-WithKubectlNoProxy {
        (& kubectl -n $script:DashboardNamespace create token admin-user --duration=87600h) 2>$null
    }
    if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($token)) {
        $token.Trim() | Set-Content -Path $tokenFile -Encoding ASCII
        return $token.Trim()
    }

    throw "Unable to create dashboard token. Make sure the dashboard admin service account exists."
}

function Show-Token {
    $token = Get-DashboardToken
    Write-Host ""
    Write-Host "Kubernetes Dashboard token:" -ForegroundColor Green
    Write-Host $token
    Write-Host ""
    Write-Host "Use with:"
    Write-Host "  kubectl proxy"
    Write-Host "  http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/"
}

function Deploy-Core {
    Apply-File "namespace.yaml"

    Apply-File "postgres/postgres-secret.yaml"
    Apply-File "postgres/postgres-pvc.yaml"
    Apply-File "postgres/postgres-deployment.yaml"
    Apply-File "postgres/postgres-service.yaml"
    Wait-ForPod -Namespace $script:Namespace -Label "app=postgres" -TimeoutSeconds 180

    Apply-File "minio/minio-secret.yaml"
    Apply-File "minio/minio-pvc.yaml"
    Apply-File "minio/minio-deployment.yaml"
    Apply-File "minio/minio-service.yaml"
    Wait-ForPod -Namespace $script:Namespace -Label "app=minio" -TimeoutSeconds 180

    Apply-File "minio/minio-bucket-job.yaml"
    Wait-ForJob -Namespace $script:Namespace -JobName "minio-create-bucket" -TimeoutSeconds 120

    Apply-File "mlflow/mlflow-config.yaml"
    Apply-File "mlflow/mlflow-deployment.yaml"
    Apply-File "mlflow/mlflow-service.yaml"
    Wait-ForPod -Namespace $script:Namespace -Label "app=mlflow" -TimeoutSeconds 240

    Apply-File "kafka/kafka-config.yaml"
    Apply-File "kafka/kafka-statefulset.yaml"
    Apply-File "kafka/kafka-service.yaml"
    Wait-ForPod -Namespace $script:Namespace -Label "app=kafka" -TimeoutSeconds 360

    Apply-File "kafka/kafka-ui-deployment.yaml"
    Apply-File "kafka/kafka-ui-service.yaml"
    Wait-ForPod -Namespace $script:Namespace -Label "app=kafka-ui" -TimeoutSeconds 180

    Apply-File "redis/redis.yaml"
    Wait-ForPod -Namespace $script:Namespace -Label "app=redis" -TimeoutSeconds 180
}

function Deploy-Dashboard {
    Apply-File "dashboard/dashboard-namespace.yaml"
    Apply-File "dashboard/dashboard-serviceaccount.yaml"
    Apply-File "dashboard/dashboard-rbac.yaml"
    Apply-File "dashboard/dashboard-secret.yaml"
    Apply-File "dashboard/dashboard-configmap.yaml"
    Apply-File "dashboard/dashboard-deployment.yaml"
    Apply-File "dashboard/dashboard-service.yaml"
    Wait-ForPod -Namespace $script:DashboardNamespace -Label "k8s-app=kubernetes-dashboard" -TimeoutSeconds 180

    try {
        $token = Get-DashboardToken
        Write-Success "Dashboard token saved to dashboard-token.txt"
        Write-Host $token
    } catch {
        Write-Warn $_.Exception.Message
    }
}

function Deploy-Airflow {
    Write-Warn "Airflow uses the official image 'apache/airflow:3.1.5-python3.11' and installs extra Python packages at container startup. First boot can take several minutes."

    Apply-File "airflow/airflow-rbac.yaml"
    Apply-File "airflow/airflow-secret.yaml"
    Apply-File "airflow/airflow-passwords.yaml"
    Apply-File "airflow/airflow-config.yaml"
    Apply-File "airflow/airflow-pvc.yaml"
    Apply-File "airflow/airflow-postgres.yaml"
    Wait-ForPod -Namespace $script:Namespace -Label "app=airflow-postgres" -TimeoutSeconds 300

    Apply-File "airflow/airflow-scheduler.yaml"
    Apply-File "airflow/airflow-webserver.yaml"
    Wait-ForPod -Namespace $script:Namespace -Label "app=airflow-webserver" -TimeoutSeconds 900
}

function Deploy-Target {
    switch ($Target) {
        "core" { Deploy-Core }
        "dashboard" { Deploy-Dashboard }
        "airflow" { Deploy-Airflow }
        "all" {
            Deploy-Core
            Deploy-Dashboard
            Deploy-Airflow
        }
    }

    Write-Success "Deployment finished for target '$Target'."
}

function Teardown-Core {
    Delete-File "redis/redis.yaml"

    Delete-File "kafka/kafka-ui-service.yaml"
    Delete-File "kafka/kafka-ui-deployment.yaml"
    Delete-File "kafka/kafka-service.yaml"
    Delete-File "kafka/kafka-statefulset.yaml"
    Delete-File "kafka/kafka-config.yaml"
    & kubectl delete pvc -l app=kafka -n $script:Namespace --ignore-not-found=true | Out-Null

    Delete-File "mlflow/mlflow-service.yaml"
    Delete-File "mlflow/mlflow-deployment.yaml"
    Delete-File "mlflow/mlflow-config.yaml"

    Delete-File "minio/minio-bucket-job.yaml"
    Delete-File "minio/minio-service.yaml"
    Delete-File "minio/minio-deployment.yaml"
    Delete-File "minio/minio-pvc.yaml"
    Delete-File "minio/minio-secret.yaml"

    Delete-File "postgres/postgres-service.yaml"
    Delete-File "postgres/postgres-deployment.yaml"
    Delete-File "postgres/postgres-pvc.yaml"
    Delete-File "postgres/postgres-secret.yaml"
}

function Teardown-Dashboard {
    Delete-File "dashboard/dashboard-service.yaml"
    Delete-File "dashboard/dashboard-deployment.yaml"
    Delete-File "dashboard/dashboard-configmap.yaml"
    Delete-File "dashboard/dashboard-secret.yaml"
    Delete-File "dashboard/dashboard-rbac.yaml"
    Delete-File "dashboard/dashboard-serviceaccount.yaml"
    Delete-File "dashboard/dashboard-namespace.yaml"

    $tokenFile = Join-Path $script:ScriptDir "dashboard-token.txt"
    if (Test-Path $tokenFile) {
        Remove-Item -LiteralPath $tokenFile -Force
    }
}

function Teardown-Airflow {
    Delete-File "airflow/airflow-webserver.yaml"
    Delete-File "airflow/airflow-scheduler.yaml"
    Delete-File "airflow/airflow-postgres.yaml"
    Delete-File "airflow/airflow-pvc.yaml"
    Wait-ForResourceDeletion -Namespace $script:Namespace -ResourceType "pvc" -ResourceName "airflow-dags-pvc" -TimeoutSeconds 240
    Wait-ForResourceDeletion -Namespace $script:Namespace -ResourceType "pvc" -ResourceName "airflow-logs-pvc" -TimeoutSeconds 240
    Wait-ForResourceDeletion -Namespace $script:Namespace -ResourceType "pvc" -ResourceName "airflow-postgres-pvc" -TimeoutSeconds 240
    Delete-File "airflow/airflow-config.yaml"
    Delete-File "airflow/airflow-passwords.yaml"
    Delete-File "airflow/airflow-secret.yaml"
    Delete-File "airflow/airflow-rbac.yaml"
}

function Teardown-Target {
    switch ($Target) {
        "core" { Teardown-Core }
        "dashboard" { Teardown-Dashboard }
        "airflow" { Teardown-Airflow }
        "all" {
            Teardown-Dashboard
            Teardown-Airflow
            Teardown-Core
        }
    }

    Write-Success "Teardown finished for target '$Target'."
}

function Show-Status {
    Write-Info "Namespace status:"
    Invoke-WithKubectlNoProxy {
        & kubectl get all -n $script:Namespace
        Write-Host ""
        & kubectl get pvc -n $script:Namespace
        Write-Host ""
        & kubectl get all -n $script:DashboardNamespace 2>$null
    }
}

function Show-Help {
    @"
Windows Kubernetes runner for this repo

Usage:
  .\run.ps1 <deploy|teardown|status|token|help> [all|core|dashboard|airflow]

Examples:
  .\run.ps1 deploy core
  .\run.ps1 deploy all
  .\run.ps1 status
  .\run.ps1 token
  .\run.ps1 teardown airflow

Notes:
  - 'core' deploys namespace, PostgreSQL, MinIO, MLflow, Kafka, and Kafka UI.
  - 'dashboard' deploys Kubernetes Dashboard and saves the token to 'dashboard-token.txt'.
  - 'airflow' pulls the official Apache Airflow image and installs extra Python packages at startup, so the first deploy is slower but works without a prebuilt custom image.
"@ | Write-Host
}

try {
    switch ($Command) {
        "help" { Show-Help }
        default {
            Ensure-Prerequisites

            switch ($Command) {
                "deploy" { Deploy-Target }
                "teardown" { Teardown-Target }
                "status" { Show-Status }
                "token" { Show-Token }
                default { Show-Help }
            }
        }
    }
} catch {
    Write-ErrorAndExit $_.Exception.Message
}
