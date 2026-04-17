param(
    [string]$TrainingDataPath = "..\data-pipeline\churn_feature_store\churn_features\feature_repo\data\processed_churn_data.parquet",
    [string]$PythonExe = "python",
    [switch]$ContinueOnError
)

$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$trainScript = Join-Path $projectRoot "src\scripts\train.py"
$configDir = Join-Path $projectRoot "src\config"

if ($PythonExe -eq "python") {
    $repoVenvPython = Join-Path (Split-Path $projectRoot -Parent) ".venv\Scripts\python.exe"
    if (Test-Path $repoVenvPython) {
        $PythonExe = $repoVenvPython
    }
}

if (-not (Test-Path $trainScript)) {
    throw "Cannot find training script: $trainScript"
}

$resolvedDataPath = [System.IO.Path]::GetFullPath((Join-Path $projectRoot $TrainingDataPath))
if (-not (Test-Path $resolvedDataPath)) {
    throw "Training data file not found: $resolvedDataPath"
}

$env:PYTHONPATH = $projectRoot

$models = @(
    @{ Prefix = "logreg"; Name = "logistic_regression"; Config = (Join-Path $configDir "logistic_regression.yaml") },
    @{ Prefix = "dtree"; Name = "decision_tree"; Config = (Join-Path $configDir "decision_tree.yaml") },
    @{ Prefix = "rf"; Name = "random_forest"; Config = (Join-Path $configDir "random_forest.yaml") },
    @{ Prefix = "xgb"; Name = "xgboost"; Config = (Join-Path $configDir "xgboost.yaml") },
    @{ Prefix = "lgbm"; Name = "lightgbm"; Config = (Join-Path $configDir "lightgbm.yaml") },
    @{ Prefix = "cat"; Name = "catboost"; Config = (Join-Path $configDir "catboost.yaml") }
)

Write-Host "Project root: $projectRoot"
Write-Host "Python executable: $PythonExe"
Write-Host "Training data: $resolvedDataPath"

foreach ($model in $models) {
    if (-not (Test-Path $model.Config)) {
        throw "Missing config file: $($model.Config)"
    }

    $runName = "{0}_{1}" -f $model.Prefix, (Get-Date -Format "yyyyMMdd_HHmmss")

    Write-Host ""
    Write-Host "========================================"
    Write-Host "Training model: $($model.Name)"
    Write-Host "Config: $($model.Config)"
    Write-Host "Run name: $runName"
    Write-Host "========================================"

    $args = @(
        $trainScript,
        "--config", $model.Config,
        "--training-data-path", $resolvedDataPath,
        "--run-name", $runName
    )

    & $PythonExe @args
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0) {
        Write-Host "Training failed for $($model.Name) with exit code $exitCode"
        if (-not $ContinueOnError) {
            exit $exitCode
        }
    } else {
        Write-Host "Training completed for $($model.Name)"
    }

    Start-Sleep -Seconds 1
}

Write-Host ""
Write-Host "Done. All requested training jobs finished."