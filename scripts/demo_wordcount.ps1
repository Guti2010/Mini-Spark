#!/usr/bin/env pwsh
$ErrorActionPreference = "Stop"

. "$PSScriptRoot\helpers.ps1"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Demo E2E: WordCount básico" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Cyan

Write-Host ""
Write-Host "1) Levantando master + 1 worker..."
docker compose up -d master worker | Out-Null

Start-Sleep -Seconds 3

Write-Host ""
Write-Host "2) Enviando job WordCount sobre /data/input/*..."

$submitOutput = docker compose run --rm client submit "wc-demo"
$submitOutput | ForEach-Object { Write-Host $_ }

$JobId = Get-JobIdFromSubmitOutput -Lines $submitOutput
if (-not $JobId) {
    Write-Host "ERROR: no pude extraer el id del job" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Job creado con id = $JobId" -ForegroundColor Green

Write-Host ""
Write-Host "3) Consultando estado hasta que termine (SUCCEEDED/FAILED)..."

while ($true) {
    $statusOutput = docker compose run --rm client status $JobId
    $estado = Get-JobEstadoFromStatusOutput -Lines $statusOutput

    Write-Host "-------------------------------"
    $statusOutput | ForEach-Object { Write-Host $_ }

    if ($estado -eq "SUCCEEDED" -or $estado -eq "FAILED") {
        break
    }

    Start-Sleep -Seconds 2
}

Write-Host ""
Write-Host "4) Resultados del job:" -ForegroundColor Yellow
docker compose run --rm client results $JobId

Write-Host ""
Write-Host "5) Métricas de workers (/api/v1/workers):" -ForegroundColor Yellow
docker compose run --rm client workers

Write-Host ""
Write-Host "Demo WordCount básica terminada." -ForegroundColor Green
