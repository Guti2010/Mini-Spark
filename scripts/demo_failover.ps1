#!/usr/bin/env pwsh
$ErrorActionPreference = "Stop"

. "$PSScriptRoot\helpers.ps1"

# ajusta estos nombres a tus servicios
$Worker1 = "worker-1"
$Worker2 = "worker-2"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Demo: tolerancia a fallos (failover)" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Cyan

Write-Host ""
Write-Host "1) Levantando master + 2 workers..."
docker compose up -d master $Worker1 $Worker2 | Out-Null

Start-Sleep -Seconds 3

Write-Host ""
Write-Host "2) Enviando job WordCount grande (wc-failover)..."

$submitOutput = docker compose run --rm client submit "wc-failover"
$submitOutput | ForEach-Object { Write-Host $_ }

$JobId = Get-JobIdFromSubmitOutput -Lines $submitOutput
if (-not $JobId) {
    Write-Host "ERROR: no pude extraer el id del job" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Job creado con id = $JobId" -ForegroundColor Green

Write-Host ""
Write-Host "3) Esperando un poco y matando $Worker1..."
Start-Sleep -Seconds 5
docker compose stop $Worker1 | Out-Null

Write-Host "Worker parado. El master debería marcarlo DEAD y reencolar tareas." -ForegroundColor Yellow

Write-Host ""
Write-Host "4) Polling de estado del job..."
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
Write-Host "5) Métricas de workers (uno debería verse dead=true, con fallos/reintentos):" -ForegroundColor Yellow
docker compose run --rm client workers

Write-Host ""
Write-Host "6) Resultados del job:" -ForegroundColor Yellow
docker compose run --rm client results $JobId

Write-Host ""
Write-Host "Demo de failover terminada." -ForegroundColor Green
