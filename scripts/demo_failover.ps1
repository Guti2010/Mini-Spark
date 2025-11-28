#!/usr/bin/env pwsh
$ErrorActionPreference = "Stop"

. "$PSScriptRoot\helpers.ps1"

# Estos son los NOMBRES DE SERVICIO en docker-compose.yml,
# no los nombres de contenedor.
$MasterService  = "master"
$Worker1Service = "worker-1"
$Worker2Service = "worker-2"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Demo: tolerancia a fallos (failover)" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Cyan


Write-Host ""
Write-Host "1) Enviando job WordCount grande (wc-failover)..."

$submitOutput = docker compose run --rm client word-count "wc-failover"
$submitOutput | ForEach-Object { Write-Host $_ }

$JobId = Get-JobIdFromSubmitOutput -Lines $submitOutput
if (-not $JobId) {
    Write-Host "ERROR: no pude extraer el id del job" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Job creado con id = $JobId" -ForegroundColor Green

Write-Host ""
Write-Host "3) Esperando un poco y matando una instancia de $Worker1Service..."


$worker1ContainerId = docker compose ps -q | Select-Object -Index 1

if (-not $worker1ContainerId) {
    Write-Host "ERROR: no encontré contenedor para el servicio $Worker1Service" -ForegroundColor Red
    exit 1
}

docker stop $worker1ContainerId | Out-Null

Start-Sleep -Seconds 3

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
