#!/usr/bin/env pwsh
$ErrorActionPreference = "Stop"

. "$PSScriptRoot\helpers.ps1"

# ajusta si tu servicio se llama distinto
$WorkerService = "worker"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Demo: memoria + spill a disco" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "NOTA: asegúrate de tener MAX_IN_MEM_KEYS configurado en el worker." -ForegroundColor DarkYellow

Write-Host ""
Write-Host "1) Levantando master + worker..."
docker compose up -d master $WorkerService | Out-Null

Start-Sleep -Seconds 3

Write-Host ""
Write-Host "2) Enviando job WordCount (wc-spill)..."

$submitOutput = docker compose run --rm client submit "wc-spill"
$submitOutput | ForEach-Object { Write-Host $_ }

$JobId = Get-JobIdFromSubmitOutput -Lines $submitOutput
if (-not $JobId) {
    Write-Host "ERROR: no pude extraer el id del job" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Job creado con id = $JobId" -ForegroundColor Green

Write-Host ""
Write-Host "3) Polling de estado hasta que termine..."
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
Write-Host "4) Revisando archivos de spill en /data/tmp (dentro del worker)..." -ForegroundColor Yellow

docker compose exec $WorkerService sh -c 'ls -R /data/tmp || echo "(sin spills o dir no existe)"'

Write-Host ""
Write-Host "5) Resultados del job:" -ForegroundColor Yellow
docker compose run --rm client results $JobId

Write-Host ""
Write-Host "Demo de spill de memoria terminada." -ForegroundColor Green
