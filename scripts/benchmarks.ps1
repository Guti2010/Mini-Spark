#!/usr/bin/env pwsh
$ErrorActionPreference = "Stop"

. "$PSScriptRoot\helpers.ps1"

$Runs = 3
$NamePrefix = "wc-bench"
$InputDesc = "1M-registros"

# ajusta nombres de servicios
$Worker1 = "worker-1"
$Worker2 = "worker-2"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Benchmarks WordCount" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Cyan

docker compose up -d master $Worker1 $Worker2 | Out-Null
Start-Sleep -Seconds 3

for ($i = 1; $i -le $Runs; $i++) {
    $name = "$NamePrefix-$InputDesc-run$i"
    Write-Host ""
    Write-Host ">> Run $i/$Runs : $name" -ForegroundColor Yellow

    $start = Get-Date

    $submitOutput = docker compose run --rm client submit $name
    $submitOutput | ForEach-Object { Write-Host $_ }

    $JobId = Get-JobIdFromSubmitOutput -Lines $submitOutput
    if (-not $JobId) {
        Write-Host "ERROR: no pude extraer el id del job" -ForegroundColor Red
        break
    }

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

    $end = Get-Date
    $duration = ($end - $start).TotalSeconds
    Write-Host "Duración run $i: $duration s" -ForegroundColor Green
}

Write-Host ""
Write-Host "Benchmarks terminados. Copia estos tiempos al reporte." -ForegroundColor Green
