#!/usr/bin/env pwsh
$ErrorActionPreference = "Stop"

. "$PSScriptRoot\helpers.ps1"

# Servicios definidos en docker-compose.yml
$MasterService  = "master"
$Worker1Service = "worker-1"
$Worker2Service = "worker-2"

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "   TEST INTEGRAL DE OPERADORES ENGINE   " -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Cyan

function Invoke-JobAndWait {
    param(
        [string]$JobName,
        [string]$ExpectedEstado
    )

    Write-Host ""
    Write-Host "Enviando job: $JobName (estado esperado: $ExpectedEstado)" -ForegroundColor Cyan

    $submitOutput = docker compose run --rm client word-count $JobName
    $submitOutput | ForEach-Object { Write-Host $_ }

    $JobId = Get-JobIdFromSubmitOutput -Lines $submitOutput
    if (-not $JobId) {
        Write-Host "ERROR: no pude extraer el JobId" -ForegroundColor Red
        exit 1
    }

    Write-Host "JobId = $JobId" -ForegroundColor Green
    Write-Host "Esperando finalizacion del job..." -ForegroundColor Cyan

    $estadoFinal = $null
    while ($true) {
        $statusOutput = docker compose run --rm client status $JobId
        $estado = Get-JobEstadoFromStatusOutput -Lines $statusOutput

        Write-Host "-------------------------------------"
        $statusOutput | ForEach-Object { Write-Host $_ }

        if ($estado -eq "SUCCEEDED" -or $estado -eq "FAILED") {
            $estadoFinal = $estado
            Write-Host "Estado final: $estadoFinal (esperado: $ExpectedEstado)" -ForegroundColor Yellow
            break
        }

        Start-Sleep -Seconds 1
    }

    if ($estadoFinal -ne $ExpectedEstado) {
        Write-Host "TEST FALLO: se esperaba $ExpectedEstado pero fue $estadoFinal" -ForegroundColor Red
        exit 1
    }

    Write-Host "TEST OK: estado final coincide con lo esperado" -ForegroundColor Green
    return $JobId
}

function Show-Results {
    param([string]$JobId)

    Write-Host ""
    Write-Host "Resultados del operador:" -ForegroundColor Yellow
    docker compose run --rm client results $JobId
}

# ============================================
# PRUEBAS POSITIVAS (esperamos SUCCEEDED)
# ============================================
Write-Host ""
Write-Host "#### TEST 1: MAP (CASO FELIZ) ####" -ForegroundColor Magenta
$jobMap = Invoke-JobAndWait -JobName "all" -ExpectedEstado "SUCCEEDED"
Show-Results -JobId $jobMap

Write-Host ""
Write-Host "#### TEST 2: FILTER (CASO FELIZ) ####" -ForegroundColor Magenta
$jobFilter = Invoke-JobAndWait -JobName "test-filter" -ExpectedEstado "SUCCEEDED"
Show-Results -JobId $jobFilter

Write-Host ""
Write-Host "#### TEST 3: FLATMAP (CASO FELIZ) ####" -ForegroundColor Magenta
$jobFM = Invoke-JobAndWait -JobName "test-flatmap" -ExpectedEstado "SUCCEEDED"
Show-Results -JobId $jobFM

Write-Host ""
Write-Host "#### TEST 4: REDUCE BY KEY (CASO FELIZ) ####" -ForegroundColor Magenta
$jobReduce = Invoke-JobAndWait -JobName "test-reduce" -ExpectedEstado "SUCCEEDED"
Show-Results -JobId $jobReduce

Write-Host ""
Write-Host "#### TEST 5: SHUFFLE + REDUCE (CASO FELIZ) ####" -ForegroundColor Magenta
$jobShuffle = Invoke-JobAndWait -JobName "test-shuffle" -ExpectedEstado "SUCCEEDED"
Show-Results -JobId $jobShuffle

Write-Host ""
Write-Host "#### TEST 6: JOIN POR CLAVE (CASO FELIZ) ####" -ForegroundColor Magenta
$jobJoin = Invoke-JobAndWait -JobName "test-join" -ExpectedEstado "SUCCEEDED"
Show-Results -JobId $jobJoin

# ============================================
# PRUEBAS NEGATIVAS (esperamos FAILED)
# ============================================
Write-Host ""
Write-Host "#### TEST 7: MAP - INPUT INEXISTENTE (DEBE FALLAR) ####" -ForegroundColor Magenta
$jobMapBad = Invoke-JobAndWait -JobName "test-map-bad-input" -ExpectedEstado "FAILED"
Show-Results -JobId $jobMapBad

Write-Host ""
Write-Host "#### TEST 8: REDUCE - VALOR NO NUMERICO (DEBE FALLAR) ####" -ForegroundColor Magenta
$jobReduceBad = Invoke-JobAndWait -JobName "test-reduce-bad-value" -ExpectedEstado "FAILED"
Show-Results -JobId $jobReduceBad

Write-Host ""
Write-Host "#### TEST 9: JOIN - CLAVE FALTANTE (DEBE FALLAR) ####" -ForegroundColor Magenta
$jobJoinBad = Invoke-JobAndWait -JobName "test-join-missing-key" -ExpectedEstado "FAILED"
Show-Results -JobId $jobJoinBad

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "    TODAS LAS PRUEBAS DE OPERADORES     " -ForegroundColor Yellow
Write-Host "        (incluyendo fallos) OK          " -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan