# scripts/test_4_1_arquitectura.ps1
# Prueba 4.1: master, workers, client (job completo de WordCount)

$ErrorActionPreference = "Stop"

function Step {
    param(
        [string]$Title,
        [scriptblock]$Body
    )
    Write-Host ""
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host $Title -ForegroundColor Yellow
    Write-Host "==========================================" -ForegroundColor Cyan
    & $Body
}

Write-Host "==== [4.1] Test de Arquitectura Basica ====" -ForegroundColor Cyan
Write-Host "Este script:" -ForegroundColor Cyan
Write-Host "  1) Build de imagenes" -ForegroundColor DarkCyan
Write-Host "  2) Levanta master + 3 workers" -ForegroundColor DarkCyan
Write-Host "  3) Verifica workers via CLI" -ForegroundColor DarkCyan
Write-Host "  4) Envia un job WordCount y muestra estado/resultados" -ForegroundColor DarkCyan
Write-Host ""

# STEP 1: Build de imagenes
Step "STEP 1 - Build de imagenes (master, worker, client)" {
    Write-Host ">> docker compose build master worker client" -ForegroundColor Yellow
    docker compose build master worker client
}

# STEP 2: Levantar master + 3 workers
Step "STEP 2 - Levantar master + 3 workers en segundo plano" {
    Write-Host ">> docker compose up -d master worker --scale worker=3" -ForegroundColor Yellow
    docker compose up -d master worker --scale worker=3

    Write-Host ""
    Write-Host ">> Servicios activos:" -ForegroundColor Yellow
    docker compose ps

    Write-Host ""
    Write-Host "Esperando 5s para que los workers se registren y manden heartbeats..." -ForegroundColor Yellow
    Start-Sleep -Seconds 5
}

# STEP 3: Ver workers desde el CLI
Step "STEP 3 - Consultar workers via CLI (client workers)" {
    Write-Host ">> docker compose run --rm client workers" -ForegroundColor Yellow
    docker compose run --rm client workers
}

# STEP 4: Enviar job WordCount
$global:jobId = $null
Step 'STEP 4 - Enviar job WordCount (submit "arch-test")' {
    Write-Host ">> docker compose run --rm client submit `\"arch-test`\"" -ForegroundColor Yellow
    $submitOut = docker compose run --rm client submit "arch-test"

    Write-Host ""
    Write-Host "Salida del submit:" -ForegroundColor Green
    $submitOut | ForEach-Object { Write-Host "  $_" }

    # Extraer el job_id de la linea "id: ..."
    $line = $submitOut | Select-String "id:" | Select-Object -First 1
    if (-not $line) {
        Write-Host ""
        Write-Host "ERROR: No pude encontrar la linea con 'id:' en la salida del submit." -ForegroundColor Red
        exit 1
    }

    $global:jobId = $line.ToString().Split(":")[1].Trim()
    Write-Host ""
    Write-Host "Job ID detectado: $global:jobId" -ForegroundColor Green
}

# STEP 5: Poll de estado hasta SUCCEEDED/FAILED
Step "STEP 5 - Consultar estado del job hasta que termine" {
    if (-not $global:jobId) {
        Write-Host "ERROR: jobId vacio, algo salio mal en el submit." -ForegroundColor Red
        exit 1
    }

    for ($i = 0; $i -lt 15; $i++) {
        Write-Host ""
        Write-Host "== Intento $($i+1) ==" -ForegroundColor DarkCyan
        $statusOut = docker compose run --rm client status $global:jobId
        $statusOut | ForEach-Object { Write-Host "  $_" }

        $statusLine = $statusOut | Select-String "estado:" | Select-Object -First 1
        if ($statusLine -and ($statusLine.ToString() -match "SUCCEEDED|FAILED")) {
            Write-Host ""
            Write-Host "Job llego a estado final." -ForegroundColor Green
            break
        }

        Write-Host "Job aun en progreso, esperando 2s..." -ForegroundColor Yellow
        Start-Sleep -Seconds 2
    }
}

# STEP 6: Ver resultados
Step "STEP 6 - Consultar resultados del job" {
    if (-not $global:jobId) {
        Write-Host "ERROR: jobId vacio, no se pueden consultar resultados." -ForegroundColor Red
        exit 1
    }

    Write-Host ">> docker compose run --rm client results $global:jobId" -ForegroundColor Yellow
    $resultsOut = docker compose run --rm client results $global:jobId
    $resultsOut | ForEach-Object { Write-Host "  $_" }
}

Write-Host ""
Write-Host "==== [4.1] Test completado. Ver arriba:" -ForegroundColor Cyan
Write-Host "  - Registro de workers (client workers)" -ForegroundColor DarkCyan
Write-Host "  - Envio de job (submit)" -ForegroundColor DarkCyan
Write-Host "  - Estado del job (status)" -ForegroundColor DarkCyan
Write-Host "  - Descarga de resultados (results)" -ForegroundColor DarkCyan
