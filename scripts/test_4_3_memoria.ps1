# scripts/test_4_3_memoria.ps1
# Prueba 4.3: uso de disco (spill) y memoria observada

$ErrorActionPreference = "Stop"

Write-Host "==== [4.3] Test de Almacenamiento y Memoria (Batch) ====" -ForegroundColor Cyan
Write-Host "Este script:" -ForegroundColor Cyan
Write-Host "  1) Genera un archivo grande en docs/input" -ForegroundColor DarkCyan
Write-Host "  2) Levanta master + workers" -ForegroundColor DarkCyan
Write-Host "  3) Ejecuta WordCount sobre ese archivo" -ForegroundColor DarkCyan
Write-Host "  4) Muestra memoria de workers y archivos en /data/tmp" -ForegroundColor DarkCyan
Write-Host ""

# 1) Generar dataset grande en Windows (docs/input/memory-big.txt)
$inputDir = "docs/input"
if (-not (Test-Path $inputDir)) {
    New-Item -ItemType Directory -Path $inputDir | Out-Null
}

$bigFile = Join-Path $inputDir "memory-big.txt"

# ~200k lineas de ejemplo
$lineCount = 200000

Write-Host ">> Generando $lineCount lineas..." -ForegroundColor Yellow

1..$lineCount | ForEach-Object {
    "esta es una linea de prueba para probar cache en memoria y spill a disco en el mini spark $_"
} | Set-Content -Encoding UTF8 $bigFile

Write-Host "Archivo generado con $($lines.Count) líneas." -ForegroundColor Green

# 2) Levantar servicios limpios
Write-Host ""
Write-Host ">> docker compose down (limpiando estado previo)..." -ForegroundColor Yellow
docker compose down

Write-Host ""
Write-Host ">> Levantando master + 2 workers..." -ForegroundColor Yellow
docker compose up -d master worker --scale worker=2

Write-Host ""
Write-Host "Servicios activos:" -ForegroundColor Yellow
docker compose ps

Write-Host ""
Write-Host "Esperando 5s para heartbeats iniciales..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

Write-Host ""
Write-Host ">> Métricas de workers ANTES del job:" -ForegroundColor Yellow
docker compose run --rm client workers

# 3) Ejecutar job "mem-test"
Write-Host ""
Write-Host ">> Enviando job 'mem-test' sobre /data/input/* (incluye memory-big.txt)..." -ForegroundColor Yellow
$submitOut = docker compose run --rm client submit "mem-test"

$line = $submitOut | Select-String "id:" | Select-Object -First 1
if (-not $line) {
    Write-Host "ERROR: No pude extraer job id del submit." -ForegroundColor Red
    exit 1
}
$jobId = $line.ToString().Split(":")[1].Trim()
Write-Host ""
Write-Host "Job ID: $jobId" -ForegroundColor Green

# Poll mientras corre para ver memoria
for ($i = 0; $i -lt 10; $i++) {
    Write-Host ""
    Write-Host "== Poll workers durante ejecución (iter $($i+1)) ==" -ForegroundColor DarkCyan
    docker compose run --rm client workers

    Write-Host ""
    Write-Host "Estado del job:" -ForegroundColor Yellow
    $statusOut = docker compose run --rm client status $jobId
    $statusOut | ForEach-Object { Write-Host "  $_" }

    $statusLine = $statusOut | Select-String "estado:" | Select-Object -First 1
    if ($statusLine -and ($statusLine.ToString() -match "SUCCEEDED|FAILED")) {
        Write-Host ""
        Write-Host "Job llegó a estado final." -ForegroundColor Green
        break
    }

    Start-Sleep -Seconds 2
}

# 4) Ver archivos de spill/shuffle en /data/tmp
Write-Host ""
Write-Host ">> Listando contenido de /data/tmp dentro de un worker..." -ForegroundColor Yellow
# Tomamos cualquier contenedor worker
$workerContainer = docker ps --format "{{.Names}}" | Select-String "worker" | Select-Object -First 1
if ($workerContainer) {
    $workerName = $workerContainer.ToString().Trim()
    docker exec $workerName sh -c "ls -R /data/tmp || echo '(/data/tmp vacío o no montado)'"
} else {
    Write-Host "No se encontró contenedor worker para inspeccionar /data/tmp." -ForegroundColor Red
}

Write-Host ""
Write-Host "==== [4.3] Test completado. En la demo puedes comentar:" -ForegroundColor Cyan
Write-Host "  - Se genera un archivo grande -> muchas particiones y uso de disco (/data/tmp)." -ForegroundColor DarkCyan
Write-Host "  - Workers muestran mem_bytes no explosivo gracias al uso de particiones + disco." -ForegroundColor DarkCyan
Write-Host "  - El job termina correctamente, demostrando que podemos procesar datasets grandes." -ForegroundColor DarkCyan


