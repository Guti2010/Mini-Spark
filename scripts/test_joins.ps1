# scripts/test_joins.ps1
# Prueba de JOIN (ventas & catalogo) usando el engine::join_csv_in_memory
# Se ejecuta TODO dentro de Docker (client), no se usa cargo en Windows.

$ErrorActionPreference = "Stop"

function Step {
    param(
        [string]$Title,
        [scriptblock]$Body
    )
    Write-Host ""
    Write-Host "==========================================" -ForegroundColor Cyan
    Write-Host $Title -ForegroundColor Yellow
    & $Body
    Write-Host ""
    Write-Host "Presiona ENTER para continuar..." -ForegroundColor DarkGray
    Read-Host | Out-Null
}

Write-Host "==== Test de JOIN ventas & catalogo ====" -ForegroundColor Cyan

$inputDir  = "docs/input"
$outputDir = "docs/output"

Step "STEP 1 - Verificar/crear CSVs de ventas y catalogo en docs/input" {
    if (-not (Test-Path $inputDir)) {
        New-Item -ItemType Directory -Path $inputDir | Out-Null
    }
    if (-not (Test-Path $outputDir)) {
        New-Item -ItemType Directory -Path $outputDir | Out-Null
    }

    $ventasPath   = Join-Path $inputDir "ventas.csv"
    $catalogoPath = Join-Path $inputDir "catalogo.csv"

    if (-not (Test-Path $ventasPath)) {
        Write-Host "No existe $ventasPath, creando CSV pequeño de ejemplo..." -ForegroundColor Yellow
        @"
product_id,cliente_id,unidades,precio_total
P001,C001,2,19.98
P002,C002,1,9.99
P001,C003,3,29.97
P003,C004,5,49.95
P002,C005,10,99.90
"@ | Set-Content -Encoding UTF8 $ventasPath
    } else {
        Write-Host "Encontrado: $ventasPath" -ForegroundColor Green
    }

    if (-not (Test-Path $catalogoPath)) {
        Write-Host "No existe $catalogoPath, creando CSV pequeño de ejemplo..." -ForegroundColor Yellow
        @"
product_id,nombre,categoria
P001,Mouse optico,Perifericos
P002,Teclado mecanico,Perifericos
P003,Monitor 24,Monitores
P004,Laptop 15,Portatiles
"@ | Set-Content -Encoding UTF8 $catalogoPath
    } else {
        Write-Host "Encontrado: $catalogoPath" -ForegroundColor Green
    }

    Write-Host "CSV de ejemplo listos en docs/input." -ForegroundColor Green
}

Step "STEP 2 - Build de la imagen client (con el nuevo subcomando join)" {
    Write-Host ">> docker compose build client" -ForegroundColor Yellow
    docker compose build client
}

Step "STEP 3 - Ejecutar join dentro del contenedor client" {
    # Ojo: dentro del contenedor las rutas son /data/input y /data/output
    $ventasInContainer   = "/data/input/ventas.csv"
    $catalogoInContainer = "/data/input/catalogo.csv"
    $outputInContainer   = "/data/output/join_ventas_catalogo.jsonl"

    Write-Host ">> docker compose run --rm client join ..." -ForegroundColor Yellow
    docker compose run --rm client `
        join `
        $ventasInContainer `
        $catalogoInContainer `
        --key "product_id" `
        --output $outputInContainer
}

Step "STEP 4 - Mostrar primeras lineas del resultado en docs/output" {
    $outputPath = Join-Path $outputDir "join_ventas_catalogo.jsonl"

    if (-not (Test-Path $outputPath)) {
        Write-Host "ERROR: no se encontro ${outputPath}. Revisa que el servicio 'client' tenga volumes ./docs/output:/data/output." -ForegroundColor Red
        exit 1
    }

    Write-Host "Primeras 10 lineas de ${outputPath}:" -ForegroundColor Cyan
    Get-Content $outputPath -TotalCount 10 | ForEach-Object {
        Write-Host "  $_"
    }
}

Write-Host ""
Write-Host "==== Test de JOIN completado ====" -ForegroundColor Cyan
Write-Host "En la defensa puedes decir que este join usa:" -ForegroundColor DarkCyan
Write-Host "  - Lectura de CSV -> Records (engine::read_csv_to_records)" -ForegroundColor DarkCyan
Write-Host "  - op_join_by_key en memoria (join por product_id)" -ForegroundColor DarkCyan
Write-Host "  - join_csv_in_memory expuesto via subcomando del client" -ForegroundColor DarkCyan
