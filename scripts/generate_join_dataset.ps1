# scripts/generate_join_dataset.ps1
# Genera:
#   docs/input/catalogo.csv
#   docs/input/ventas.csv
# con tamano total aproximado entre 100 y 500 MB (segun los parametros)

$ErrorActionPreference = "Stop"

# -------------------------------
# Parametros ajustables
# -------------------------------

# cantidad de productos distintos en el catalogo
$NumProducts = 2000

# cantidad de filas de ventas (sube/baja esto para cambiar el tamano total)
# 1_000_000 suele dar del orden de cientos de MB segun el contenido
$NumSalesRows = 1000000

# carpeta de salida
$inputDir = "docs/input"

# -------------------------------
# Preparar directorio
# -------------------------------

if (-not (Test-Path $inputDir)) {
    New-Item -ItemType Directory -Path $inputDir | Out-Null
}

$catalogPath = Join-Path $inputDir "catalogo.csv"
$salesPath   = Join-Path $inputDir "ventas.csv"

# borrar si existen
Remove-Item $catalogPath, $salesPath -ErrorAction SilentlyContinue

Write-Host "Generando catalogo en $catalogPath..." -ForegroundColor Yellow

# -------------------------------
# Generar catalogo.csv
# -------------------------------

$encoding = [System.Text.Encoding]::UTF8

$catWriter = New-Object System.IO.StreamWriter($catalogPath, $false, $encoding)
$catWriter.WriteLine("product_id,description,unit_price")

$rand = New-Object System.Random

for ($p = 1; $p -le $NumProducts; $p++) {
    $productId = ("P{0:00000}" -f $p)
    $desc = "Producto $p"
    $price = [Math]::Round(1 + $rand.NextDouble() * 99, 2)
    $line = "{0},{1},{2}" -f $productId, $desc, $price
    $catWriter.WriteLine($line)
}
$catWriter.Dispose()

Write-Host "Catalogo listo con $NumProducts productos." -ForegroundColor Green

# -------------------------------
# Generar ventas.csv
# -------------------------------

Write-Host "Generando ventas en $salesPath..." -ForegroundColor Yellow

$venWriter = New-Object System.IO.StreamWriter($salesPath, $false, $encoding)
$venWriter.WriteLine("invoice_id,invoice_date,customer_id,country,product_id,quantity,unit_price")

$startDate = Get-Date "2024-01-01 00:00:00"

for ($i = 1; $i -le $NumSalesRows; $i++) {
    if ($i % 100000 -eq 0) {
        Write-Host "  -> fila $i / $NumSalesRows" -ForegroundColor DarkCyan
    }

    $invoiceId  = ("F{0:0000000}" -f $i)
    $date       = $startDate.AddMinutes($i % 1440).AddDays([Math]::Floor($i / 1440.0))
    $dateStr    = $date.ToString("yyyy-MM-dd HH:mm:ss")

    $customerId = ("C{0:00000}" -f ($rand.Next(1, 50001)))
    $country    = "Pais{0}" -f ($rand.Next(1, 6))

    $prodIndex  = $rand.Next(1, $NumProducts + 1)
    $productId  = ("P{0:00000}" -f $prodIndex)

    $qty        = $rand.Next(1, 11)
    $price      = [Math]::Round(1 + $rand.NextDouble() * 99, 2)

    $line = "{0},{1},{2},{3},{4},{5},{6}" -f `
        $invoiceId, $dateStr, $customerId, $country, $productId, $qty, $price

    $venWriter.WriteLine($line)
}

$venWriter.Dispose()

Write-Host "Ventas listas con $NumSalesRows filas." -ForegroundColor Green
Write-Host ""
Write-Host "Archivos generados:" -ForegroundColor Cyan
Write-Host "  $catalogPath"
Write-Host "  $salesPath"
Write-Host ""
Write-Host "Puedes ajustar NumProducts y NumSalesRows al inicio del script" -ForegroundColor Cyan

