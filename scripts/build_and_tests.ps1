param(
    [switch]$SkipCargoTests
)

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Build de imágenes Docker" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Cyan

docker compose build

if (-not $SkipCargoTests) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  Tests common (cargo test -p common)" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Cyan
    cargo test -p common

    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  Tests master (si tienes tests)" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Cyan
    cargo test -p master

    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host "  Tests worker (si tienes tests)" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Cyan
    cargo test -p worker
}

Write-Host ""
Write-Host "Build + tests listo." -ForegroundColor Green
