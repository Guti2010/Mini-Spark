# scripts/test_5_1_api.ps1
# Prueba 5.1: contrato HTTP/JSON basico de la API batch

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

    Write-Host ""
    Read-Host "Press Enter to continue with next step..."
}

$baseUrl = "http://localhost:8080"

Step "STEP 1 - Levantar master + 2 workers (si no estan arriba)" {
    Write-Host ">> docker compose up -d master worker --scale worker=2" -ForegroundColor Yellow
    docker compose up -d master worker --scale worker=2

    Write-Host ""
    Write-Host "Servicios activos:" -ForegroundColor Yellow
    docker compose ps

    Write-Host ""
    Write-Host "Esperando 5s para que arranque el master..." -ForegroundColor Yellow
    Start-Sleep -Seconds 5
}

Step "STEP 2 - Construir el JSON del job (formato del enunciado adaptado a DagNode.params)" {
    $dag = @{
        nodes = @(
            @{
                id = "read"
                op = "read_csv"
                params = @{
                    path        = "/data/input/*.csv"
                    partitions  = "4"
                    format      = "csv"
                    text_field  = "text"
                }
            },
            @{
                id = "flat"
                op = "flat_map"
                params = @{
                    fn = "tokenize"
                }
            },
            @{
                id = "map1"
                op = "map"
                params = @{
                    fn = "to_lower"
                }
            },
            @{
                id = "agg"
                op = "reduce_by_key"
                params = @{
                    key = "token"
                    fn  = "sum"
                }
            }
        )
        edges = @(
            @("read","flat"),
            @("flat","map1"),
            @("map1","agg")
        )
    }

    $body = @{
        name         = "wordcount-batch-api"
        dag          = $dag
        parallelism  = 4
        input_glob   = "/data/input/*.csv"
        output_dir   = "/data/output"
    }

    $global:jsonBody = $body | ConvertTo-Json -Depth 6

    Write-Host "JSON que se envia a POST /api/v1/jobs:" -ForegroundColor Green
    Write-Host $global:jsonBody
}

Step "STEP 3 - POST /api/v1/jobs (crear job batch)" {
    $url = "$baseUrl/api/v1/jobs"
    Write-Host ">> POST $url" -ForegroundColor Yellow

    $global:job = Invoke-RestMethod -Uri $url `
        -Method Post `
        -ContentType "application/json" `
        -Body $global:jsonBody

    Write-Host ""
    Write-Host "Respuesta del master:" -ForegroundColor Green
    $global:job | ConvertTo-Json -Depth 6 | Write-Host

    $global:jobId = $global:job.id
    Write-Host ""
    Write-Host "Job ID: $($global:jobId)" -ForegroundColor Green
}

Step "STEP 4 - GET /api/v1/jobs/{id} (estado, progreso, metricas)" {
    if (-not $global:jobId) {
        Write-Host "No hay jobId, algo fallo en el POST." -ForegroundColor Red
        exit 1
    }

    $url = "$baseUrl/api/v1/jobs/$($global:jobId)"
    Write-Host ">> GET $url" -ForegroundColor Yellow

    $jobInfo = Invoke-RestMethod -Uri $url -Method Get

    Write-Host ""
    Write-Host "JobInfo actual:" -ForegroundColor Green
    $jobInfo | ConvertTo-Json -Depth 6 | Write-Host
}

Step "STEP 5 - Poll GET /api/v1/jobs/{id} hasta SUCCEEDED/FAILED" {
    if (-not $global:jobId) {
        Write-Host "No hay jobId, algo fallo en el POST." -ForegroundColor Red
        exit 1
    }

    for ($i = 0; $i -lt 15; $i++) {
        Write-Host ""
        Write-Host "== Poll $($i+1) ==" -ForegroundColor Cyan

        $url = "$baseUrl/api/v1/jobs/$($global:jobId)"
        $jobInfo = Invoke-RestMethod -Uri $url -Method Get

        $estado = $jobInfo.status
        $done   = $jobInfo.completed_tasks + $jobInfo.failed_tasks
        $total  = $jobInfo.total_tasks

        Write-Host ("estado: {0}, done={1}/{2}" -f $estado, $done, $total) -ForegroundColor Green

        if ($estado -eq "SUCCEEDED" -or $estado -eq "FAILED") {
            Write-Host "Job llego a estado final." -ForegroundColor Green
            break
        }

        Write-Host "Job aun en progreso, esperando 2s..." -ForegroundColor Yellow
        Start-Sleep -Seconds 2
    }
}

Step "STEP 6 - GET /api/v1/jobs/{id}/results (paths de salida)" {
    if (-not $global:jobId) {
        Write-Host "No hay jobId, algo fallo en el POST." -ForegroundColor Red
        exit 1
    }

    $url = "$baseUrl/api/v1/jobs/$($global:jobId)/results"
    Write-Host ">> GET $url" -ForegroundColor Yellow

    $results = Invoke-RestMethod -Uri $url -Method Get

    Write-Host ""
    Write-Host "Respuesta de results:" -ForegroundColor Green
    $results | ConvertTo-Json -Depth 6 | Write-Host

    Write-Host ""
    Write-Host "Archivos de salida reportados:" -ForegroundColor Green
    foreach ($f in $results.files) {
        Write-Host "  - $f"
    }
}

Write-Host ""
Write-Host "==== [5.1] Test de API batch completado ====" -ForegroundColor Cyan
Write-Host "En la demo puedes mostrar:" -ForegroundColor Cyan
Write-Host "  - El JSON del job (dag.nodes, dag.edges, parallelism, input_glob, output_dir)" -ForegroundColor DarkCyan
Write-Host "  - La respuesta de POST /api/v1/jobs (JobInfo con dag embebido)" -ForegroundColor DarkCyan
Write-Host "  - GET /api/v1/jobs/{id} con estado, progreso, metricas" -ForegroundColor DarkCyan
Write-Host "  - GET /api/v1/jobs/{id}/results con paths de salida" -ForegroundColor DarkCyan
