function Get-JobIdFromSubmitOutput {
    param(
        [string[]]$Lines
    )

    $jobLine = $Lines | Where-Object { $_ -match '^\s*id:' } | Select-Object -First 1
    if (-not $jobLine) { return $null }

    $id = $jobLine -replace '^\s*id:\s*', ''
    return $id.Trim()
}

function Get-JobEstadoFromStatusOutput {
    param(
        [string[]]$Lines
    )

    $line = $Lines | Where-Object { $_ -match '^\s*estado:' } | Select-Object -First 1
    if (-not $line) { return $null }

    $estado = $line -replace '^\s*estado:\s*', ''
    return $estado.Trim()
}
