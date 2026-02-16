param(
  [string]$EnvFile
)

# Se não foi passado, assume que o .env está na MESMA pasta do script
if (-not $EnvFile -or [string]::IsNullOrWhiteSpace($EnvFile)) {
  $EnvFile = Join-Path -Path $PSScriptRoot -ChildPath ".env"
}

if (-not (Test-Path -LiteralPath $EnvFile)) {
  throw "Arquivo .env não encontrado em: $EnvFile"
}

# Carrega as variáveis no processo atual
Get-Content -LiteralPath $EnvFile | ForEach-Object {
  if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
    $name = $Matches[1].Trim()
    $val  = $Matches[2].Trim().Trim('"').Trim("'")
    [System.Environment]::SetEnvironmentVariable($name, $val, "Process")
  }
}

Write-Host "Carregado: $EnvFile"
