param(
    [string]$Database = 'postgres',
    [string]$User = 'postgres'
)

$ErrorActionPreference = 'Stop'

# Derive project folder name (e.g., 'third-places-data') as used in container names
$projectName = (Split-Path (Split-Path $PSScriptRoot -Parent) -Leaf)
$dbContainer = "supabase_db_$projectName"

if (-not (docker ps --format "{{.Names}}" | Select-String -SimpleMatch $dbContainer)) {
    throw "Database container '$dbContainer' not running. Start Supabase first."
}

Write-Host "Running validation SQL..." -ForegroundColor Cyan

Get-Content -Raw "$PSScriptRoot\tests\validate.sql" |
    docker exec -i $dbContainer psql -U $User -d $Database -v ON_ERROR_STOP=1 -P pager=off

if ($LASTEXITCODE -ne 0) {
    throw "validate.sql execution failed with exit code $LASTEXITCODE"
}

Write-Host 'Validation complete.' -ForegroundColor Green
