$ErrorActionPreference = 'Stop'

Write-Host "Starting Supabase..." -ForegroundColor Cyan
npx supabase start
if ($LASTEXITCODE -ne 0) { throw "supabase start failed with exit code $LASTEXITCODE" }

Write-Host "Resetting local database..." -ForegroundColor Cyan
npx supabase db reset --debug
if ($LASTEXITCODE -ne 0) { throw "supabase db reset failed with exit code $LASTEXITCODE" }

Write-Host "Done." -ForegroundColor Green
