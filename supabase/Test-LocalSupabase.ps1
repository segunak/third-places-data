$ErrorActionPreference = 'Stop'

Write-Host "Stopping Supabase..." -ForegroundColor Cyan
npx supabase stop --all --no-backup
if ($LASTEXITCODE -ne 0) { throw "supabase stop failed with exit code $LASTEXITCODE" }
Start-Sleep -Seconds 2

Write-Host "Cleaning up stray Docker containers..." -ForegroundColor Cyan
# Derive project folder name (e.g., 'third-places-data') as used in container names
$projectName = (Split-Path (Split-Path $PSScriptRoot -Parent) -Leaf)
# Find containers with names that include the project name and start with 'supabase_'
$leftovers = & docker ps -a --format "{{.ID}} {{.Names}}" --filter "name=$projectName" 2>$null
if ($LASTEXITCODE -eq 0 -and $leftovers) {
	$idsToRemove = $leftovers |
		Where-Object { $_ -match '^\w+\s+supabase_' } |
		ForEach-Object { ($_ -split '\s+')[0] }
	if ($idsToRemove) {
		foreach ($cid in $idsToRemove) {
			& docker rm -f $cid | Out-Null
		}
		Write-Host "Removed $($idsToRemove.Count) leftover container(s)." -ForegroundColor Yellow
	}
}

Write-Host "Starting Supabase..." -ForegroundColor Cyan
npx supabase start
if ($LASTEXITCODE -ne 0) { throw "supabase start failed with exit code $LASTEXITCODE" }
Start-Sleep -Seconds 2

Write-Host "Resetting local database..." -ForegroundColor Cyan
npx supabase db reset --debug
if ($LASTEXITCODE -ne 0) { throw "supabase db reset failed with exit code $LASTEXITCODE" }
Start-Sleep -Seconds 2

# Bring all services back up after the DB reset (which can stop non-DB services)
Write-Host 'Starting Supabase services...' -ForegroundColor Cyan
npx supabase start
if ($LASTEXITCODE -ne 0) { throw "supabase start failed after reset ($LASTEXITCODE)" }
Start-Sleep -Seconds 2

Write-Host 'Running validation SQL...' -ForegroundColor Cyan
& "$PSScriptRoot\Invoke-Validate.sql.ps1" -Database 'postgres' -User 'postgres'
if ($LASTEXITCODE -ne 0) { throw "Validation script failed with exit code $LASTEXITCODE" }

Write-Host 'All services should now be reachable (API/Studio). Validation complete.' -ForegroundColor Green
