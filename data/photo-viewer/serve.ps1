# Third Places Photo Viewer Server
# This script starts a local HTTP server to serve the photo viewer and avoid CORS issues

$photoViewerDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $photoViewerDir

Write-Host "Third Places Photo Viewer" -ForegroundColor Green
Write-Host "=========================" -ForegroundColor Green

# Generate place names mapping
Write-Host "Generating place names mapping..." -ForegroundColor Yellow

try { 
    & ".\generate-place-names.ps1"
    Write-Host "Place names mapping generated" -ForegroundColor Green
}
catch { 
    Write-Host "Failed to generate place names mapping. Make sure generate-place-names.ps1 exists." -ForegroundColor Red
    exit 1
}

Write-Host ""
$port = 8000

# Try to find an available port
$serverStarted = $false
for ($testPort = $port; $testPort -lt ($port + 10); $testPort++) {
    try {
        Write-Host "Third Places Photo Viewer" -ForegroundColor Green
        Write-Host "Attempting to start server on port $testPort..." -ForegroundColor Yellow
        
        # Start our custom Python server
        $process = Start-Process -FilePath "python" -ArgumentList "photo-server.py", "$testPort" -PassThru -NoNewWindow
        
        # Wait a moment for the server to start
        Start-Sleep -Seconds 3
        
        # Check if the process is still running
        if (-not $process.HasExited) {
            Write-Host "Server running at: http://localhost:$testPort" -ForegroundColor Green
            Write-Host "Opening browser automatically..." -ForegroundColor Cyan
            Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Red
            
            # Open browser
            Start-Process "http://localhost:$testPort"
            
            # Wait for the process to exit
            $process.WaitForExit()
            $serverStarted = $true
            break
        }
    }
    catch {
        Write-Host "Port $testPort is in use, trying next port..." -ForegroundColor Yellow
        continue
    }
}

if (-not $serverStarted) {
    Write-Host "Could not start server. Make sure Python is installed and photo-server.py exists." -ForegroundColor Red
    Write-Host "Try running manually: python photo-server.py" -ForegroundColor White
}
