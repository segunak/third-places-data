# This script tests all Azure Function endpoints (including Durable Functions) sequentially.
# It logs each step and waits for completion before moving to the next test.

$ErrorActionPreference = 'Stop'

# Test configuration variables - modify these to test different scenarios
$script:SequentialMode = $false        # Set to $true for sequential processing, $false for parallel
$script:ForceRefresh = $false         # Set to $true to bypass caching, $false to use cache when available
$script:City = "charlotte"            # Set to the city you want to use for caching
$script:ProviderType = "outscraper"   # Set to 'google' or 'outscraper'
$script:InsufficientOnly = $true      # Set to $true to only process records from the "Insufficient" view

# Endpoint test toggles (set individual values to $false to skip a test)
$script:RunEndpointTests = @{
    Smoke                      = $true
    EnrichAirtableBase         = $true
    RefreshPlaceData           = $true
    RefreshOperationalStatuses = $true
    RefreshSinglePlace         = $true
    RefreshAllPhotos           = $true
    PurgeOrchestrations        = $true
} 

# Array to store test results
$script:TestResults = @()

# Set up paths
$script:Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$script:AzureFunctionDir = Split-Path -Parent $script:Root
$script:RootDir = Split-Path -Parent $script:AzureFunctionDir
$script:InvokeAzureFunction = Join-Path $script:RootDir "scripts\Invoke-AzureFunction.ps1"
$script:InvokeDurableFunction = Join-Path $script:RootDir "scripts\Invoke-AzureDurableFunction.ps1"

# Config
$baseUrl = 'http://localhost:7071'
$functionKey = $env:AZURE_FUNCTION_KEY

# If no key in environment, try to load from local.settings.json
if (-not $functionKey) {
    $settingsPath = Join-Path $script:AzureFunctionDir "local.settings.json"
    if (Test-Path $settingsPath) {
        $settings = Get-Content $settingsPath -Raw | ConvertFrom-Json
        $functionKey = $settings.Values.AZURE_FUNCTION_KEY
    }
}

# Function to log with timestamp
function Write-Log {
    param (
        [Parameter(Mandatory=$true)]
        [string]$Message
    )
    
    Write-Host "[$(Get-Date -Format 'u')] $Message"
}

# First test: ensure the local Azure Functions host is running on the expected port
function Assert-FunctionHostRunning {
    param(
        [Parameter(Mandatory=$false)][string]$PingUrl = 'http://localhost:7071'
    )

    Write-Log "Preflight: checking Azure Functions host at $PingUrl"
    try {
        # Quick connectivity probe; any HTTP status code means the host responded
        $resp = Invoke-WebRequest -Uri $PingUrl -Method GET -TimeoutSec 3 -UseBasicParsing
        Write-Log "Azure Functions host responded (HTTP $($resp.StatusCode))."
    } catch {
        Write-Log "Azure Functions host is not reachable at $PingUrl. Aborting tests."
        Write-Host "Ensure the local host is running before tests."
        Write-Host "Tip: Start the function host in the 'third-places-data/azure-function' folder (VS Code task: 'func: host start')."
        exit 1
    }
}

# Helper to print a compact request metadata block
function Write-RequestMetadata {
    param(
        [Parameter(Mandatory=$true)][string]$Name,
        [Parameter(Mandatory=$true)][string]$Method,
        [Parameter(Mandatory=$true)][string]$Url,
        [Parameter(Mandatory=$true)][bool]$HasKey,
        [Parameter(Mandatory=$false)][string]$Body = '',
        [Parameter(Mandatory=$false)][int]$TimeoutSeconds = 0
    )

    Write-Host "==== REQUEST ====================================="
    Write-Host "Name     : $Name"
    Write-Host "Method   : $Method"
    Write-Host "URL      : $Url"
    Write-Host "Has Key  : $(if ($HasKey) { 'Yes' } else { 'No' })"
    if ($TimeoutSeconds -gt 0) {
        Write-Host "Timeout  : ${TimeoutSeconds}s"
    }
    if (-not [string]::IsNullOrWhiteSpace($Body)) {
        # Truncate very long bodies for readability
        $bodyToShow = if ($Body.Length -gt 2000) { $Body.Substring(0, 2000) + ' ... [truncated]' } else { $Body }
        Write-Host "Body     : $bodyToShow"
    }
    Write-Host "=================================================="
}

# Function to test regular HTTP functions
function Test-HttpFunction {
    param (
        [Parameter(Mandatory=$true)]
        [string]$Endpoint,
        
        [Parameter(Mandatory=$false)]
        [string]$Body = '{}',
        
        [Parameter(Mandatory=$false)]
        [string]$Description = '',
        
        [Parameter(Mandatory=$false)]
        [int]$TimeoutSeconds = 3600
    )
    
    Write-Log "Testing HTTP Function: $Endpoint $Description"
    $url = "$baseUrl/api/$Endpoint"
    Write-RequestMetadata -Name $Endpoint -Method 'POST' -Url $url -HasKey ([bool]$functionKey) -Body $Body -TimeoutSeconds $TimeoutSeconds
    
    Write-Log "Using script: $script:InvokeAzureFunction"
    if (-not (Test-Path $script:InvokeAzureFunction)) {
        Write-Log "ERROR: Script not found at path: $script:InvokeAzureFunction"
        Write-Log "Available files in directory: $(Get-ChildItem (Split-Path $script:InvokeAzureFunction -Parent))"
    }
    
    $testStartTime = Get-Date
    $testSuccess = $false
    
    try {
        $invokeOutput = & $script:InvokeAzureFunction -FunctionUrl $url -FunctionKey $functionKey -RequestBody $Body -TimeoutSeconds $TimeoutSeconds
        if ($null -ne $invokeOutput) { $invokeOutput | Write-Host }
        $exitCode = $LASTEXITCODE
        if ($exitCode -eq 0) {
            $testSuccess = $true
            Write-Log "Child script exit code: $exitCode (success)"
        } else {
            $testSuccess = $false
            Write-Log "Child script exit code: $exitCode (failure)"
        }
    } catch {
        Write-Log "ERROR: Test failed: $_"
    }
    
    $testDuration = (Get-Date) - $testStartTime
    
    # Add result to test results array
    $script:TestResults += [PSCustomObject]@{
        TestName = "HTTP: $Endpoint"
        Description = $Description
        Success = $testSuccess
        Duration = $testDuration
        ExecutedAt = $testStartTime
    Method = 'POST'
    Url = $url
    }
    
    Write-Log "Finished: $Endpoint"
    Write-Host "-----------------------------"
}

# Function to test Durable Functions
function Test-DurableFunction {
    param (
        [Parameter(Mandatory=$true)]
        [string]$Endpoint,
        
        [Parameter(Mandatory=$false)]
        [string]$Body = '{}',
        
        [Parameter(Mandatory=$false)]
        [string]$Description = '',
        
        [Parameter(Mandatory=$false)]
        [int]$TimeoutSeconds = 3600
    )
    
    Write-Log "Testing Durable Function: $Endpoint $Description"
    $url = "$baseUrl/api/$Endpoint"
    # Durable starter uses GET by default unless your route expects POST; still useful to show
    Write-RequestMetadata -Name $Endpoint -Method 'HTTP (starter)' -Url $url -HasKey ([bool]$functionKey) -TimeoutSeconds $TimeoutSeconds
    
    Write-Log "Using script: $script:InvokeDurableFunction"
    if (-not (Test-Path $script:InvokeDurableFunction)) {
        Write-Log "ERROR: Script not found at path: $script:InvokeDurableFunction"
        Write-Log "Available files in directory: $(Get-ChildItem (Split-Path $script:InvokeDurableFunction -Parent))"
    }
    
    $testStartTime = Get-Date
    $testSuccess = $false
    
    try {
        $invokeOutput = & $script:InvokeDurableFunction -FunctionUrl $url -FunctionKey $functionKey -TimeoutSeconds $TimeoutSeconds
        if ($null -ne $invokeOutput) { $invokeOutput | Write-Host }
        $exitCode = $LASTEXITCODE
        if ($exitCode -eq 0) {
            $testSuccess = $true
            Write-Log "Child script exit code: $exitCode (success)"
        } else {
            $testSuccess = $false
            Write-Log "Child script exit code: $exitCode (failure)"
        }
    } catch {
        Write-Log "ERROR: Test failed: $_"
    }
    
    $testDuration = (Get-Date) - $testStartTime
    
    # Add result to test results array
    $script:TestResults += [PSCustomObject]@{
        TestName = "Durable: $Endpoint"
        Description = $Description
        Success = $testSuccess
        Duration = $testDuration
        ExecutedAt = $testStartTime
    Method = 'Durable (starter)'
    Url = $url
    }
    
    Write-Log "Finished: $Endpoint"
    Write-Host "-----------------------------"
}

# Display test start information
Write-Log "Starting Azure Function endpoint tests"
Write-Log "Base URL: $baseUrl"
Write-Log "Using key: $(if ($functionKey) { 'Yes' } else { 'No' })"
Write-Log "Scripts directory: $(Split-Path $script:InvokeAzureFunction -Parent)"
Write-Log "Test configuration: Sequential=$script:SequentialMode, ForceRefresh=$script:ForceRefresh, City=$script:City, Provider=$script:ProviderType"

# Preflight check: ensure function host is running locally
Assert-FunctionHostRunning -PingUrl $baseUrl

if ($script:RunEndpointTests.Smoke) {
    # Smoke Test (HTTP)
    Test-HttpFunction -Endpoint 'smoke-test' -Body '{"House": "Martell"}' -Description 'API health check'
}

if ($script:RunEndpointTests.EnrichAirtableBase) {
    # Enrich Airtable Base (Durable)
    $enrichEndpoint = "enrich-airtable-base?provider_type=$script:ProviderType&sequential_mode=$($script:SequentialMode.ToString().ToLower())&force_refresh=$($script:ForceRefresh.ToString().ToLower())&insufficient_only=$($script:InsufficientOnly.ToString().ToLower())&city=$script:City"
    Test-DurableFunction -Endpoint $enrichEndpoint -Description "Enrich Airtable base ($script:ProviderType, sequential_mode=$script:SequentialMode, insufficient_only=$script:InsufficientOnly)"
}

if ($script:RunEndpointTests.RefreshPlaceData) {
    # Refresh Place Data (Durable)
    $refreshEndpoint = "refresh-place-data?provider_type=$script:ProviderType&sequential_mode=$($script:SequentialMode.ToString().ToLower())&force_refresh=$($script:ForceRefresh.ToString().ToLower())&insufficient_only=$($script:InsufficientOnly.ToString().ToLower())&city=$script:City"
    Test-DurableFunction -Endpoint $refreshEndpoint -Description "Refresh all place data ($script:ProviderType, sequential_mode=$script:SequentialMode, insufficient_only=$script:InsufficientOnly)"
}

if ($script:RunEndpointTests.RefreshOperationalStatuses) {
    # Refresh Operational Statuses (HTTP)
    $opsEndpoint = "refresh-airtable-operational-statuses?provider_type=$script:ProviderType&sequential_mode=$($script:SequentialMode.ToString().ToLower())&city=$script:City"
    Test-DurableFunction -Endpoint $opsEndpoint -Description "Refresh operational statuses ($script:ProviderType, sequential_mode=$script:SequentialMode)"
}

if ($script:RunEndpointTests.RefreshSinglePlace) {
    # Refresh Single Place (Durable) - Test with a known place ID
    $singlePlaceId = "ChIJqX87rVYOVIgRjYMMiz1W5Sg"  # Cabarrus County Library | Concord
    $singlePlaceEndpoint = "refresh-single-place?place_id=$singlePlaceId&provider_type=$script:ProviderType&city=$script:City"
    Test-DurableFunction -Endpoint $singlePlaceEndpoint -Description "Refresh single place data (place_id=$singlePlaceId, provider=$script:ProviderType)"
}

if ($script:RunEndpointTests.RefreshAllPhotos) {
    # Refresh All Photos (Durable)
    $photosEndpoint = "refresh-all-photos?provider_type=$script:ProviderType&city=$script:City&dry_run=true&sequential_mode=$($script:SequentialMode.ToString().ToLower())"
    Test-DurableFunction -Endpoint $photosEndpoint -Description "Refresh all photos in dry run mode ($script:ProviderType, sequential_mode=$script:SequentialMode)"
}

if ($script:RunEndpointTests.PurgeOrchestrations) {
    # Purge Orchestrations (HTTP)
    Test-HttpFunction -Endpoint 'purge-orchestrations' -Description 'Purge completed orchestrations'
}

# Display test completion
Write-Log "All Azure Function endpoint tests completed."

# Generate and display test report
Write-Host ""
Write-Host "======================================================="
Write-Host "            AZURE FUNCTIONS TEST REPORT                "
Write-Host "======================================================="
Write-Host ""

$totalTests = $script:TestResults.Count

# Ensure Success property is treated as a boolean (defensive; handles any accidental string coercion)
foreach ($r in $script:TestResults) { $r.Success = [bool]$r.Success }

# Robust counting using explicit groupings instead of arithmetic on potentially null values
$passedSet = @($script:TestResults | Where-Object { $_.Success })
$failedSet = @($script:TestResults | Where-Object { -not $_.Success })
[int]$passedTests = $passedSet.Count
[int]$failedTests = $failedSet.Count

# Fallback integrity check — if counts don't add up, recalc failed via subtraction and emit a warning
if (($passedTests + $failedTests) -ne $totalTests) {
    Write-Host "WARNING: Inconsistent test counts detected (passed=$passedTests failed=$failedTests total=$totalTests). Recomputing failed count via subtraction." -ForegroundColor Yellow
    [int]$failedTests = $totalTests - $passedTests
}

$passRate = if ($totalTests -gt 0) { [math]::Round(($passedTests / $totalTests) * 100, 2) } else { 0 }

Write-Host "SUMMARY:"
Write-Host "--------------------------"
Write-Host "Total Tests  : $totalTests"
Write-Host "Passed Tests : $passedTests"
Write-Host "Failed Tests : $failedTests"
Write-Host "Pass Rate    : $passRate%"
Write-Host ""
Write-Host "DETAILED RESULTS:"
Write-Host "--------------------------"

foreach ($result in $script:TestResults) {
    $status = if ($result.Success) { "[PASSED]" } else { "[FAILED]" }
    $durationStr = "{0:mm\:ss\.fff}" -f $result.Duration
    Write-Host "$status $($result.TestName) - $($result.Description)"
    if ($result.Url) { Write-Host "         URL     : $($result.Url)" }
    if ($result.Method) { Write-Host "         Method  : $($result.Method)" }
    Write-Host "         Duration: $durationStr"
}

Write-Host ""
Write-Host "======================================================="

# Return test results - for pipeline usage
if ($failedTests -gt 0) {
    exit 1
} else {
    exit 0
}
