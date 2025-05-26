# This script tests all Azure Function endpoints (including Durable Functions) sequentially.
# It logs each step and waits for completion before moving to the next test.

$ErrorActionPreference = 'Stop'

# Test configuration variables - modify these to test different scenarios
$script:SequentialMode = $false        # Set to $true for sequential processing, $false for parallel
$script:ForceRefresh = $false         # Set to $true to bypass caching, $false to use cache when available
$script:City = "charlotte"            # Set to the city you want to use for caching
$script:ProviderType = "outscraper"   # Set to 'google' or 'outscraper'
$script:InsufficientOnly = $true      # Set to $true to only process records from the "Insufficient" view

# Array to store test results
$script:TestResults = @()

# Set up paths
$script:Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$script:AzureFunctionDir = Split-Path -Parent $script:Root
$script:RootDir = Split-Path -Parent $script:AzureFunctionDir
$script:InvokeAzureFunction = Join-Path $script:RootDir "scripts\Invoke-AzureFunction.ps1"
$script:InvokeDurableFunction = Join-Path $script:RootDir "scripts\Invoke-AzureDurableFunction.ps1"

# Config
$baseUrl = 'http://localhost:7071/api'
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
    $url = "$baseUrl/$Endpoint"
    
    Write-Log "Using script: $script:InvokeAzureFunction"
    if (-not (Test-Path $script:InvokeAzureFunction)) {
        Write-Log "ERROR: Script not found at path: $script:InvokeAzureFunction"
        Write-Log "Available files in directory: $(Get-ChildItem (Split-Path $script:InvokeAzureFunction -Parent))"
    }
    
    $testStartTime = Get-Date
    $testSuccess = $false
    
    try {
        & $script:InvokeAzureFunction -FunctionUrl $url -FunctionKey $functionKey -RequestBody $Body -TimeoutSeconds $TimeoutSeconds
        $testSuccess = $true
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
    $url = "$baseUrl/$Endpoint"
    
    Write-Log "Using script: $script:InvokeDurableFunction"
    if (-not (Test-Path $script:InvokeDurableFunction)) {
        Write-Log "ERROR: Script not found at path: $script:InvokeDurableFunction"
        Write-Log "Available files in directory: $(Get-ChildItem (Split-Path $script:InvokeDurableFunction -Parent))"
    }
    
    $testStartTime = Get-Date
    $testSuccess = $false
    
    try {
        & $script:InvokeDurableFunction -FunctionUrl $url -FunctionKey $functionKey -TimeoutSeconds $TimeoutSeconds
        $testSuccess = $true
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

# 1. Smoke Test (HTTP)
Test-HttpFunction -Endpoint 'smoke-test' -Body '{"House": "Martell"}' -Description 'API health check'

# 2. Purge Orchestrations (HTTP)
Test-HttpFunction -Endpoint 'purge-orchestrations' -Description 'Purge completed orchestrations'

# 3. Enrich Airtable Base (Durable)
$enrichEndpoint = "enrich-airtable-base?provider_type=$script:ProviderType&sequential_mode=$($script:SequentialMode.ToString().ToLower())&force_refresh=$($script:ForceRefresh.ToString().ToLower())&insufficient_only=$($script:InsufficientOnly.ToString().ToLower())&city=$script:City"
Test-DurableFunction -Endpoint $enrichEndpoint -Description "Enrich Airtable base ($script:ProviderType, sequential_mode=$script:SequentialMode, insufficient_only=$script:InsufficientOnly)"

# 4. Refresh Place Data (Durable)
$refreshEndpoint = "refresh-place-data?provider_type=$script:ProviderType&sequential_mode=$($script:SequentialMode.ToString().ToLower())&force_refresh=$($script:ForceRefresh.ToString().ToLower())&city=$script:City"
Test-DurableFunction -Endpoint $refreshEndpoint -Description "Refresh all place data ($script:ProviderType, sequential_mode=$script:SequentialMode)"

# 5. Refresh Operational Statuses (HTTP)
$opsEndpoint = "refresh-airtable-operational-statuses?provider_type=$script:ProviderType&sequential_mode=$($script:SequentialMode.ToString().ToLower())&city=$script:City"
Test-DurableFunction -Endpoint $opsEndpoint -Description "Refresh operational statuses ($script:ProviderType, sequential_mode=$script:SequentialMode)"

# 6. Refresh All Photos (HTTP)
$photosEndpoint = "refresh-all-photos?provider_type=$script:ProviderType&city=$script:City&dry_run=true"
Test-HttpFunction -Endpoint $photosEndpoint -Description "Refresh all photos in dry run mode ($script:ProviderType)"

# Display test completion
Write-Log "All Azure Function endpoint tests completed."

# Generate and display test report
Write-Host ""
Write-Host "======================================================="
Write-Host "            AZURE FUNCTIONS TEST REPORT                "
Write-Host "======================================================="
Write-Host ""

$totalTests = $script:TestResults.Count
$passedTests = ($script:TestResults | Where-Object { $_.Success -eq $true }).Count
$failedTests = $totalTests - $passedTests
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
