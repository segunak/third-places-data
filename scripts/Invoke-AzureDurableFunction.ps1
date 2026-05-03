param(
    [Parameter(Mandatory = $true)]
    [string]$FunctionUrl,

    [Parameter(Mandatory = $false)]
    [string]$FunctionKey,

    [Parameter(Mandatory = $false)]
    [int]$TimeoutSeconds = 300,

    [Parameter(Mandatory = $false)]
    [int]$MaxStatusPollFailures = 1,

    [Parameter(Mandatory = $false)]
    [string]$StatusOutputPath,

    [Parameter(Mandatory = $false)]
    [switch]$PrintCustomStatus,

    [Parameter(Mandatory = $false)]
    [switch]$PrintOutputSummary,

    [Parameter(Mandatory = $false)]
    [switch]$PrintOrchestrationId,

    [Parameter(Mandatory = $false)]
    [int]$FinalOutputJsonDepth = 10
)

<#
Azure Durable Functions Runtime Status Return Values

Pending: The instance has been scheduled but has not yet started running.
Running: The instance has started running.
Completed: The instance has completed normally.
Canceled: The instance has been canceled.
ContinuedAsNew: The instance has restarted itself with a new history. This state is a transient state.
Failed: The instance failed with an error.
Terminated: The instance was stopped abruptly.
Suspended: The instance was suspended and may be resumed at a later point in time.

Reference 
    https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-http-api#response-1 
    https://learn.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-instance-management?tabs=csharp#query-instances
for more details on what Durable Functions return from their status query URL.
#>


$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
$statusResponse = $null
$status = $null
$consecutiveStatusPollFailures = 0
$lastCustomStatusJson = ""
$lastNonEmptyCustomStatus = $null
$lastNonEmptyCustomStatusRuntimeStatus = $null
$lastNonEmptyCustomStatusLastUpdatedTime = $null

function Save-StatusSnapshot {
    param([Parameter(Mandatory = $true)]$Status)

    if ([string]::IsNullOrWhiteSpace($StatusOutputPath)) {
        return
    }

    $parentDirectory = Split-Path -Parent $StatusOutputPath
    if (-not [string]::IsNullOrWhiteSpace($parentDirectory) -and -not (Test-Path $parentDirectory)) {
        New-Item -ItemType Directory -Path $parentDirectory -Force | Out-Null
    }

    if ($null -ne $Status.customStatus) {
        $script:lastNonEmptyCustomStatus = $Status.customStatus
        $script:lastNonEmptyCustomStatusRuntimeStatus = $Status.runtimeStatus
        $script:lastNonEmptyCustomStatusLastUpdatedTime = $Status.lastUpdatedTime
    }

    $snapshot = [ordered]@{}
    foreach ($property in $Status.PSObject.Properties) {
        $snapshot[$property.Name] = $property.Value
    }
    if ($null -ne $script:lastNonEmptyCustomStatus) {
        $snapshot["lastNonEmptyCustomStatus"] = $script:lastNonEmptyCustomStatus
        $snapshot["lastNonEmptyCustomStatusRuntimeStatus"] = $script:lastNonEmptyCustomStatusRuntimeStatus
        $snapshot["lastNonEmptyCustomStatusLastUpdatedTime"] = $script:lastNonEmptyCustomStatusLastUpdatedTime
    }

    $snapshot | ConvertTo-Json -Depth 100 | Set-Content -Path $StatusOutputPath -Encoding utf8
}

function Get-ErrorDetails {
    param([Parameter(Mandatory = $true)]$ErrorRecord)

    $details = @()
    if ($ErrorRecord.Exception -and $ErrorRecord.Exception.Message) {
        $details += $ErrorRecord.Exception.Message
    }
    if ($ErrorRecord.Exception -and $ErrorRecord.Exception.Response -and $ErrorRecord.Exception.Response.StatusCode) {
        $details += "HTTP status: $([int]$ErrorRecord.Exception.Response.StatusCode) $($ErrorRecord.Exception.Response.StatusCode)"
    }
    if ($ErrorRecord.ErrorDetails -and $ErrorRecord.ErrorDetails.Message) {
        $details += $ErrorRecord.ErrorDetails.Message
    }

    return ($details -join "`n")
}

function Write-MigrationOutputSummary {
    param($Output)

    if ($null -eq $Output) {
        return
    }

    if ($Output.PSObject.Properties.Name -contains 'data' -and $null -ne $Output.data) {
        $data = $Output.data

        if ($data.PSObject.Properties.Name -contains 'totals' -and $null -ne $data.totals) {
            Write-Output "Migration totals:`n$($data.totals | ConvertTo-Json -Depth 20)"
        }

        if ($data.PSObject.Properties.Name -contains 'place_results' -and $null -ne $data.place_results) {
            $placeResults = @($data.place_results)
            $errorResults = @($placeResults | Where-Object { $_.status -eq 'error' })
            if ($errorResults.Count -gt 0) {
                Write-Output "Migration error samples ($($errorResults.Count) total):`n$($errorResults | Select-Object -First 20 | ConvertTo-Json -Depth 30)"
            }

            $skippedResults = @($placeResults | Where-Object { $_.status -eq 'skipped' })
            if ($skippedResults.Count -gt 0) {
                Write-Output "Migration skipped samples ($($skippedResults.Count) total):`n$($skippedResults | Select-Object -First 10 | ConvertTo-Json -Depth 20)"
            }
        }
    }
}

try {
    if ([string]::IsNullOrWhiteSpace($FunctionKey)) {
        Write-Output "Info: No FunctionKey provided, sending the request without headers."
        $initialResponse = Invoke-WebRequest -Uri $FunctionUrl
    } 
    else { 
        Write-Output "Info: FunctionKey provided, sending the request with headers."
        $headers = @{
            "x-functions-key" = $FunctionKey
            "Content-Type"    = "application/json"
        }
        $initialResponse = Invoke-WebRequest -Uri $FunctionUrl -Headers $headers
    }

    $initialPayload = $initialResponse.Content | ConvertFrom-Json
    $statusUri = $initialPayload.statusQueryGetUri

    Write-Output "Orchestration started. Initial status: $($initialResponse.StatusCode) $($initialResponse.StatusDescription)"
    if ($PrintOrchestrationId -or $PrintCustomStatus -or -not [string]::IsNullOrWhiteSpace($StatusOutputPath)) {
        Write-Output "Durable orchestration id: $($initialPayload.id)"
    }
    if (-not [string]::IsNullOrWhiteSpace($StatusOutputPath)) {
        Write-Output "Writing Durable status snapshots to: $StatusOutputPath"
    }
    Write-Output $initialResponse

    while ($true) {
        if ($stopwatch.Elapsed.TotalSeconds -ge $TimeoutSeconds) {
            Write-Output "Exiting after $TimeoutSeconds second timeout."
            break
        }

        try {
            $statusResponse = Invoke-WebRequest -Uri $statusUri -UseBasicParsing -ErrorAction Stop
            $status = $statusResponse.Content | ConvertFrom-Json
            Save-StatusSnapshot -Status $status
            $consecutiveStatusPollFailures = 0

            Write-Output "Polling status. Current runtime status: $($status.runtimeStatus)"

            if ($PrintCustomStatus -and $null -ne $status.customStatus) {
                $customStatusJson = $status.customStatus | ConvertTo-Json -Depth 30 -Compress
                if ($customStatusJson -ne $lastCustomStatusJson) {
                    Write-Output "Durable custom status:`n$($status.customStatus | ConvertTo-Json -Depth 30)"
                    $lastCustomStatusJson = $customStatusJson
                }
            }

            if ($status.runtimeStatus -in ("Completed", "Failed", "Canceled", "Terminated")) { break }
        }
        catch {
            if ($MaxStatusPollFailures -le 1) {
                throw
            }

            $consecutiveStatusPollFailures++
            Write-Warning "Durable status poll failed ($consecutiveStatusPollFailures/$MaxStatusPollFailures)."
            Write-Warning (Get-ErrorDetails -ErrorRecord $_)

            if ($consecutiveStatusPollFailures -ge $MaxStatusPollFailures) {
                throw "Durable status polling failed $consecutiveStatusPollFailures consecutive times for orchestration $($initialPayload.id)."
            }
        }

        Start-Sleep -Seconds 5
    }

    Write-Output "Job complete. Parsing result."
    if ($null -ne $statusResponse) {
        Write-Output "Final HTTP Status: $($statusResponse.StatusCode) $($statusResponse.StatusDescription)"
    } else {
        Write-Output "Final HTTP Status: unavailable"
    }
    if ($null -ne $status) {
        Write-Output "Final Azure Function Output:`n$($status | ConvertTo-Json -Depth $FinalOutputJsonDepth)"
    } else {
        Write-Output "Final Azure Function Output: unavailable"
    }

    $runtimeStatus = if ($null -ne $status) { $status.runtimeStatus } else { $null }
    $output = if ($null -ne $status) { $status.output } else { $null }
    if ($PrintOutputSummary) {
        Write-MigrationOutputSummary -Output $output
    }

    if ($runtimeStatus -in ('Failed','Canceled','Terminated')) {
        Write-Output "Runtime status indicates failure state: $runtimeStatus. Exiting with failure."
        exit 1
    }
    elseif ($runtimeStatus -eq 'Completed') {
        if ($null -ne $output) {
            $success = $false
            try { 
                if ($output.PSObject.Properties.Name -contains 'success') { 
                    $success = [bool]$output.success 
                } 
            } 
            catch { 
                $success = $false 
            }
            if ($success) {
                Write-Output "Completed with success=true. Exiting 0."
                exit 0
            } else {
                Write-Output "Completed but success flag missing or false. Exiting 1."
                exit 1
            }
        } else {
            Write-Output "Warning: Completed with no output payload. Exiting with failure for safety."
            exit 1
        }
    } else {
        Write-Output "Orchestration did not reach a terminal successful state (status=$runtimeStatus). Exiting with failure."
        exit 1
    }
}
catch {
    Write-Output "An error occurred: $_"
    exit 1
}
finally {
    Write-Output "Total execution time: $($stopwatch.Elapsed.TotalSeconds) seconds"
}