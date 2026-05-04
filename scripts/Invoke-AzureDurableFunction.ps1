param(
    [Parameter(Mandatory = $true)]
    [string]$FunctionUrl,

    [Parameter(Mandatory = $false)]
    [string]$FunctionKey,

    [Parameter(Mandatory = $false)]
    [int]$TimeoutSeconds = 300
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
$lastCustomStatusJson = $null
$orchestrationId = $null

function Get-ContentPreview {
    param(
        [AllowNull()]
        [object]$Content,

        [int]$MaxCharacters = 2000
    )

    if ($null -eq $Content) {
        return "<empty>"
    }

    $text = [string]$Content
    if ([string]::IsNullOrWhiteSpace($text)) {
        return "<empty>"
    }

    if ($text.Length -le $MaxCharacters) {
        return $text
    }

    return "$($text.Substring(0, $MaxCharacters))`n... truncated $($text.Length - $MaxCharacters) characters"
}

function Write-LastKnownDurableContext {
    param(
        [AllowNull()]
        [object]$Status,

        [AllowNull()]
        [string]$CustomStatusJson,

        [AllowNull()]
        [string]$OrchestrationId,

        [string]$StatusUri
    )

    if (-not [string]::IsNullOrWhiteSpace($OrchestrationId)) {
        Write-Output "Durable orchestration id: $OrchestrationId"
    }
    if (-not [string]::IsNullOrWhiteSpace($StatusUri)) {
        Write-Output "Durable status URL: $StatusUri"
    }
    if ($null -ne $Status) {
        Write-Output "Last known runtime status: $($Status.runtimeStatus)"
    }
    else {
        Write-Output "Last known runtime status: unavailable"
    }

    if (-not [string]::IsNullOrWhiteSpace($CustomStatusJson)) {
        Write-Output "Last known custom status:`n$CustomStatusJson"
    }
}

function Convert-DurableStatusResponse {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Response
    )

    try {
        return $Response.Content | ConvertFrom-Json -ErrorAction Stop
    }
    catch {
        Write-Output "Status query response was not JSON. HTTP Status: $($Response.StatusCode) $($Response.StatusDescription)"
        Write-Output "Status query response body preview:`n$(Get-ContentPreview -Content $Response.Content)"
        throw
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
    if ($initialPayload.PSObject.Properties.Name -contains "id") {
        $orchestrationId = [string]$initialPayload.id
    }

    Write-Output "Orchestration started. Initial status: $($initialResponse.StatusCode) $($initialResponse.StatusDescription)"
    if (-not [string]::IsNullOrWhiteSpace($orchestrationId)) {
        Write-Output "Durable orchestration id: $orchestrationId"
    }
    Write-Output $initialResponse

    while ($true) {
        if ($stopwatch.Elapsed.TotalSeconds -ge $TimeoutSeconds) {
            Write-Output "Exiting after $TimeoutSeconds second timeout."
            break
        }

        try {
            $statusResponse = Invoke-WebRequest -Uri $statusUri -UseBasicParsing -SkipHttpErrorCheck -ErrorAction Stop
            try {
                $status = Convert-DurableStatusResponse -Response $statusResponse
            }
            catch {
                Write-LastKnownDurableContext -Status $status -CustomStatusJson $lastCustomStatusJson -OrchestrationId $orchestrationId -StatusUri $statusUri
                Write-Output "Retrying Durable status query once after non-JSON response."
                Start-Sleep -Seconds 5
                $statusResponse = Invoke-WebRequest -Uri $statusUri -UseBasicParsing -SkipHttpErrorCheck -ErrorAction Stop
                $status = Convert-DurableStatusResponse -Response $statusResponse
            }
        }
        catch {
            Write-Output "Status polling failed before a terminal Durable status could be parsed."
            Write-LastKnownDurableContext -Status $status -CustomStatusJson $lastCustomStatusJson -OrchestrationId $orchestrationId -StatusUri $statusUri
            throw
        }

        if ([int]$statusResponse.StatusCode -ge 400) {
            Write-Output "Status query returned HTTP $($statusResponse.StatusCode) $($statusResponse.StatusDescription); parsing Durable payload."
        }

        Write-Output "Polling status. Current runtime status: $($status.runtimeStatus)"

        if ($status.PSObject.Properties.Name -contains "customStatus" -and $null -ne $status.customStatus) {
            $customStatusJson = $status.customStatus | ConvertTo-Json -Depth 50
            if ($customStatusJson -ne $lastCustomStatusJson) {
                Write-Output "Custom status:`n$customStatusJson"
                $lastCustomStatusJson = $customStatusJson
            }
        }

        if ($status.runtimeStatus -in ("Completed", "Failed", "Canceled", "Terminated")) {
            break
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
        Write-Output "Final Azure Function Output:`n$($status | ConvertTo-Json -Depth 100)"
    } else {
        Write-Output "Final Azure Function Output: unavailable"
    }

    $runtimeStatus = if ($null -ne $status) { $status.runtimeStatus } else { $null }
    $output = if ($null -ne $status) { $status.output } else { $null }

    if ($runtimeStatus -in ('Failed', 'Canceled', 'Terminated')) {
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
    Write-Output "An error occurred: $($_.Exception.Message)"
    if ($_.ErrorDetails -and -not [string]::IsNullOrWhiteSpace($_.ErrorDetails.Message)) {
        Write-Output "Error response body:`n$($_.ErrorDetails.Message)"
    }
    exit 1
}
finally {
    Write-Output "Total execution time: $($stopwatch.Elapsed.TotalSeconds) seconds"
}
