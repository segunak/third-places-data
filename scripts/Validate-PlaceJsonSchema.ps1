<#
.SYNOPSIS
    Validates JSON files against the expected schema used by Cosmos sync.
    
.DESCRIPTION
    Scans all place JSON files and reports schema inconsistencies based on
    how cosmos_service.py and cosmos.py parse the data.
    
    Expected Schema (based on Cosmos sync code):
    - details.raw_data: dict with fields like name, full_address, city, etc.
    - reviews.raw_data: dict containing reviews_data array
    - reviews.raw_data.reviews_data: array of review objects
    - photos.raw_data: dict containing photos_data array
    - photos.raw_data.photos_data: array of photo objects
    
.PARAMETER PlacesDir
    Path to the places directory (default: data/places/charlotte)
    
.PARAMETER OutputFile
    Optional path to write JSON report (default: console output only)
    
.PARAMETER IncludeSummary
    If specified, only outputs summary statistics.
    
.EXAMPLE
    .\Validate-PlaceJsonSchema.ps1
    .\Validate-PlaceJsonSchema.ps1 -OutputFile "validation-report.json"
    .\Validate-PlaceJsonSchema.ps1 -IncludeSummary
#>

param(
    [string]$PlacesDir = "..\data\places\charlotte",
    [string]$OutputFile = "",
    [switch]$IncludeSummary
)

$ErrorActionPreference = "Stop"

$placesPath = Join-Path $PSScriptRoot $PlacesDir

if (-not (Test-Path $placesPath)) {
    Write-Host "Error: Places directory not found at $placesPath" -ForegroundColor Red
    exit 1
}

Write-Host "=== Place JSON Schema Validator ===" -ForegroundColor Cyan
Write-Host "Directory: $placesPath" -ForegroundColor Gray
Write-Host ""

# Track all issues
$allIssues = @()
$fileStats = @{
    total = 0
    valid = 0
    withIssues = 0
}

# Section stats
$sectionStats = @{
    details = @{ present = 0; missing = 0; issues = 0 }
    reviews = @{ present = 0; missing = 0; issues = 0 }
    photos = @{ present = 0; missing = 0; issues = 0 }
}

# Get all JSON files
$jsonFiles = Get-ChildItem -Path $placesPath -Filter "*.json" -File

Write-Host "Found $($jsonFiles.Count) JSON files" -ForegroundColor Yellow
Write-Host ""

foreach ($file in $jsonFiles) {
    $fileStats.total++
    $fileIssues = @()
    
    try {
        $json = Get-Content -Path $file.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
        
        # === DETAILS SECTION ===
        $details = $json.details
        if ($null -eq $details) {
            $fileIssues += @{
                section = "details"
                issue = "MISSING_SECTION"
                message = "No 'details' section found"
            }
            $sectionStats.details.missing++
        } else {
            $sectionStats.details.present++
            
            $detailsRawData = $details.raw_data
            if ($null -eq $detailsRawData) {
                # Missing raw_data is just incomplete data, not a schema violation
                # Only track, don't flag as issue
            }
            elseif ($detailsRawData -is [System.Array]) {
                $fileIssues += @{
                    section = "details"
                    issue = "RAW_DATA_IS_ARRAY"
                    message = "details.raw_data is array (should be dict)"
                    itemCount = $detailsRawData.Count
                }
                $sectionStats.details.issues++
            }
            elseif ($detailsRawData -is [PSCustomObject]) {
                # Expected type - check required fields
                $requiredFields = @("name", "full_address")
                $missingFields = @()
                foreach ($field in $requiredFields) {
                    if (-not ($detailsRawData.PSObject.Properties.Name -contains $field)) {
                        $missingFields += $field
                    }
                }
                if ($missingFields.Count -gt 0) {
                    $fileIssues += @{
                        section = "details"
                        issue = "MISSING_EXPECTED_FIELDS"
                        message = "details.raw_data missing fields: $($missingFields -join ', ')"
                        fields = $missingFields
                    }
                    $sectionStats.details.issues++
                }
            }
        }
        
        # === REVIEWS SECTION ===
        $reviews = $json.reviews
        if ($null -eq $reviews) {
            # reviews section is optional, just track
            $sectionStats.reviews.missing++
        } else {
            $sectionStats.reviews.present++
            
            $reviewsRawData = $reviews.raw_data
            if ($null -eq $reviewsRawData) {
                # Missing raw_data is just incomplete data, not a schema violation
                # Only track, don't flag as issue
            }
            elseif ($reviewsRawData -is [System.Array]) {
                $fileIssues += @{
                    section = "reviews"
                    issue = "RAW_DATA_IS_ARRAY"
                    message = "reviews.raw_data is array (should be dict with reviews_data key)"
                    itemCount = $reviewsRawData.Count
                }
                $sectionStats.reviews.issues++
            }
            elseif ($reviewsRawData -is [PSCustomObject]) {
                # Check for reviews_data array
                if (-not ($reviewsRawData.PSObject.Properties.Name -contains "reviews_data")) {
                    $fileIssues += @{
                        section = "reviews"
                        issue = "MISSING_REVIEWS_DATA_KEY"
                        message = "reviews.raw_data is dict but missing reviews_data key"
                    }
                    $sectionStats.reviews.issues++
                }
                else {
                    $reviewsData = $reviewsRawData.reviews_data
                    if ($null -ne $reviewsData -and -not ($reviewsData -is [System.Array])) {
                        $fileIssues += @{
                            section = "reviews"
                            issue = "REVIEWS_DATA_NOT_ARRAY"
                            message = "reviews.raw_data.reviews_data is not an array"
                            actualType = $reviewsData.GetType().Name
                        }
                        $sectionStats.reviews.issues++
                    }
                }
            }
        }
        
        # === PHOTOS SECTION ===
        $photos = $json.photos
        if ($null -eq $photos) {
            # photos section is optional, just track
            $sectionStats.photos.missing++
        } else {
            $sectionStats.photos.present++
            
            $photosRawData = $photos.raw_data
            if ($null -eq $photosRawData) {
                # Missing raw_data is just incomplete data, not a schema violation
                # Only track, don't flag as issue
            }
            elseif ($photosRawData -is [System.Array]) {
                $fileIssues += @{
                    section = "photos"
                    issue = "RAW_DATA_IS_ARRAY"
                    message = "photos.raw_data is array (should be dict with photos_data key)"
                    itemCount = $photosRawData.Count
                }
                $sectionStats.photos.issues++
            }
            elseif ($photosRawData -is [PSCustomObject]) {
                # Check for photos_data array
                if (-not ($photosRawData.PSObject.Properties.Name -contains "photos_data")) {
                    $fileIssues += @{
                        section = "photos"
                        issue = "MISSING_PHOTOS_DATA_KEY"
                        message = "photos.raw_data is dict but missing photos_data key"
                    }
                    $sectionStats.photos.issues++
                }
                else {
                    $photosData = $photosRawData.photos_data
                    if ($null -ne $photosData -and -not ($photosData -is [System.Array])) {
                        $fileIssues += @{
                            section = "photos"
                            issue = "PHOTOS_DATA_NOT_ARRAY"
                            message = "photos.raw_data.photos_data is not an array"
                            actualType = $photosData.GetType().Name
                        }
                        $sectionStats.photos.issues++
                    }
                }
            }
        }
        
        # === RECORD ISSUES ===
        if ($fileIssues.Count -gt 0) {
            $fileStats.withIssues++
            
            foreach ($issue in $fileIssues) {
                $issue.file = $file.Name
                $allIssues += $issue
            }
        } else {
            $fileStats.valid++
        }
    }
    catch {
        $fileStats.withIssues++
        $allIssues += @{
            file = $file.Name
            section = "PARSE"
            issue = "JSON_PARSE_ERROR"
            message = $_.Exception.Message
        }
    }
}

# === OUTPUT RESULTS ===

Write-Host "=== Validation Results ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "File Statistics:" -ForegroundColor Yellow
Write-Host "  Total files:  $($fileStats.total)"
Write-Host "  Valid files:  $($fileStats.valid)" -ForegroundColor Green
Write-Host "  With issues:  $($fileStats.withIssues)" -ForegroundColor $(if ($fileStats.withIssues -gt 0) { "Red" } else { "Green" })
Write-Host ""

Write-Host "Section Statistics:" -ForegroundColor Yellow
Write-Host "  Details:"
Write-Host "    Present: $($sectionStats.details.present), Missing: $($sectionStats.details.missing), Issues: $($sectionStats.details.issues)"
Write-Host "  Reviews:"
Write-Host "    Present: $($sectionStats.reviews.present), Missing: $($sectionStats.reviews.missing), Issues: $($sectionStats.reviews.issues)"
Write-Host "  Photos:"
Write-Host "    Present: $($sectionStats.photos.present), Missing: $($sectionStats.photos.missing), Issues: $($sectionStats.photos.issues)"
Write-Host ""

if (-not $IncludeSummary -and $allIssues.Count -gt 0) {
    Write-Host "=== Issues by Type ===" -ForegroundColor Cyan
    
    # Group issues by type
    $issuesByType = $allIssues | Group-Object -Property issue
    
    foreach ($group in $issuesByType | Sort-Object -Property Count -Descending) {
        Write-Host ""
        Write-Host "$($group.Name) ($($group.Count) occurrences)" -ForegroundColor Red
        
        foreach ($issue in $group.Group | Select-Object -First 5) {
            Write-Host "  - $($issue.file): $($issue.message)" -ForegroundColor Gray
        }
        
        if ($group.Count -gt 5) {
            Write-Host "  ... and $($group.Count - 5) more" -ForegroundColor DarkGray
        }
    }
}

# Output to JSON file if specified
if ($OutputFile -ne "") {
    $report = @{
        timestamp = (Get-Date).ToString("o")
        directory = $placesPath
        fileStats = $fileStats
        sectionStats = $sectionStats
        issues = $allIssues
    }
    
    $reportJson = $report | ConvertTo-Json -Depth 10
    [System.IO.File]::WriteAllText($OutputFile, $reportJson, [System.Text.UTF8Encoding]::new($false))
    
    Write-Host ""
    Write-Host "Report saved to: $OutputFile" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "=== Done ===" -ForegroundColor Cyan

# Exit with error code if issues found
if ($allIssues.Count -gt 0) {
    exit 1
} else {
    exit 0
}
