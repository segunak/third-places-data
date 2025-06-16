# Generate Place Names Mapping
# This script reads all JSON files in the charlotte folder and extracts place names
# Creates a lightweight mapping file for the web app to use

param(
    [string]$PlacesDir = "..\places\charlotte",
    [string]$OutputFile = "place-names.json"
)

Write-Host "Third Places - Place Names Generator" -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Green

# Get the absolute path to the places directory
$placesPath = Join-Path $PSScriptRoot $PlacesDir
Write-Host "Scanning directory: $placesPath" -ForegroundColor Yellow

if (-not (Test-Path $placesPath)) {
    Write-Host "Error: Places directory not found at $placesPath" -ForegroundColor Red
    exit 1
}

# Get all JSON files
$jsonFiles = Get-ChildItem -Path $placesPath -Filter "*.json" -File
Write-Host "Found $($jsonFiles.Count) JSON files" -ForegroundColor Cyan

$placeMapping = @{}
$processedCount = 0
$errorCount = 0

foreach ($file in $jsonFiles) {
    try {
        Write-Progress -Activity "Processing JSON files" -Status "Processing $($file.Name)" -PercentComplete (($processedCount / $jsonFiles.Count) * 100)
        
        # Read and parse JSON
        $jsonContent = Get-Content -Path $file.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
        
        # Extract place name using the same logic as the JavaScript
        $placeName = $null
        
        if ($jsonContent.place_name) {
            $placeName = $jsonContent.place_name
        }
        elseif ($jsonContent.details -and $jsonContent.details.place_name) {
            $placeName = $jsonContent.details.place_name
        }
        elseif ($jsonContent.details -and $jsonContent.details.raw_data -and $jsonContent.details.raw_data.name) {
            $placeName = $jsonContent.details.raw_data.name
        }
        elseif ($jsonContent.name) {
            $placeName = $jsonContent.name
        }
        elseif ($jsonContent.title) {
            $placeName = $jsonContent.title
        }
        elseif ($jsonContent.business_name) {
            $placeName = $jsonContent.business_name
        }
        else {
            # Fallback to filename without extension
            $placeName = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)
        }
        
        # Add to mapping
        $placeMapping[$file.Name] = @{
            name = $placeName
            filename = $file.Name
            hasPhotos = $false
        }
        
        # Check if place has photos
        if ($jsonContent.photos) {
            $hasPhotoUrls = $jsonContent.photos.photo_urls -and $jsonContent.photos.photo_urls.Count -gt 0
            $hasRawData = $jsonContent.photos.raw_data -and $jsonContent.photos.raw_data.Count -gt 0
            $placeMapping[$file.Name].hasPhotos = $hasPhotoUrls -or $hasRawData
        }
        
        $processedCount++
        
    }
    catch {
        Write-Host "Error processing $($file.Name): $($_.Exception.Message)" -ForegroundColor Red
        $errorCount++
    }
}

Write-Progress -Activity "Processing JSON files" -Completed

# Generate output file
$outputPath = Join-Path $PSScriptRoot $OutputFile
$sortedMapping = [ordered]@{}

# Sort by place name
$placeMapping.GetEnumerator() | Sort-Object { $_.Value.name } | ForEach-Object {
    $sortedMapping[$_.Key] = $_.Value
}

# Convert to JSON and save
$jsonOutput = $sortedMapping | ConvertTo-Json -Depth 3
$jsonOutput | Out-File -FilePath $outputPath -Encoding UTF8

Write-Host ""
Write-Host "Results:" -ForegroundColor Green
Write-Host "  Processed: $processedCount files" -ForegroundColor White
Write-Host "  Errors: $errorCount files" -ForegroundColor White
Write-Host "  Output file: $outputPath" -ForegroundColor White

# Show sample of places found
Write-Host ""
Write-Host "Sample places found:" -ForegroundColor Cyan
$sampleCount = [Math]::Min(5, $sortedMapping.Count)
$sortedMapping.GetEnumerator() | Select-Object -First $sampleCount | ForEach-Object {
    $hasPhotosText = if ($_.Value.hasPhotos) { " (has photos)" } else { " (no photos)" }
    Write-Host "  - $($_.Value.name)$hasPhotosText" -ForegroundColor Gray
}

if ($sortedMapping.Count -gt $sampleCount) {
    Write-Host "  ... and $($sortedMapping.Count - $sampleCount) more" -ForegroundColor Gray
}

Write-Host ""
Write-Host "Place names mapping generated successfully!" -ForegroundColor Green
