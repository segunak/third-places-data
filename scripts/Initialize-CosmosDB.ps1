<#
.SYNOPSIS
    Initializes Azure Cosmos DB for the Charlotte Third Places RAG chatbot.

.DESCRIPTION
    This script creates the database and containers needed for the Charlotte Third Places
    AI chatbot. It sets up vector search capabilities for semantic search over places and
    review chunks.

    Run this script ONCE to set up the infrastructure. After that, use the Azure Functions
    to sync data from Airtable and JSON files into Cosmos DB.

.PREREQUISITES
    1. Azure CLI installed and logged in (run 'az login' first)
    2. The "Vector Search for NoSQL API" feature must be enabled on your Cosmos DB account
       - Go to Azure Portal > Cosmos DB Account > Settings > Features
       - Enable "Vector Search for NoSQL API"
       - Wait ~15 minutes for it to take effect

.USAGE
    # From Azure Cloud Shell or local PowerShell with Azure CLI:
    ./Initialize-CosmosDB.ps1

.NOTES    
    This script is idempotent - safe to run multiple times. It will skip resources that
    already exist.
#>

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Azure resource identifiers - update these if your resource names differ
$ResourceGroup = "third-places-data"
$CosmosAccountName = "cosmos-third-places"
$DatabaseName = "third-places"

# Container configurations
$PlacesContainerName = "places"
$ChunksContainerName = "chunks"

# Temp directory for policy files (defined early for cleanup in finally block)
$TempDir = Join-Path ([System.IO.Path]::GetTempPath()) "cosmos-setup-$PID"

# ------------------------------------------------------------------------------
# VECTOR EMBEDDING CONFIGURATION
# ------------------------------------------------------------------------------
# 
# What is an embedding?
# ---------------------
# An embedding converts text into a list of numbers (a "vector") that captures
# its meaning. Similar texts have similar numbers. This enables "semantic search"
# where we find content by meaning, not just keyword matching.
#
# Example:
#   "cozy coffee shop with fireplace" → [0.12, -0.45, 0.89, ...] (1536 numbers)
#   "warm café with a fire"           → [0.11, -0.44, 0.88, ...] (very similar!)
#   "loud sports bar"                 → [-0.67, 0.22, -0.15, ...] (very different)
#
# Current configuration:
# ----------------------
# - dimensions: 1536 (text-embedding-3-small model)
# - distanceFunction: "cosine" - measures angle between vectors. Standard for text.
# - dataType: "float32" - standard precision for embeddings.
# - vectorIndexType: "quantizedFlat" (see rationale below)
#
# ------------------------------------------------------------------------------
# WHY quantizedFlat INSTEAD OF diskANN?
# ------------------------------------------------------------------------------
#
# Microsoft's guidance (https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/vector-search):
#   "quantizedFlat is recommended when the number of vectors to be indexed is
#   somewhere around 50,000 or fewer per physical partition. This is a good
#   option for smaller scenarios, or scenarios where you're using query filters
#   to narrow down the vector search to a relatively small set of vectors."
#
# Our data scale (as of December 2025):
#   - places container: ~380 vectors currently, ~500 max projected
#   - chunks container: ~125,000 vectors max (500 places × 250 reviews each)
#   - Physical partitions: 1 (Azure assigns based on data size)
#
# Our query patterns (see charlotte-third-places/lib/ai/rag.ts):
#   - General chat (/chat page): Vector search on places only (~500 vectors, no filter)
#   - Place-specific chat: Vector search on chunks WITH placeId filter (~250 vectors)
#   - Cross-partition chunk search is SKIPPED for general queries (performance optimization)
#
# Why quantizedFlat wins for us:
#   1. places container: ~380 vectors (500 max) is 100x below the 50k threshold
#   2. chunks container: Always filtered by placeId, so each query searches ~250 vectors
#   3. Brute-force search (quantizedFlat) = 100% accuracy vs diskANN's ~95%
#   4. Lower storage overhead than diskANN's graph index
#   5. Simpler - no tuning parameters (indexingSearchListSize, quantizationByteSize)
#
# When to reconsider diskANN:
#   - If we add cross-partition chunk search for general queries (currently skipped)
#   - If we exceed 50k+ places (currently ~500)
#   - If latency becomes problematic (not observed)
#
# Vector index types summary:
# - "flat": Exact brute-force, 100% accuracy. Max 505 dimensions. Slowest.
# - "quantizedFlat": Compressed brute-force, ~99% accuracy. Good for <50k vectors/partition.
# - "diskANN": Graph-based ANN, ~95%+ accuracy. Best for >50k vectors/partition.
#
# Note: quantizedFlat/diskANN require at least 1,000 vectors for accurate quantization;
# below that, Cosmos falls back to full scan (which is fine for small datasets).
#
# To change these values, edit the --vector-embeddings and --idx JSON in the
# container creation commands below.
# ------------------------------------------------------------------------------

# ==============================================================================
# MAIN SCRIPT (wrapped in try/finally for guaranteed cleanup)
# ==============================================================================

try {

# ==============================================================================
# VALIDATION
# ==============================================================================

Write-Host "`n" -NoNewline
Write-Host "═══════════════════════════════════════════════════════════════════" -ForegroundColor Magenta
Write-Host "  Charlotte Third Places - Cosmos DB Initialization Script" -ForegroundColor Magenta
Write-Host "═══════════════════════════════════════════════════════════════════" -ForegroundColor Magenta

Write-Host "`nValidating Azure CLI login..."

# Check if logged in to Azure
$account = az account show 2>$null | ConvertFrom-Json
if (-not $account) {
    throw "Not logged in to Azure CLI. Run 'az login' first."
}

Write-Host "Logged in as: $($account.user.name)"
Write-Host "Subscription: $($account.name)"

# Verify the Cosmos DB account exists
Write-Host "`nVerifying Cosmos DB account exists..."
$cosmosAccount = az cosmosdb show `
    --name $CosmosAccountName `
    --resource-group $ResourceGroup `
    2>$null | ConvertFrom-Json

if (-not $cosmosAccount) {
    Write-Warning "Create it in the Azure Portal first, then re-run this script."
    throw "Cosmos DB account '$CosmosAccountName' not found in resource group '$ResourceGroup'."
}

Write-Host "Found Cosmos DB account: $CosmosAccountName"
Write-Host "Endpoint: $($cosmosAccount.documentEndpoint)"

# ==============================================================================
# DATABASE CREATION
# ==============================================================================

Write-Host "`nCreating database '$DatabaseName'..."

# Check if database already exists
$existingDb = az cosmosdb sql database show `
    --account-name $CosmosAccountName `
    --resource-group $ResourceGroup `
    --name $DatabaseName `
    2>$null | ConvertFrom-Json

if ($existingDb) {
    Write-Host "Database '$DatabaseName' already exists. Skipping creation."
} else {
    # Create database WITHOUT shared throughput
    # Vector indexing requires dedicated (container-level) throughput, not shared (database-level)
    # Each container will get its own 400 RU/s (minimum for dedicated throughput)
    az cosmosdb sql database create `
        --account-name $CosmosAccountName `
        --resource-group $ResourceGroup `
        --name $DatabaseName `
        | Out-Null
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Database '$DatabaseName' created (no shared throughput - containers will have dedicated RUs)."
    } else {
        throw "Failed to create database '$DatabaseName'."
    }
}

# ==============================================================================
# POLICY FILES (Azure CLI recommended approach for complex JSON)
# ==============================================================================
# The Azure CLI has known issues with inline JSON containing nested objects.
# Microsoft recommends: "When working with JSON parameter values, consider using
# Azure CLI's @<file> convention and bypass the shell's interpretation mechanisms."
# See: https://learn.microsoft.com/cli/azure/use-azure-cli-successfully-quoting#json-strings

# Create temp directory for policy files
if (-not (Test-Path $TempDir)) {
    New-Item -ItemType Directory -Path $TempDir -Force | Out-Null
}

# Vector embedding policy file
$VectorPolicyPath = Join-Path $TempDir "vector-policy.json"
@'
{
    "vectorEmbeddings": [
        {
            "path": "/embedding",
            "dataType": "float32",
            "distanceFunction": "cosine",
            "dimensions": 1536
        }
    ]
}
'@ | Set-Content -Path $VectorPolicyPath -Encoding UTF8

# Indexing policy with vector index
$IndexPolicyPath = Join-Path $TempDir "index-policy.json"
@'
{
    "indexingMode": "consistent",
    "includedPaths": [
        {"path": "/*"}
    ],
    "excludedPaths": [
        {"path": "/embedding/*"},
        {"path": "/_etag/?"}
    ],
    "vectorIndexes": [
        {"path": "/embedding", "type": "quantizedFlat"}
    ]
}
'@ | Set-Content -Path $IndexPolicyPath -Encoding UTF8

Write-Host "`nPolicy files created in: $TempDir"

# ==============================================================================
# PLACES CONTAINER
# ==============================================================================
#
# The 'places' container stores one document per place (coffee shop, library, etc.)
# 
# Document structure:
# {
#   "id": "ChIJYXyT1nHBVogRO0z_uPKgq4g",  // Google Maps Place Id (partition key)
#   "name": "The Wandering Cup | Gastonia",
#   "neighborhood": "Gastonia",
#   "type": ["Coffee Shop"],
#   "tags": ["co-working", "loft", "WFH"],
#   ... (all Airtable + JSON fields merged) ...
#   "embedding": [0.12, -0.45, ...],  // 1536 floats for semantic search
#   "lastSynced": "2025-11-28T00:00:00Z"
# }
#
# Partition key: /id (the Google Maps Place Id)
# - Each place is its own partition (perfect distribution)
# - Point reads by ID are cheap (1 RU)
# - Vector search is cross-partition anyway, so partition choice doesn't affect it
#

Write-Host "`nCreating 'places' container..."

$existingPlaces = az cosmosdb sql container show `
    --account-name $CosmosAccountName `
    --resource-group $ResourceGroup `
    --database-name $DatabaseName `
    --name $PlacesContainerName `
    2>$null | ConvertFrom-Json

if ($existingPlaces) {
    Write-Host "Container '$PlacesContainerName' already exists. Skipping creation."
    Write-Host "To recreate with different settings, delete it first in the Azure Portal."
} else {
    # Create container with vector search enabled
    # Using @file syntax as recommended by Azure for complex JSON
    # Vector indexing requires dedicated throughput (400 RU/s minimum per container)
    # Using 500 RU/s (half of 1000 free tier limit, split between 2 containers)
    az cosmosdb sql container create `
        --account-name $CosmosAccountName `
        --resource-group $ResourceGroup `
        --database-name $DatabaseName `
        --name $PlacesContainerName `
        --partition-key-path "/id" `
        --throughput 500 `
        --vector-embeddings "@$VectorPolicyPath" `
        --idx "@$IndexPolicyPath" `
        | Out-Null
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Container '$PlacesContainerName' created with 500 RU/s."
        Write-Host "  Partition key: /id"
        Write-Host "  Vector path: /embedding (1536 dimensions, cosine)"
        Write-Host "  Vector index: quantizedFlat"
    } else {
        Write-Warning "Make sure 'Vector Search for NoSQL API' is enabled in account Features."
        throw "Failed to create '$PlacesContainerName' container."
    }
}

# ==============================================================================
# CHUNKS CONTAINER
# ==============================================================================
#
# The 'chunks' container stores review chunks for precise RAG retrieval.
# 
# Why separate from places?
# - A single place embedding is a "blurry average" of all its reviews
# - Individual chunks let the chatbot find THE specific review that answers a question
# - Enables quotable citations: "One reviewer said: 'Perfect for laptop work...'"
#
# Document structure:
# {
#   "id": "ChIJYXyT1nHBVogRO0z_uPKgq4g_review_001",
#   "placeId": "ChIJYXyT1nHBVogRO0z_uPKgq4g",  // partition key
#   "placeName": "The Wandering Cup | Gastonia",
#   "neighborhood": "Gastonia",
#   "source": "review",
#   "text": "Perfect co-working spot. The loft area is quiet and there's good wifi.",
#   "rating": 5,
#   "date": "2025-09-20T00:00:00Z",
#   "embedding": [0.34, -0.12, ...],  // 1536 floats
# }
#
# Partition key: /placeId
# - All chunks for a place live in the same partition
# - Efficient to fetch all chunks for a place after vector search
# - Cross-partition vector search finds relevant chunks across all places

Write-Host "`nCreating 'chunks' container..."

$existingChunks = az cosmosdb sql container show `
    --account-name $CosmosAccountName `
    --resource-group $ResourceGroup `
    --database-name $DatabaseName `
    --name $ChunksContainerName `
    2>$null | ConvertFrom-Json

if ($existingChunks) {
    Write-Host "Container '$ChunksContainerName' already exists. Skipping creation."
    Write-Host "To recreate with different settings, delete it first in the Azure Portal."
} else {
    # Create container with vector search enabled
    # Using @file syntax as recommended by Azure for complex JSON
    # Vector indexing requires dedicated throughput (400 RU/s minimum per container)
    # Using 500 RU/s (half of 1000 free tier limit, split between 2 containers)
    az cosmosdb sql container create `
        --account-name $CosmosAccountName `
        --resource-group $ResourceGroup `
        --database-name $DatabaseName `
        --name $ChunksContainerName `
        --partition-key-path "/placeId" `
        --throughput 500 `
        --vector-embeddings "@$VectorPolicyPath" `
        --idx "@$IndexPolicyPath" `
        | Out-Null
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Container '$ChunksContainerName' created with 500 RU/s."
        Write-Host "  Partition key: /placeId"
        Write-Host "  Vector path: /embedding (1536 dimensions, cosine)"
        Write-Host "  Vector index: quantizedFlat"
    } else {
        Write-Warning "Make sure 'Vector Search for NoSQL API' is enabled in account Features."
        throw "Failed to create '$ChunksContainerName' container."
    }
}

# ==============================================================================
# SUCCESS SUMMARY
# ==============================================================================

Write-Host "`n"
Write-Host "═══════════════════════════════════════════════════════════════════" -ForegroundColor Green
Write-Host "  Setup Complete!" -ForegroundColor Green
Write-Host "═══════════════════════════════════════════════════════════════════" -ForegroundColor Green
Write-Host ""
Write-Host "  Database: $DatabaseName" -ForegroundColor White
Write-Host "  Containers:" -ForegroundColor White
Write-Host "    • $PlacesContainerName (partition key: /id)" -ForegroundColor White
Write-Host "    • $ChunksContainerName (partition key: /placeId)" -ForegroundColor White
Write-Host ""
Write-Host "  Vector Configuration:" -ForegroundColor White
Write-Host "    • Dimensions: 1536" -ForegroundColor White
Write-Host "    • Distance: cosine" -ForegroundColor White
Write-Host "    • Index: quantizedFlat" -ForegroundColor White

} # end try
finally {
    # ==============================================================================
    # CLEANUP (always runs, even on error or exit)
    # ==============================================================================
    if ($TempDir -and (Test-Path $TempDir)) {
        Remove-Item -Path $TempDir -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "`nCleaned up temporary policy files." -ForegroundColor DarkGray
    }
}
