"""
Azure Functions blueprint for Cosmos DB RAG sync operations.
Provides endpoints to sync places and chunks with embeddings.

Uses Azure Durable Functions for the bulk sync operation to handle
long-running syncs that exceed the 10-minute HTTP timeout.
"""

import json
import logging
from typing import Dict, Any, List
import azure.functions as func
import azure.durable_functions as df

from services.cosmos_service import (
    CosmosService,
    transform_airtable_to_place,
    transform_review_to_chunk,
    extract_place_context,
    should_sync_place,
)

from services.embedding_service import (
    EmbeddingService,
    compose_place_embedding_text,
    compose_chunk_embedding_text,
)

from services.utils import fetch_data_github

# Configure logging
logger = logging.getLogger(__name__)

# Default batch size for parallel processing.
# Keep this LOW (5-10) to avoid Cosmos DB 429 rate limiting errors.
# With 500 RU/s per container, high parallelism causes 429 errors.
# Each place sync does: 1 place upsert + N chunk deletes + N chunk upserts.
# Can be overridden via the batch_size query parameter.
DEFAULT_COSMOS_SYNC_BATCH_SIZE = 1

# Create Durable Functions blueprint
bp = df.Blueprint()


def _get_airtable_service():
    """Lazy import to avoid circular dependencies."""
    from services.airtable_service import AirtableService
    return AirtableService(provider_type="outscraper")


def _sync_single_place_logic(
    place_id: str,
    airtable_record: Dict[str, Any],
    cosmos_service: CosmosService,
    embedding_service: EmbeddingService,
    city: str = "charlotte",
    force: bool = True
) -> Dict[str, Any]:
    """
    Sync a single place and its reviews to Cosmos DB.
    
    Args:
        place_id: Google Maps Place Id.
        airtable_record: Airtable record for this place.
        cosmos_service: Cosmos DB service instance.
        embedding_service: Embedding service instance.
        city: City folder name for JSON files.
        force: If False, skip sync if no changes detected since last sync.
               If True (default), always sync regardless of timestamps.
        
    Returns:
        Dict with sync results for this place.
        
    Raises:
        Exception: On any error (fail-fast behavior).
    """
    place_name = airtable_record.get("fields", {}).get("Place", place_id)
    logger.info(f"Syncing place: {place_name} ({place_id})")

    # Fetch JSON data from GitHub
    json_path = f"data/places/{city}/{place_id}.json"
    success, json_data, message = fetch_data_github(json_path)

    if not success:
        logger.warning(f"No JSON data for {place_name}: {message}")
        json_data = None

    # Incremental sync check (only when force=False)
    if not force:
        # Get existing Cosmos document to check lastSynced
        existing_place = cosmos_service.get_place(place_id)
        cosmos_last_synced = existing_place.get("lastSynced") if existing_place else None
        
        # Get timestamps from sources
        airtable_modified = airtable_record.get("fields", {}).get("Last Modified Time")
        json_last_updated = json_data.get("last_updated") if json_data else None
        
        # Check if sync is needed
        needs_sync, reason = should_sync_place(airtable_modified, json_last_updated, cosmos_last_synced)
        
        if not needs_sync:
            logger.info(f"Skipping {place_name} ({place_id}): {reason}")
            return {
                "placeId": place_id,
                "placeName": place_name,
                "skipped": True,
                "skipReason": reason,
                "chunksProcessed": 0,
                "chunksSkipped": 0,
                "hasJsonData": json_data is not None,
            }
        else:
            logger.info(f"Syncing {place_name} ({place_id}): {reason}")

    # Transform Airtable record to place document
    place_doc = transform_airtable_to_place(airtable_record, json_data)

    # Generate embedding text and embedding for place
    embedding_text = compose_place_embedding_text(place_doc)
    place_doc["embeddingText"] = embedding_text

    if embedding_text:
        place_embedding = embedding_service.get_embedding(embedding_text)
        place_doc["embedding"] = place_embedding
    else:
        logger.warning(f"Empty embedding text for place: {place_name}")
        place_doc["embedding"] = None

    # Upsert place to Cosmos DB
    cosmos_service.upsert_place(place_doc)

    # Process reviews (chunks)
    chunks_processed = 0
    chunks_skipped = 0

    if json_data:
        reviews_raw_data = json_data.get("reviews", {}).get("raw_data", {})
        reviews_data = reviews_raw_data.get("reviews_data", [])
        details_raw_data = json_data.get("details", {}).get("raw_data", {})
        place_context = extract_place_context(airtable_record)

        if reviews_data:
            logger.info(f"Processing {len(reviews_data)} reviews for {place_name}")

            # Delete existing chunks for this place (fresh sync)
            cosmos_service.delete_chunks_for_place(place_id)

            # Process reviews in batches for embedding efficiency.
            # Batching reduces API calls (e.g., 100 reviews = 7 calls instead of 100).
            # 16 matches EmbeddingService.max_batch_size - the safe limit for Azure OpenAI.
            batch_size = 16
            for i in range(0, len(reviews_data), batch_size):
                batch = reviews_data[i:i + batch_size]

                # Transform reviews to chunk documents
                chunk_docs = []
                for review in batch:
                    # Skip reviews without text
                    review_text = review.get("review_text")
                    if not review_text or not review_text.strip():
                        chunks_skipped += 1
                        continue

                    # Skip reviews without review_id
                    review_id = review.get("review_id")
                    if not review_id:
                        chunks_skipped += 1
                        continue

                    chunk_doc = transform_review_to_chunk(review, place_context, details_raw_data)
                    chunk_docs.append(chunk_doc)

                if not chunk_docs:
                    continue

                # Generate embedding texts
                embedding_texts = [compose_chunk_embedding_text(doc) for doc in chunk_docs]

                # Filter out empty texts
                valid_docs = []
                valid_texts = []
                for doc, text in zip(chunk_docs, embedding_texts):
                    if text and text.strip():
                        valid_docs.append(doc)
                        valid_texts.append(text)
                        doc["embeddingText"] = text
                    else:
                        chunks_skipped += 1

                if not valid_texts:
                    continue

                # Get embeddings for batch
                embeddings = embedding_service.get_embeddings(valid_texts)

                # Assign embeddings and upsert
                for doc, embedding in zip(valid_docs, embeddings):
                    doc["embedding"] = embedding
                    cosmos_service.upsert_chunk(doc)
                    chunks_processed += 1

    return {
        "placeId": place_id,
        "placeName": place_name,
        "skipped": False,
        "chunksProcessed": chunks_processed,
        "chunksSkipped": chunks_skipped,
        "hasJsonData": json_data is not None,
    }


# =============================================================================
# Durable Functions for Bulk Sync
# =============================================================================

@bp.function_name("CosmosSyncPlaces")
@bp.route(route="cosmos/sync-places", auth_level=func.AuthLevel.FUNCTION)
@bp.durable_client_input(client_name="client")
async def cosmos_sync_places(req: func.HttpRequest, client) -> func.HttpResponse:
    """
    HTTP trigger to start bulk sync of all places to Cosmos DB.
    
    This is a Durable Function orchestration starter. It returns immediately
    with a status URL that can be polled to check progress.
    
    Query params:
        limit: Optional max number of places to sync (for testing).
        batch_size: Degree of parallelism for processing places (default: 1 = sequential).
                    Keep low to avoid Cosmos DB 429 rate limiting errors.
        force: If "true", sync all places regardless of whether they've changed.
               If "false" (default), only sync places that have been modified since
               the last sync (based on Airtable Last Modified Time and JSON last_updated).
        
    Returns:
        HTTP 202 with status check URLs.
    """
    logger.info("Starting Cosmos DB bulk sync orchestration")
    
    # Parse optional limit parameter
    limit_param = req.params.get("limit")
    limit = int(limit_param) if limit_param else None
    
    # Parse optional batch_size parameter (degree of parallelism)
    batch_size_param = req.params.get("batch_size")
    batch_size = int(batch_size_param) if batch_size_param else DEFAULT_COSMOS_SYNC_BATCH_SIZE
    
    # Parse optional force parameter (default: False for incremental sync)
    force_param = req.params.get("force", "false").lower()
    force = force_param in ("true", "1", "yes")
    
    # Configuration to pass to orchestrator
    config = {
        "limit": limit,
        "batch_size": batch_size,
        "force": force,
    }
    
    # Start the orchestration
    instance_id = await client.start_new("cosmos_sync_places_orchestrator", client_input=config)
    logger.info(f"Started orchestration with ID: {instance_id}, force={force}")
    
    # Return status check response
    response = client.create_check_status_response(req, instance_id)
    return response


@bp.orchestration_trigger(context_name="context")
def cosmos_sync_places_orchestrator(context: df.DurableOrchestrationContext):
    """
    Orchestrator function for bulk Cosmos DB sync.
    
    Fetches all places from Airtable, then processes them in parallel batches.
    Uses fan-out/fan-in pattern for efficient parallel processing.
    
    Config params:
        limit: Max number of places to sync (optional).
        batch_size: Degree of parallelism (default: 1 = sequential).
        force: If True, sync all places. If False, only sync modified places.
    """
    config = context.get_input() or {}
    limit = config.get("limit")
    batch_size = config.get("batch_size", DEFAULT_COSMOS_SYNC_BATCH_SIZE)
    force = config.get("force", False)
    
    # Get all places from Airtable (activity function)
    all_places = yield context.call_activity("cosmos_get_all_places", {"limit": limit})
    
    if not all_places:
        return {
            "success": True,
            "placesProcessed": 0,
            "placesSkipped": 0,
            "totalChunksProcessed": 0,
            "totalChunksSkipped": 0,
            "message": "No places to sync",
            "force": force,
        }
    
    # Track overall results
    results = {
        "success": True,
        "placesProcessed": 0,
        "placesSkipped": 0,
        "totalChunksProcessed": 0,
        "totalChunksSkipped": 0,
        "placeDetails": [],
        "skippedPlaces": [],
        "error": None,
        "failedAt": None,
        "batchSize": batch_size,
        "force": force,
        "totalPlaces": len(all_places),
    }
    
    # Process in batches for parallel execution (batch_size from config)
    for i in range(0, len(all_places), batch_size):
        batch = all_places[i:i + batch_size]
        
        # Add force flag to each place data
        batch_with_force = [
            {**place_data, "force": force}
            for place_data in batch
        ]
        
        # Create parallel tasks for this batch
        batch_tasks = [
            context.call_activity("cosmos_sync_single_place", place_data)
            for place_data in batch_with_force
        ]
        
        # Execute batch in parallel and wait for all to complete
        batch_results = yield context.task_all(batch_tasks)
        
        # Process batch results
        for place_result in batch_results:
            if place_result.get("success", False):
                if place_result.get("skipped", False):
                    # Place was skipped (no changes since last sync)
                    results["placesSkipped"] += 1
                    results["skippedPlaces"].append({
                        "placeId": place_result.get("placeId"),
                        "placeName": place_result.get("placeName"),
                        "reason": place_result.get("skipReason"),
                    })
                else:
                    # Place was synced
                    results["placesProcessed"] += 1
                    results["totalChunksProcessed"] += place_result.get("chunksProcessed", 0)
                    results["totalChunksSkipped"] += place_result.get("chunksSkipped", 0)
                    results["placeDetails"].append(place_result)
            else:
                # Fail fast on any error
                results["success"] = False
                results["error"] = place_result.get("error")
                results["failedAt"] = place_result.get("placeId")
                return results
    
    return results


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("cosmos_get_all_places")
def cosmos_get_all_places(activityInput: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Activity function to fetch all places from Airtable.
    
    Returns a list of place data dicts containing place_id and airtable_record.
    """
    limit = activityInput.get("limit") if activityInput else None
    
    logger.info("Fetching all places from Airtable")
    airtable_service = _get_airtable_service()
    all_places = airtable_service.all_third_places
    
    logger.info(f"Retrieved {len(all_places)} places from Airtable")
    
    if limit:
        all_places = all_places[:limit]
        logger.info(f"Limited to {limit} places for sync")
    
    # Transform to list of place data for activity processing
    place_data_list = []
    for record in all_places:
        place_id = record.get("fields", {}).get("Google Maps Place Id")
        if place_id:
            place_data_list.append({
                "place_id": place_id,
                "airtable_record": record,
            })
        else:
            logger.warning(f"Skipping record without Google Maps Place Id: {record.get('id')}")
    
    return place_data_list


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("cosmos_sync_single_place")
def cosmos_sync_single_place(activityInput: Dict[str, Any]) -> Dict[str, Any]:
    """
    Activity function to sync a single place to Cosmos DB.
    
    This wraps the core sync logic and adds success/error handling
    appropriate for the Durable Functions pattern.
    
    Input params:
        place_id: Google Maps Place Id.
        airtable_record: Airtable record dict.
        force: If True, sync regardless of timestamps. If False, skip if no changes.
    """
    place_id = activityInput.get("place_id")
    airtable_record = activityInput.get("airtable_record")
    force = activityInput.get("force", True)  # Default to True for backward compatibility
    
    if not place_id or not airtable_record:
        return {
            "success": False,
            "placeId": place_id,
            "error": "Missing place_id or airtable_record",
        }
    
    try:
        # Initialize services (each activity gets fresh instances)
        cosmos_service = CosmosService()
        embedding_service = EmbeddingService()
        
        # Call the core sync logic with force flag
        result = _sync_single_place_logic(
            place_id=place_id,
            airtable_record=airtable_record,
            cosmos_service=cosmos_service,
            embedding_service=embedding_service,
            force=force,
        )
        
        result["success"] = True
        return result
        
    except Exception as e:
        error_msg = f"Error syncing place {place_id}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "success": False,
            "placeId": place_id,
            "error": error_msg,
        }


# =============================================================================
# Single Place Sync (Regular HTTP Function - fast enough without Durable)
# =============================================================================

@bp.function_name("CosmosSyncPlace")
@bp.route(route="cosmos/sync-place/{place_id}", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def cosmos_sync_place(req: func.HttpRequest) -> func.HttpResponse:
    """
    Sync a single place to Cosmos DB with embeddings.
    
    Fetches the place from Airtable by Google Maps Place Id, retrieves JSON
    data from GitHub, generates embeddings, and upserts to Cosmos DB.
    
    Path params:
        place_id: Google Maps Place Id to sync.
        
    Returns:
        JSON response with sync results or error details.
    """
    place_id = req.route_params.get("place_id")

    if not place_id:
        return func.HttpResponse(
            body=json.dumps({
                "success": False,
                "error": "place_id is required in the URL path",
            }, indent=2),
            status_code=400,
            mimetype="application/json"
        )

    logger.info(f"Starting single place sync for: {place_id}")

    try:
        # Initialize services
        cosmos_service = CosmosService()
        embedding_service = EmbeddingService()
        airtable_service = _get_airtable_service()

        # Find the place in Airtable by Google Maps Place Id
        airtable_record = None
        for record in airtable_service.all_third_places:
            if record.get("fields", {}).get("Google Maps Place Id") == place_id:
                airtable_record = record
                break

        if not airtable_record:
            return func.HttpResponse(
                body=json.dumps({
                    "success": False,
                    "error": f"Place not found in Airtable: {place_id}",
                    "failedAt": place_id,
                }, indent=2),
                status_code=404,
                mimetype="application/json"
            )

        # Sync the place
        place_result = _sync_single_place_logic(
            place_id=place_id,
            airtable_record=airtable_record,
            cosmos_service=cosmos_service,
            embedding_service=embedding_service,
        )

        result = {
            "success": True,
            "placesProcessed": 1,
            "totalChunksProcessed": place_result["chunksProcessed"],
            "totalChunksSkipped": place_result["chunksSkipped"],
            "placeDetails": [place_result],
            "error": None,
            "failedAt": None,
        }

        logger.info(
            f"Single place sync complete: {place_result['placeName']} - "
            f"{place_result['chunksProcessed']} chunks processed"
        )

        return func.HttpResponse(
            body=json.dumps(result, indent=2),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        error_msg = f"Error syncing place {place_id}: {str(e)}"
        logger.error(error_msg, exc_info=True)

        return func.HttpResponse(
            body=json.dumps({
                "success": False,
                "error": error_msg,
                "failedAt": place_id,
                "placesProcessed": 0,
                "totalChunksProcessed": 0,
            }, indent=2),
            status_code=500,
            mimetype="application/json"
        )


# =============================================================================
# Health Check / Sync Status Report
# =============================================================================

def _get_github_json_file_count(city: str = "charlotte") -> Dict[str, Any]:
    """
    Get the count of JSON files in the GitHub repository for a given city.
    
    Args:
        city: City folder name (default: "charlotte")
        
    Returns:
        Dict with count, file list, and any errors.
    """
    import os
    import requests
    
    try:
        github_token = os.environ.get('GITHUB_PERSONAL_ACCESS_TOKEN')
        if not github_token:
            return {"count": None, "files": [], "error": "GitHub token not configured"}
        
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        
        repo_name = "segunak/third-places-data"
        branch = "master"
        path = f"data/places/{city}"
        
        # Use GitHub API to list directory contents
        url = f"https://api.github.com/repos/{repo_name}/contents/{path}?ref={branch}"
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code != 200:
            return {"count": None, "files": [], "error": f"GitHub API returned {response.status_code}"}
        
        contents = response.json()
        
        # Get only .json files and extract place IDs (filename without extension)
        json_files = [f for f in contents if f.get("name", "").endswith(".json")]
        file_info = [
            {
                "filename": f.get("name"),
                "placeId": f.get("name", "").replace(".json", ""),
                "downloadUrl": f.get("download_url")
            }
            for f in json_files
        ]
        
        return {
            "count": len(json_files),
            "files": file_info,
            "path": f"data/places/{city}",
            "error": None
        }
        
    except Exception as e:
        logger.error(f"Error getting GitHub file count: {e}", exc_info=True)
        return {"count": None, "files": [], "error": str(e)}


def _get_place_name_from_github_file(download_url: str) -> str:
    """
    Fetch a JSON file from GitHub and extract the place_name field.
    
    Args:
        download_url: Direct download URL for the raw JSON file
        
    Returns:
        The place_name value or "Unknown" if not found.
    """
    import requests
    
    try:
        response = requests.get(download_url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data.get("place_name", "Unknown")
    except Exception as e:
        logger.warning(f"Could not fetch place name from {download_url}: {e}")
    
    return "Unknown"


def _get_orphaned_json_files(github_files: list, airtable_place_ids: set) -> list:
    """
    Find JSON files in GitHub that don't have corresponding Airtable records.
    
    Args:
        github_files: List of file info dicts from _get_github_json_file_count
        airtable_place_ids: Set of Place IDs from Airtable
        
    Returns:
        List of orphaned file details with place names.
    """
    orphaned = []
    
    for file_info in github_files:
        place_id = file_info.get("placeId")
        if place_id and place_id not in airtable_place_ids:
            # Fetch the place name from the JSON file
            place_name = _get_place_name_from_github_file(file_info.get("downloadUrl", ""))
            orphaned.append({
                "filename": file_info.get("filename"),
                "placeId": place_id,
                "placeName": place_name
            })
    
    return orphaned


def _get_status_description(status: str) -> str:
    """Get a human-readable description for each status value."""
    descriptions = {
        "healthy": "All systems are in sync. No action required.",
        "healthy_with_warnings": "Systems are operational but have data inconsistencies that should be addressed. Check the 'discrepancies' section for details.",
        "degraded": "One or more data sources could not be reached. Check the 'errors' section for details."
    }
    return descriptions.get(status, "Unknown status")


@bp.function_name("CosmosHealthCheck")
@bp.route(route="cosmos/health", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def cosmos_health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Health check endpoint that returns sync status and statistics.
    
    Provides a comprehensive view of the data ecosystem:
    - Cosmos DB document counts and sync timestamps
    - Airtable record count (Production view)
    - GitHub JSON file count
    - Discrepancy indicators for monitoring
    
    Query params:
        city: City folder for GitHub files (default: "charlotte")
        
    Returns:
        JSON report with counts, timestamps, and health indicators.
    """
    from datetime import datetime, timezone
    
    city = req.params.get("city", "charlotte")
    
    logger.info(f"Running health check for city: {city}")
    
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "city": city,
        "status": "healthy",
        "sources": {},
        "cosmos": {},
        "discrepancies": [],
        "errors": [],
    }
    
    # 1. Get Cosmos DB stats
    try:
        cosmos_service = CosmosService()
        cosmos_stats = cosmos_service.get_sync_stats()
        report["cosmos"] = cosmos_stats
    except Exception as e:
        error_msg = f"Failed to get Cosmos DB stats: {str(e)}"
        logger.error(error_msg, exc_info=True)
        report["errors"].append({"source": "cosmos", "error": error_msg})
        report["status"] = "degraded"
    
    # 2. Get Airtable record count
    airtable_place_ids = set()  # Track Place IDs for orphan detection
    try:
        airtable_service = _get_airtable_service()
        airtable_records = airtable_service.all_third_places
        airtable_count = len(airtable_records) if airtable_records else 0
        
        # Get additional Airtable metadata
        operational_count = sum(
            1 for r in airtable_records 
            if r.get("fields", {}).get("Operational") == "Yes"
        ) if airtable_records else 0
        
        has_data_file_count = sum(
            1 for r in airtable_records 
            if r.get("fields", {}).get("Has Data File") == "Yes"
        ) if airtable_records else 0
        
        # Extract Place IDs for orphan detection
        airtable_place_ids = {
            r.get("fields", {}).get("Google Maps Place Id")
            for r in airtable_records
            if r.get("fields", {}).get("Google Maps Place Id")
        } if airtable_records else set()
        
        report["sources"]["airtable"] = {
            "totalRecords": airtable_count,
            "operationalRecords": operational_count,
            "recordsWithDataFile": has_data_file_count,
            "view": "Production",
            "error": None
        }
    except Exception as e:
        error_msg = f"Failed to get Airtable stats: {str(e)}"
        logger.error(error_msg, exc_info=True)
        report["errors"].append({"source": "airtable", "error": error_msg})
        report["sources"]["airtable"] = {"totalRecords": None, "error": error_msg}
        report["status"] = "degraded"
    
    # 3. Get GitHub JSON file count
    github_stats = _get_github_json_file_count(city)
    github_files = github_stats.get("files", [])
    
    # Don't include the full file list in the response (too verbose)
    report["sources"]["github"] = {
        "count": github_stats.get("count"),
        "path": github_stats.get("path"),
        "error": github_stats.get("error")
    }
    
    if github_stats.get("error"):
        report["errors"].append({"source": "github", "error": github_stats["error"]})
        report["status"] = "degraded"
    
    # 4. Find orphaned JSON files (in GitHub but not in Airtable)
    orphaned_files = []
    if github_files and airtable_place_ids:
        logger.info(f"Checking for orphaned files: {len(github_files)} GitHub files vs {len(airtable_place_ids)} Airtable Place IDs")
        orphaned_files = _get_orphaned_json_files(github_files, airtable_place_ids)
        
        if orphaned_files:
            report["orphanedFiles"] = {
                "description": "JSON files in GitHub that have no corresponding place in Airtable. These are typically from places that were deleted from the website. This is informational only and not necessarily an issue.",
                "count": len(orphaned_files),
                "files": orphaned_files
            }
            logger.info(f"Found {len(orphaned_files)} orphaned JSON files")
    
    # 5. Calculate discrepancies
    cosmos_places_count = report.get("cosmos", {}).get("places", {}).get("count")
    airtable_count = report.get("sources", {}).get("airtable", {}).get("totalRecords")
    github_count = report.get("sources", {}).get("github", {}).get("count")
    
    if cosmos_places_count is not None and airtable_count is not None:
        diff = airtable_count - cosmos_places_count
        if diff != 0:
            report["discrepancies"].append({
                "type": "cosmos_vs_airtable",
                "description": f"Cosmos has {cosmos_places_count} places, Airtable has {airtable_count} records",
                "difference": diff,
                "action": "Run cosmos/sync-places to sync missing places" if diff > 0 else "Check for deleted Airtable records"
            })
    
    if airtable_count is not None and github_count is not None:
        has_data_file_count = report.get("sources", {}).get("airtable", {}).get("recordsWithDataFile", 0)
        diff = github_count - has_data_file_count
        if abs(diff) > 5:  # Allow small variance
            report["discrepancies"].append({
                "type": "github_vs_airtable_datafile",
                "description": f"GitHub has {github_count} JSON files, Airtable shows {has_data_file_count} with 'Has Data File'",
                "difference": diff,
                "action": "Update Airtable 'Has Data File' flags or check for orphaned JSON files"
            })
    
    # Check for places without embeddings
    places_without_embeddings = report.get("cosmos", {}).get("places", {}).get("withoutEmbeddings", 0)
    if places_without_embeddings and places_without_embeddings > 0:
        report["discrepancies"].append({
            "type": "missing_place_embeddings",
            "description": f"{places_without_embeddings} places in Cosmos DB are missing embeddings",
            "count": places_without_embeddings,
            "action": "Run cosmos/sync-places?force=true to regenerate embeddings"
        })
    
    chunks_without_embeddings = report.get("cosmos", {}).get("chunks", {}).get("withoutEmbeddings", 0)
    if chunks_without_embeddings and chunks_without_embeddings > 0:
        report["discrepancies"].append({
            "type": "missing_chunk_embeddings",
            "description": f"{chunks_without_embeddings} chunks in Cosmos DB are missing embeddings",
            "count": chunks_without_embeddings,
            "action": "Run cosmos/sync-places?force=true to regenerate embeddings"
        })
    
    # Set final status
    if report["errors"]:
        report["status"] = "degraded"
    elif report["discrepancies"]:
        report["status"] = "healthy_with_warnings"
    else:
        report["status"] = "healthy"
    
    # Summary for quick reference with clear descriptions
    report["summary"] = {
        "status": {
            "value": report["status"],
            "description": _get_status_description(report["status"])
        },
        "cosmosPlaces": {
            "value": cosmos_places_count,
            "description": "Number of place documents in Cosmos DB 'places' container. Each place synced from Airtable becomes one document."
        },
        "cosmosChunks": {
            "value": report.get("cosmos", {}).get("chunks", {}).get("count"),
            "description": "Number of text chunks in Cosmos DB 'chunks' container. Each place is split into ~140 searchable chunks for RAG/vector search."
        },
        "airtableRecords": {
            "value": airtable_count,
            "description": "Number of records in Airtable's 'Production' view. This is the source of truth for all places."
        },
        "githubFiles": {
            "value": github_count,
            "description": f"Number of JSON files in GitHub 'data/places/{city}/' folder. These contain enriched data (reviews, photos, metadata) from external APIs."
        },
        "errorCount": {
            "value": len(report["errors"]),
            "description": "Number of errors encountered while gathering stats. Errors indicate a data source was unreachable."
        },
        "discrepancyCount": {
            "value": len(report["discrepancies"]),
            "description": "Number of data inconsistencies detected. See 'discrepancies' array for details and recommended actions."
        },
        "orphanedFileCount": {
            "value": len(orphaned_files),
            "description": "Number of JSON files in GitHub with no corresponding Airtable record. These are from deleted places and are informational only."
        },
    }
    
    # Add explanation section
    report["explanations"] = {
        "dataFlow": "Airtable (source of truth) → Cosmos DB (sync target for app). GitHub JSON files provide supplementary data during sync.",
        "expectedState": "Cosmos places count should match Airtable records. GitHub files may have extras (orphaned files from deleted places).",
        "orphanedFiles": "JSON files in GitHub that don't have a matching Place ID in Airtable. This happens when a place is deleted from Airtable but the JSON file remains. These files are harmless - they're simply not used during sync.",
        "discrepancies": [
            {
                "type": "cosmos_vs_airtable",
                "meaning": "Cosmos has fewer places than Airtable, meaning some places haven't been synced yet.",
                "fix": "Run the sync-places endpoint to sync missing places to Cosmos DB."
            },
            {
                "type": "github_vs_airtable_datafile", 
                "meaning": "Mismatch between GitHub JSON files and Airtable's 'Has Data File' field. Could be orphaned files or outdated flags.",
                "fix": "Check for JSON files in GitHub that correspond to deleted Airtable records, or update Airtable flags."
            },
            {
                "type": "missing_place_embeddings",
                "meaning": "Some place documents in Cosmos don't have vector embeddings, which breaks similarity search.",
                "fix": "Re-sync with force=true to regenerate embeddings for all places."
            },
            {
                "type": "missing_chunk_embeddings",
                "meaning": "Some chunk documents in Cosmos don't have vector embeddings, which breaks RAG search.",
                "fix": "Re-sync with force=true to regenerate embeddings for all chunks."
            }
        ]
    }
    
    logger.info(f"Health check complete: status={report['status']}, discrepancies={len(report['discrepancies'])}, orphaned_files={len(orphaned_files)}")
    
    return func.HttpResponse(
        body=json.dumps(report, indent=2),
        status_code=200,
        mimetype="application/json"
    )
