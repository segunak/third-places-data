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
)

from services.embedding_service import (
    EmbeddingService,
    compose_place_embedding_text,
    compose_chunk_embedding_text,
)

from services.utils import fetch_data_github

# Configure logging
logger = logging.getLogger(__name__)

COSMOS_SYNC_BATCH_SIZE = 100

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
    city: str = "charlotte"
) -> Dict[str, Any]:
    """
    Sync a single place and its reviews to Cosmos DB.
    
    Args:
        place_id: Google Maps Place ID.
        airtable_record: Airtable record for this place.
        cosmos_service: Cosmos DB service instance.
        embedding_service: Embedding service instance.
        city: City folder name for JSON files.
        
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
        
    Returns:
        HTTP 202 with status check URLs.
    """
    logger.info("Starting Cosmos DB bulk sync orchestration")
    
    # Parse optional limit parameter
    limit_param = req.params.get("limit")
    limit = int(limit_param) if limit_param else None
    
    # Configuration to pass to orchestrator
    config = {
        "limit": limit,
    }
    
    # Start the orchestration
    instance_id = await client.start_new("cosmos_sync_places_orchestrator", client_input=config)
    logger.info(f"Started orchestration with ID: {instance_id}")
    
    # Return status check response
    response = client.create_check_status_response(req, instance_id)
    return response


@bp.orchestration_trigger(context_name="context")
def cosmos_sync_places_orchestrator(context: df.DurableOrchestrationContext):
    """
    Orchestrator function for bulk Cosmos DB sync.
    
    Fetches all places from Airtable, then processes them in parallel batches.
    Uses fan-out/fan-in pattern for efficient parallel processing.
    """
    config = context.get_input()
    limit = config.get("limit") if config else None
    
    # Get all places from Airtable (activity function)
    all_places = yield context.call_activity("cosmos_get_all_places", {"limit": limit})
    
    if not all_places:
        return {
            "success": True,
            "placesProcessed": 0,
            "totalChunksProcessed": 0,
            "totalChunksSkipped": 0,
            "message": "No places to sync",
        }
    
    # Track overall results
    results = {
        "success": True,
        "placesProcessed": 0,
        "totalChunksProcessed": 0,
        "totalChunksSkipped": 0,
        "placeDetails": [],
        "error": None,
        "failedAt": None,
    }
    
    # Process in batches for parallel execution
    batch_size = COSMOS_SYNC_BATCH_SIZE
    
    for i in range(0, len(all_places), batch_size):
        batch = all_places[i:i + batch_size]
        
        # Create parallel tasks for this batch
        batch_tasks = [
            context.call_activity("cosmos_sync_single_place", place_data)
            for place_data in batch
        ]
        
        # Execute batch in parallel and wait for all to complete
        batch_results = yield context.task_all(batch_tasks)
        
        # Process batch results
        for place_result in batch_results:
            if place_result.get("success", False):
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
    """
    place_id = activityInput.get("place_id")
    airtable_record = activityInput.get("airtable_record")
    
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
        
        # Call the core sync logic
        result = _sync_single_place_logic(
            place_id=place_id,
            airtable_record=airtable_record,
            cosmos_service=cosmos_service,
            embedding_service=embedding_service,
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
    
    Fetches the place from Airtable by Google Maps Place ID, retrieves JSON
    data from GitHub, generates embeddings, and upserts to Cosmos DB.
    
    Path params:
        place_id: Google Maps Place ID to sync.
        
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

        # Find the place in Airtable by Google Maps Place ID
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
