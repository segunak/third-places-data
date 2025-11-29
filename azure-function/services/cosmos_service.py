"""
Cosmos DB service for RAG data storage.
Manages places and chunks containers with vector embeddings.
"""

import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosResourceNotFoundError

# Configure logging
logger = logging.getLogger(__name__)

# Database and container configuration
DATABASE_NAME = "third-places"
PLACES_CONTAINER = "places"
CHUNKS_CONTAINER = "chunks"


class CosmosService:
    """Service for Cosmos DB operations on places and chunks containers."""

    def __init__(self):
        """Initialize the Cosmos DB service."""
        connection_string = os.environ.get("COSMOS_DB_CONNECTION_STRING")
        if not connection_string:
            raise ValueError("COSMOS_DB_CONNECTION_STRING environment variable is required")

        self.client = CosmosClient.from_connection_string(connection_string)
        self.database = self.client.get_database_client(DATABASE_NAME)
        self.places_container = self.database.get_container_client(PLACES_CONTAINER)
        self.chunks_container = self.database.get_container_client(CHUNKS_CONTAINER)

        logger.info(f"CosmosService initialized for database: {DATABASE_NAME}")

    def upsert_place(self, place_doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Upsert a place document to the places container.
        
        Args:
            place_doc: Place document with 'id' field as partition key.
            
        Returns:
            The upserted document.
        """
        if "id" not in place_doc:
            raise ValueError("Place document must have 'id' field")

        result = self.places_container.upsert_item(place_doc)
        logger.info(f"Upserted place: {place_doc['id']}")
        return result

    def upsert_chunk(self, chunk_doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Upsert a chunk document to the chunks container.
        
        Args:
            chunk_doc: Chunk document with 'placeId' as partition key.
            
        Returns:
            The upserted document.
        """
        if "id" not in chunk_doc:
            raise ValueError("Chunk document must have 'id' field")
        if "placeId" not in chunk_doc:
            raise ValueError("Chunk document must have 'placeId' field (partition key)")

        result = self.chunks_container.upsert_item(chunk_doc)
        logger.info(f"Upserted chunk: {chunk_doc['id']} for place: {chunk_doc['placeId']}")
        return result

    def get_place(self, place_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a place document by ID.
        
        Args:
            place_id: The place ID (also partition key).
            
        Returns:
            The place document or None if not found.
        """
        try:
            return self.places_container.read_item(item=place_id, partition_key=place_id)
        except CosmosResourceNotFoundError:
            return None

    def get_all_place_ids(self) -> List[str]:
        """
        Get all place IDs from the places container.
        
        Returns:
            List of place IDs.
        """
        query = "SELECT c.id FROM c"
        items = list(self.places_container.query_items(query=query, enable_cross_partition_query=True))
        return [item["id"] for item in items]

    def delete_chunks_for_place(self, place_id: str) -> int:
        """
        Delete all chunks for a given place.
        
        Args:
            place_id: The place ID (partition key for chunks).
            
        Returns:
            Number of chunks deleted.
        """
        query = "SELECT c.id FROM c WHERE c.placeId = @placeId"
        parameters = [{"name": "@placeId", "value": place_id}]
        chunks = list(self.chunks_container.query_items(
            query=query,
            parameters=parameters,
            partition_key=place_id
        ))

        deleted_count = 0
        for chunk in chunks:
            self.chunks_container.delete_item(item=chunk["id"], partition_key=place_id)
            deleted_count += 1

        if deleted_count > 0:
            logger.info(f"Deleted {deleted_count} chunks for place: {place_id}")

        return deleted_count

    def get_places_count(self) -> int:
        """
        Get the total count of documents in the places container.
        
        Returns:
            Number of place documents.
        """
        query = "SELECT VALUE COUNT(1) FROM c"
        result = list(self.places_container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        return result[0] if result else 0

    def get_chunks_count(self) -> int:
        """
        Get the total count of documents in the chunks container.
        
        Returns:
            Number of chunk documents.
        """
        query = "SELECT VALUE COUNT(1) FROM c"
        result = list(self.chunks_container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        return result[0] if result else 0

    def get_places_with_chunks_count(self) -> int:
        """
        Get the count of unique places that have chunks (reviews).
        
        Returns:
            Number of unique placeIds in the chunks container.
        """
        query = "SELECT VALUE COUNT(1) FROM (SELECT DISTINCT c.placeId FROM c)"
        result = list(self.chunks_container.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        return result[0] if result else 0

    def get_sync_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive sync statistics from Cosmos DB.
        
        Returns:
            Dict with counts, timestamps, and health indicators.
        """
        # Get basic counts
        places_count = self.get_places_count()
        chunks_count = self.get_chunks_count()
        places_with_chunks = self.get_places_with_chunks_count()
        
        # Get latest and oldest sync timestamps from places
        latest_sync_query = "SELECT TOP 1 c.lastSynced, c.id, c.place FROM c ORDER BY c.lastSynced DESC"
        oldest_sync_query = "SELECT TOP 1 c.lastSynced, c.id, c.place FROM c ORDER BY c.lastSynced ASC"
        
        latest_sync_result = list(self.places_container.query_items(
            query=latest_sync_query,
            enable_cross_partition_query=True
        ))
        
        oldest_sync_result = list(self.places_container.query_items(
            query=oldest_sync_query,
            enable_cross_partition_query=True
        ))
        
        latest_sync = latest_sync_result[0] if latest_sync_result else None
        oldest_sync = oldest_sync_result[0] if oldest_sync_result else None
        
        # Get places without embeddings (potential issues)
        places_without_embeddings_query = "SELECT VALUE COUNT(1) FROM c WHERE NOT IS_DEFINED(c.embedding) OR c.embedding = null"
        places_without_embeddings = list(self.places_container.query_items(
            query=places_without_embeddings_query,
            enable_cross_partition_query=True
        ))
        
        # Get chunks without embeddings
        chunks_without_embeddings = list(self.chunks_container.query_items(
            query=places_without_embeddings_query,
            enable_cross_partition_query=True
        ))
        
        # Calculate average chunks per place
        avg_chunks_per_place = round(chunks_count / places_with_chunks, 2) if places_with_chunks > 0 else 0
        
        return {
            "places": {
                "count": places_count,
                "withoutEmbeddings": places_without_embeddings[0] if places_without_embeddings else 0,
            },
            "chunks": {
                "count": chunks_count,
                "uniquePlaces": places_with_chunks,
                "averagePerPlace": avg_chunks_per_place,
                "withoutEmbeddings": chunks_without_embeddings[0] if chunks_without_embeddings else 0,
            },
            "sync": {
                "latestSync": {
                    "timestamp": latest_sync.get("lastSynced") if latest_sync else None,
                    "placeId": latest_sync.get("id") if latest_sync else None,
                    "placeName": latest_sync.get("place") if latest_sync else None,
                },
                "oldestSync": {
                    "timestamp": oldest_sync.get("lastSynced") if oldest_sync else None,
                    "placeId": oldest_sync.get("id") if oldest_sync else None,
                    "placeName": oldest_sync.get("place") if oldest_sync else None,
                },
            },
        }


def transform_airtable_to_place(
    airtable_record: Dict[str, Any],
    json_data: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Transform an Airtable record and optional JSON data into a Cosmos DB place document.
    
    Args:
        airtable_record: Airtable record with 'fields' containing place data.
        json_data: Optional JSON file data with details.raw_data.
        
    Returns:
        Place document ready for Cosmos DB (without embedding - add separately).
    """
    fields = airtable_record.get("fields", {})

    # Start with system fields
    place_doc = {
        "id": fields.get("Google Maps Place Id"),
        "lastSynced": datetime.now(timezone.utc).isoformat(),
    }

    # Map Airtable fields (using camelCase for Cosmos)
    airtable_mappings = {
        "address": "Address",
        "appleMapsProfileUrl": "Apple Maps Profile URL",
        "comments": "Comments",
        "createdTime": "Created Time",
        "curatorPhotos": "Curator Photos",
        "description": "Description",
        "facebook": "Facebook",
        "featured": "Featured",
        "freeWifi": "Free Wi-Fi",
        "googleMapsPlaceId": "Google Maps Place Id",
        "googleMapsProfileUrl": "Google Maps Profile URL",
        "hasCinnamonRolls": "Has Cinnamon Rolls",
        "hasDataFile": "Has Data File",
        "instagram": "Instagram",
        "lastModifiedTime": "Last Modified Time",
        "lastRevalidated": "Last Revalidated",
        "latitude": "Latitude",
        "linkedIn": "LinkedIn",
        "longitude": "Longitude",
        "neighborhood": "Neighborhood",
        "operational": "Operational",
        "parking": "Parking",
        "photos": "Photos",
        "place": "Place",
        "purchaseRequired": "Purchase Required",
        "size": "Size",
        "tags": "Tags",
        "tikTok": "TikTok",
        "twitter": "Twitter",
        "type": "Type",
        "website": "Website",
        "youTube": "YouTube",
    }

    for cosmos_field, airtable_field in airtable_mappings.items():
        value = fields.get(airtable_field)
        if value is not None:
            place_doc[cosmos_field] = value

    # Map JSON fields from details.raw_data
    if json_data:
        raw_data = json_data.get("details", {}).get("raw_data", {})

        json_mappings = {
            "popularTimes": "popular_times",
            "typicalTimeSpent": "typical_time_spent",
            "workingHours": "working_hours",
            "about": "about",
            "category": "category",
            "subtypes": "subtypes",
            "reviewsLink": "reviews_link",
            "streetView": "street_view",
            "phone": "phone",
            "locatedIn": "located_in",
            "reviewsTags": "reviews_tags",
            "placeRating": "rating",
            "reviewsCount": "reviews",
        }

        for cosmos_field, json_field in json_mappings.items():
            value = raw_data.get(json_field)
            if value is not None:
                place_doc[cosmos_field] = value

        # Raw Google Maps API photos data, not always present as it hasn't always been extracted
        photos_data = json_data.get("photos", {}).get("raw_data", {}).get("photos_data", {})
        
        if photos_data is not None:
            place_doc["photosData"] = photos_data

    return place_doc


def transform_review_to_chunk(
    review: Dict[str, Any],
    place_context: Dict[str, Any],
    details_raw_data: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Transform a review object into a Cosmos DB chunk document.
    
    Args:
        review: Review object from reviews.raw_data.reviews_data[].
        place_context: Dict with denormalized place fields (from Airtable).
        details_raw_data: The details.raw_data object for aggregate context.
        
    Returns:
        Chunk document ready for Cosmos DB (without embedding - add separately).
    """
    chunk_doc = {
        # System fields
        "id": review.get("review_id"),
        "placeId": place_context.get("googleMapsPlaceId"),
        "lastSynced": datetime.now(timezone.utc).isoformat(),

        # Denormalized place fields
        "placeName": place_context.get("place"),
        "neighborhood": place_context.get("neighborhood"),
        "address": place_context.get("address"),
        "googleMapsProfileUrl": place_context.get("googleMapsProfileUrl"),
        "appleMapsProfileUrl": place_context.get("appleMapsProfileUrl"),
        "placeType": place_context.get("type"),
        "placeTags": place_context.get("tags"),

        # Review fields
        "reviewText": review.get("review_text"),
        "reviewLink": review.get("review_link"),
        "reviewRating": review.get("review_rating"),
        "reviewDatetimeUtc": review.get("review_datetime_utc"),
        "reviewTimestamp": review.get("review_timestamp"),
        "reviewQuestions": review.get("review_questions"),

        # Owner response
        "hasOwnerResponse": review.get("owner_answer") is not None,
        "ownerAnswer": review.get("owner_answer"),

        # Review media
        "reviewImgUrls": review.get("review_img_urls"),

        # Aggregate context from details.raw_data
        "reviewsTags": details_raw_data.get("reviews_tags"),
        "placeRating": details_raw_data.get("rating"),
        "placeReviewsCount": details_raw_data.get("reviews"),
    }

    return chunk_doc


def extract_place_context(airtable_record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract place context fields from an Airtable record for denormalization into chunks.
    
    Args:
        airtable_record: Airtable record with 'fields'.
        
    Returns:
        Dict with fields needed for chunk denormalization.
    """
    fields = airtable_record.get("fields", {})

    return {
        "googleMapsPlaceId": fields.get("Google Maps Place Id"),
        "place": fields.get("Place"),
        "neighborhood": fields.get("Neighborhood"),
        "address": fields.get("Address"),
        "googleMapsProfileUrl": fields.get("Google Maps Profile URL"),
        "appleMapsProfileUrl": fields.get("Apple Maps Profile URL"),
        "type": fields.get("Type"),
        "tags": fields.get("Tags"),
    }


def parse_timestamp(timestamp_str: Optional[str]) -> Optional[datetime]:
    """
    Parse an ISO timestamp string to a timezone-aware datetime in UTC.
    
    Handles various formats:
    - Airtable: "2025-11-28T12:00:00.000Z"
    - JSON last_updated: "2025-09-22T12:40:00.502075"
    - Cosmos lastSynced: "2025-11-28T12:00:00+00:00"
    
    Args:
        timestamp_str: ISO format timestamp string, or None.
        
    Returns:
        Timezone-aware datetime in UTC, or None if parsing fails.
    """
    if not timestamp_str:
        return None
    
    try:
        # Handle 'Z' suffix (Airtable format)
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1] + '+00:00'
        
        # Parse the timestamp
        dt = datetime.fromisoformat(timestamp_str)
        
        # If naive (no timezone), assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        return dt
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to parse timestamp '{timestamp_str}': {e}")
        return None


def should_sync_place(
    airtable_modified: Optional[str],
    json_last_updated: Optional[str],
    cosmos_last_synced: Optional[str]
) -> Tuple[bool, str]:
    """
    Determine if a place needs to be synced based on timestamp comparison.
    
    Compares the Airtable last modified time and JSON file last_updated time
    against the Cosmos document's lastSynced time. A place needs syncing if
    either source has been modified since the last sync.
    
    Args:
        airtable_modified: Airtable "Last Modified Time" field value.
        json_last_updated: JSON file "last_updated" field value.
        cosmos_last_synced: Cosmos document "lastSynced" field value.
        
    Returns:
        Tuple of (should_sync: bool, reason: str).
        Reasons: "new_place", "airtable_modified", "json_modified", 
                 "missing_timestamps", "no_changes"
    """
    # Parse timestamps to UTC datetime
    airtable_dt = parse_timestamp(airtable_modified)
    json_dt = parse_timestamp(json_last_updated)
    cosmos_dt = parse_timestamp(cosmos_last_synced)
    
    # If no Cosmos record exists, it's a new place - must sync
    if cosmos_dt is None:
        return True, "new_place"
    
    # If we have no source timestamps to compare, sync to be safe
    if airtable_dt is None and json_dt is None:
        return True, "missing_timestamps"
    
    # Check if Airtable record was modified after last sync
    if airtable_dt is not None and airtable_dt > cosmos_dt:
        return True, "airtable_modified"
    
    # Check if JSON file was updated after last sync
    if json_dt is not None and json_dt > cosmos_dt:
        return True, "json_modified"
    
    # No changes detected
    return False, "no_changes"
