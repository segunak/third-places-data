"""
Embedding service for generating vector embeddings using Microsoft Foundry.
Uses text-embedding-3-small model for 1536-dimensional embeddings.
"""

import os
import logging
from typing import List, Dict, Any, Optional
from openai import AzureOpenAI

# Configure logging
logger = logging.getLogger(__name__)


class EmbeddingService:
    """Service for generating embeddings via Microsoft Foundry."""

    def __init__(self):
        """Initialize the embedding service with Microsoft Foundry credentials."""
        self.api_key = os.environ.get("FOUNDRY_API_KEY")
        if not self.api_key:
            raise ValueError("FOUNDRY_API_KEY environment variable is required")

        self.endpoint = "https://foundry-third-places.services.ai.azure.com/"
        self.model = "text-embedding-3-small"
        self.dimensions = 1536
        # Max texts per API call. Batching reduces API calls and latency.
        # 16 is a safe default that works reliably across Azure OpenAI deployments.
        self.max_batch_size = 16

        # Initialize Azure OpenAI client
        # Microsoft Foundry uses the Azure OpenAI API surface for model inference
        self.client = AzureOpenAI(
            api_key=self.api_key,
            api_version="2024-10-21",  # See: https://learn.microsoft.com/en-us/azure/ai-foundry/openai/api-version-lifecycle
            azure_endpoint=self.endpoint
        )

        logger.info(f"EmbeddingService initialized with endpoint: {self.endpoint}")

    def get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a list of texts.
        
        Args:
            texts: List of strings to embed. Max 16 texts per batch.
            
        Returns:
            List of embedding vectors (each 1536 floats).
            
        Raises:
            ValueError: If texts list is empty or exceeds batch size.
            Exception: If API call fails.
        """
        if not texts:
            raise ValueError("texts list cannot be empty")

        if len(texts) > self.max_batch_size:
            raise ValueError(f"texts list exceeds max batch size of {self.max_batch_size}")

        # Filter out empty strings and track their positions
        valid_texts = []
        valid_indices = []
        for i, text in enumerate(texts):
            if text and text.strip():
                valid_texts.append(text.strip())
                valid_indices.append(i)

        if not valid_texts:
            raise ValueError("All texts are empty after filtering")

        logger.info(f"Generating embeddings for {len(valid_texts)} texts")

        response = self.client.embeddings.create(
            input=valid_texts,
            model=self.model,
            dimensions=self.dimensions
        )

        # Extract embeddings from response
        embeddings = [item.embedding for item in response.data]

        logger.info(f"Successfully generated {len(embeddings)} embeddings")
        return embeddings

    def get_embedding(self, text: str) -> List[float]:
        """
        Generate embedding for a single text.
        
        Args:
            text: String to embed.
            
        Returns:
            Embedding vector (1536 floats).
        """
        embeddings = self.get_embeddings([text])
        return embeddings[0]


def format_field_for_embedding(field_name: str, value: Any) -> Optional[str]:
    """
    Format a field value for inclusion in embedding text.
    
    Handles special formatting for different field types:
    - Booleans: "field_name: yes/no"
    - Lists: comma-separated values
    - Dicts (about): flattened key-value pairs
    - Strings: as-is or with label prefix for certain fields
    
    Args:
        field_name: The Cosmos DB field name.
        value: The field value (any type).
        
    Returns:
        Formatted string for embedding, or None if value is empty/None.
    """
    if value is None:
        return None
    
    # Boolean fields - format as "field: yes/no"
    boolean_fields = {"freeWifi", "hasCinnamonRolls", "purchaseRequired"}
    if field_name in boolean_fields:
        if value is True:
            # Convert camelCase to readable label
            labels = {
                "freeWifi": "free wifi",
                "hasCinnamonRolls": "has cinnamon rolls",
                "purchaseRequired": "purchase required"
            }
            return f"{labels.get(field_name, field_name)}: yes"
        elif value is False:
            labels = {
                "freeWifi": "free wifi",
                "hasCinnamonRolls": "has cinnamon rolls",
                "purchaseRequired": "purchase required"
            }
            return f"{labels.get(field_name, field_name)}: no"
        return None
    
    # Labeled string fields - format as "field: value"
    labeled_fields = {"size"}
    if field_name in labeled_fields:
        if value:
            return f"{field_name}: {value}"
        return None
    
    # List fields - join with commas (some with label prefix for context)
    list_fields = {"tags", "type", "reviewsTags", "parking"}
    if field_name in list_fields:
        if isinstance(value, list):
            if value:
                joined = ", ".join(str(v) for v in value)
                # Add label prefix for fields that need context
                if field_name == "parking":
                    return f"parking: {joined}"
                return joined
            return None
        elif value:
            # Single value (not a list)
            if field_name == "parking":
                return f"parking: {value}"
            return str(value)
        return None
    
    # About field - flatten nested dict
    if field_name == "about":
        if isinstance(value, dict):
            about_parts = []
            for category, features in value.items():
                if isinstance(features, dict):
                    for feature, feat_value in features.items():
                        if feat_value is True:
                            about_parts.append(f"{feature}: yes")
                        elif feat_value is False:
                            about_parts.append(f"{feature}: no")
                elif features:
                    about_parts.append(f"{category}: {features}")
            if about_parts:
                return ", ".join(about_parts)
        return None
    
    # Working hours - format as "hours: Monday 7am-3pm, Tuesday 7am-3pm, ..."
    if field_name == "workingHours":
        if isinstance(value, dict) and value:
            hours_parts = []
            for day, hours in value.items():
                if hours:
                    hours_parts.append(f"{day} {hours}")
            if hours_parts:
                return "hours: " + ", ".join(hours_parts)
        return None
    
    # Popular times - summarize peak hours per day
    if field_name == "popularTimes":
        if isinstance(value, list) and value:
            popular_parts = []
            for day_data in value:
                day_text = day_data.get("day_text")
                popular_times_list = day_data.get("popular_times", [])
                
                # Find peak hours (percentage >= 70)
                peak_hours = []
                for pt in popular_times_list:
                    if pt.get("percentage", 0) >= 70:
                        peak_hours.append(pt.get("time", ""))
                
                if day_text and peak_hours:
                    # Dedupe and format peak times
                    unique_peaks = list(dict.fromkeys(peak_hours))
                    popular_parts.append(f"{day_text} busy at {', '.join(unique_peaks)}")
            
            if popular_parts:
                return "busy times: " + "; ".join(popular_parts)
        return None
    
    # Plain string fields - return as-is
    if isinstance(value, str):
        return value if value.strip() else None
    
    # Fallback for any other types
    return str(value) if value else None


def compose_place_embedding_text(place_doc: Dict[str, Any]) -> str:
    """
    Compose the text to embed for a place document.
    
    Uses the embedding field configuration from cosmos_service to determine
    which fields to include. Each field is formatted appropriately by
    format_field_for_embedding().
    
    Args:
        place_doc: Place document dictionary with Cosmos DB field names.
        
    Returns:
        Composed text string for embedding.
    """
    from services.cosmos_service import get_place_embedding_fields
    
    embedding_fields = get_place_embedding_fields()
    parts = []
    
    for field_name in embedding_fields:
        value = place_doc.get(field_name)
        formatted = format_field_for_embedding(field_name, value)
        if formatted:
            parts.append(formatted)
    
    # Join all parts with separator
    embedding_text = " | ".join(parts)
    
    return embedding_text


def compose_chunk_embedding_text(chunk_doc: Dict[str, Any]) -> str:
    """
    Compose the text to embed for a chunk (review) document.
    
    Combines: review text (primary), place name, neighborhood, place type,
    place tags, and owner answer if present.
    
    Args:
        chunk_doc: Chunk document dictionary with Cosmos DB field names.
        
    Returns:
        Composed text string for embedding.
    """
    parts = []

    # Review text is the primary semantic payload
    if chunk_doc.get("reviewText"):
        parts.append(chunk_doc["reviewText"])

    # Denormalized place context for grounding
    if chunk_doc.get("placeName"):
        parts.append(chunk_doc["placeName"])

    if chunk_doc.get("neighborhood"):
        parts.append(chunk_doc["neighborhood"])

    # Place type (can be string or list from Airtable)
    place_type = chunk_doc.get("placeType")
    if place_type:
        if isinstance(place_type, list):
            parts.append(", ".join(place_type))
        else:
            parts.append(str(place_type))

    # Place tags (list to comma-separated)
    place_tags = chunk_doc.get("placeTags")
    if place_tags:
        if isinstance(place_tags, list):
            parts.append(", ".join(place_tags))
        else:
            parts.append(str(place_tags))

    # Owner answer provides additional context
    if chunk_doc.get("ownerAnswer"):
        parts.append(chunk_doc["ownerAnswer"])

    # Join all parts with separator
    embedding_text = " | ".join(filter(None, parts))

    return embedding_text
