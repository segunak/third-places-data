"""
Embedding service for generating vector embeddings using Microsoft Foundry.
Model and dimensions are configured as class constants below.
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


def sanitize_field_value(value: str) -> str:
    """
    Sanitize a field value for embedding text.
    
    - Replaces newlines (\n, \r\n, \r) with single space
    - Collapses multiple consecutive spaces into one
    - Strips leading/trailing whitespace
    
    Args:
        value: The string value to sanitize.
        
    Returns:
        Sanitized string.
    """
    import re
    
    # Replace all newline variants with space
    result = value.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
    
    # Collapse multiple spaces into one
    result = re.sub(r' +', ' ', result)
    
    # Strip leading/trailing whitespace
    return result.strip()


def format_field_for_embedding(field_name: str, value: Any) -> Optional[str]:
    """
    Format a single field value for inclusion in embedding text.
    
    Called once per field during embedding composition. Returns a formatted
    string like "fieldName: value" for the field, or None if the field is
    empty/missing (in which case it's simply skipped in the final output).
    
    Special formatting for complex types:
    - Lists: comma-separated values
    - about dict: flattened nested key-value pairs
    - workingHours dict: "Day HH:MM" format
    - popularTimes list: summarized peak hours per day
    
    All other fields: "fieldName: value" with sanitized string value.
    
    Args:
        field_name: The Cosmos DB field name (used as the label).
        value: The field value (any type).
        
    Returns:
        Formatted string like "fieldName: value", or None if empty/missing.
    """
    if value is None:
        return None
    
    # List fields - join with commas
    if isinstance(value, list):
        # Special case: popularTimes has nested structure
        if field_name == "popularTimes" and value:
            popular_parts = []
            for day_data in value:
                if not isinstance(day_data, dict):
                    continue
                day_text = day_data.get("day_text")
                popular_times_list = day_data.get("popular_times", [])
                
                # Find peak hours (percentage >= 70)
                peak_hours = []
                for pt in popular_times_list:
                    if pt.get("percentage", 0) >= 70:
                        peak_hours.append(pt.get("time", ""))
                
                if day_text and peak_hours:
                    unique_peaks = list(dict.fromkeys(peak_hours))
                    popular_parts.append(f"{day_text} busy at {', '.join(unique_peaks)}")
            
            return f"{field_name}: {'; '.join(popular_parts)}" if popular_parts else None
        
        # Regular list - join non-empty values with commas
        if value:
            joined = ", ".join(sanitize_field_value(str(v)) for v in value if v)
            return f"{field_name}: {joined}" if joined else None
        return None
    
    # Dict fields - flatten to key-value pairs
    if isinstance(value, dict):
        # Special case: about has nested dicts with boolean values
        if field_name == "about":
            about_parts = []
            for category, features in value.items():
                if isinstance(features, dict):
                    for feature, feat_value in features.items():
                        if feat_value is True:
                            about_parts.append(f"{feature}: yes")
                        elif feat_value is False:
                            about_parts.append(f"{feature}: no")
                elif features:
                    sanitized = sanitize_field_value(str(features))
                    if sanitized:
                        about_parts.append(f"{category}: {sanitized}")
            return f"{field_name}: {', '.join(about_parts)}" if about_parts else None
        
        # workingHours - format as "Day HH:MM"
        if field_name == "workingHours" and value:
            hours_parts = [f"{day} {hours}" for day, hours in value.items() if hours]
            return f"{field_name}: {', '.join(hours_parts)}" if hours_parts else None
        
        # reviewQuestions - flatten with context about ratings
        # Example: {"Food": "5", "Service": "4", "Price per person": "$20-30"}
        # Output: "reviewQuestions (ratings on 5-point scale): Food 5, Service 4, Price per person $20-30"
        if field_name == "reviewQuestions" and value:
            # Check if any value is a plain digit 1-5 (indicates rating scale)
            has_numeric_rating = any(
                str(v).strip() in ("1", "2", "3", "4", "5")
                for v in value.values()
            )
            
            question_parts = [f"{k} {v}" for k, v in value.items() if v]
            if not question_parts:
                return None
            
            label = "reviewQuestions (ratings on 5-point scale)" if has_numeric_rating else "reviewQuestions"
            return f"{label}: {', '.join(question_parts)}"
        
        # Generic dict - just stringify (unlikely to hit this)
        return None
    
    # String and other scalar types - sanitize and return with label
    if isinstance(value, str):
        sanitized = sanitize_field_value(value)
        # Use 'placeName' instead of 'place' for clarity in embeddings
        label = "placeName" if field_name == "place" else field_name
        return f"{label}: {sanitized}" if sanitized else None
    
    # Fallback for other types (int, float, bool, etc.)
    sanitized = sanitize_field_value(str(value))
    return f"{field_name}: {sanitized}" if sanitized else None


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
    
    # Join all parts with newline separator for readability
    embedding_text = "\n".join(parts)
    
    return embedding_text


def compose_chunk_embedding_text(chunk_doc: Dict[str, Any]) -> str:
    """
    Compose the text to embed for a chunk (review) document.
    
    Uses format_field_for_embedding() for consistency with place embeddings.
    Fields are ordered context-first to ground the review semantically:
    - Place context: placeName, neighborhood, placeType, placeTags
    - Review metadata: reviewRating, reviewsTags
    - Review content: reviewText
    - Owner response: hasOwnerResponse, ownerAnswer (only if hasOwnerResponse is True)
    - Reviewer ratings: reviewQuestions
    
    Args:
        chunk_doc: Chunk document dictionary with Cosmos DB field names.
        
    Returns:
        Composed text string for embedding with newline separators.
    """
    parts = []

    # Context-first ordering: place context grounds the review semantically
    context_fields = ["placeName", "neighborhood", "placeType", "placeTags"]
    for field_name in context_fields:
        value = chunk_doc.get(field_name)
        formatted = format_field_for_embedding(field_name, value)
        if formatted:
            parts.append(formatted)

    # Review metadata
    if chunk_doc.get("reviewRating") is not None:
        parts.append(f"reviewRating: {chunk_doc['reviewRating']}")

    if chunk_doc.get("reviewsTags"):
        formatted = format_field_for_embedding("reviewsTags", chunk_doc["reviewsTags"])
        if formatted:
            parts.append(formatted)

    # Primary content: the review text
    if chunk_doc.get("reviewText"):
        formatted = format_field_for_embedding("reviewText", chunk_doc["reviewText"])
        if formatted:
            parts.append(formatted)

    # Owner response - only include if hasOwnerResponse is True
    if chunk_doc.get("hasOwnerResponse") is True:
        parts.append("hasOwnerResponse: yes")
        if chunk_doc.get("ownerAnswer"):
            formatted = format_field_for_embedding("ownerAnswer", chunk_doc["ownerAnswer"])
            if formatted:
                parts.append(formatted)

    # Reviewer ratings/questions
    if chunk_doc.get("reviewQuestions"):
        formatted = format_field_for_embedding("reviewQuestions", chunk_doc["reviewQuestions"])
        if formatted:
            parts.append(formatted)

    # Join all parts with newline separator for readability (consistent with places)
    embedding_text = "\n".join(parts)

    return embedding_text
