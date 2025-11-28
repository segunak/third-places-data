"""
Embedding service for generating vector embeddings using Microsoft Foundry.
Uses text-embedding-3-small model for 1536-dimensional embeddings.
"""

import os
import logging
from typing import List, Dict, Any
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


def compose_place_embedding_text(place_doc: Dict[str, Any]) -> str:
    """
    Compose the text to embed for a place document.
    
    Combines semantic fields: place name, description, neighborhood, address,
    type, tags, category, subtypes, and about (flattened).
    
    Args:
        place_doc: Place document dictionary with Cosmos DB field names.
        
    Returns:
        Composed text string for embedding.
    """
    parts = []

    # Place name
    if place_doc.get("place"):
        parts.append(place_doc["place"])

    # Description
    if place_doc.get("description"):
        parts.append(place_doc["description"])

    # Location context
    if place_doc.get("neighborhood"):
        parts.append(place_doc["neighborhood"])

    if place_doc.get("address"):
        parts.append(place_doc["address"])

    # Type
    if place_doc.get("type"):
        parts.append(place_doc["type"])

    # Tags (list to comma-separated string)
    tags = place_doc.get("tags")
    if tags:
        if isinstance(tags, list):
            parts.append(", ".join(tags))
        else:
            parts.append(str(tags))

    # Category
    if place_doc.get("category"):
        parts.append(place_doc["category"])

    # Subtypes (can be string or list)
    subtypes = place_doc.get("subtypes")
    if subtypes:
        if isinstance(subtypes, list):
            parts.append(", ".join(subtypes))
        else:
            parts.append(str(subtypes))

    # About (flatten nested dict to key-value pairs)
    about = place_doc.get("about")
    if about and isinstance(about, dict):
        about_parts = []
        for category, features in about.items():
            if isinstance(features, dict):
                for feature, value in features.items():
                    if value is True:
                        about_parts.append(f"{feature}: yes")
                    elif value is False:
                        about_parts.append(f"{feature}: no")
            elif features:
                about_parts.append(f"{category}: {features}")
        if about_parts:
            parts.append(", ".join(about_parts))

    # Join all parts with separator
    embedding_text = " | ".join(filter(None, parts))

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

    if chunk_doc.get("placeType"):
        parts.append(chunk_doc["placeType"])

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
