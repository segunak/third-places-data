"""
Pytest configuration and shared fixtures for the Azure Function test suite.

This module provides:
- Mock environment variables for all tests
- Fixture loading utilities
- Common mock objects for external APIs (Airtable, Google Maps, Outscraper)
"""

import os
import json
import pytest
from pathlib import Path
from unittest import mock
from typing import Dict, Any


# =============================================================================
# Constants
# =============================================================================

FIXTURES_DIR = Path(__file__).parent / "fixtures"

TEST_PLACE_NAME = "Mattie Ruth's Coffee House"
TEST_PLACE_ID = "ChIJH9S7TOcPVIgRnG5eHqW4DE0"


# =============================================================================
# Environment Variable Fixtures
# =============================================================================

@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """
    Automatically mock all required environment variables for every test.
    This prevents tests from requiring real API keys or credentials.
    """
    env_vars = {
        "GOOGLE_MAPS_API_KEY": "test-google-maps-api-key",
        "OUTSCRAPER_API_KEY": "test-outscraper-api-key",
        "AIRTABLE_BASE_ID": "appTestBaseId123",
        "AIRTABLE_PERSONAL_ACCESS_TOKEN": "patTestToken123.abc123xyz",
        "AIRTABLE_WORKSPACE_ID": "wspTestWorkspaceId",
        "AIRTABLE_TABLE_NAME": "Charlotte Third Places",
        "FOUNDRY_API_KEY": "test-foundry-api-key",
        "COSMOS_DB_CONNECTION_STRING": "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=dGVzdC1rZXk=",
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return env_vars


# =============================================================================
# Fixture Loading Utilities
# =============================================================================

def load_fixture(filename: str) -> Dict[str, Any]:
    """
    Load a JSON fixture file from the fixtures directory.
    
    Args:
        filename: Name of the fixture file (e.g., 'google_maps_place_details.json')
        
    Returns:
        Parsed JSON data as a dictionary or list
        
    Raises:
        FileNotFoundError: If the fixture file doesn't exist
    """
    fixture_path = FIXTURES_DIR / filename
    if not fixture_path.exists():
        raise FileNotFoundError(f"Fixture file not found: {fixture_path}")
    with open(fixture_path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def google_maps_place_details():
    """Load the Google Maps place details fixture."""
    return load_fixture("google_maps_place_details.json")


@pytest.fixture
def google_maps_place_photos():
    """Load the Google Maps place photos fixture."""
    return load_fixture("google_maps_place_photos.json")


@pytest.fixture
def google_maps_photo_media():
    """Load the Google Maps photo media fixture."""
    return load_fixture("google_maps_photo_media.json")


@pytest.fixture
def google_maps_find_place():
    """Load the Google Maps find place fixture."""
    return load_fixture("google_maps_find_place.json")


@pytest.fixture
def google_maps_business_status():
    """Load the Google Maps business status fixture."""
    return load_fixture("google_maps_business_status.json")


@pytest.fixture
def google_maps_validate_place_id():
    """Load the Google Maps validate place ID fixture."""
    return load_fixture("google_maps_validate_place_id.json")


@pytest.fixture
def outscraper_place_details():
    """Load the Outscraper place details fixture."""
    return load_fixture("outscraper_place_details.json")


@pytest.fixture
def outscraper_reviews():
    """Load the Outscraper reviews fixture."""
    return load_fixture("outscraper_reviews.json")


@pytest.fixture
def outscraper_photos():
    """Load the Outscraper photos fixture."""
    return load_fixture("outscraper_photos.json")


@pytest.fixture
def outscraper_balance_sufficient():
    """Load the Outscraper sufficient balance fixture."""
    return load_fixture("outscraper_balance_sufficient.json")


@pytest.fixture
def outscraper_balance_low():
    """Load the Outscraper low balance fixture."""
    return load_fixture("outscraper_balance_low.json")


@pytest.fixture
def airtable_records():
    """Load the Airtable records fixture."""
    return load_fixture("airtable_records.json")


# =============================================================================
# Mock Response Builders
# =============================================================================

class MockResponse:
    """A mock HTTP response object for use with the responses library or manual mocking."""
    
    def __init__(self, json_data: Any, status_code: int = 200):
        self._json_data = json_data
        self.status_code = status_code
        self.text = json.dumps(json_data) if json_data else ""
    
    def json(self):
        return self._json_data
    
    def raise_for_status(self):
        if self.status_code >= 400:
            from requests.exceptions import HTTPError
            raise HTTPError(f"HTTP Error: {self.status_code}")


def create_mock_response(json_data: Any, status_code: int = 200) -> MockResponse:
    """Create a mock response object for testing HTTP calls."""
    return MockResponse(json_data, status_code)


# =============================================================================
# Provider Mock Fixtures
# =============================================================================

@pytest.fixture
def mock_google_maps_requests(
    google_maps_place_details,
    google_maps_place_photos,
    google_maps_photo_media,
    google_maps_find_place,
    google_maps_business_status,
    google_maps_validate_place_id
):
    """
    Create a mock for requests.get/post that returns appropriate fixtures
    based on the URL being called.
    """
    def mock_request(method):
        def inner(url, **kwargs):
            # Photo media request (e.g., places/{id}/photos/{ref}/media)
            if "/photos/" in url and "/media" in url:
                return create_mock_response(google_maps_photo_media)
            
            # Place details or photo list (places/{id} with field mask)
            if "places.googleapis.com/v1/places/" in url and "searchText" not in url:
                headers = kwargs.get("headers", {})
                field_mask = headers.get("X-Goog-FieldMask", "")
                
                # Validate place ID request (just 'id' field)
                if field_mask == "id":
                    return create_mock_response(google_maps_validate_place_id)
                
                # Business status request
                if "businessStatus" in field_mask and "displayName" not in field_mask:
                    return create_mock_response(google_maps_business_status)
                
                # Photos list request
                if field_mask == "photos":
                    return create_mock_response(google_maps_place_photos)
                
                # Full place details request (default for complex field masks)
                return create_mock_response(google_maps_place_details)
            
            # Find Place (searchText)
            if "places.googleapis.com/v1/places:searchText" in url:
                return create_mock_response(google_maps_find_place)
            
            # Default - not found
            return create_mock_response({}, 404)
        return inner
    
    return {
        "get": mock_request("GET"),
        "post": mock_request("POST")
    }


@pytest.fixture
def mock_outscraper_client(outscraper_place_details, outscraper_reviews, outscraper_photos):
    """
    Create a mock Outscraper ApiClient with predefined responses.
    """
    mock_client = mock.MagicMock()
    mock_client.google_maps_search.return_value = outscraper_place_details
    mock_client.google_maps_reviews.return_value = outscraper_reviews
    mock_client.google_maps_photos.return_value = outscraper_photos
    return mock_client


@pytest.fixture
def mock_airtable_table(airtable_records):
    """
    Create a mock pyairtable.Table with predefined responses.
    """
    mock_table = mock.MagicMock()
    mock_table.all.return_value = airtable_records["records"]
    
    def mock_get(record_id):
        for record in airtable_records["records"]:
            if record["id"] == record_id:
                return record
        return None
    
    mock_table.get.side_effect = mock_get
    mock_table.update.return_value = {"id": "recABC123", "fields": {}}
    
    return mock_table


# =============================================================================
# Cosmos DB Mock Fixtures
# =============================================================================

@pytest.fixture
def mock_cosmos_container():
    """Create a mock Cosmos DB container client."""
    container = mock.MagicMock()
    return container


@pytest.fixture  
def mock_cosmos_client(mock_cosmos_container):
    """Create a mock Cosmos DB client with database and container structure."""
    mock_client = mock.MagicMock()
    mock_database = mock.MagicMock()
    mock_places_container = mock.MagicMock()
    mock_chunks_container = mock.MagicMock()
    
    mock_client.get_database_client.return_value = mock_database
    mock_database.get_container_client.side_effect = lambda name: (
        mock_places_container if name == "places" else mock_chunks_container
    )
    
    return {
        "client": mock_client,
        "database": mock_database,
        "places_container": mock_places_container,
        "chunks_container": mock_chunks_container
    }


@pytest.fixture
def sample_place_doc():
    """Sample place document for Cosmos DB tests."""
    return {
        "id": "ChIJH9S7TOcPVIgRnG5eHqW4DE0",
        "place": "Amelie's French Bakery",
        "neighborhood": "NoDa",
        "address": "2424 N Davidson St, Charlotte, NC 28205",
        "type": ["Bakery", "Cafe"],
        "tags": ["cozy", "pastries", "coffee"],
        "description": "French bakery with pastries and coffee",
        "lastSynced": "2024-12-03T10:00:00Z",
        "embedding": [0.1] * 1536
    }


@pytest.fixture
def sample_chunk_doc():
    """Sample chunk document for Cosmos DB tests."""
    return {
        "id": "ChIJH9S7TOcPVIgRnG5eHqW4DE0_review_1",
        "placeId": "ChIJH9S7TOcPVIgRnG5eHqW4DE0",
        "chunkType": "review",
        "reviewText": "Great pastries and coffee!",
        "reviewRating": 5,
        "embedding": [0.2] * 1536
    }


# =============================================================================
# Embedding Service Mock Fixtures
# =============================================================================

@pytest.fixture
def mock_openai_client():
    """Create a mock Azure OpenAI client for embedding generation."""
    mock_client = mock.MagicMock()
    
    # Create mock embedding response
    def create_embedding_response(texts):
        mock_response = mock.MagicMock()
        mock_data = []
        for i, text in enumerate(texts):
            mock_embedding = mock.MagicMock()
            # Generate deterministic fake embedding based on text length
            mock_embedding.embedding = [0.1 * (i + 1)] * 1536
            mock_data.append(mock_embedding)
        mock_response.data = mock_data
        return mock_response
    
    mock_client.embeddings.create.side_effect = lambda input, **kwargs: create_embedding_response(input)
    
    return mock_client
