"""
Unit tests for the CosmosService class from cosmos_service.py.

All tests use mocked Cosmos DB clients - no live database calls are made.
"""

import pytest
from unittest import mock
from azure.cosmos.exceptions import CosmosResourceNotFoundError


# =============================================================================
# CosmosService Class Tests
# =============================================================================

class TestCosmosServiceInit:
    """Tests for CosmosService initialization."""

    def test_init_with_connection_string(self, mock_env_vars):
        """Test successful initialization with connection string."""
        with mock.patch.dict("os.environ", {
            "COSMOS_DB_CONNECTION_STRING": "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test123=="
        }):
            with mock.patch("services.cosmos_service.CosmosClient") as mock_cosmos:
                mock_client = mock.MagicMock()
                mock_database = mock.MagicMock()
                mock_cosmos.from_connection_string.return_value = mock_client
                mock_client.get_database_client.return_value = mock_database
                
                from services.cosmos_service import CosmosService
                service = CosmosService()
                
                mock_cosmos.from_connection_string.assert_called_once()
                mock_client.get_database_client.assert_called_with("third-places")
                assert service.places_container is not None
                assert service.chunks_container is not None

    def test_init_without_connection_string_raises_error(self, mock_env_vars):
        """Test that missing connection string raises ValueError."""
        import os
        # Remove the connection string if it exists
        original = os.environ.pop("COSMOS_DB_CONNECTION_STRING", None)
        
        try:
            from services.cosmos_service import CosmosService
            
            with pytest.raises(ValueError, match="COSMOS_DB_CONNECTION_STRING"):
                CosmosService()
        finally:
            if original:
                os.environ["COSMOS_DB_CONNECTION_STRING"] = original


class TestCosmosServiceUpsertPlace:
    """Tests for upsert_place method."""

    @pytest.fixture
    def cosmos_service(self, mock_env_vars, mock_cosmos_client):
        """Create a CosmosService with mocked clients."""
        with mock.patch.dict("os.environ", {
            "COSMOS_DB_CONNECTION_STRING": "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test123=="
        }):
            with mock.patch("services.cosmos_service.CosmosClient") as mock_cosmos:
                mock_cosmos.from_connection_string.return_value = mock_cosmos_client["client"]
                
                from services.cosmos_service import CosmosService
                service = CosmosService()
                # Replace containers with our mocks
                service.places_container = mock_cosmos_client["places_container"]
                service.chunks_container = mock_cosmos_client["chunks_container"]
                return service

    def test_upsert_place_success(self, cosmos_service, sample_place_doc):
        """Test successful place upsert."""
        cosmos_service.places_container.upsert_item.return_value = sample_place_doc
        
        result = cosmos_service.upsert_place(sample_place_doc)
        
        cosmos_service.places_container.upsert_item.assert_called_once_with(sample_place_doc)
        assert result == sample_place_doc

    def test_upsert_place_without_id_raises_error(self, cosmos_service):
        """Test that missing 'id' field raises ValueError."""
        doc_without_id = {"place": "Test Cafe"}
        
        with pytest.raises(ValueError, match="must have 'id' field"):
            cosmos_service.upsert_place(doc_without_id)


class TestCosmosServiceUpsertChunk:
    """Tests for upsert_chunk method."""

    @pytest.fixture
    def cosmos_service(self, mock_env_vars, mock_cosmos_client):
        """Create a CosmosService with mocked clients."""
        with mock.patch.dict("os.environ", {
            "COSMOS_DB_CONNECTION_STRING": "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test123=="
        }):
            with mock.patch("services.cosmos_service.CosmosClient") as mock_cosmos:
                mock_cosmos.from_connection_string.return_value = mock_cosmos_client["client"]
                
                from services.cosmos_service import CosmosService
                service = CosmosService()
                service.places_container = mock_cosmos_client["places_container"]
                service.chunks_container = mock_cosmos_client["chunks_container"]
                return service

    def test_upsert_chunk_success(self, cosmos_service, sample_chunk_doc):
        """Test successful chunk upsert."""
        cosmos_service.chunks_container.upsert_item.return_value = sample_chunk_doc
        
        result = cosmos_service.upsert_chunk(sample_chunk_doc)
        
        cosmos_service.chunks_container.upsert_item.assert_called_once_with(sample_chunk_doc)
        assert result == sample_chunk_doc

    def test_upsert_chunk_without_id_raises_error(self, cosmos_service):
        """Test that missing 'id' field raises ValueError."""
        doc = {"placeId": "test-place-id", "reviewText": "Great!"}
        
        with pytest.raises(ValueError, match="must have 'id' field"):
            cosmos_service.upsert_chunk(doc)

    def test_upsert_chunk_without_place_id_raises_error(self, cosmos_service):
        """Test that missing 'placeId' field raises ValueError."""
        doc = {"id": "chunk-1", "reviewText": "Great!"}
        
        with pytest.raises(ValueError, match="must have 'placeId' field"):
            cosmos_service.upsert_chunk(doc)


class TestCosmosServiceGetPlace:
    """Tests for get_place method."""

    @pytest.fixture
    def cosmos_service(self, mock_env_vars, mock_cosmos_client):
        """Create a CosmosService with mocked clients."""
        with mock.patch.dict("os.environ", {
            "COSMOS_DB_CONNECTION_STRING": "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test123=="
        }):
            with mock.patch("services.cosmos_service.CosmosClient") as mock_cosmos:
                mock_cosmos.from_connection_string.return_value = mock_cosmos_client["client"]
                
                from services.cosmos_service import CosmosService
                service = CosmosService()
                service.places_container = mock_cosmos_client["places_container"]
                service.chunks_container = mock_cosmos_client["chunks_container"]
                return service

    def test_get_place_found(self, cosmos_service, sample_place_doc):
        """Test retrieving an existing place."""
        cosmos_service.places_container.read_item.return_value = sample_place_doc
        
        result = cosmos_service.get_place("ChIJH9S7TOcPVIgRnG5eHqW4DE0")
        
        cosmos_service.places_container.read_item.assert_called_once_with(
            item="ChIJH9S7TOcPVIgRnG5eHqW4DE0",
            partition_key="ChIJH9S7TOcPVIgRnG5eHqW4DE0"
        )
        assert result == sample_place_doc

    def test_get_place_not_found(self, cosmos_service):
        """Test retrieving a non-existent place returns None."""
        cosmos_service.places_container.read_item.side_effect = CosmosResourceNotFoundError()
        
        result = cosmos_service.get_place("non-existent-id")
        
        assert result is None


class TestCosmosServiceGetAllPlaceIds:
    """Tests for get_all_place_ids method."""

    @pytest.fixture
    def cosmos_service(self, mock_env_vars, mock_cosmos_client):
        """Create a CosmosService with mocked clients."""
        with mock.patch.dict("os.environ", {
            "COSMOS_DB_CONNECTION_STRING": "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test123=="
        }):
            with mock.patch("services.cosmos_service.CosmosClient") as mock_cosmos:
                mock_cosmos.from_connection_string.return_value = mock_cosmos_client["client"]
                
                from services.cosmos_service import CosmosService
                service = CosmosService()
                service.places_container = mock_cosmos_client["places_container"]
                service.chunks_container = mock_cosmos_client["chunks_container"]
                return service

    def test_get_all_place_ids(self, cosmos_service):
        """Test retrieving all place IDs."""
        mock_items = [{"id": "place-1"}, {"id": "place-2"}, {"id": "place-3"}]
        cosmos_service.places_container.query_items.return_value = iter(mock_items)
        
        result = cosmos_service.get_all_place_ids()
        
        assert result == ["place-1", "place-2", "place-3"]

    def test_get_all_place_ids_empty(self, cosmos_service):
        """Test retrieving place IDs when none exist."""
        cosmos_service.places_container.query_items.return_value = iter([])
        
        result = cosmos_service.get_all_place_ids()
        
        assert result == []


class TestCosmosServiceDeleteChunksForPlace:
    """Tests for delete_chunks_for_place method."""

    @pytest.fixture
    def cosmos_service(self, mock_env_vars, mock_cosmos_client):
        """Create a CosmosService with mocked clients."""
        with mock.patch.dict("os.environ", {
            "COSMOS_DB_CONNECTION_STRING": "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test123=="
        }):
            with mock.patch("services.cosmos_service.CosmosClient") as mock_cosmos:
                mock_cosmos.from_connection_string.return_value = mock_cosmos_client["client"]
                
                from services.cosmos_service import CosmosService
                service = CosmosService()
                service.places_container = mock_cosmos_client["places_container"]
                service.chunks_container = mock_cosmos_client["chunks_container"]
                return service

    def test_delete_chunks_for_place(self, cosmos_service):
        """Test deleting chunks for a place."""
        mock_chunks = [{"id": "chunk-1"}, {"id": "chunk-2"}]
        cosmos_service.chunks_container.query_items.return_value = iter(mock_chunks)
        
        result = cosmos_service.delete_chunks_for_place("test-place-id")
        
        assert result == 2
        assert cosmos_service.chunks_container.delete_item.call_count == 2

    def test_delete_chunks_for_place_none_exist(self, cosmos_service):
        """Test deleting chunks when none exist."""
        cosmos_service.chunks_container.query_items.return_value = iter([])
        
        result = cosmos_service.delete_chunks_for_place("test-place-id")
        
        assert result == 0
        cosmos_service.chunks_container.delete_item.assert_not_called()


class TestCosmosServiceGetCounts:
    """Tests for count methods."""

    @pytest.fixture
    def cosmos_service(self, mock_env_vars, mock_cosmos_client):
        """Create a CosmosService with mocked clients."""
        with mock.patch.dict("os.environ", {
            "COSMOS_DB_CONNECTION_STRING": "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test123=="
        }):
            with mock.patch("services.cosmos_service.CosmosClient") as mock_cosmos:
                mock_cosmos.from_connection_string.return_value = mock_cosmos_client["client"]
                
                from services.cosmos_service import CosmosService
                service = CosmosService()
                service.places_container = mock_cosmos_client["places_container"]
                service.chunks_container = mock_cosmos_client["chunks_container"]
                return service

    def test_get_places_count(self, cosmos_service):
        """Test getting places count."""
        cosmos_service.places_container.query_items.return_value = iter([42])
        
        result = cosmos_service.get_places_count()
        
        assert result == 42

    def test_get_places_count_empty(self, cosmos_service):
        """Test getting places count when empty."""
        cosmos_service.places_container.query_items.return_value = iter([])
        
        result = cosmos_service.get_places_count()
        
        assert result == 0

    def test_get_chunks_count(self, cosmos_service):
        """Test getting chunks count."""
        cosmos_service.chunks_container.query_items.return_value = iter([100])
        
        result = cosmos_service.get_chunks_count()
        
        assert result == 100

    def test_get_places_with_chunks_count(self, cosmos_service):
        """Test getting count of places with chunks."""
        cosmos_service.chunks_container.query_items.return_value = iter([25])
        
        result = cosmos_service.get_places_with_chunks_count()
        
        assert result == 25


class TestCosmosServiceVectorSearch:
    """Tests for vector search methods."""

    @pytest.fixture
    def cosmos_service(self, mock_env_vars, mock_cosmos_client):
        """Create a CosmosService with mocked clients."""
        with mock.patch.dict("os.environ", {
            "COSMOS_DB_CONNECTION_STRING": "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test123=="
        }):
            with mock.patch("services.cosmos_service.CosmosClient") as mock_cosmos:
                mock_cosmos.from_connection_string.return_value = mock_cosmos_client["client"]
                
                from services.cosmos_service import CosmosService
                service = CosmosService()
                service.places_container = mock_cosmos_client["places_container"]
                service.chunks_container = mock_cosmos_client["chunks_container"]
                return service

    def test_vector_search_places(self, cosmos_service):
        """Test vector search on places."""
        mock_results = [
            {"id": "place-1", "place": "Cafe A", "distance": 0.1},
            {"id": "place-2", "place": "Cafe B", "distance": 0.2}
        ]
        cosmos_service.places_container.query_items.return_value = iter(mock_results)
        
        query_embedding = [0.1] * 1536
        results = cosmos_service.vector_search_places(query_embedding, top_k=5)
        
        assert len(results) == 2
        # Distance should be converted to similarity score
        assert results[0]["similarityScore"] == 0.9
        assert results[1]["similarityScore"] == 0.8
        assert "distance" not in results[0]

    def test_vector_search_chunks(self, cosmos_service):
        """Test vector search on chunks."""
        mock_results = [
            {"id": "chunk-1", "reviewText": "Great!", "distance": 0.15}
        ]
        cosmos_service.chunks_container.query_items.return_value = iter(mock_results)
        
        query_embedding = [0.1] * 1536
        results = cosmos_service.vector_search_chunks(query_embedding, top_k=10)
        
        assert len(results) == 1
        assert results[0]["similarityScore"] == 0.85

    def test_vector_search_chunks_with_place_filter(self, cosmos_service):
        """Test vector search on chunks filtered by place_id."""
        mock_results = [
            {"id": "chunk-1", "placeId": "place-1", "reviewText": "Great!", "distance": 0.1}
        ]
        cosmos_service.chunks_container.query_items.return_value = iter(mock_results)
        
        query_embedding = [0.1] * 1536
        results = cosmos_service.vector_search_chunks(
            query_embedding, 
            top_k=10, 
            place_id="place-1"
        )
        
        assert len(results) == 1
        # Verify partition_key was used instead of cross-partition query
        call_kwargs = cosmos_service.chunks_container.query_items.call_args[1]
        assert call_kwargs.get("partition_key") == "place-1"


class TestCosmosServiceGetSyncStats:
    """Tests for get_sync_stats method."""

    @pytest.fixture
    def cosmos_service(self, mock_env_vars, mock_cosmos_client):
        """Create a CosmosService with mocked clients."""
        with mock.patch.dict("os.environ", {
            "COSMOS_DB_CONNECTION_STRING": "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test123=="
        }):
            with mock.patch("services.cosmos_service.CosmosClient") as mock_cosmos:
                mock_cosmos.from_connection_string.return_value = mock_cosmos_client["client"]
                
                from services.cosmos_service import CosmosService
                service = CosmosService()
                service.places_container = mock_cosmos_client["places_container"]
                service.chunks_container = mock_cosmos_client["chunks_container"]
                return service

    def test_get_sync_stats(self, cosmos_service):
        """Test getting comprehensive sync statistics."""
        # Mock the various queries
        def mock_query(query, **kwargs):
            if "COUNT(1)" in query and "places" in str(kwargs):
                return iter([10])  # places count
            if "COUNT(1)" in query:
                return iter([50])  # chunks count or unique places
            if "TOP 1" in query and "DESC" in query:
                return iter([{"lastSynced": "2024-12-03T10:00:00Z", "id": "place-1", "place": "Latest Cafe"}])
            if "TOP 1" in query and "ASC" in query:
                return iter([{"lastSynced": "2024-01-01T10:00:00Z", "id": "place-2", "place": "Oldest Cafe"}])
            if "NOT IS_DEFINED" in query:
                return iter([0])  # without embeddings
            return iter([])
        
        cosmos_service.places_container.query_items.side_effect = mock_query
        cosmos_service.chunks_container.query_items.side_effect = mock_query
        
        # Need to patch the individual count methods since they're called separately
        with mock.patch.object(cosmos_service, 'get_places_count', return_value=10):
            with mock.patch.object(cosmos_service, 'get_chunks_count', return_value=50):
                with mock.patch.object(cosmos_service, 'get_places_with_chunks_count', return_value=8):
                    stats = cosmos_service.get_sync_stats()
        
        assert "places" in stats
        assert "chunks" in stats
        assert "sync" in stats
        assert stats["places"]["count"] == 10
        assert stats["chunks"]["count"] == 50
        assert stats["chunks"]["averagePerPlace"] == 6.25  # 50 / 8


# =============================================================================
# Helper Function Tests
# =============================================================================

class TestParseTimestamp:
    """Tests for parse_timestamp helper function."""

    def test_parse_timestamp_with_z_suffix(self, mock_env_vars):
        """Test parsing ISO timestamp with Z suffix."""
        from services.cosmos_service import parse_timestamp
        
        result = parse_timestamp("2024-12-03T10:00:00Z")
        
        assert result is not None
        assert result.year == 2024
        assert result.month == 12
        assert result.day == 3

    def test_parse_timestamp_with_timezone(self, mock_env_vars):
        """Test parsing ISO timestamp with timezone offset."""
        from services.cosmos_service import parse_timestamp
        
        result = parse_timestamp("2024-12-03T10:00:00+00:00")
        
        assert result is not None
        assert result.year == 2024

    def test_parse_timestamp_naive_datetime(self, mock_env_vars):
        """Test parsing naive timestamp - assumes UTC."""
        from services.cosmos_service import parse_timestamp
        
        result = parse_timestamp("2024-12-03T10:00:00")
        
        assert result is not None
        from datetime import timezone
        assert result.tzinfo == timezone.utc

    def test_parse_timestamp_none_returns_none(self, mock_env_vars):
        """Test that None input returns None."""
        from services.cosmos_service import parse_timestamp
        
        result = parse_timestamp(None)
        
        assert result is None

    def test_parse_timestamp_empty_string_returns_none(self, mock_env_vars):
        """Test that empty string returns None."""
        from services.cosmos_service import parse_timestamp
        
        result = parse_timestamp("")
        
        assert result is None

    def test_parse_timestamp_invalid_format_returns_none(self, mock_env_vars):
        """Test that invalid format returns None."""
        from services.cosmos_service import parse_timestamp
        
        result = parse_timestamp("not-a-timestamp")
        
        assert result is None


class TestShouldSyncPlace:
    """Tests for should_sync_place helper function."""

    def test_should_sync_new_place(self, mock_env_vars):
        """Test that new place (no Cosmos record) should sync."""
        from services.cosmos_service import should_sync_place
        
        should_sync, reason = should_sync_place(
            airtable_modified="2024-12-03T10:00:00Z",
            json_last_updated="2024-12-03T10:00:00",
            cosmos_last_synced=None
        )
        
        assert should_sync is True
        assert reason == "new_place"

    def test_should_sync_airtable_modified(self, mock_env_vars):
        """Test sync when Airtable was modified after last sync."""
        from services.cosmos_service import should_sync_place
        
        should_sync, reason = should_sync_place(
            airtable_modified="2024-12-03T12:00:00Z",  # Newer
            json_last_updated="2024-12-01T10:00:00",
            cosmos_last_synced="2024-12-03T10:00:00+00:00"
        )
        
        assert should_sync is True
        assert reason == "airtable_modified"

    def test_should_sync_json_modified(self, mock_env_vars):
        """Test sync when JSON file was updated after last sync."""
        from services.cosmos_service import should_sync_place
        
        should_sync, reason = should_sync_place(
            airtable_modified="2024-12-01T10:00:00Z",
            json_last_updated="2024-12-03T12:00:00",  # Newer
            cosmos_last_synced="2024-12-03T10:00:00+00:00"
        )
        
        assert should_sync is True
        assert reason == "json_modified"

    def test_should_not_sync_no_changes(self, mock_env_vars):
        """Test no sync when nothing changed."""
        from services.cosmos_service import should_sync_place
        
        should_sync, reason = should_sync_place(
            airtable_modified="2024-12-01T10:00:00Z",
            json_last_updated="2024-12-01T10:00:00",
            cosmos_last_synced="2024-12-03T10:00:00+00:00"  # Newer than sources
        )
        
        assert should_sync is False
        assert reason == "no_changes"

    def test_should_sync_missing_timestamps(self, mock_env_vars):
        """Test sync when source timestamps are missing."""
        from services.cosmos_service import should_sync_place
        
        should_sync, reason = should_sync_place(
            airtable_modified=None,
            json_last_updated=None,
            cosmos_last_synced="2024-12-03T10:00:00+00:00"
        )
        
        assert should_sync is True
        assert reason == "missing_timestamps"
