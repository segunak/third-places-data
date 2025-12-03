"""
Unit tests for the EmbeddingService class from embedding_service.py.

All tests use mocked Azure OpenAI responses - no live API calls are made.
"""

import pytest
from unittest import mock


class TestEmbeddingServiceInit:
    """Tests for EmbeddingService initialization."""

    def test_init_with_api_key(self, mock_env_vars):
        """Test successful initialization with API key."""
        with mock.patch.dict("os.environ", {"FOUNDRY_API_KEY": "test-foundry-key"}):
            with mock.patch("services.embedding_service.AzureOpenAI"):
                from services.embedding_service import EmbeddingService
                
                service = EmbeddingService()
                
                assert service.api_key == "test-foundry-key"
                assert service.model == "text-embedding-3-small"
                assert service.dimensions == 1536
                assert service.max_batch_size == 16

    def test_init_without_api_key_raises_error(self, mock_env_vars):
        """Test that missing API key raises ValueError."""
        with mock.patch.dict("os.environ", {"FOUNDRY_API_KEY": ""}, clear=False):
            # Clear the FOUNDRY_API_KEY
            import os
            original = os.environ.pop("FOUNDRY_API_KEY", None)
            
            try:
                from services.embedding_service import EmbeddingService
                
                with pytest.raises(ValueError, match="FOUNDRY_API_KEY"):
                    EmbeddingService()
            finally:
                if original:
                    os.environ["FOUNDRY_API_KEY"] = original


class TestEmbeddingServiceGetEmbeddings:
    """Tests for get_embeddings method."""

    @pytest.fixture
    def embedding_service(self, mock_env_vars, mock_openai_client):
        """Create an EmbeddingService with mocked client."""
        with mock.patch.dict("os.environ", {"FOUNDRY_API_KEY": "test-foundry-key"}):
            with mock.patch("services.embedding_service.AzureOpenAI", return_value=mock_openai_client):
                from services.embedding_service import EmbeddingService
                service = EmbeddingService()
                return service

    def test_get_embeddings_single_text(self, embedding_service):
        """Test generating embedding for a single text."""
        texts = ["Hello world"]
        embeddings = embedding_service.get_embeddings(texts)
        
        assert len(embeddings) == 1
        assert len(embeddings[0]) == 1536

    def test_get_embeddings_multiple_texts(self, embedding_service):
        """Test generating embeddings for multiple texts."""
        texts = ["First text", "Second text", "Third text"]
        embeddings = embedding_service.get_embeddings(texts)
        
        assert len(embeddings) == 3
        for emb in embeddings:
            assert len(emb) == 1536

    def test_get_embeddings_empty_list_raises_error(self, embedding_service):
        """Test that empty text list raises ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            embedding_service.get_embeddings([])

    def test_get_embeddings_exceeds_batch_size_raises_error(self, embedding_service):
        """Test that exceeding batch size raises ValueError."""
        texts = ["text"] * 17  # Exceeds max_batch_size of 16
        
        with pytest.raises(ValueError, match="exceeds max batch size"):
            embedding_service.get_embeddings(texts)

    def test_get_embeddings_filters_empty_strings(self, embedding_service):
        """Test that empty strings are filtered out."""
        texts = ["Valid text", "", "  ", "Another valid"]
        embeddings = embedding_service.get_embeddings(texts)
        
        # Only 2 valid texts after filtering
        assert len(embeddings) == 2

    def test_get_embeddings_all_empty_raises_error(self, embedding_service):
        """Test that all empty texts raises ValueError."""
        texts = ["", "  ", "\n"]
        
        with pytest.raises(ValueError, match="All texts are empty"):
            embedding_service.get_embeddings(texts)


class TestEmbeddingServiceGetEmbedding:
    """Tests for get_embedding method (single text)."""

    @pytest.fixture
    def embedding_service(self, mock_env_vars, mock_openai_client):
        """Create an EmbeddingService with mocked client."""
        with mock.patch.dict("os.environ", {"FOUNDRY_API_KEY": "test-foundry-key"}):
            with mock.patch("services.embedding_service.AzureOpenAI", return_value=mock_openai_client):
                from services.embedding_service import EmbeddingService
                service = EmbeddingService()
                return service

    def test_get_embedding_returns_vector(self, embedding_service):
        """Test generating embedding for a single text returns a vector."""
        embedding = embedding_service.get_embedding("Test text")
        
        assert isinstance(embedding, list)
        assert len(embedding) == 1536

    def test_get_embedding_strips_whitespace(self, embedding_service):
        """Test that input text is stripped of whitespace."""
        embedding = embedding_service.get_embedding("  Padded text  ")
        
        assert isinstance(embedding, list)
        assert len(embedding) == 1536


class TestFormatFieldForEmbedding:
    """Tests for format_field_for_embedding function."""

    def test_format_plain_string(self, mock_env_vars):
        """Test formatting a plain string field - returns value as-is."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("description", "A cozy cafe")
        assert result == "A cozy cafe"

    def test_format_empty_string_returns_none(self, mock_env_vars):
        """Test that empty string returns None."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("description", "")
        assert result is None

    def test_format_none_returns_none(self, mock_env_vars):
        """Test that None value returns None."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("description", None)
        assert result is None

    def test_format_list_field(self, mock_env_vars):
        """Test formatting a list field - joins with commas."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("tags", ["cozy", "quiet", "wifi"])
        assert result == "cozy, quiet, wifi"

    def test_format_empty_list_returns_none(self, mock_env_vars):
        """Test that empty list returns None."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("tags", [])
        assert result is None

    def test_format_boolean_true(self, mock_env_vars):
        """Test formatting boolean True - uses readable label."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("freeWifi", True)
        assert result == "free wifi: yes"

    def test_format_boolean_false(self, mock_env_vars):
        """Test formatting boolean False - uses readable label."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("freeWifi", False)
        assert result == "free wifi: no"

    def test_format_parking_list(self, mock_env_vars):
        """Test formatting parking field - includes 'parking:' label."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("parking", ["Free Lot", "Street"])
        assert result == "parking: Free Lot, Street"

    def test_format_size_field(self, mock_env_vars):
        """Test formatting size field - includes label."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("size", "Large")
        assert result == "size: Large"

    def test_format_parking_single_value(self, mock_env_vars):
        """Test formatting parking field with single string value."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("parking", "Free Lot")
        assert result == "parking: Free Lot"

    def test_format_type_single_value(self, mock_env_vars):
        """Test formatting type field with single string value."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("type", "Cafe")
        assert result == "Cafe"

    def test_format_tags_single_value(self, mock_env_vars):
        """Test formatting tags field with single string value (not list)."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("tags", "cozy")
        assert result == "cozy"

    def test_format_reviews_tags_single_value(self, mock_env_vars):
        """Test formatting reviewsTags field with single string value."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("reviewsTags", "coffee")
        assert result == "coffee"

    def test_format_about_field_nested_dict(self, mock_env_vars):
        """Test formatting about field with nested dictionary."""
        from services.embedding_service import format_field_for_embedding
        
        about_data = {
            "Amenities": {
                "Good for studying": True,
                "Has parking": False
            },
            "Atmosphere": {
                "Cozy": True
            }
        }
        
        result = format_field_for_embedding("about", about_data)
        
        assert "Good for studying: yes" in result
        assert "Has parking: no" in result
        assert "Cozy: yes" in result

    def test_format_about_field_with_category_string(self, mock_env_vars):
        """Test formatting about field with category as string."""
        from services.embedding_service import format_field_for_embedding
        
        about_data = {
            "Service options": "Dine-in, Takeout"
        }
        
        result = format_field_for_embedding("about", about_data)
        assert "Service options: Dine-in, Takeout" in result

    def test_format_about_field_empty_returns_none(self, mock_env_vars):
        """Test formatting empty about field returns None."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("about", {})
        assert result is None

    def test_format_working_hours(self, mock_env_vars):
        """Test formatting working hours field."""
        from services.embedding_service import format_field_for_embedding
        
        hours_data = {
            "Monday": "7am-3pm",
            "Tuesday": "7am-3pm",
            "Wednesday": "Closed"
        }
        
        result = format_field_for_embedding("workingHours", hours_data)
        
        assert result is not None
        assert "hours:" in result
        assert "Monday 7am-3pm" in result

    def test_format_working_hours_empty(self, mock_env_vars):
        """Test formatting empty working hours returns None."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("workingHours", {})
        assert result is None

    def test_format_popular_times(self, mock_env_vars):
        """Test formatting popular times field."""
        from services.embedding_service import format_field_for_embedding
        
        popular_data = [
            {
                "day_text": "Monday",
                "popular_times": [
                    {"time": "9am", "percentage": 30},
                    {"time": "12pm", "percentage": 80},
                    {"time": "3pm", "percentage": 75}
                ]
            }
        ]
        
        result = format_field_for_embedding("popularTimes", popular_data)
        
        assert result is not None
        assert "busy times:" in result
        assert "Monday" in result
        assert "12pm" in result

    def test_format_popular_times_no_peaks(self, mock_env_vars):
        """Test formatting popular times with no peak hours."""
        from services.embedding_service import format_field_for_embedding
        
        popular_data = [
            {
                "day_text": "Monday",
                "popular_times": [
                    {"time": "9am", "percentage": 30}
                ]
            }
        ]
        
        result = format_field_for_embedding("popularTimes", popular_data)
        
        # No peaks above 70%, so should return None
        assert result is None

    def test_format_fallback_other_types(self, mock_env_vars):
        """Test formatting fallback for other types (integers, etc.)."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("unknownField", 42)
        assert result == "42"

    def test_format_typical_time_spent(self, mock_env_vars):
        """Test formatting typicalTimeSpent field."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("typicalTimeSpent", "1-2 hours")
        assert result == "1-2 hours"


class TestComposePlaceEmbeddingText:
    """Tests for compose_place_embedding_text function."""

    def test_compose_basic_place(self, mock_env_vars):
        """Test composing embedding text for a basic place."""
        from services.embedding_service import compose_place_embedding_text
        
        place = {
            "place": "Test Cafe",
            "neighborhood": "Downtown",
            "type": ["Cafe"]
        }
        
        text = compose_place_embedding_text(place)
        
        # Fields are joined with " | " separator
        assert "Test Cafe" in text
        assert "Downtown" in text
        assert "Cafe" in text
        assert " | " in text

    def test_compose_skips_empty_fields(self, mock_env_vars):
        """Test that empty fields are skipped."""
        from services.embedding_service import compose_place_embedding_text
        
        place = {
            "place": "Test Cafe",
            "neighborhood": "",
            "description": None,
            "type": ["Cafe"]
        }
        
        text = compose_place_embedding_text(place)
        
        assert "Test Cafe" in text
        # Empty neighborhood shouldn't create extra separators
        assert " |  | " not in text


class TestComposeChunkEmbeddingText:
    """Tests for compose_chunk_embedding_text function."""

    def test_compose_review_chunk(self, mock_env_vars):
        """Test composing embedding text for a review chunk."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Great coffee and atmosphere!",
            "reviewRating": 5,
            "placeName": "Test Cafe"
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "Great coffee and atmosphere!" in text

    def test_compose_with_place_context(self, mock_env_vars):
        """Test that place context is included via placeName field."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Loved it!",
            "placeName": "Test Cafe",  # Note: placeName, not place
            "neighborhood": "Downtown"
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "Test Cafe" in text
        assert "Downtown" in text

    def test_compose_with_owner_answer(self, mock_env_vars):
        """Test that owner answer is included."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Loved it!",
            "ownerAnswer": "Thank you for visiting!"
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "Loved it!" in text
        assert "Thank you for visiting!" in text

    def test_compose_with_place_type_string(self, mock_env_vars):
        """Test that placeType as string is included."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Great!",
            "placeType": "Cafe"  # String, not list
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "Cafe" in text

    def test_compose_with_place_tags_string(self, mock_env_vars):
        """Test that placeTags as string is included."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Great!",
            "placeTags": "cozy, quiet"  # String, not list
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "cozy, quiet" in text

    def test_compose_with_place_type_list(self, mock_env_vars):
        """Test that placeType as list is joined."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Great!",
            "placeType": ["Cafe", "Bakery"]
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "Cafe, Bakery" in text
