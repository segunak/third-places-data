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
        """Test formatting a plain string field - returns value with label."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("description", "A cozy cafe")
        assert result == "description: A cozy cafe"

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
        """Test formatting a list field - joins with commas and adds label."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("tags", ["cozy", "quiet", "wifi"])
        assert result == "tags: cozy, quiet, wifi"

    def test_format_empty_list_returns_none(self, mock_env_vars):
        """Test that empty list returns None."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("tags", [])
        assert result is None

    def test_format_boolean_true(self, mock_env_vars):
        """Test formatting boolean True - stringifies to 'True'."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("freeWifi", True)
        assert result == "freeWifi: True"

    def test_format_boolean_false(self, mock_env_vars):
        """Test formatting boolean False - stringifies to 'False'."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("freeWifi", False)
        assert result == "freeWifi: False"

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
        assert result == "type: Cafe"

    def test_format_tags_single_value(self, mock_env_vars):
        """Test formatting tags field with single string value (not list)."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("tags", "cozy")
        assert result == "tags: cozy"

    def test_format_reviews_tags_single_value(self, mock_env_vars):
        """Test formatting reviewsTags field with single string value."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("reviewsTags", "coffee")
        assert result == "reviewsTags: coffee"

    def test_format_about_field_nested_dict(self, mock_env_vars):
        """Test formatting about field with nested dictionary - only true values shown."""
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
        
        # Only true values are included (what the place HAS)
        assert "Good for studying" in result
        assert "Cozy" in result
        # False values are NOT included
        assert "Has parking" not in result

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
        assert "workingHours:" in result
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
        assert "popularTimes:" in result
        assert "Monday" in result
        assert "12pm" in result

    def test_format_popular_times_formatted_string(self, mock_env_vars):
        """Test formatting popularTimesFormatted (pre-computed string)."""
        from services.embedding_service import format_field_for_embedding
        
        # popularTimesFormatted is now a pre-computed string from utils.format_popular_times
        formatted = "Mon: busy 9-11am; moderate 12pm"
        result = format_field_for_embedding("popularTimesFormatted", formatted)
        
        assert result == "popularTimesFormatted: Mon: busy 9-11am; moderate 12pm"

    def test_format_fallback_other_types(self, mock_env_vars):
        """Test formatting fallback for other types (integers, etc.)."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("unknownField", 42)
        assert result == "unknownField: 42"

    def test_format_typical_time_spent(self, mock_env_vars):
        """Test formatting typicalTimeSpent field."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("typicalTimeSpent", "1-2 hours")
        assert result == "typicalTimeSpent: 1-2 hours"

    def test_format_review_questions_with_numeric_ratings(self, mock_env_vars):
        """Test formatting reviewQuestions with numeric ratings adds scale context."""
        from services.embedding_service import format_field_for_embedding
        
        questions_data = {
            "Food": "5",
            "Service": "4",
            "Price per person": "$20-30"
        }
        
        result = format_field_for_embedding("reviewQuestions", questions_data)
        
        assert result is not None
        assert "reviewQuestions (ratings on 5-point scale):" in result
        assert "Food 5" in result
        assert "Service 4" in result
        assert "Price per person $20-30" in result

    def test_format_review_questions_without_numeric_ratings(self, mock_env_vars):
        """Test formatting reviewQuestions without numeric ratings uses plain label."""
        from services.embedding_service import format_field_for_embedding
        
        questions_data = {
            "Price per person": "$20-30",
            "Wait time": "Up to 10 min"
        }
        
        result = format_field_for_embedding("reviewQuestions", questions_data)
        
        assert result is not None
        assert "reviewQuestions:" in result
        assert "(ratings on 5-point scale)" not in result
        assert "Price per person $20-30" in result
        assert "Wait time Up to 10 min" in result

    def test_format_review_questions_empty(self, mock_env_vars):
        """Test formatting empty reviewQuestions returns None."""
        from services.embedding_service import format_field_for_embedding
        
        result = format_field_for_embedding("reviewQuestions", {})
        assert result is None

    def test_format_review_questions_all_rating_values(self, mock_env_vars):
        """Test that all rating values 1-5 are detected as numeric ratings."""
        from services.embedding_service import format_field_for_embedding
        
        for rating in ["1", "2", "3", "4", "5"]:
            result = format_field_for_embedding("reviewQuestions", {"Quality": rating})
            assert "(ratings on 5-point scale)" in result


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
        
        # Fields are joined with newline separator and have labels
        assert "placeName: Test Cafe" in text
        assert "neighborhood: Downtown" in text
        assert "type: Cafe" in text
        assert "\n" in text

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
    """Tests for compose_chunk_embedding_text function with labeled newline format."""

    def test_compose_review_chunk_with_labels(self, mock_env_vars):
        """Test composing embedding text for a review chunk uses labeled format."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Great coffee and atmosphere!",
            "reviewRating": 5,
            "placeName": "Test Cafe"
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "placeName: Test Cafe" in text
        assert "reviewRating: 5" in text
        assert "reviewText: Great coffee and atmosphere!" in text
        # Should use newline separator
        assert "\n" in text

    def test_compose_with_place_context_first(self, mock_env_vars):
        """Test that place context comes before review text (context-first ordering)."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Loved it!",
            "placeName": "Test Cafe",
            "neighborhood": "Downtown"
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        # Context should come before review text
        place_idx = text.find("placeName:")
        neighborhood_idx = text.find("neighborhood:")
        review_idx = text.find("reviewText:")
        
        assert place_idx < review_idx
        assert neighborhood_idx < review_idx

    def test_compose_with_owner_response_true(self, mock_env_vars):
        """Test that owner answer is only included when hasOwnerResponse is True."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Loved it!",
            "hasOwnerResponse": True,
            "ownerAnswer": "Thank you for visiting!"
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "hasOwnerResponse: yes" in text
        assert "ownerAnswer: Thank you for visiting!" in text

    def test_compose_excludes_owner_response_when_false(self, mock_env_vars):
        """Test that owner answer is excluded when hasOwnerResponse is False."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Loved it!",
            "hasOwnerResponse": False,
            "ownerAnswer": "Thank you for visiting!"
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "hasOwnerResponse" not in text
        assert "ownerAnswer" not in text

    def test_compose_excludes_owner_response_when_missing(self, mock_env_vars):
        """Test that owner answer is excluded when hasOwnerResponse is not present."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Loved it!",
            "ownerAnswer": "Thank you for visiting!"
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "hasOwnerResponse" not in text
        assert "ownerAnswer" not in text

    def test_compose_with_place_type_string(self, mock_env_vars):
        """Test that placeType as string is included with label."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Great!",
            "placeType": "Cafe"
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "placeType: Cafe" in text

    def test_compose_with_place_tags_list(self, mock_env_vars):
        """Test that placeTags as list is joined with label."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Great!",
            "placeTags": ["cozy", "quiet"]
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "placeTags: cozy, quiet" in text

    def test_compose_with_place_type_list(self, mock_env_vars):
        """Test that placeType as list is joined with label."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Great!",
            "placeType": ["Cafe", "Bakery"]
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "placeType: Cafe, Bakery" in text

    def test_compose_with_review_questions_numeric(self, mock_env_vars):
        """Test reviewQuestions with numeric ratings gets scale context."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Great!",
            "reviewQuestions": {
                "Food": "5",
                "Service": "4",
                "Price per person": "$20-30"
            }
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "reviewQuestions (ratings on 5-point scale):" in text
        assert "Food 5" in text
        assert "Service 4" in text
        assert "Price per person $20-30" in text

    def test_compose_with_review_questions_non_numeric(self, mock_env_vars):
        """Test reviewQuestions without numeric ratings uses plain label."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Great!",
            "reviewQuestions": {
                "Price per person": "$20-30",
                "Wait time": "Up to 10 min"
            }
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        # Should use plain label without scale context
        assert "reviewQuestions:" in text
        assert "(ratings on 5-point scale)" not in text

    def test_compose_with_reviews_tags(self, mock_env_vars):
        """Test that reviewsTags is included with label."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "reviewText": "Great!",
            "reviewsTags": ["cozy atmosphere", "great coffee"]
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        assert "reviewsTags: cozy atmosphere, great coffee" in text

    def test_compose_full_chunk(self, mock_env_vars):
        """Test composing a chunk with all fields."""
        from services.embedding_service import compose_chunk_embedding_text
        
        chunk = {
            "placeName": "Test Cafe",
            "neighborhood": "Downtown",
            "placeType": ["Cafe"],
            "placeTags": ["cozy", "quiet"],
            "reviewRating": 5,
            "reviewsTags": ["great coffee"],
            "reviewText": "Amazing place!",
            "hasOwnerResponse": True,
            "ownerAnswer": "Thank you!",
            "reviewQuestions": {"Food": "5", "Service": "4"}
        }
        
        text = compose_chunk_embedding_text(chunk)
        
        # Verify all fields present with labels
        assert "placeName: Test Cafe" in text
        assert "neighborhood: Downtown" in text
        assert "placeType: Cafe" in text
        assert "placeTags: cozy, quiet" in text
        assert "reviewRating: 5" in text
        assert "reviewsTags: great coffee" in text
        assert "reviewText: Amazing place!" in text
        assert "hasOwnerResponse: yes" in text
        assert "ownerAnswer: Thank you!" in text
        assert "reviewQuestions (ratings on 5-point scale):" in text


# =============================================================================
# Tests for sanitize_field_value function
# =============================================================================

class TestSanitizeFieldValue:
    """Tests for sanitize_field_value function."""
    
    def test_replaces_newlines_with_space(self, mock_env_vars):
        """Newlines should be replaced with single space."""
        from services.embedding_service import sanitize_field_value
        
        assert sanitize_field_value("line1\nline2") == "line1 line2"
        assert sanitize_field_value("line1\r\nline2") == "line1 line2"
        assert sanitize_field_value("line1\rline2") == "line1 line2"
    
    def test_collapses_multiple_spaces(self, mock_env_vars):
        """Multiple consecutive spaces should collapse to one."""
        from services.embedding_service import sanitize_field_value
        
        assert sanitize_field_value("word1    word2") == "word1 word2"
        assert sanitize_field_value("a  b   c    d") == "a b c d"
    
    def test_strips_whitespace(self, mock_env_vars):
        """Leading and trailing whitespace should be stripped."""
        from services.embedding_service import sanitize_field_value
        
        assert sanitize_field_value("  hello  ") == "hello"
        assert sanitize_field_value("\n\nhello\n\n") == "hello"
    
    def test_combined_sanitization(self, mock_env_vars):
        """Multiple newlines and spaces should all be handled."""
        from services.embedding_service import sanitize_field_value
        
        text = "  There are book clubs\n\nthat meet here.  \nLots of space.  "
        assert sanitize_field_value(text) == "There are book clubs that meet here. Lots of space."
    
    def test_preserves_markdown_links(self, mock_env_vars):
        """Markdown links should be preserved."""
        from services.embedding_service import sanitize_field_value
        
        text = "Created by [@napoletanoart](https://www.instagram.com/napoletanoart/)"
        assert sanitize_field_value(text) == text


# =============================================================================
# Tests for format_field_for_embedding with boolean-like fields
# =============================================================================

class TestFormatFieldForEmbeddingStringFields:
    """Tests for format_field_for_embedding with string fields from Airtable.
    
    Note: Fields like freeWifi, hasCinnamonRolls, purchaseRequired are stored
    as strings in Airtable (Yes, No, Unsure, Sometimes), not Python booleans.
    They are treated as regular strings and passed through with their original case.
    """
    
    # --- freeWifi ---
    def test_freeWifi_string_yes(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("freeWifi", "Yes") == "freeWifi: Yes"
    
    def test_freeWifi_string_no(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("freeWifi", "No") == "freeWifi: No"
    
    def test_freeWifi_string_unsure(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("freeWifi", "Unsure") == "freeWifi: Unsure"
    
    def test_freeWifi_various_cases(self, mock_env_vars):
        """String values preserve their original case."""
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("freeWifi", "YES") == "freeWifi: YES"
        assert format_field_for_embedding("freeWifi", "no") == "freeWifi: no"
        assert format_field_for_embedding("freeWifi", "UNSURE") == "freeWifi: UNSURE"
    
    def test_freeWifi_python_bool_true(self, mock_env_vars):
        """Python booleans stringify to True/False."""
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("freeWifi", True) == "freeWifi: True"
    
    def test_freeWifi_python_bool_false(self, mock_env_vars):
        """Python booleans stringify to True/False."""
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("freeWifi", False) == "freeWifi: False"
    
    def test_freeWifi_none(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("freeWifi", None) is None
    
    # --- hasCinnamonRolls ---
    def test_hasCinnamonRolls_string_yes(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("hasCinnamonRolls", "Yes") == "hasCinnamonRolls: Yes"
    
    def test_hasCinnamonRolls_string_no(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("hasCinnamonRolls", "No") == "hasCinnamonRolls: No"
    
    def test_hasCinnamonRolls_string_sometimes(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("hasCinnamonRolls", "Sometimes") == "hasCinnamonRolls: Sometimes"
    
    def test_hasCinnamonRolls_string_unsure(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("hasCinnamonRolls", "Unsure") == "hasCinnamonRolls: Unsure"
    
    def test_hasCinnamonRolls_various_cases(self, mock_env_vars):
        """String values preserve their original case."""
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("hasCinnamonRolls", "SOMETIMES") == "hasCinnamonRolls: SOMETIMES"
    
    # --- purchaseRequired ---
    def test_purchaseRequired_string_yes(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("purchaseRequired", "Yes") == "purchaseRequired: Yes"
    
    def test_purchaseRequired_string_no(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("purchaseRequired", "No") == "purchaseRequired: No"
    
    def test_purchaseRequired_string_unsure(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("purchaseRequired", "Unsure") == "purchaseRequired: Unsure"


# =============================================================================
# Tests for format_field_for_embedding field labels
# =============================================================================

class TestFormatFieldForEmbeddingLabels:
    """Tests for format_field_for_embedding field labels."""
    
    def test_placeName_has_label(self, mock_env_vars):
        """Test that placeName field is formatted with its own name."""
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("placeName", "Starbucks")
        assert result == "placeName: Starbucks"
    
    def test_description_has_label(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("description", "A great coffee shop")
        assert result == "description: A great coffee shop"
    
    def test_comments_has_label(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("comments", "Love this place")
        assert result == "comments: Love this place"
    
    def test_neighborhood_has_label(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("neighborhood", "South End")
        assert result == "neighborhood: South End"
    
    def test_address_has_label(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("address", "123 Main St")
        assert result == "address: 123 Main St"
    
    def test_type_has_label(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("type", ["Coffee Shop", "Cafe"])
        assert result == "type: Coffee Shop, Cafe"
    
    def test_tags_has_label(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("tags", ["Outdoor Outlets", "Black Owned"])
        assert result == "tags: Outdoor Outlets, Black Owned"
    
    def test_parking_has_label(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("parking", ["Free", "Limited"])
        assert result == "parking: Free, Limited"
    
    def test_size_has_label(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("size", "Large")
        assert result == "size: Large"
    
    def test_reviewsTags_has_label(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("reviewsTags", ["coffee", "pastries"])
        assert result == "reviewsTags: coffee, pastries"
    
    def test_typicalTimeSpent_has_label(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("typicalTimeSpent", "30-60 min")
        assert result == "typicalTimeSpent: 30-60 min"
    
    def test_about_has_label(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("about", {"Service options": {"Takeaway": True, "Dine-in": False}})
        # Only true values shown as feature names
        assert result == "about: Takeaway"
    
    def test_workingHours_has_label(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("workingHours", {"Monday": "7AM-5PM", "Tuesday": "7AM-5PM"})
        assert result == "workingHours: Monday 7AM-5PM, Tuesday 7AM-5PM"
    
    def test_popularTimesFormatted_has_label(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        # popularTimesFormatted is now a pre-computed string
        result = format_field_for_embedding("popularTimesFormatted", "Monday: busy 9AM")
        assert result == "popularTimesFormatted: Monday: busy 9AM"


# =============================================================================
# Tests for format_field_for_embedding value sanitization
# =============================================================================

class TestFormatFieldForEmbeddingSanitization:
    """Tests for format_field_for_embedding value sanitization."""
    
    def test_description_with_newlines(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        text = "Line 1\nLine 2\nLine 3"
        result = format_field_for_embedding("description", text)
        assert result == "description: Line 1 Line 2 Line 3"
    
    def test_comments_with_multiple_spaces(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        text = "Word1    Word2     Word3"
        result = format_field_for_embedding("comments", text)
        assert result == "comments: Word1 Word2 Word3"
    
    def test_preserves_markdown(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        text = "Check out [@user](https://instagram.com/user) for more"
        result = format_field_for_embedding("comments", text)
        assert result == f"comments: {text}"
    
    def test_list_values_sanitized(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("tags", ["Tag\nOne", "Tag  Two"])
        assert result == "tags: Tag One, Tag Two"


# =============================================================================
# Tests for format_field_for_embedding edge cases
# =============================================================================

class TestFormatFieldForEmbeddingEdgeCases:
    """Tests for edge cases in format_field_for_embedding."""
    
    def test_none_value_returns_none(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("description", None) is None
    
    def test_empty_string_returns_none(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("description", "") is None
    
    def test_whitespace_only_returns_none(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("description", "   ") is None
    
    def test_empty_list_returns_none(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("tags", []) is None
    
    def test_empty_dict_returns_none(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        assert format_field_for_embedding("about", {}) is None
    
    def test_single_value_in_list_field(self, mock_env_vars):
        from services.embedding_service import format_field_for_embedding
        result = format_field_for_embedding("type", "Coffee Shop")
        assert result == "type: Coffee Shop"


# =============================================================================
# Tests for compose_place_embedding_text function
# =============================================================================

class TestComposePlaceEmbeddingTextFormatting:
    """Tests for compose_place_embedding_text output formatting."""
    
    def test_uses_newline_separator(self, mock_env_vars):
        from services.embedding_service import compose_place_embedding_text
        
        place_doc = {
            "place": "Test Place",
            "neighborhood": "Test Area",
            "description": "A test description"
        }
        result = compose_place_embedding_text(place_doc)
        assert "\n" in result
        assert " | " not in result
    
    def test_includes_all_labeled_fields(self, mock_env_vars):
        from services.embedding_service import compose_place_embedding_text
        
        place_doc = {
            "place": "Starbucks",
            "description": "Coffee shop",
            "comments": "Great place",
            "neighborhood": "South End",
            "address": "123 Main St",
            "type": ["Coffee Shop"],
            "tags": ["WiFi"],
            "freeWifi": "Yes",
            "hasCinnamonRolls": "No",
            "purchaseRequired": "Yes",
            "parking": ["Free"],
            "size": "Large"
        }
        result = compose_place_embedding_text(place_doc)
        
        assert "placeName: Starbucks" in result
        assert "description: Coffee shop" in result
        assert "comments: Great place" in result
        assert "neighborhood: South End" in result
        assert "address: 123 Main St" in result
        assert "type: Coffee Shop" in result
        assert "tags: WiFi" in result
        assert "freeWifi: Yes" in result
        assert "hasCinnamonRolls: No" in result
        assert "purchaseRequired: Yes" in result
        assert "parking: Free" in result
        assert "size: Large" in result
    
    def test_skips_none_values(self, mock_env_vars):
        from services.embedding_service import compose_place_embedding_text
        
        place_doc = {
            "place": "Test Place",
            "description": None,
            "neighborhood": "Test Area"
        }
        result = compose_place_embedding_text(place_doc)
        
        assert "placeName: Test Place" in result
        assert "neighborhood: Test Area" in result
        assert "description" not in result
    
    def test_sanitizes_multiline_values(self, mock_env_vars):
        from services.embedding_service import compose_place_embedding_text
        
        place_doc = {
            "place": "Test Place",
            "comments": "Line 1\nLine 2\nLine 3"
        }
        result = compose_place_embedding_text(place_doc)
        
        assert "comments: Line 1 Line 2 Line 3" in result
        # The newline should be the separator between fields, not within values
        lines = result.split("\n")
        for line in lines:
            if line.startswith("comments:"):
                # The value after "comments: " should not have newlines
                assert "\n" not in line[9:]
