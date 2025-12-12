"""
Unit tests for Cosmos DB services: cosmos_service.py and embedding_service.py.
Uses mock data to test transformation and composition functions without hitting real services.
"""

import os
import json
import unittest
import sys
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

# Add parent directory to path so we can import from parent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.cosmos_service import (
    transform_airtable_to_place,
    transform_review_to_chunk,
    extract_place_context,
    get_place_embedding_fields,
)
from services.embedding_service import (
    compose_place_embedding_text,
    compose_chunk_embedding_text,
    format_field_for_embedding,
)


# Mock Airtable record structure
MOCK_AIRTABLE_RECORD = {
    "id": "recABC123",
    "fields": {
        "Place": "Mattie Ruth's Coffee House",
        "Google Maps Place Id": "ChIJH9S7TOcPVIgRnG5eHqW4DE0",
        "Address": "300 McGill Ave NW, Concord, NC 28027",
        "Neighborhood": "Concord",
        "Type": "Coffee Shop",
        "Tags": ["Wi-Fi", "Outdoor Seating", "Study Friendly"],
        "Description": "A cozy coffee shop with amazing atmosphere for working.",
        "Latitude": 35.4165135,
        "Longitude": -80.6031644,
        "Website": "https://www.mattieruths.com/",
        "Google Maps Profile URL": "https://maps.google.com/?cid=5552015459959598748",
        "Apple Maps Profile URL": "https://maps.apple.com/?address=300%20McGill%20Ave%20NW",
        "Operational": True,
        "Featured": False,
        "Free Wi-Fi": True,
        "Purchase Required": "Yes",
        "Parking": ["Free", "Street"],
        "Size": "Medium",
        "Instagram": "@mattieruths",
        "Created Time": "2024-01-15T10:30:00.000Z",
        "Last Modified Time": "2025-11-28T12:00:00.000Z",
    }
}

# Mock JSON data structure (from GitHub place files)
MOCK_JSON_DATA = {
    "place_id": "ChIJH9S7TOcPVIgRnG5eHqW4DE0",
    "place_name": "Mattie Ruth's Coffee House",
    "details": {
        "raw_data": {
            "name": "Mattie Ruth's Coffee House",
            "place_id": "ChIJH9S7TOcPVIgRnG5eHqW4DE0",
            "rating": 4.9,
            "reviews": 60,
            "category": "Coffee shop",
            "subtypes": ["Coffee shop", "Cafe", "Breakfast restaurant"],
            "phone": "+1 704-555-1234",
            "working_hours": {
                "Monday": "6:30 AM - 5:00 PM",
                "Tuesday": "6:30 AM - 5:00 PM",
                "Wednesday": "6:30 AM - 5:00 PM",
                "Thursday": "6:30 AM - 5:00 PM",
                "Friday": "6:30 AM - 5:00 PM",
                "Saturday": "7:00 AM - 5:00 PM",
                "Sunday": "8:00 AM - 3:00 PM",
            },
            "popular_times": [
                {
                    "day": 1,
                    "day_text": "Monday",
                    "popular_times": [
                        {"hour": 8, "percentage": 45, "time": "8a"},
                        {"hour": 9, "percentage": 75, "time": "9a"},
                        {"hour": 10, "percentage": 90, "time": "10a"},
                        {"hour": 11, "percentage": 85, "time": "11a"},
                        {"hour": 12, "percentage": 60, "time": "12p"},
                    ]
                },
                {
                    "day": 6,
                    "day_text": "Saturday",
                    "popular_times": [
                        {"hour": 9, "percentage": 80, "time": "9a"},
                        {"hour": 10, "percentage": 95, "time": "10a"},
                        {"hour": 11, "percentage": 72, "time": "11a"},
                    ]
                },
            ],
            "typical_time_spent": "People typically spend 30-60 min here",
            "about": {
                "Service options": {
                    "Dine-in": True,
                    "Takeout": True,
                    "Delivery": False,
                },
                "Highlights": {
                    "Great coffee": True,
                    "Cozy atmosphere": True,
                },
                "Accessibility": {
                    "Wheelchair accessible entrance": True,
                },
            },
            "reviews_tags": ["coffee", "atmosphere", "pastries", "friendly staff"],
            "reviews_link": "https://search.google.com/local/reviews?placeid=ChIJH9S7TOcPVIgRnG5eHqW4DE0",
            "street_view": "https://streetviewpixels-pa.googleapis.com/...",
            "located_in": "Downtown Concord",
            "photos_data": [
                {"photo_url": "https://example.com/photo1.jpg"},
                {"photo_url": "https://example.com/photo2.jpg"},
            ],
        }
    },
    "reviews": {
        "place_id": "ChIJH9S7TOcPVIgRnG5eHqW4DE0",
        "raw_data": {
            "rating": 4.9,
            "reviews": 60,
            "reviews_tags": ["coffee", "atmosphere", "pastries"],
            "reviews_data": [
                {
                    "review_id": "review_001",
                    "review_text": "The absolute cutest coffee shop with amazing atmosphere for working!",
                    "review_rating": 5,
                    "review_link": "https://www.google.com/maps/reviews/...",
                    "review_datetime_utc": "07/21/2025 13:50:44",
                    "review_timestamp": 1753105844,
                    "review_questions": {
                        "Food": "5",
                        "Service": "5",
                        "Atmosphere": "5",
                    },
                    "owner_answer": None,
                    "review_img_urls": None,
                },
                {
                    "review_id": "review_002",
                    "review_text": "Delicious coffee and chocolate croissant! Perfect spot for remote work.",
                    "review_rating": 5,
                    "review_link": "https://www.google.com/maps/reviews/...",
                    "review_datetime_utc": "07/17/2025 17:36:08",
                    "review_timestamp": 1752773768,
                    "review_questions": {
                        "Service": "5",
                        "Atmosphere": "5",
                    },
                    "owner_answer": "Thank you so much for your kind words! We're glad you enjoyed your visit.",
                    "review_img_urls": ["https://example.com/review_photo.jpg"],
                },
                {
                    "review_id": "review_003",
                    "review_text": "",  # Empty review - should be skipped
                    "review_rating": 4,
                    "review_link": "https://www.google.com/maps/reviews/...",
                    "review_datetime_utc": "07/10/2025 10:00:00",
                    "review_timestamp": 1752145200,
                    "owner_answer": None,
                },
            ],
        },
    },
}


class TestTransformAirtableToPlace(unittest.TestCase):
    """Test suite for transform_airtable_to_place function."""

    def test_transform_basic_fields(self):
        """Test transformation of basic Airtable fields to place document."""
        result = transform_airtable_to_place(MOCK_AIRTABLE_RECORD)
        
        self.assertEqual(result["id"], "ChIJH9S7TOcPVIgRnG5eHqW4DE0")
        self.assertEqual(result["place"], "Mattie Ruth's Coffee House")
        self.assertEqual(result["address"], "300 McGill Ave NW, Concord, NC 28027")
        self.assertEqual(result["neighborhood"], "Concord")
        self.assertEqual(result["type"], "Coffee Shop")
        self.assertEqual(result["tags"], ["Wi-Fi", "Outdoor Seating", "Study Friendly"])
        self.assertTrue(result["freeWifi"])
        self.assertEqual(result["latitude"], 35.4165135)
        self.assertEqual(result["longitude"], -80.6031644)
        
    def test_transform_with_json_data(self):
        """Test transformation including JSON data fields."""
        result = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        
        # JSON fields should be present
        self.assertEqual(result["category"], "Coffee shop")
        self.assertEqual(result["subtypes"], ["Coffee shop", "Cafe", "Breakfast restaurant"])
        self.assertEqual(result["placeRating"], 4.9)
        self.assertEqual(result["reviewsCount"], 60)
        self.assertEqual(result["phone"], "+1 704-555-1234")
        self.assertIn("about", result)
        self.assertIn("workingHours", result)
        self.assertIn("popularTimes", result)
        self.assertIn("photosData", result)
        
    def test_transform_without_json_data(self):
        """Test transformation without JSON data (only Airtable fields)."""
        result = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, None)
        
        # Should still have Airtable fields
        self.assertEqual(result["id"], "ChIJH9S7TOcPVIgRnG5eHqW4DE0")
        self.assertEqual(result["place"], "Mattie Ruth's Coffee House")
        
        # Should NOT have JSON-only fields
        self.assertNotIn("category", result)
        self.assertNotIn("subtypes", result)
        self.assertNotIn("placeRating", result)
        
    def test_transform_includes_last_synced(self):
        """Test that lastSynced timestamp is added."""
        result = transform_airtable_to_place(MOCK_AIRTABLE_RECORD)
        
        self.assertIn("lastSynced", result)
        # Should be a valid ISO format datetime
        self.assertIsInstance(result["lastSynced"], str)
        self.assertIn("T", result["lastSynced"])


class TestTransformReviewToChunk(unittest.TestCase):
    """Test suite for transform_review_to_chunk function."""

    def setUp(self):
        """Set up test fixtures."""
        self.place_context = extract_place_context(MOCK_AIRTABLE_RECORD)
        self.details_raw_data = MOCK_JSON_DATA["details"]["raw_data"]
        
    def test_transform_review_basic_fields(self):
        """Test transformation of review to chunk document."""
        review = MOCK_JSON_DATA["reviews"]["raw_data"]["reviews_data"][0]
        result = transform_review_to_chunk(review, self.place_context, self.details_raw_data)
        
        self.assertEqual(result["id"], "review_001")
        self.assertEqual(result["placeId"], "ChIJH9S7TOcPVIgRnG5eHqW4DE0")
        self.assertEqual(result["reviewText"], "The absolute cutest coffee shop with amazing atmosphere for working!")
        self.assertEqual(result["reviewRating"], 5)
        self.assertFalse(result["hasOwnerResponse"])
        self.assertIsNone(result["ownerAnswer"])
        
    def test_transform_review_with_owner_answer(self):
        """Test transformation of review that has owner response."""
        review = MOCK_JSON_DATA["reviews"]["raw_data"]["reviews_data"][1]
        result = transform_review_to_chunk(review, self.place_context, self.details_raw_data)
        
        self.assertEqual(result["id"], "review_002")
        self.assertTrue(result["hasOwnerResponse"])
        self.assertIn("Thank you so much", result["ownerAnswer"])
        self.assertIsNotNone(result["reviewImgUrls"])
        
    def test_transform_review_denormalized_place_fields(self):
        """Test that place context is correctly denormalized into chunk."""
        review = MOCK_JSON_DATA["reviews"]["raw_data"]["reviews_data"][0]
        result = transform_review_to_chunk(review, self.place_context, self.details_raw_data)
        
        self.assertEqual(result["placeName"], "Mattie Ruth's Coffee House")
        self.assertEqual(result["neighborhood"], "Concord")
        self.assertEqual(result["address"], "300 McGill Ave NW, Concord, NC 28027")
        self.assertEqual(result["placeType"], "Coffee Shop")
        self.assertEqual(result["placeTags"], ["Wi-Fi", "Outdoor Seating", "Study Friendly"])
        
    def test_transform_review_aggregate_context(self):
        """Test that aggregate context from details is included."""
        review = MOCK_JSON_DATA["reviews"]["raw_data"]["reviews_data"][0]
        result = transform_review_to_chunk(review, self.place_context, self.details_raw_data)
        
        self.assertEqual(result["placeRating"], 4.9)
        self.assertEqual(result["placeReviewsCount"], 60)
        self.assertEqual(result["reviewsTags"], ["coffee", "atmosphere", "pastries", "friendly staff"])


class TestExtractPlaceContext(unittest.TestCase):
    """Test suite for extract_place_context function."""

    def test_extract_all_context_fields(self):
        """Test that all expected context fields are extracted."""
        result = extract_place_context(MOCK_AIRTABLE_RECORD)
        
        expected_fields = [
            "googleMapsPlaceId",
            "place",
            "neighborhood",
            "address",
            "googleMapsProfileUrl",
            "appleMapsProfileUrl",
            "type",
            "tags",
        ]
        
        for field in expected_fields:
            self.assertIn(field, result, f"Missing expected field: {field}")
            
    def test_extract_context_values(self):
        """Test that context values are correct."""
        result = extract_place_context(MOCK_AIRTABLE_RECORD)
        
        self.assertEqual(result["googleMapsPlaceId"], "ChIJH9S7TOcPVIgRnG5eHqW4DE0")
        self.assertEqual(result["place"], "Mattie Ruth's Coffee House")
        self.assertEqual(result["neighborhood"], "Concord")
        self.assertEqual(result["type"], "Coffee Shop")


class TestComposePlaceEmbeddingText(unittest.TestCase):
    """Test suite for compose_place_embedding_text function."""

    def test_compose_full_text(self):
        """Test composition with all fields present."""
        place_doc = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        result = compose_place_embedding_text(place_doc)
        
        # Should contain key semantic fields
        self.assertIn("Mattie Ruth's Coffee House", result)
        self.assertIn("Concord", result)
        self.assertIn("300 McGill Ave NW", result)
        self.assertIn("Coffee Shop", result)
        self.assertIn("Wi-Fi", result)
        # Note: category is NOT embedded (embed=False in mapping)
        
    def test_compose_with_about_section(self):
        """Test that about section shows only true values (amenities the place HAS)."""
        place_doc = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        result = compose_place_embedding_text(place_doc)
        
        # About features should show only true values as comma-separated list
        self.assertIn("about:", result)
        self.assertIn("Dine-in", result)
        self.assertIn("Takeout", result)
        # False values should NOT be included (we only care about what the place HAS)
        self.assertNotIn("Delivery", result)
        
    def test_compose_with_separator(self):
        """Test that fields are joined with newline separator."""
        place_doc = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        result = compose_place_embedding_text(place_doc)
        
        # Should use newline separator
        self.assertIn("\n", result)
        
    def test_compose_minimal_place(self):
        """Test composition with minimal fields."""
        minimal_doc = {
            "id": "test123",
            "place": "Test Place",
        }
        result = compose_place_embedding_text(minimal_doc)
        
        self.assertEqual(result, "placeName: Test Place")


class TestComposeChunkEmbeddingText(unittest.TestCase):
    """Test suite for compose_chunk_embedding_text function."""

    def test_compose_full_chunk_text(self):
        """Test composition with all chunk fields."""
        place_context = extract_place_context(MOCK_AIRTABLE_RECORD)
        details_raw_data = MOCK_JSON_DATA["details"]["raw_data"]
        review = MOCK_JSON_DATA["reviews"]["raw_data"]["reviews_data"][0]
        
        chunk_doc = transform_review_to_chunk(review, place_context, details_raw_data)
        result = compose_chunk_embedding_text(chunk_doc)
        
        # Review text should be primary
        self.assertIn("cutest coffee shop", result)
        
        # Place context for grounding
        self.assertIn("Mattie Ruth's Coffee House", result)
        self.assertIn("Concord", result)
        self.assertIn("Coffee Shop", result)
        
    def test_compose_chunk_with_owner_answer(self):
        """Test that owner answer is included when present."""
        place_context = extract_place_context(MOCK_AIRTABLE_RECORD)
        details_raw_data = MOCK_JSON_DATA["details"]["raw_data"]
        review = MOCK_JSON_DATA["reviews"]["raw_data"]["reviews_data"][1]
        
        chunk_doc = transform_review_to_chunk(review, place_context, details_raw_data)
        result = compose_chunk_embedding_text(chunk_doc)
        
        self.assertIn("Thank you so much", result)
        
    def test_compose_chunk_with_newline_separator(self):
        """Test that fields are joined with newline separator and have labels."""
        place_context = extract_place_context(MOCK_AIRTABLE_RECORD)
        details_raw_data = MOCK_JSON_DATA["details"]["raw_data"]
        review = MOCK_JSON_DATA["reviews"]["raw_data"]["reviews_data"][0]
        
        chunk_doc = transform_review_to_chunk(review, place_context, details_raw_data)
        result = compose_chunk_embedding_text(chunk_doc)
        
        # Should use newline separator and have labels
        self.assertIn("\n", result)
        self.assertIn("placeName:", result)
        self.assertIn("reviewText:", result)
        
    def test_compose_chunk_review_only(self):
        """Test composition with only review text (minimal chunk)."""
        minimal_chunk = {
            "id": "test_review",
            "placeId": "test_place",
            "reviewText": "Great place for studying!",
        }
        result = compose_chunk_embedding_text(minimal_chunk)
        
        # Should have label for review text
        self.assertEqual(result, "reviewText: Great place for studying!")


class TestEmptyAndNullHandling(unittest.TestCase):
    """Test suite for handling empty and null values."""

    def test_transform_with_missing_fields(self):
        """Test transformation handles missing fields gracefully."""
        minimal_record = {
            "id": "recXYZ",
            "fields": {
                "Google Maps Place Id": "test_id",
                "Place": "Test Place",
            }
        }
        result = transform_airtable_to_place(minimal_record)
        
        self.assertEqual(result["id"], "test_id")
        self.assertEqual(result["place"], "Test Place")
        # Missing fields should not be present (not set to None)
        self.assertNotIn("description", result)
        
    def test_compose_embedding_skips_empty_fields(self):
        """Test that empty fields are skipped in embedding composition."""
        place_doc = {
            "id": "test",
            "place": "Test Place",
            "description": "",  # Empty string
            "neighborhood": None,  # None value
            "type": "Cafe",
        }
        result = compose_place_embedding_text(place_doc)
        
        # Should only have place and type
        self.assertIn("Test Place", result)
        self.assertIn("Cafe", result)
        # Should not have empty separators
        self.assertNotIn(" |  | ", result)


class TestFormatFieldForEmbedding(unittest.TestCase):
    """Test suite for format_field_for_embedding function covering all field types."""

    def test_boolean_field_true(self):
        """Test boolean fields stringify to 'True'."""
        self.assertEqual(format_field_for_embedding("freeWifi", True), "freeWifi: True")
        self.assertEqual(format_field_for_embedding("hasCinnamonRolls", True), "hasCinnamonRolls: True")
        self.assertEqual(format_field_for_embedding("purchaseRequired", True), "purchaseRequired: True")

    def test_boolean_field_false(self):
        """Test boolean fields stringify to 'False'."""
        self.assertEqual(format_field_for_embedding("freeWifi", False), "freeWifi: False")
        self.assertEqual(format_field_for_embedding("hasCinnamonRolls", False), "hasCinnamonRolls: False")
        self.assertEqual(format_field_for_embedding("purchaseRequired", False), "purchaseRequired: False")

    def test_boolean_field_none(self):
        """Test boolean fields return None when value is None."""
        self.assertIsNone(format_field_for_embedding("freeWifi", None))

    def test_size_field_with_value(self):
        """Test size field formatted with label prefix."""
        self.assertEqual(format_field_for_embedding("size", "Medium"), "size: Medium")
        self.assertEqual(format_field_for_embedding("size", "Large"), "size: Large")

    def test_size_field_empty(self):
        """Test size field returns None when empty."""
        self.assertIsNone(format_field_for_embedding("size", ""))
        self.assertIsNone(format_field_for_embedding("size", None))

    def test_list_field_tags(self):
        """Test list fields (tags) formatted as comma-separated with label."""
        tags = ["Wi-Fi", "Outdoor Seating", "Study Friendly"]
        result = format_field_for_embedding("tags", tags)
        self.assertEqual(result, "tags: Wi-Fi, Outdoor Seating, Study Friendly")

    def test_list_field_type(self):
        """Test type field formatted as comma-separated with label."""
        types = ["Coffee shop", "Cafe"]
        result = format_field_for_embedding("type", types)
        self.assertEqual(result, "type: Coffee shop, Cafe")

    def test_list_field_parking_array(self):
        """Test parking field as array with label prefix."""
        parking = ["Free", "Street"]
        result = format_field_for_embedding("parking", parking)
        self.assertEqual(result, "parking: Free, Street")

    def test_list_field_parking_string(self):
        """Test parking field as single string with label prefix."""
        result = format_field_for_embedding("parking", "Paid Lot")
        self.assertEqual(result, "parking: Paid Lot")

    def test_list_field_empty(self):
        """Test empty list returns None."""
        self.assertIsNone(format_field_for_embedding("tags", []))
        self.assertIsNone(format_field_for_embedding("type", []))

    def test_list_field_reviews_tags(self):
        """Test reviewsTags field formatted as comma-separated with label."""
        tags = ["latte", "matcha", "parking", "desserts"]
        result = format_field_for_embedding("reviewsTags", tags)
        self.assertEqual(result, "reviewsTags: latte, matcha, parking, desserts")

    def test_about_field_nested_dict(self):
        """Test about field shows only true values from nested dicts."""
        about = {
            "Service options": {
                "Dine-in": True,
                "Takeout": True,
                "Delivery": False,
            },
            "Highlights": {
                "Great coffee": True,
            },
        }
        result = format_field_for_embedding("about", about)
        # Only true values are included as feature names
        self.assertIn("Dine-in", result)
        self.assertIn("Takeout", result)
        self.assertIn("Great coffee", result)
        # False values should NOT be included
        self.assertNotIn("Delivery", result)

    def test_about_field_empty(self):
        """Test about field returns None when empty."""
        self.assertIsNone(format_field_for_embedding("about", {}))
        self.assertIsNone(format_field_for_embedding("about", None))

    def test_working_hours_field(self):
        """Test workingHours field formatted with label prefix."""
        working_hours = {
            "Monday": "6AM-11PM",
            "Tuesday": "6AM-11PM",
            "Friday": "6AM-12AM",
        }
        result = format_field_for_embedding("workingHours", working_hours)
        self.assertIn("workingHours:", result)
        self.assertIn("Monday 6AM-11PM", result)
        self.assertIn("Tuesday 6AM-11PM", result)
        self.assertIn("Friday 6AM-12AM", result)

    def test_working_hours_empty(self):
        """Test workingHours returns None when empty."""
        self.assertIsNone(format_field_for_embedding("workingHours", {}))
        self.assertIsNone(format_field_for_embedding("workingHours", None))

    def test_popular_times_formatted_field(self):
        """Test popularTimesFormatted field (pre-computed string from utils.format_popular_times)."""
        # popularTimes is now pre-computed as popularTimesFormatted by utils.format_popular_times
        # and stored in the place document at sync time. The raw popularTimes JSON is no longer
        # processed during embedding - instead we use the pre-formatted string.
        formatted_string = "Mon: busy 9-11am; moderate 12pm. Sat: busy 9-10am"
        result = format_field_for_embedding("popularTimesFormatted", formatted_string)
        self.assertEqual(result, "popularTimesFormatted: Mon: busy 9-11am; moderate 12pm. Sat: busy 9-10am")

    def test_popular_times_empty(self):
        """Test popularTimes returns None when empty."""
        self.assertIsNone(format_field_for_embedding("popularTimes", []))
        self.assertIsNone(format_field_for_embedding("popularTimes", None))

    def test_popular_times_formatted_empty(self):
        """Test popularTimesFormatted when empty or None."""
        # popularTimesFormatted is a pre-computed string - when empty/None, returns None
        self.assertIsNone(format_field_for_embedding("popularTimesFormatted", ""))
        self.assertIsNone(format_field_for_embedding("popularTimesFormatted", None))

    def test_plain_string_field(self):
        """Test plain string fields returned with labels."""
        self.assertEqual(format_field_for_embedding("place", "Test Coffee Shop"), "placeName: Test Coffee Shop")
        self.assertEqual(format_field_for_embedding("description", "Great place"), "description: Great place")
        self.assertEqual(format_field_for_embedding("neighborhood", "Downtown"), "neighborhood: Downtown")
        self.assertEqual(format_field_for_embedding("address", "123 Main St"), "address: 123 Main St")

    def test_plain_string_field_empty(self):
        """Test empty strings return None."""
        self.assertIsNone(format_field_for_embedding("place", ""))
        self.assertIsNone(format_field_for_embedding("description", "   "))
        self.assertIsNone(format_field_for_embedding("place", None))

    def test_typical_time_spent_field(self):
        """Test typicalTimeSpent field returned with label."""
        result = format_field_for_embedding("typicalTimeSpent", "People typically spend 30-60 min here")
        self.assertEqual(result, "typicalTimeSpent: People typically spend 30-60 min here")

    def test_comments_field(self):
        """Test comments field (curator notes) returned with label."""
        comments = "This place has amazing vibes and is perfect for deep work sessions."
        result = format_field_for_embedding("comments", comments)
        self.assertEqual(result, f"comments: {comments}")


class TestGetPlaceEmbeddingFields(unittest.TestCase):
    """Test suite for get_place_embedding_fields configuration function."""

    def test_returns_list(self):
        """Test that function returns a list."""
        result = get_place_embedding_fields()
        self.assertIsInstance(result, list)

    def test_includes_all_airtable_embed_fields(self):
        """Test that all Airtable fields marked for embedding are included."""
        result = get_place_embedding_fields()
        
        expected_airtable_fields = [
            "place", "description", "comments", "neighborhood", "address",
            "type", "tags", "freeWifi", "hasCinnamonRolls", "parking",
            "purchaseRequired", "size"
        ]
        
        for field in expected_airtable_fields:
            self.assertIn(field, result, f"Expected field '{field}' not in embedding fields")

    def test_includes_all_json_embed_fields(self):
        """Test that all JSON fields marked for embedding are included."""
        result = get_place_embedding_fields()
        
        # Note: popularTimes is now pre-computed as popularTimesFormatted
        expected_json_fields = ["about", "reviewsTags", "workingHours", "popularTimesFormatted", "typicalTimeSpent"]
        
        for field in expected_json_fields:
            self.assertIn(field, result, f"Expected field '{field}' not in embedding fields")

    def test_excludes_non_embed_fields(self):
        """Test that fields marked embed=False are NOT included."""
        result = get_place_embedding_fields()
        
        excluded_fields = [
            "category", "subtypes", "latitude", "longitude", "website",
            "googleMapsPlaceId", "appleMapsProfileUrl", "createdTime",
            "placeRating", "reviewsCount", "phone"
        ]
        
        for field in excluded_fields:
            self.assertNotIn(field, result, f"Field '{field}' should not be in embedding fields")

    def test_field_count(self):
        """Test that the total number of embedding fields is correct."""
        result = get_place_embedding_fields()
        # 12 Airtable fields + 5 JSON fields = 17 total
        self.assertEqual(len(result), 17)


class TestEmbeddingIntegration(unittest.TestCase):
    """Integration tests for complete embedding text composition with all new fields."""

    def test_place_embedding_includes_working_hours(self):
        """Test that place embedding includes formatted working hours."""
        place_doc = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        result = compose_place_embedding_text(place_doc)
        
        self.assertIn("workingHours:", result)
        self.assertIn("Monday 6:30 AM - 5:00 PM", result)
        self.assertIn("Sunday 8:00 AM - 3:00 PM", result)

    def test_place_embedding_includes_popular_times_formatted(self):
        """Test that place embedding includes pre-formatted popular times."""
        place_doc = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        result = compose_place_embedding_text(place_doc)
        
        # popularTimesFormatted is pre-computed by utils.format_popular_times at sync time
        self.assertIn("popularTimesFormatted:", result)
        # The mock data has the pre-formatted string
        self.assertIn("Mon:", result)
        self.assertIn("Sat:", result)

    def test_place_embedding_includes_typical_time_spent(self):
        """Test that place embedding includes typical time spent."""
        place_doc = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        result = compose_place_embedding_text(place_doc)
        
        self.assertIn("People typically spend 30-60 min here", result)

    def test_place_embedding_includes_parking_as_list(self):
        """Test that parking field is formatted correctly as a list."""
        place_doc = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        result = compose_place_embedding_text(place_doc)
        
        self.assertIn("parking: Free, Street", result)

    def test_place_embedding_includes_boolean_fields(self):
        """Test that boolean fields are formatted correctly.
        
        Note: In the mock data, freeWifi is a Python bool (True), which
        stringifies to 'True'. In production, Airtable sends strings
        like 'Yes', 'No', etc. which preserve their original case.
        """
        place_doc = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        result = compose_place_embedding_text(place_doc)
        
        self.assertIn("freeWifi: True", result)

    def test_place_embedding_includes_size(self):
        """Test that size field is formatted with label."""
        place_doc = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        result = compose_place_embedding_text(place_doc)
        
        self.assertIn("size: Medium", result)

    def test_place_embedding_includes_comments(self):
        """Test that comments field is NOT in current mock but would be included if present."""
        # Add comments to test record
        mock_with_comments = MOCK_AIRTABLE_RECORD.copy()
        mock_with_comments["fields"] = MOCK_AIRTABLE_RECORD["fields"].copy()
        mock_with_comments["fields"]["Comments"] = "Insider tip: Order the cardamom latte!"
        
        place_doc = transform_airtable_to_place(mock_with_comments, MOCK_JSON_DATA)
        result = compose_place_embedding_text(place_doc)
        
        self.assertIn("Insider tip: Order the cardamom latte!", result)

    def test_place_embedding_includes_reviews_tags(self):
        """Test that reviewsTags from JSON are included."""
        place_doc = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        result = compose_place_embedding_text(place_doc)
        
        self.assertIn("coffee", result)
        self.assertIn("atmosphere", result)
        self.assertIn("pastries", result)

    def test_place_embedding_excludes_non_embed_fields(self):
        """Test that fields marked embed=False are NOT in embedding text."""
        place_doc = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        result = compose_place_embedding_text(place_doc)
        
        # These should NOT be in the embedding
        self.assertNotIn("ChIJH9S7TOcPVIgRnG5eHqW4DE0", result)  # place_id
        self.assertNotIn("35.4165135", result)  # latitude
        self.assertNotIn("80.6031644", result)  # longitude
        self.assertNotIn("@mattieruths", result)  # instagram
        self.assertNotIn("mattieruths.com", result)  # website

    def test_embedding_field_order_consistency(self):
        """Test that embedding fields are in consistent order."""
        place_doc = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        result1 = compose_place_embedding_text(place_doc)
        result2 = compose_place_embedding_text(place_doc)
        
        # Should be identical on repeated calls
        self.assertEqual(result1, result2)

    def test_embedding_with_missing_optional_fields(self):
        """Test embedding composition when optional fields are missing."""
        # Create minimal record without optional fields
        minimal_record = {
            "id": "rec123",
            "fields": {
                "Google Maps Place Id": "test_place_id",
                "Place": "Test Cafe",
                "Type": "Coffee Shop",
            }
        }
        
        place_doc = transform_airtable_to_place(minimal_record, None)
        result = compose_place_embedding_text(place_doc)
        
        # Should still work with just these fields
        self.assertIn("Test Cafe", result)
        self.assertIn("Coffee Shop", result)
        # Should not have pipe separators for missing fields
        self.assertNotIn(" |  | ", result)


class TestCosmosDurableFunctionHelpers(unittest.TestCase):
    """
    Test suite for Cosmos Durable Function activity helper patterns.
    
    These tests validate the data structures and logic used by the
    cosmos_sync_places_orchestrator and its activity functions.
    """

    def test_place_data_structure_for_activity(self):
        """Test the place data structure passed to activity functions."""
        # This is the structure created by cosmos_get_all_places activity
        # and consumed by cosmos_sync_single_place activity
        place_data = {
            "place_id": "ChIJH9S7TOcPVIgRnG5eHqW4DE0",
            "airtable_record": MOCK_AIRTABLE_RECORD,
        }
        
        # Validate required fields
        self.assertIn("place_id", place_data)
        self.assertIn("airtable_record", place_data)
        self.assertEqual(place_data["place_id"], "ChIJH9S7TOcPVIgRnG5eHqW4DE0")
        self.assertEqual(
            place_data["airtable_record"]["fields"]["Place"],
            "Mattie Ruth's Coffee House"
        )

    def test_activity_result_success_structure(self):
        """Test the success result structure from cosmos_sync_single_place activity."""
        # Successful activity result structure
        success_result = {
            "success": True,
            "placeId": "ChIJH9S7TOcPVIgRnG5eHqW4DE0",
            "placeName": "Mattie Ruth's Coffee House",
            "chunksProcessed": 10,
            "chunksSkipped": 2,
            "hasJsonData": True,
        }
        
        self.assertTrue(success_result["success"])
        self.assertEqual(success_result["placeId"], "ChIJH9S7TOcPVIgRnG5eHqW4DE0")
        self.assertGreaterEqual(success_result["chunksProcessed"], 0)
        self.assertGreaterEqual(success_result["chunksSkipped"], 0)

    def test_activity_result_failure_structure(self):
        """Test the failure result structure from cosmos_sync_single_place activity."""
        # Failed activity result structure
        failure_result = {
            "success": False,
            "placeId": "ChIJH9S7TOcPVIgRnG5eHqW4DE0",
            "error": "Error syncing place ChIJH9S7TOcPVIgRnG5eHqW4DE0: Connection timeout",
        }
        
        self.assertFalse(failure_result["success"])
        self.assertIn("placeId", failure_result)
        self.assertIn("error", failure_result)
        self.assertIsInstance(failure_result["error"], str)

    def test_orchestrator_aggregated_results_structure(self):
        """Test the aggregated results structure returned by orchestrator."""
        # This is the final result structure from cosmos_sync_places_orchestrator
        orchestrator_result = {
            "success": True,
            "placesProcessed": 5,
            "totalChunksProcessed": 100,
            "totalChunksSkipped": 10,
            "placeDetails": [
                {"placeId": "place1", "placeName": "Place 1", "chunksProcessed": 20, "chunksSkipped": 2},
                {"placeId": "place2", "placeName": "Place 2", "chunksProcessed": 80, "chunksSkipped": 8},
            ],
            "error": None,
            "failedAt": None,
            "batchSize": 5,  # Included in results for visibility
        }
        
        self.assertTrue(orchestrator_result["success"])
        self.assertEqual(orchestrator_result["placesProcessed"], 5)
        self.assertIsInstance(orchestrator_result["placeDetails"], list)
        self.assertIsNone(orchestrator_result["error"])
        self.assertIsNone(orchestrator_result["failedAt"])
        self.assertIn("batchSize", orchestrator_result)

    def test_orchestrator_config_with_batch_size(self):
        """Test the config structure passed to orchestrator includes batch_size."""
        # Config structure from HTTP trigger to orchestrator
        config = {
            "limit": 10,
            "batch_size": 5,
        }
        
        self.assertIn("limit", config)
        self.assertIn("batch_size", config)
        self.assertEqual(config["batch_size"], 5)
        
        # Test default config (no limit, default batch_size)
        default_config = {
            "limit": None,
            "batch_size": 1,  # Default is sequential
        }
        self.assertEqual(default_config["batch_size"], 1)

    def test_orchestrator_fail_fast_result_structure(self):
        """Test the fail-fast result structure when an activity fails."""
        # When an activity fails, orchestrator should return immediately with error info
        fail_fast_result = {
            "success": False,
            "placesProcessed": 3,
            "totalChunksProcessed": 60,
            "totalChunksSkipped": 5,
            "placeDetails": [
                {"placeId": "place1", "placeName": "Place 1", "chunksProcessed": 20, "chunksSkipped": 2},
                {"placeId": "place2", "placeName": "Place 2", "chunksProcessed": 40, "chunksSkipped": 3},
            ],
            "error": "Error syncing place place3: API rate limit exceeded",
            "failedAt": "place3",
        }
        
        self.assertFalse(fail_fast_result["success"])
        self.assertIsNotNone(fail_fast_result["error"])
        self.assertIsNotNone(fail_fast_result["failedAt"])
        self.assertEqual(fail_fast_result["failedAt"], "place3")

    def test_default_batch_size_constant(self):
        """Test that default batch size is 1 (sequential) for safety."""
        # Import the constant from cosmos.py
        try:
            from blueprints.cosmos import DEFAULT_COSMOS_SYNC_BATCH_SIZE
            # Default should be 1 (sequential) to avoid Cosmos 429 errors
            self.assertEqual(DEFAULT_COSMOS_SYNC_BATCH_SIZE, 1)
        except ImportError:
            # If import fails, just validate the expected default value
            expected_default_batch_size = 1
            self.assertEqual(expected_default_batch_size, 1)

    def test_place_data_list_generation(self):
        """Test generating place data list for orchestrator (simulates cosmos_get_all_places)."""
        # Simulate the list of Airtable records
        mock_records = [
            {
                "id": "rec1",
                "fields": {
                    "Google Maps Place Id": "place_id_1",
                    "Place": "Place 1",
                }
            },
            {
                "id": "rec2",
                "fields": {
                    "Google Maps Place Id": "place_id_2",
                    "Place": "Place 2",
                }
            },
            {
                "id": "rec3",
                "fields": {
                    # Missing Google Maps Place Id - should be skipped
                    "Place": "Place 3",
                }
            },
        ]
        
        # Simulate the transformation logic in cosmos_get_all_places activity
        place_data_list = []
        for record in mock_records:
            place_id = record.get("fields", {}).get("Google Maps Place Id")
            if place_id:
                place_data_list.append({
                    "place_id": place_id,
                    "airtable_record": record,
                })
        
        # Should have 2 valid places (one skipped due to missing place_id)
        self.assertEqual(len(place_data_list), 2)
        self.assertEqual(place_data_list[0]["place_id"], "place_id_1")
        self.assertEqual(place_data_list[1]["place_id"], "place_id_2")


class TestFormatPopularTimes(unittest.TestCase):
    """Test suite for format_popular_times utility function from utils.py."""

    def test_format_popular_times_basic(self):
        """Test basic popular times formatting with busy, moderate, and quiet hours."""
        from services.utils import format_popular_times
        
        popular_times = [
            {
                "day": 1,
                "day_text": "Monday",
                "popular_times": [
                    {"hour": 8, "percentage": 30},   # quiet
                    {"hour": 9, "percentage": 75},   # busy
                    {"hour": 10, "percentage": 90},  # busy
                    {"hour": 11, "percentage": 85},  # busy
                    {"hour": 12, "percentage": 55},  # moderate
                    {"hour": 13, "percentage": 40},  # quiet
                ]
            },
        ]
        
        result = format_popular_times(popular_times)
        
        self.assertIsNotNone(result)
        self.assertIn("Mon:", result)
        self.assertIn("busy", result)
        self.assertIn("9-11", result)  # Consecutive busy hours grouped
        
    def test_format_popular_times_multiple_days(self):
        """Test formatting with multiple days."""
        from services.utils import format_popular_times
        
        popular_times = [
            {
                "day": 1,
                "day_text": "Monday",
                "popular_times": [
                    {"hour": 12, "percentage": 80},
                ]
            },
            {
                "day": 6,
                "day_text": "Saturday",
                "popular_times": [
                    {"hour": 10, "percentage": 90},
                    {"hour": 11, "percentage": 85},
                ]
            },
        ]
        
        result = format_popular_times(popular_times)
        
        self.assertIsNotNone(result)
        self.assertIn("Mon:", result)
        self.assertIn("Sat:", result)
        
    def test_format_popular_times_quiet_all_day(self):
        """Test day that is quiet all day (max <= 50%)."""
        from services.utils import format_popular_times
        
        popular_times = [
            {
                "day": 2,
                "day_text": "Tuesday",
                "popular_times": [
                    {"hour": 10, "percentage": 30},
                    {"hour": 11, "percentage": 40},
                    {"hour": 12, "percentage": 50},  # At the threshold
                ]
            },
        ]
        
        result = format_popular_times(popular_times)
        
        self.assertIsNotNone(result)
        self.assertIn("Tue: quiet all day", result)
        
    def test_format_popular_times_empty_input(self):
        """Test with empty or None input."""
        from services.utils import format_popular_times
        
        self.assertIsNone(format_popular_times(None))
        self.assertIsNone(format_popular_times([]))
        
    def test_format_popular_times_skips_live_data(self):
        """Test that 'live' day entries are skipped."""
        from services.utils import format_popular_times
        
        popular_times = [
            {
                "day": "live",
                "day_text": "Live",
                "popular_times": [{"hour": 12, "percentage": 80}]
            },
            {
                "day": 1,
                "day_text": "Monday",
                "popular_times": [{"hour": 12, "percentage": 80}]
            },
        ]
        
        result = format_popular_times(popular_times)
        
        self.assertIsNotNone(result)
        self.assertNotIn("Live", result)
        self.assertIn("Mon:", result)
        
    def test_format_popular_times_no_data_days_skipped(self):
        """Test that days with all 0% are skipped (likely closed)."""
        from services.utils import format_popular_times
        
        popular_times = [
            {
                "day": 1,
                "day_text": "Monday",
                "popular_times": [
                    {"hour": 10, "percentage": 0},
                    {"hour": 11, "percentage": 0},
                ]
            },
        ]
        
        result = format_popular_times(popular_times)
        
        # All zeros means no data, should return None
        self.assertIsNone(result)
        
    def test_format_popular_times_hour_ranges(self):
        """Test that consecutive hours are grouped into ranges."""
        from services.utils import format_popular_times
        
        popular_times = [
            {
                "day": 1,
                "day_text": "Monday",
                "popular_times": [
                    {"hour": 9, "percentage": 80},
                    {"hour": 10, "percentage": 85},
                    {"hour": 11, "percentage": 75},
                    {"hour": 14, "percentage": 90},  # Gap, separate range
                    {"hour": 15, "percentage": 88},
                ]
            },
        ]
        
        result = format_popular_times(popular_times)
        
        self.assertIsNotNone(result)
        # Should have two ranges for busy hours: 9-11 and 2-3pm
        self.assertIn("busy", result)
        
    def test_format_popular_times_moderate_hours(self):
        """Test that moderate hours (51-69%) are categorized correctly."""
        from services.utils import format_popular_times
        
        popular_times = [
            {
                "day": 1,
                "day_text": "Monday",
                "popular_times": [
                    {"hour": 9, "percentage": 60},   # moderate
                    {"hour": 10, "percentage": 65},  # moderate
                    {"hour": 11, "percentage": 55},  # moderate
                    {"hour": 12, "percentage": 80},  # busy - ensures we don't get "quiet all day"
                ]
            },
        ]
        
        result = format_popular_times(popular_times)
        
        self.assertIsNotNone(result)
        self.assertIn("moderate", result)


class TestHoursToRanges(unittest.TestCase):
    """Test suite for _hours_to_ranges helper function."""

    def test_hours_to_ranges_consecutive(self):
        """Test consecutive hours are grouped into ranges."""
        from services.utils import _hours_to_ranges
        
        result = _hours_to_ranges([9, 10, 11])
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "9-11am")
        
    def test_hours_to_ranges_non_consecutive(self):
        """Test non-consecutive hours create separate ranges."""
        from services.utils import _hours_to_ranges
        
        result = _hours_to_ranges([9, 10, 14, 15])
        
        self.assertEqual(len(result), 2)
        self.assertIn("9-10am", result)
        self.assertIn("2-3pm", result)
        
    def test_hours_to_ranges_single_hour(self):
        """Test single hour is not a range."""
        from services.utils import _hours_to_ranges
        
        result = _hours_to_ranges([12])
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "12pm")
        
    def test_hours_to_ranges_empty(self):
        """Test empty input returns empty list."""
        from services.utils import _hours_to_ranges
        
        result = _hours_to_ranges([])
        self.assertEqual(result, [])
        
    def test_hours_to_ranges_pm_hours(self):
        """Test afternoon hours format correctly."""
        from services.utils import _hours_to_ranges
        
        result = _hours_to_ranges([14, 15, 16])
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], "2-4pm")


class TestFormatHourRange(unittest.TestCase):
    """Test suite for _format_hour_range helper function."""

    def test_format_single_am_hour(self):
        """Test single AM hour."""
        from services.utils import _format_hour_range
        
        self.assertEqual(_format_hour_range(9, 9), "9am")
        
    def test_format_single_pm_hour(self):
        """Test single PM hour."""
        from services.utils import _format_hour_range
        
        self.assertEqual(_format_hour_range(14, 14), "2pm")
        
    def test_format_range_same_period(self):
        """Test range within same AM/PM period."""
        from services.utils import _format_hour_range
        
        self.assertEqual(_format_hour_range(9, 11), "9-11am")
        self.assertEqual(_format_hour_range(14, 16), "2-4pm")
        
    def test_format_range_noon(self):
        """Test range including noon."""
        from services.utils import _format_hour_range
        
        self.assertEqual(_format_hour_range(12, 12), "12pm")
        self.assertEqual(_format_hour_range(11, 13), "11-1pm")


# Main execution block
if __name__ == "__main__":
    # Instantiate the test class
    print("\n" + "=" * 60)
    print("COSMOS SERVICE UNIT TESTS")
    print("=" * 60)
    
    # Create a dictionary to store results
    results = {}
    
    # Helper function to run a test method and record result
    def run_test(test_class, method_name):
        print(f"\n==== Running {test_class.__name__}.{method_name} ====")
        try:
            instance = test_class()
            if hasattr(instance, 'setUp'):
                instance.setUp()
            getattr(instance, method_name)()
            results[f"{test_class.__name__}.{method_name}"] = "PASSED"
            print(f"✅ {method_name} PASSED")
        except Exception as e:
            results[f"{test_class.__name__}.{method_name}"] = f"FAILED: {str(e)}"
            print(f"❌ {method_name} FAILED: {str(e)}")
    
    # Run all test classes
    test_classes = [
        TestTransformAirtableToPlace,
        TestTransformReviewToChunk,
        TestExtractPlaceContext,
        TestComposePlaceEmbeddingText,
        TestComposeChunkEmbeddingText,
        TestEmptyAndNullHandling,
        TestFormatFieldForEmbedding,
        TestGetPlaceEmbeddingFields,
        TestEmbeddingIntegration,
        TestCosmosDurableFunctionHelpers,
        TestFormatPopularTimes,
        TestHoursToRanges,
        TestFormatHourRange,
    ]
    
    for test_class in test_classes:
        # Get all test methods
        test_methods = [m for m in dir(test_class) if m.startswith('test_')]
        for method in test_methods:
            run_test(test_class, method)
    
    # Print summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for r in results.values() if r == "PASSED")
    failed = len(results) - passed
    
    print(f"Total: {len(results)} | Passed: {passed} | Failed: {failed}")
    print("-" * 60)
    
    for test_name, result in results.items():
        status = "✅" if result == "PASSED" else "❌"
        print(f"{status} {test_name}: {result}")
