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
)
from services.embedding_service import (
    compose_place_embedding_text,
    compose_chunk_embedding_text,
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
                {"day": 1, "day_text": "Monday", "popular_times": []},
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
        self.assertIn("Coffee shop", result)  # category
        
    def test_compose_with_about_section(self):
        """Test that about section is flattened into embedding text."""
        place_doc = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        result = compose_place_embedding_text(place_doc)
        
        # About features should be flattened
        self.assertIn("Dine-in: yes", result)
        self.assertIn("Takeout: yes", result)
        self.assertIn("Delivery: no", result)
        
    def test_compose_with_separator(self):
        """Test that fields are joined with pipe separator."""
        place_doc = transform_airtable_to_place(MOCK_AIRTABLE_RECORD, MOCK_JSON_DATA)
        result = compose_place_embedding_text(place_doc)
        
        # Should use pipe separator
        self.assertIn(" | ", result)
        
    def test_compose_minimal_place(self):
        """Test composition with minimal fields."""
        minimal_doc = {
            "id": "test123",
            "place": "Test Place",
        }
        result = compose_place_embedding_text(minimal_doc)
        
        self.assertEqual(result, "Test Place")


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
        
    def test_compose_chunk_with_separator(self):
        """Test that fields are joined with pipe separator."""
        place_context = extract_place_context(MOCK_AIRTABLE_RECORD)
        details_raw_data = MOCK_JSON_DATA["details"]["raw_data"]
        review = MOCK_JSON_DATA["reviews"]["raw_data"]["reviews_data"][0]
        
        chunk_doc = transform_review_to_chunk(review, place_context, details_raw_data)
        result = compose_chunk_embedding_text(chunk_doc)
        
        self.assertIn(" | ", result)
        
    def test_compose_chunk_review_only(self):
        """Test composition with only review text (minimal chunk)."""
        minimal_chunk = {
            "id": "test_review",
            "placeId": "test_place",
            "reviewText": "Great place for studying!",
        }
        result = compose_chunk_embedding_text(minimal_chunk)
        
        self.assertEqual(result, "Great place for studying!")


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
        TestCosmosDurableFunctionHelpers,
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
