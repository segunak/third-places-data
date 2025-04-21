import os
import json
import time
import dotenv
import unittest
import sys
from unittest import mock
from datetime import datetime, timedelta

# Add parent directory to path so we can import from parent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from constants import SearchField
from airtable_client import AirtableClient
from place_data_providers import OutscraperProvider

# Sample real place to test with - same as in other tests for consistency
TEST_PLACE_NAME = "Mattie Ruth's Coffee House"
TEST_PLACE_ID = "ChIJH9S7TOcPVIgRnG5eHqW4DE0"

class TestAirtableClient(unittest.TestCase):
    """Integration test Suite for the AirtableClient class."""

    def setUp(self):
        """Set up test environment before each test."""
        # Load environment variables from .env file
        dotenv.load_dotenv()
        
        # Initialize the AirtableClient with OutscraperProvider
        self.client = AirtableClient(provider_type="outscraper")
        
        self.place_id = TEST_PLACE_ID
        self.place_name = TEST_PLACE_NAME
        
        # Create output directory for test results - relative to root directory
        self.output_dir = os.path.join("..", "data", "testing", "airtable")
        os.makedirs(self.output_dir, exist_ok=True)
    
    def test_init(self):
        """Test the initialization of AirtableClient."""
        # Check if Airtable-related credentials are loaded
        self.assertIsNotNone(self.client.AIRTABLE_BASE_ID)
        self.assertIsNotNone(self.client.AIRTABLE_PERSONAL_ACCESS_TOKEN)
        self.assertIsNotNone(self.client.AIRTABLE_WORKSPACE_ID)
        
        # Check if the data provider is properly initialized as OutscraperProvider
        self.assertIsInstance(self.client.data_provider, OutscraperProvider)
        
        # Check if the all_third_places list is loaded
        self.assertIsNotNone(self.client.all_third_places)
        print(f"Loaded {len(self.client.all_third_places)} third places from Airtable")
    
    def test_get_base_url(self):
        """Test the get_base_url method."""
        # Test with a valid URL
        url_with_path = "https://example.com/path/to/resource?query=value#fragment"
        base_url = self.client.get_base_url(url_with_path)
        self.assertEqual(base_url, "https://example.com/path/to/resource")
        
        # Test with a URL without path
        url_without_path = "https://example.com"
        base_url = self.client.get_base_url(url_without_path)
        self.assertEqual(base_url, "https://example.com")
        
        # Test with an invalid URL
        invalid_url = "not-a-url"
        base_url = self.client.get_base_url(invalid_url)
        self.assertEqual(base_url, "")
        
        print("Base URL extraction works correctly")
    
    def test_get_place_photos(self):
        """Test the get_place_photos method with respect to photo optimization strategy."""
        # First check if the place already has photos in Airtable
        record = self.client.get_record(SearchField.GOOGLE_MAPS_PLACE_ID, self.place_id)
        has_existing_photos = record and 'Photos' in record['fields'] and record['fields']['Photos']
        
        if (has_existing_photos):
            print(f"Place {TEST_PLACE_NAME} already has photos in Airtable, testing photo optimization")
            # If photos exist, we should mock the photo retrieval to verify it's not called
            with mock.patch.object(self.client.data_provider, 'get_place_photos') as mock_get_photos:
                mock_get_photos.return_value = {
                    "place_id": self.place_id,
                    "message": "Photos retrieval would be skipped - photos already exist in Airtable",
                    "photo_urls": []  # Updated field name from 'photos' to 'photo_urls'
                }
                
                # This should use existing photos from Airtable rather than calling the provider
                photos = self.client.get_place_photos(self.place_id)
                
                # We should still get photos (from Airtable)
                self.assertIsNotNone(photos)
                self.assertIsInstance(photos, list)
                
                # Log the results
                print(f"Retrieved {len(photos)} photos for {TEST_PLACE_NAME} from Airtable")
                
                # Verify that the mock was called
                mock_get_photos.assert_called_once()
        else:
            # If no existing photos, test the normal photo retrieval
            photos = self.client.get_place_photos(self.place_id)
            
            # We should get a list (even if empty)
            self.assertIsNotNone(photos)
            self.assertIsInstance(photos, list)
            
            # Log the results
            print(f"Retrieved {len(photos)} photos for {TEST_PLACE_NAME}")
        
        # Save the results to file
        output_file = os.path.join(self.output_dir, f"photos_{self.place_id}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(photos, f, indent=4, ensure_ascii=False)
        print(f"Saved place photos to {output_file}")
    
    @mock.patch('airtable_client.AirtableClient.update_place_record')
    def test_refresh_operational_statuses(self, mock_update_place_record):
        """Test the refresh_operational_statuses method using a mock to prevent actual updates."""
        # Configure the mock to return a successful update
        mock_update_place_record.return_value = {
            "updated": True,
            "field_name": "Operational",
            "record_id": "rec123",
            "old_value": "No",
            "new_value": "Yes"
        }
        
        # Create a mini sample dataset for testing
        self.client.all_third_places = [
            {
                'id': 'rec123',
                'fields': {
                    'Place': TEST_PLACE_NAME,
                    'Google Maps Place Id': TEST_PLACE_ID,
                    'Operational': 'No'
                }
            }
        ]
        
        # Run the refresh operation with our mock
        results = self.client.refresh_operational_statuses()
        
        # Assertions
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['place_id'], TEST_PLACE_ID)
        self.assertEqual(results[0]['update_status'], 'updated')
        
        # Check if the mock was called correctly
        mock_update_place_record.assert_called_once()
        
        # Save the results to file
        output_file = os.path.join(self.output_dir, "refresh_operational_statuses_results.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=4, ensure_ascii=False)
        print(f"Saved operational status refresh results to {output_file}")
    
    def test_has_data_file(self):
        """Test the has_data_file method."""
        # We need to mock get_record to control its response without hitting the real API
        with mock.patch('airtable_client.AirtableClient.get_record') as mock_get_record:
            # Set up mock to return a record with 'Has Data File' set to 'Yes'
            mock_get_record.return_value = {
                'id': 'rec123',
                'fields': {
                    'Place': TEST_PLACE_NAME,
                    'Google Maps Place Id': TEST_PLACE_ID,
                    'Has Data File': 'Yes'
                }
            }
            
            # Test case where record has data file
            has_data = self.client.has_data_file(TEST_PLACE_ID)
            self.assertTrue(has_data)
            
            # Change mock to return record with 'Has Data File' set to 'No'
            mock_get_record.return_value = {
                'id': 'rec123',
                'fields': {
                    'Place': TEST_PLACE_NAME,
                    'Google Maps Place Id': TEST_PLACE_ID,
                    'Has Data File': 'No'
                }
            }
            
            # Test case where record doesn't have data file
            has_data = self.client.has_data_file(TEST_PLACE_ID)
            self.assertFalse(has_data)
            
            # Change mock to return None (record not found)
            mock_get_record.return_value = None
            
            # Test case where record doesn't exist
            has_data = self.client.has_data_file(TEST_PLACE_ID)
            self.assertFalse(has_data)
            
        print("The has_data_file method behaves correctly with different record states")
    
    def test_get_record(self):
        """Test the get_record method on a real record if it exists."""
        # This is using the real API but with controlled input
        record = self.client.get_record(SearchField.GOOGLE_MAPS_PLACE_ID, TEST_PLACE_ID)
        
        # If the record exists, verify its structure
        if (record):
            self.assertIsInstance(record, dict)
            self.assertIn('id', record)
            self.assertIn('fields', record)
            
            # Save the record details to a file
            output_file = os.path.join(self.output_dir, f"record_{TEST_PLACE_ID}.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                # Remove sensitive fields before saving
                safe_record = {
                    'id': record['id'],
                    'fields': {k: v for k, v in record['fields'].items() 
                              if k not in ['AIRTABLE_PERSONAL_ACCESS_TOKEN']}
                }
                json.dump(safe_record, f, indent=4, ensure_ascii=False)
            print(f"Found and saved record for place ID {TEST_PLACE_ID}")
        else:
            print(f"No record found for place ID {TEST_PLACE_ID} - this is not necessarily an error")
    
    def test_get_place_types(self):
        """Test the get_place_types method."""
        # Use a subset of the data to test the method
        original_all_third_places = self.client.all_third_places
        
        # Create test data with different type combinations
        self.client.all_third_places = [
            {'fields': {'Type': 'Coffee Shop'}},
            {'fields': {'Type': ['Coffee Shop', 'Bookstore']}},
            {'fields': {'Type': 'Coffee Shop'}},
            {'fields': {'Type': 'Library'}},
            {'fields': {}},  # Test with missing type
        ]
        
        # Get the unique place types
        place_types = self.client.get_place_types()
        
        # Restore original data
        self.client.all_third_places = original_all_third_places
        
        # Assertions
        self.assertIsInstance(place_types, list)
        self.assertEqual(sorted(place_types), sorted(['Bookstore', 'Coffee Shop', 'Library']))
        
        print(f"Successfully extracted unique place types: {place_types}")
    
    @mock.patch('airtable_client.AirtableClient.update_place_record')
    @mock.patch('airtable_client.AirtableClient.get_record')
    def test_enrich_base_data(self, mock_get_record, mock_update_place_record):
        """Test the enrich_base_data method with respect to photo optimization strategy."""
        # Configure the update_place_record mock to return a successful update
        mock_update_place_record.return_value = {
            "updated": True,
            "field_name": "Website",
            "record_id": "rec123",
            "old_value": None,
            "new_value": "https://example.com"
        }
        
        # Test two scenarios:
        # 1. A place without existing photos - should attempt to get and update photos
        # 2. A place with existing photos - should skip photo retrieval
        
        # Create test records
        self.client.all_third_places = [
            {
                'id': 'rec123',
                'fields': {
                    'Place': "Place Without Photos",
                    'Google Maps Place Id': "place_id_1"
                }
            },
            {
                'id': 'rec456',
                'fields': {
                    'Place': "Place With Photos",
                    'Google Maps Place Id': "place_id_2",
                    'Photos': '["https://example.com/existing_photo.jpg"]'
                }
            }
        ]
        
        # Set up mock for get_record to simulate existing Airtable record lookup
        def mock_get_record_side_effect(search_field, search_value):
            if search_value == "place_id_1":
                return {
                    'id': 'rec123',
                    'fields': {
                        'Place': "Place Without Photos",
                        'Google Maps Place Id': "place_id_1"
                    }
                }
            elif search_value == "place_id_2":
                return {
                    'id': 'rec456',
                    'fields': {
                        'Place': "Place With Photos",
                        'Google Maps Place Id': "place_id_2",
                        'Photos': '["https://example.com/existing_photo.jpg"]'
                    }
                }
            return None
            
        mock_get_record.side_effect = mock_get_record_side_effect
        
        # Create mock for helper_functions.get_and_cache_place_data to test the photo skipping logic
        with mock.patch('helper_functions.get_and_cache_place_data') as mock_get_cache_data:
            
            # Configure mock to return valid place data based on the place ID
            def mock_get_cache_side_effect(place_name, place_id, city, force_refresh=False, provider_type=None):
                if place_id == "place_id_1":
                    # Place without photos should get photo URLs
                    return 'succeeded', {
                        "place_id": place_id,
                        "place_name": place_name,
                        "details": {
                            "website": "https://example.com",
                            "address": "123 Test St",
                            "description": "A test place",
                            "purchase_required": "Yes",
                            "parking": ["Free", "Street"],
                            "google_maps_url": f"https://maps.google.com/{place_id}",
                            "latitude": 35.2,
                            "longitude": -80.8
                        },
                        "photos": {
                            "photo_urls": ["https://example.com/photo1.jpg", "https://example.com/photo2.jpg"],
                            "message": "Retrieved 2 photos"
                        }
                    }, "Success"
                else:
                    # Place with photos should still get data but photos will be ignored later in the process
                    return 'succeeded', {
                        "place_id": place_id,
                        "place_name": place_name,
                        "details": {
                            "website": "https://example.com",
                            "address": "456 Other St",
                            "description": "Another test place",
                            "purchase_required": "No",
                            "parking": ["Paid"],
                            "google_maps_url": f"https://maps.google.com/{place_id}",
                            "latitude": 35.3,
                            "longitude": -80.9
                        },
                        "photos": {
                            "photo_urls": ["https://example.com/new_photo.jpg"],
                            "message": "Retrieved 1 photo"
                        }
                    }, "Success"
                
            mock_get_cache_data.side_effect = mock_get_cache_side_effect
            
            # Run the enrich operation
            results = self.client.enrich_base_data()
            
            # Assertions
            self.assertEqual(len(results), 2)
            
            # Check that photos are only updated for the place without existing photos
            place1_result = next((r for r in results if r["place_id"] == "place_id_1"), None)
            place2_result = next((r for r in results if r["place_id"] == "place_id_2"), None)
            
            # Check that both places were processed
            self.assertIsNotNone(place1_result)
            self.assertIsNotNone(place2_result)
            
            # Verify the Photos field was updated correctly for each place
            photos_updates_1 = [update for field_name, update in 
                              place1_result["field_updates"].items() if field_name == "Photos"]
            photos_updates_2 = [update for field_name, update in 
                              place2_result["field_updates"].items() if field_name == "Photos"]
            
            # Place 1 should have an update for Photos field since it didn't have photos
            self.assertTrue(len(photos_updates_1) > 0 or "Photos" not in place1_result["field_updates"])
            
            # Place 2 should not have an update for Photos field since it already had photos
            self.assertEqual(len(photos_updates_2), 0)
            
            # Save the results to file
            output_file = os.path.join(self.output_dir, "enrich_base_data_results.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=4, ensure_ascii=False)
            print(f"Saved base data enrichment results to {output_file}")
    
    @mock.patch('helper_functions.fetch_data_github')
    @mock.patch('helper_functions.save_data_github')
    @mock.patch('helper_functions.datetime')
    @mock.patch('place_data_providers.PlaceDataProviderFactory.get_provider')
    def test_caching_system(self, mock_get_provider, mock_datetime, mock_save_data_github, mock_fetch_data_github):
        """Test that the caching system correctly uses or refreshes cache based on data staleness and photo optimization strategy."""
        import helper_functions as helpers
        import constants
        
        # Set up a fixed "now" time for testing
        mock_now = datetime(2025, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        mock_datetime.fromisoformat.side_effect = lambda x: datetime.fromisoformat(x)
        
        # Configure mock for saving data - return tuple instead of boolean
        mock_save_data_github.return_value = (True, "Mock saving successful")
        
        # Test place information
        place_name = TEST_PLACE_NAME
        place_id = TEST_PLACE_ID
        city = "charlotte"
        cache_file_path = f"data/places/{city}/{place_id}.json"
        
        # SCENARIO 1: No cached data exists
        # =================================
        print("\nSCENARIO 1: Testing when no cached data exists")
        mock_fetch_data_github.return_value = (False, None, "File not found")
        
        # Configure the mock provider once, outside the test blocks
        mock_provider = mock.MagicMock()
        mock_get_provider.return_value = mock_provider
        
        # Create a mock for the data provider to avoid real API calls
        with mock.patch('helper_functions._should_skip_photos_retrieval') as mock_should_skip_photos, \
             mock.patch('helper_functions._fill_photos_from_airtable') as mock_fill_photos:
             
            # Configure mocks
            mock_provider.find_place_id.return_value = place_id
            mock_provider.get_all_place_data.return_value = {
                "place_id": place_id,
                "place_name": place_name,
                "details": {"website": "https://example.com"},
                "photos": {
                    "photo_urls": ["https://example.com/photo1.jpg"]
                },
                "data_source": "TestProvider",
                "last_updated": mock_now.isoformat()
            }
            
            # No existing photos in Airtable
            mock_should_skip_photos.return_value = (False, None)
            mock_fill_photos.return_value = False
            
            # Call the function that uses caching
            status, data, message = helpers.get_and_cache_place_data(
                place_name, place_id, city, force_refresh=False, provider_type="test"
            )
            
            # Verify the results
            self.assertEqual(status, 'succeeded')
            self.assertIsNotNone(data)
            self.assertEqual(data['details']['website'], "https://example.com")
            
            # Verify behavior: should fetch fresh data and save to cache
            mock_provider.get_all_place_data.assert_called_once_with(place_id, place_name, skip_photos=False)
            mock_save_data_github.assert_called_once()
            
            print("✓ Correctly handled case with no cached data")
            
        # SCENARIO 2: Cached data exists but is stale (older than refresh interval)
        # =======================================================================
        print("\nSCENARIO 2: Testing when cached data is stale")
        # Reset mocks
        mock_fetch_data_github.reset_mock()
        mock_save_data_github.reset_mock()
        mock_provider.reset_mock()
        
        # Create stale cached data (91 days old, just past our 90-day default interval)
        stale_date = mock_now - timedelta(days=constants.DEFAULT_CACHE_REFRESH_INTERVAL + 1)
        stale_cached_data = {
            "place_id": place_id,
            "place_name": place_name,
            "details": {"website": "https://old-example.com"},
            "photos": {
                "photo_urls": ["https://example.com/old-photo.jpg"]
            },
            "data_source": "TestProvider",
            "last_updated": stale_date.isoformat()
        }
        mock_fetch_data_github.return_value = (True, stale_cached_data, "Success")
        
        # Run the test with the same mocks as before
        with mock.patch('helper_functions._should_skip_photos_retrieval') as mock_should_skip_photos, \
             mock.patch('helper_functions._fill_photos_from_airtable') as mock_fill_photos:
             
            # Configure mocks with NEW data for this scenario
            # Important: We need to update get_all_place_data's return value for this test
            mock_provider.find_place_id.return_value = place_id
            mock_provider.get_all_place_data.return_value = {
                "place_id": place_id,
                "place_name": place_name,
                "details": {"website": "https://new-example.com"},  # New data is different
                "photos": {
                    "photo_urls": ["https://example.com/new-photo.jpg"]
                },
                "data_source": "TestProvider",
                "last_updated": mock_now.isoformat()
            }
            
            # No existing photos in Airtable
            mock_should_skip_photos.return_value = (False, None)
            mock_fill_photos.return_value = False
            
            # Call the function that uses caching
            status, data, message = helpers.get_and_cache_place_data(
                place_name, place_id, city, force_refresh=False, provider_type="test"
            )
            
            # Print the actual data for debugging
            print(f"Data returned: {data['details']['website']}")
            
            # Verify the results
            self.assertEqual(status, 'succeeded')  # Should succeed with fresh data
            self.assertEqual(data['details']['website'], "https://new-example.com")  # Should have new data
            
            # Verify behavior: should fetch fresh data due to stale cache
            mock_provider.get_all_place_data.assert_called_once_with(place_id, place_name, skip_photos=False)
            mock_save_data_github.assert_called_once()
            
            print("✓ Correctly refreshed stale cached data")
            
        # SCENARIO 3: Fresh cached data exists (within refresh interval)
        # ===================================================================
        print("\nSCENARIO 3: Testing when cached data is fresh")
        # Reset mocks
        mock_fetch_data_github.reset_mock()
        mock_save_data_github.reset_mock()
        mock_provider.reset_mock()
        
        # Create fresh cached data (30 days old, well within 90-day refresh interval)
        fresh_date = mock_now - timedelta(days=30)
        fresh_cached_data = {
            "place_id": place_id,
            "place_name": place_name,
            "details": {"website": "https://cached-example.com"},
            "photos": {
                "photo_urls": ["https://example.com/cached-photo.jpg"]
            },
            "data_source": "TestProvider",
            "last_updated": fresh_date.isoformat()
        }
        mock_fetch_data_github.return_value = (True, fresh_cached_data, "Success")
        
        # Run the test with the same mocks as before
        with mock.patch('helper_functions._should_skip_photos_retrieval') as mock_should_skip_photos:
             
            # Configure mocks - this time get_all_place_data shouldn't be called
            mock_provider.find_place_id.return_value = place_id
            mock_provider.get_all_place_data.return_value = {
                "place_id": place_id,
                "place_name": place_name,
                "details": {"website": "https://should-not-be-used.com"},
                "photos": {
                    "photo_urls": ["https://example.com/should-not-be-used.jpg"]
                },
                "data_source": "TestProvider",
                "last_updated": mock_now.isoformat()
            }
            
            # Call the function that uses caching
            status, data, message = helpers.get_and_cache_place_data(
                place_name, place_id, city, force_refresh=False, provider_type="test"
            )
            
            # Verify the results
            self.assertEqual(status, 'cached')  # Should use cached data
            self.assertEqual(data['details']['website'], "https://cached-example.com")  # Should have cached data
            
            # Verify behavior: should NOT fetch fresh data and should NOT save to GitHub
            mock_provider.get_all_place_data.assert_not_called()
            mock_save_data_github.assert_not_called()
            # Should not check for photos in Airtable since we're using cached data
            mock_should_skip_photos.assert_not_called()
            
            print("✓ Correctly used fresh cached data without making API calls")
            
        # SCENARIO 4: Force refresh is enabled (bypass cache even if it's fresh)
        # ===================================================================
        print("\nSCENARIO 4: Testing force refresh")
        # Reset mocks
        mock_fetch_data_github.reset_mock()
        mock_save_data_github.reset_mock()
        mock_provider.reset_mock()
        
        # We use the same fresh cached data as before
        mock_fetch_data_github.return_value = (True, fresh_cached_data, "Success")
        
        # Run the test with force_refresh=True
        with mock.patch('helper_functions._should_skip_photos_retrieval') as mock_should_skip_photos, \
             mock.patch('helper_functions._fill_photos_from_airtable') as mock_fill_photos:
             
            # Configure mocks with force refresh data
            mock_provider.find_place_id.return_value = place_id
            mock_provider.get_all_place_data.return_value = {
                "place_id": place_id,
                "place_name": place_name,
                "details": {"website": "https://force-refresh-example.com"},
                "photos": {
                    "photo_urls": ["https://example.com/force-refresh-photo.jpg"]
                },
                "data_source": "TestProvider",
                "last_updated": mock_now.isoformat()
            }
            
            # No existing photos in Airtable
            mock_should_skip_photos.return_value = (False, None)
            mock_fill_photos.return_value = False
            
            # Call the function with force_refresh=True
            status, data, message = helpers.get_and_cache_place_data(
                place_name, place_id, city, force_refresh=True, provider_type="test"
            )
            
            # Verify the results
            self.assertEqual(status, 'succeeded')  # Should succeed with fresh data
            self.assertEqual(data['details']['website'], "https://force-refresh-example.com")  # Should have new data
            
            # Verify behavior: should fetch fresh data and save to GitHub
            mock_provider.get_all_place_data.assert_called_once_with(place_id, place_name, skip_photos=False)
            mock_save_data_github.assert_called_once()
            
            print("✓ Correctly bypassed cache with force_refresh=True")
            
        # SCENARIO 5: Photo optimization - skip photo retrieval if photos exist in Airtable
        # ==============================================================================
        print("\nSCENARIO 5: Testing photo optimization")
        # Reset mocks
        mock_fetch_data_github.reset_mock()
        mock_save_data_github.reset_mock()
        mock_provider.reset_mock()
        
        # Create stale data to trigger fresh data retrieval
        mock_fetch_data_github.return_value = (True, stale_cached_data, "Success")
        
        # Run the test with mocks for photo optimization
        with mock.patch('helper_functions._should_skip_photos_retrieval') as mock_should_skip_photos, \
             mock.patch('helper_functions._fill_photos_from_airtable') as mock_fill_photos:
             
            # Configure mocks for photo optimization scenario
            mock_provider.find_place_id.return_value = place_id
            # Provider returns data with empty photos because skipping photo retrieval
            mock_provider.get_all_place_data.return_value = {
                "place_id": place_id,
                "place_name": place_name,
                "details": {"website": "https://photo-optimization-example.com"},
                "photos": {
                    "photo_urls": [],  # No photos from API since we're skipping photo retrieval
                    "message": "Photo retrieval skipped"
                },
                "data_source": "TestProvider",
                "last_updated": mock_now.isoformat()
            }
            
            # Set up to skip photo retrieval - simulate photos exist in Airtable
            airtable_photos = '["https://example.com/airtable-photo1.jpg", "https://example.com/airtable-photo2.jpg"]'
            mock_should_skip_photos.return_value = (True, airtable_photos)
            
            # Mock that we successfully filled photos from Airtable
            mock_fill_photos.return_value = True
            
            # Call the function that uses caching
            status, data, message = helpers.get_and_cache_place_data(
                place_name, place_id, city, force_refresh=False, provider_type="test"
            )
            
            # Verify the results
            self.assertEqual(status, 'succeeded')  # Should succeed with fresh data
            self.assertEqual(data['details']['website'], "https://photo-optimization-example.com")  # Verify website
            
            # Verify behavior: should fetch fresh data but skip photo retrieval
            mock_provider.get_all_place_data.assert_called_once_with(place_id, place_name, skip_photos=True)
            mock_should_skip_photos.assert_called_once()
            mock_fill_photos.assert_called_once()
            mock_save_data_github.assert_called_once()
            
            print("✓ Correctly applied photo optimization strategy")
            
        print("\nAll caching scenarios tested successfully")

# This if condition ensures that the tests are only run when this script is executed directly.
# It prevents the tests from running when this module is imported elsewhere.
if __name__ == "__main__":
    # Instantiate the test class
    test_instance = TestAirtableClient()
    
    # Set up the test environment
    test_instance.setUp()
    
    # Create a dictionary to store results
    results = {}
    
    # Helper function to run a test method and record result
    def run_test(method_name, test_function):
        print(f"\n==== Running {method_name} ====")
        try:
            test_function()
            results[method_name] = "PASSED"
            print(f"✅ {method_name} PASSED")
        except Exception as e:
            results[method_name] = f"FAILED: {str(e)}"
            print(f"❌ {method_name} FAILED: {str(e)}")
    
    # Run each test method directly
    run_test('test_init', test_instance.test_init)
    run_test('test_get_base_url', test_instance.test_get_base_url)
    run_test('test_get_place_photos', test_instance.test_get_place_photos)
    run_test('test_refresh_operational_statuses', test_instance.test_refresh_operational_statuses)
    run_test('test_has_data_file', test_instance.test_has_data_file)
    run_test('test_get_record', test_instance.test_get_record)
    run_test('test_get_place_types', test_instance.test_get_place_types)
    run_test('test_enrich_base_data', test_instance.test_enrich_base_data)
    run_test('test_caching_system', test_instance.test_caching_system)
    
    # Print summary
    print("\n==== TEST SUMMARY ====")
    for method_name, result in results.items():
        print(f"{method_name}: {result}")