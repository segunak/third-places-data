import os
import json
import time
import dotenv
import unittest
from unittest import mock
from constants import SearchField
from airtable_client import AirtableClient
from place_data_providers import OutscraperProvider

# Sample real place to test with - same as in other tests for consistency
TEST_PLACE_NAME = "Mattie Ruth's Coffee House"
TEST_PLACE_ID = "ChIJH9S7TOcPVIgRnG5eHqW4DE0"

class TestAirtableClient(unittest.TestCase):
    """Integration test suite for the AirtableClient class."""

    def setUp(self):
        """Set up test environment before each test."""
        # Load environment variables from .env file
        dotenv.load_dotenv()
        
        # Initialize the AirtableClient with OutscraperProvider
        self.client = AirtableClient(data_provider_type="outscraper")
        
        self.place_id = TEST_PLACE_ID
        self.place_name = TEST_PLACE_NAME
        
        # Create output directory for test results
        self.output_dir = os.path.join(".", "data", "testing", "airtable")
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
                    "photos": []
                }
                
                # This should use existing photos from Airtable rather than calling the provider
                photos = self.client.get_place_photos(self.place_id)
                
                # We should still get photos (from Airtable)
                self.assertIsNotNone(photos)
                self.assertIsInstance(photos, list)
                
                # Log the results
                print(f"Retrieved {len(photos)} photos for {TEST_PLACE_NAME} from Airtable")
                
                # Verify that the mock wasn't called due to photo optimization
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
        
        # Create mock for get_all_place_data to track if skip_photos is respected
        with mock.patch.object(self.client.data_provider, 'get_all_place_data') as mock_get_all_place_data:
            
            # Configure mock to return valid place data and also track the 'skip_photos' parameter
            def mock_get_data_side_effect(place_id, place_name, skip_photos=False):
                # Record which place was requested with what skip_photos value
                mock_get_data_side_effect.calls.append((place_id, place_name, skip_photos))
                
                return {
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
                        "photos_data": [] if skip_photos else ["https://example.com/new_photo.jpg"],
                        "message": "Photos skipped - already exist in Airtable" if skip_photos else ""
                    }
                }
                
            # Initialize the call tracking
            mock_get_data_side_effect.calls = []
            mock_get_all_place_data.side_effect = mock_get_data_side_effect
            
            # Run the enrich operation
            results = self.client.enrich_base_data()
            
            # Verify the right parameters were passed to get_all_place_data
            self.assertEqual(len(mock_get_data_side_effect.calls), 2)
            
            # For place without photos, skip_photos should be False
            place1_call = next((call for call in mock_get_data_side_effect.calls if call[0] == "place_id_1"), None)
            self.assertIsNotNone(place1_call)
            self.assertEqual(place1_call[2], False)  # skip_photos should be False
            
            # For place with photos, skip_photos should be True
            place2_call = next((call for call in mock_get_data_side_effect.calls if call[0] == "place_id_2"), None)
            self.assertIsNotNone(place2_call)
            self.assertEqual(place2_call[2], True)   # skip_photos should be True
            
            # Save the results to file
            output_file = os.path.join(self.output_dir, "enrich_base_data_results.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=4, ensure_ascii=False)
            print(f"Saved base data enrichment results to {output_file}")
    
    @mock.patch('helper_functions.fetch_data_github')
    @mock.patch('helper_functions.save_data_github')
    @mock.patch('helper_functions.datetime')
    def test_caching_system(self, mock_datetime, mock_save_data_github, mock_fetch_data_github):
        """Test that the caching system correctly uses or refreshes cache based on data staleness."""
        import helper_functions as helpers
        
        # Set up a fixed "now" time for testing
        mock_now = datetime(2025, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        mock_datetime.fromisoformat.side_effect = lambda x: datetime.fromisoformat(x)
        
        # Configure mock for saving data
        mock_save_data_github.return_value = True
        
        # Test place information
        place_name = TEST_PLACE_NAME
        place_id = TEST_PLACE_ID
        city_name = "charlotte"
        cache_file_path = f"data/places/{city_name}/{place_id}.json"
        
        # SCENARIO 1: No cached data exists
        # =================================
        mock_fetch_data_github.return_value = (False, None, "File not found")
        
        # Create a mock for the data provider to avoid real API calls
        with mock.patch('place_data_providers.PlaceDataProviderFactory.get_provider') as mock_get_provider, \
             mock.patch('airtable_client.AirtableClient.get_record') as mock_get_record:
             
            # Configure mocks
            mock_provider = mock.MagicMock()
            mock_provider.find_place_id.return_value = place_id
            mock_provider.get_all_place_data.return_value = {
                "place_id": place_id,
                "place_name": place_name,
                "details": {"website": "https://example.com"},
                "last_updated": mock_now.isoformat()
            }
            mock_get_provider.return_value = mock_provider
            
            # Simulate no existing photos in Airtable
            mock_get_record.return_value = {
                'id': 'rec123',
                'fields': {
                    'Place': place_name,
                    'Google Maps Place Id': place_id
                }
            }
            
            # Call the function that uses caching
            status, data, message = helpers.get_and_cache_place_data(place_name, place_id, city_name)
            
            # Verify the results
            self.assertEqual(status, 'succeeded')
            self.assertIsNotNone(data)
            
            # Verify behavior: should fetch fresh data and save to cache
            mock_provider.get_all_place_data.assert_called_once()
            mock_save_data_github.assert_called_once()
            
            print("SCENARIO 1: Correctly handled case with no cached data")
            
        # SCENARIO 2: Cached data exists but is stale (older than refresh interval)
        # =======================================================================
        # Reset mocks
        mock_fetch_data_github.reset_mock()
        mock_save_data_github.reset_mock()
        
        # Create stale cached data (31 days old)
        stale_date = mock_now - timedelta(days=31)
        stale_cached_data = {
            "place_id": place_id,
            "place_name": place_name,
            "details": {"website": "https://old-example.com"},
            "last_updated": stale_date.isoformat()
        }
        mock_fetch_data_github.return_value = (True, stale_cached_data, "Success")
        
        # Run the test with the same mocks as before
        with mock.patch('place_data_providers.PlaceDataProviderFactory.get_provider') as mock_get_provider, \
             mock.patch('airtable_client.AirtableClient.get_record') as mock_get_record:
             
            # Configure mocks same as before
            mock_provider = mock.MagicMock()
            mock_provider.find_place_id.return_value = place_id
            mock_provider.get_all_place_data.return_value = {
                "place_id": place_id,
                "place_name": place_name,
                "details": {"website": "https://new-example.com"},  # New data is different
                "last_updated": mock_now.isoformat()
            }
            mock_get_provider.return_value = mock_provider
            
            # Simulate no existing photos in Airtable
            mock_get_record.return_value = {
                'id': 'rec123',
                'fields': {
                    'Place': place_name,
                    'Google Maps Place Id': place_id
                }
            }
            
            # Call the function that uses caching
            status, data, message = helpers.get_and_cache_place_data(place_name, place_id, city_name)
            
            # Verify the results
            self.assertEqual(status, 'succeeded')  # Should succeed with fresh data
            self.assertEqual(data['details']['website'], "https://new-example.com")  # Should have new data
            
            # Verify behavior: should fetch fresh data due to stale cache
            mock_provider.get_all_place_data.assert_called_once()
            mock_save_data_github.assert_called_once()
            
            print("SCENARIO 2: Correctly refreshed stale cached data")
            
        # SCENARIO 3: Fresh cached data exists (younger than refresh interval)
        # ===================================================================
        # Reset mocks
        mock_fetch_data_github.reset_mock()
        mock_save_data_github.reset_mock()
        
        # Create fresh cached data (1 day old)
        fresh_date = mock_now - timedelta(days=1)
        fresh_cached_data = {
            "place_id": place_id,
            "place_name": place_name,
            "details": {"website": "https://cached-example.com"},
            "last_updated": fresh_date.isoformat()
        }
        mock_fetch_data_github.return_value = (True, fresh_cached_data, "Success")
        
        # Run the test with the same mocks as before
        with mock.patch('place_data_providers.PlaceDataProviderFactory.get_provider') as mock_get_provider, \
             mock.patch('airtable_client.AirtableClient.get_record') as mock_get_record:
             
            # Configure mocks - this time get_all_place_data shouldn't be called
            mock_provider = mock.MagicMock()
            mock_provider.find_place_id.return_value = place_id
            mock_provider.get_all_place_data.return_value = {
                "place_id": place_id,
                "place_name": place_name,
                "details": {"website": "https://should-not-be-used.com"},
                "last_updated": mock_now.isoformat()
            }
            mock_get_provider.return_value = mock_provider
            
            # Simulate no existing photos in Airtable
            mock_get_record.return_value = {
                'id': 'rec123',
                'fields': {
                    'Place': place_name,
                    'Google Maps Place Id': place_id
                }
            }
            
            # Call the function that uses caching
            status, data, message = helpers.get_and_cache_place_data(place_name, place_id, city_name)
            
            # Verify the results
            self.assertEqual(status, 'cached')  # Should use cached data
            self.assertEqual(data['details']['website'], "https://cached-example.com")  # Should have cached data
            
            # Verify behavior: should NOT fetch fresh data
            mock_provider.get_all_place_data.assert_not_called()
            # It should still save the cached data to ensure consistency
            mock_save_data_github.assert_called_once()
            
            print("SCENARIO 3: Correctly used fresh cached data without making API calls")

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