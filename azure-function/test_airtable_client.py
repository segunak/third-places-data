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
        """Test the get_place_photos method."""
        # This tests our updated method that uses the data provider
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
        if record:
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
    def test_enrich_base_data(self, mock_update_place_record):
        """Test the enrich_base_data method using mocks to prevent real updates."""
        # Configure the mock to return a successful update
        mock_update_place_record.return_value = {
            "updated": True,
            "field_name": "Website",
            "record_id": "rec123",
            "old_value": None,
            "new_value": "https://example.com"
        }
        
        # Create a mini sample dataset for testing
        self.client.all_third_places = [
            {
                'id': 'rec123',
                'fields': {
                    'Place': TEST_PLACE_NAME,
                    'Google Maps Place Id': TEST_PLACE_ID
                }
            }
        ]
        
        # Create mock for get_all_place_data to return test data
        with mock.patch.object(self.client.data_provider, 'get_all_place_data') as mock_get_all_place_data, \
             mock.patch.object(self.client.data_provider, 'place_id_handler', return_value=TEST_PLACE_ID):
            
            # Configure mock to return valid place data
            mock_get_all_place_data.return_value = {
                "place_id": TEST_PLACE_ID,
                "place_name": TEST_PLACE_NAME,
                "details": {
                    "website": "https://example.com",
                    "address": "123 Test St",
                    "neighborhood": "Test Neighborhood",
                    "description": "A test place",
                    "purchase_required": "Yes",
                    "parking": ["Free", "Street"],
                    "google_maps_url": "https://maps.google.com/test",
                    "latitude": 35.2,
                    "longitude": -80.8
                },
                "photos": {
                    "photos_data": ["https://example.com/photo1.jpg"]
                }
            }
            
            # Run the enrich operation
            results = self.client.enrich_base_data()
            
            # Assertions - we should have one place that was processed
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]['place_name'], TEST_PLACE_NAME)
            self.assertEqual(results[0]['place_id'], TEST_PLACE_ID)
            
            # Save the results to file
            output_file = os.path.join(self.output_dir, "enrich_base_data_results.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=4, ensure_ascii=False)
            print(f"Saved base data enrichment results to {output_file}")

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
    
    # Print summary
    print("\n==== TEST SUMMARY ====")
    for method_name, result in results.items():
        print(f"{method_name}: {result}")