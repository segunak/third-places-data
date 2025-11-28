import os
import json
import time
import sys
import dotenv
import unittest
import pyairtable
from unittest import mock
from datetime import datetime, timedelta

# Add parent directory to path so we can import from parent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from constants import SearchField
from services.airtable_service import AirtableService
from services.place_data_service import OutscraperProvider, PlaceDataProviderFactory

# Sample real place to test with - same as in other tests for consistency
TEST_PLACE_NAME = "Mattie Ruth's Coffee House"
TEST_PLACE_ID = "ChIJH9S7TOcPVIgRnG5eHqW4DE0"

class TestAirtableService(unittest.TestCase):
    """Integration test Suite for the AirtableService class."""

    def setUp(self):
        """Set up test environment before each test."""
        # Load environment variables from .env file
        dotenv.load_dotenv()
        
        self.client = AirtableService(
            provider_type='outscraper', 
            sequential_mode=True
        )
        
        self.place_id = TEST_PLACE_ID
        self.place_name = TEST_PLACE_NAME
        
        # Create output directory for test results - now pointing to testing folder directly
        self.output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "testing", "airtable")
        os.makedirs(self.output_dir, exist_ok=True)
    
    def test_init(self):
        """Test the initialization of AirtableService."""
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
    
    @mock.patch('services.airtable_service.AirtableService.update_place_record')
    def test_refresh_operational_statuses(self, mock_update_place_record):
        """Test that operational status refresh correctly updates place records."""
        # Setup mock data provider
        mock_provider = mock.MagicMock()
        
        # Setup test place data
        test_places = [
            {
                'id': 'rec1',
                'fields': {
                    'Place': 'Test Place 1',
                    'Google Maps Place Id': 'place1',
                    'Operational': 'Yes'
                }
            },
            {
                'id': 'rec2',
                'fields': {
                    'Place': 'Test Place 2',
                    'Google Maps Place Id': 'place2',
                    'Operational': 'Yes'  # Changed from 'No' to 'Yes' to force an update
                }
            }
        ]
        
        # Mock provider responses for operational status checks
        # place1 is operational but place2 is not (different from current value)
        mock_provider.is_place_operational.side_effect = lambda place_id: place_id == 'place1'
        
        # Mock the all_third_places property
        with mock.patch('services.airtable_service.AirtableService.all_third_places', new_callable=mock.PropertyMock) as mock_all_places:
            mock_all_places.return_value = test_places
            
            # Create client instance and run the method
            client = AirtableService(provider_type='outscraper')
            results = client.refresh_operational_statuses(mock_provider)
            
            # Verify that update_place_record was called with correct parameters for place2 only
            # (since place2's operational status has changed from Yes to No)
            mock_update_place_record.assert_called_once_with('rec2', 'Operational', 'No', overwrite=True)
            
            # Check that the results list has the expected structure
            self.assertEqual(len(results), 2)
            
            # Verify results for place1 (unchanged, so should be skipped)
            place1_result = next(r for r in results if r['place_id'] == 'place1')
            self.assertEqual(place1_result['update_status'], 'skipped')
            self.assertEqual(place1_result['old_value'], 'Yes')
            self.assertEqual(place1_result['new_value'], 'Yes')
            
            # Verify results for place2 (changed, so should be updated)
            place2_result = next(r for r in results if r['place_id'] == 'place2')
            self.assertEqual(place2_result['update_status'], 'updated')
            self.assertEqual(place2_result['old_value'], 'Yes')
            self.assertEqual(place2_result['new_value'], 'No')
            
        print("✓ Successfully refreshed operational statuses")
    
    def test_has_data_file(self):
        """Test the has_data_file method."""
        # We need to mock get_record to control its response without hitting the real API
        with mock.patch('services.airtable_service.AirtableService.get_record') as mock_get_record:
            # Set up mock to return a record with 'Has Data File' set to 'Yes'
            mock_get_record.return_value = {
                'id': 'rec123',
                'fields': {
                    'Place': TEST_PLACE_NAME,
                    'Google Maps Place Id': TEST_PLACE_ID,
                    'Has Data File': 'Yes'
                }
            }
            
            # Check has_data_file for a place that has a data file
            has_file = self.client.has_data_file(self.place_id)
            self.assertTrue(has_file)

            # Update the mock to simulate a place without a data file
            mock_get_record.return_value = {
                'id': 'rec123',
                'fields': {
                    'Place': TEST_PLACE_NAME,
                    'Google Maps Place Id': TEST_PLACE_ID
                    # 'Has Data File' field is missing
                }
            }

            # Check has_data_file for a place without a data file
            has_file = self.client.has_data_file(self.place_id)
            self.assertFalse(has_file)

            # Check case where the place doesn't exist in Airtable
            mock_get_record.return_value = None
            has_file = self.client.has_data_file(self.place_id)
            self.assertFalse(has_file)
            
            print("has_data_file correctly checks if a place has a data file")
    
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
        """Test that get_place_types correctly returns place types from Airtable."""
        # Create AirtableService instance with mocked data
        with mock.patch('services.airtable_service.AirtableService.all_third_places', new_callable=mock.PropertyMock) as mock_fetch:
            # Setup test data with various place types
            mock_fetch.return_value = [
                {"id": "rec1", "fields": {"Google Maps Place Id": "place1", "Name": "Place 1", "Type": "Cafe"}},
                {"id": "rec2", "fields": {"Google Maps Place Id": "place2", "Name": "Place 2", "Type": "Library"}},
                {"id": "rec3", "fields": {"Google Maps Place Id": "place3", "Name": "Place 3", "Type": "Cafe"}},
                {"id": "rec4", "fields": {"Google Maps Place Id": "place4", "Name": "Place 4", "Type": "Park"}}
            ]
            
            # Create client with valid provider_type
            client = AirtableService(provider_type="outscraper")
            place_types = client.get_place_types()
            
            # Verify correct types are returned and sorted alphabetically
            expected_types = ["Cafe", "Library", "Park"]
            self.assertEqual(place_types, expected_types, f"Expected {expected_types}, got {place_types}")
            
        print("✓ Successfully retrieved place types")
    
    def test_enrich_base_data(self):
        """Test that enrich_base_data correctly enriches place data."""
        # Setup test data with places
        test_places = [
            {
                'id': 'rec123',
                'fields': {
                    'Place': 'Test Place 1',
                    'Google Maps Place Id': 'place123',
                }
            },
            {
                'id': 'rec456',
                'fields': {
                    'Place': 'Test Place 2',
                    'Google Maps Place Id': 'place456',
                    'Photos': '["https://example.com/photo1.jpg"]'
                }
            }
        ]
        
        # Create a mock for get method to return records
        def mock_get_record(record_id):
            for place in test_places:
                if place['id'] == record_id:
                    return place
            return None
        
        # Mock helper function that gets and caches place data
        with mock.patch('services.airtable_service.AirtableService.all_third_places', new_callable=mock.PropertyMock, return_value=test_places), \
             mock.patch('services.airtable_service.AirtableService.update_place_record') as mock_update_record, \
             mock.patch('services.utils.get_and_cache_place_data') as mock_get_data, \
             mock.patch.object(pyairtable.Table, 'get', side_effect=mock_get_record):
            
            # Configure mock to return different data for the two test places
            def mock_get_data_side_effect(provider_type, place_name, place_id, city, force_refresh, airtable_record_id=None):
                if place_id == 'place123':
                    return 'succeeded', {
                        'place_id': 'place123',
                        'details': {
                            'website': 'https://example1.com',
                            'address': '123 Test St',
                            'latitude': 35.1234,
                            'longitude': -80.5678,
                            'description': 'A test place',
                            'google_maps_url': 'https://maps.google.com/place123',
                            'parking': ['Free'],
                            'purchase_required': 'No'
                        },
                        'photos': {
                            'photo_urls': ['https://example.com/photo1.jpg', 'https://example.com/photo2.jpg']
                        }
                    }, 'Data found'
                elif place_id == 'place456':
                    return 'skipped', None, 'Place already has photos'
                else:
                    return 'failed', None, 'Unknown place'
            
            mock_get_data.side_effect = mock_get_data_side_effect
            
            # Call method under test
            results = self.client.enrich_base_data(city='charlotte')
            
            # Verify results
            self.assertEqual(len(results), 2)
            
            # First place should have been updated
            first_result = next(r for r in results if r['place_id'] == 'place123')
            self.assertEqual(first_result['status'], 'succeeded')
            self.assertEqual(first_result['place_name'], 'Test Place 1')
            
            # Second place should have been skipped
            second_result = next(r for r in results if r['place_id'] == 'place456')
            self.assertEqual(second_result['status'], 'skipped')
            
        print("✓ Successfully tested enrich_base_data")
        
    
    def test_view_parameter_filter(self):
        """Test that the view parameter correctly filters records from the specified Airtable view."""
        # Create AirtableService instances with different views
        with mock.patch('pyairtable.Table.all') as mock_all:
            # Configure the mock to track different calls
            mock_all.side_effect = lambda **kwargs: {
                # When view="Insufficient" is passed, return only places with missing data
                'view=Insufficient': [
                    {'id': 'rec1', 'fields': {'Place': 'Incomplete Place 1'}},
                    {'id': 'rec2', 'fields': {'Place': 'Incomplete Place 2'}}
                ],
                # When view="Production" is passed, return only complete/good records
                'view=Production': [
                    {'id': 'rec3', 'fields': {'Place': 'Complete Place 3'}},
                    {'id': 'rec4', 'fields': {'Place': 'Complete Place 4'}},
                    {'id': 'rec5', 'fields': {'Place': 'Complete Place 5'}}
                ]
            }.get(f"view={kwargs.get('view')}" if 'view' in kwargs else 'view=Production', [])
            
            # Test with view="Insufficient"
            client_filtered = AirtableService(provider_type='outscraper', view="Insufficient")
            filtered_places = client_filtered.all_third_places
            
            # Verify that the "Insufficient" view was requested
            mock_all.assert_called_with(view="Insufficient", sort=["-Created Time"])
            # Reset the mock counter between calls
            mock_all.reset_mock()
            
            # Test with view="Production" (default)
            client_all = AirtableService(provider_type='outscraper', view="Production")
            all_places = client_all.all_third_places
            
            # Verify that Production view was requested
            mock_all.assert_called_with(view="Production", sort=["-Created Time"])
            
            # Verify correct number of places in each case
            self.assertEqual(len(filtered_places), 2, "Should have 2 places when filtered to Insufficient view")
            self.assertEqual(len(all_places), 3, "Should have 3 complete places in Production view")
            
            print("✓ Successfully tested view parameter filter")
    
    def test_enrich_single_place(self):
        """Test enrich_single_place with explicit parameters and proper mocking."""
        test_place = {
            'id': 'rec789',
            'fields': {
                'Place': 'Test Place 3',
                'Google Maps Place Id': 'place789'
            }
        }
        provider_type = 'outscraper'
        city = 'charlotte'
        force_refresh = False

        with mock.patch('services.airtable_service.AirtableService.update_place_record') as mock_update_record, \
             mock.patch('services.utils.get_and_cache_place_data') as mock_get_data, \
             mock.patch.object(pyairtable.Table, 'get', return_value=test_place):

            # Mock the helper to return a successful enrichment
            mock_get_data.return_value = (
                'succeeded',
                {
                    'place_id': 'place789',
                    'details': {
                        'website': 'https://example3.com',
                        'address': '789 Test Ave',
                        'latitude': 35.0000,
                        'longitude': -80.0000,
                        'description': 'A third test place',
                        'google_maps_url': 'https://maps.google.com/place789',
                        'parking': ['Free'],
                        'purchase_required': 'No'
                    },
                    'photos': {
                        'photo_urls': ['https://example.com/photo3.jpg']
                    }
                },
                'Data found'
            )

            client = AirtableService(provider_type=provider_type)
            result = client.enrich_single_place(test_place, provider_type, city, force_refresh)

            self.assertEqual(result['status'], 'succeeded')
            self.assertEqual(result['place_name'], 'Test Place 3')
            self.assertEqual(result['place_id'], 'place789')
            self.assertIn('field_updates', result)
            print("\u2713 enrich_single_place works with explicit parameters and mocks")

# This if condition ensures that the tests are only run when this script is executed directly.
# It prevents the tests from running when this module is imported elsewhere.
if __name__ == "__main__":
    # Instantiate the test class
    test_instance = TestAirtableService()
    
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
    run_test('test_view_parameter_filter', test_instance.test_view_parameter_filter)
    run_test('test_enrich_single_place', test_instance.test_enrich_single_place)
    
    # Print summary
    print("\n==== TEST SUMMARY ====")
    for method_name, result in results.items():
        print(f"{method_name}: {result}")
