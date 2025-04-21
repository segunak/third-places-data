"""
Integration tests for the OutscraperDataProvider class from place_data_providers.py.
"""

import os
import json
import dotenv
import unittest
import sys
from datetime import datetime

# Add parent directory to path so we can import from parent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from constants import DEFAULT_REVIEWS_LIMIT
from place_data_providers import OutscraperProvider

# Sample real place to test with
TEST_PLACE_NAME = "Mattie Ruth's Coffee House"
TEST_PLACE_ID = "ChIJH9S7TOcPVIgRnG5eHqW4DE0"

class TestOutscraperProvider(unittest.TestCase):
    """Integration test Suite for the OutscraperProvider class."""

    def setUp(self):
        """Set up test environment before each test."""
        # Load environment variables from .env file
        dotenv.load_dotenv()
        
        # Initialize the real OutscraperProvider
        self.provider = OutscraperProvider()
        
        self.place_id = TEST_PLACE_ID
        self.place_name = TEST_PLACE_NAME
        
        # Create output directory for test results - relative to parent directory
        self.output_dir = os.path.join("..", "data", "testing", "outscraper")
        os.makedirs(self.output_dir, exist_ok=True)
    
    def test_init(self):
        """Test the initialization of OutscraperProvider."""
        self.assertIsNotNone(self.provider.API_KEY)
        self.assertIsNotNone(self.provider.GOOGLE_MAPS_API_KEY)
        self.assertIsNotNone(self.provider.client)
        self.assertEqual(self.provider.provider_type, 'outscraper')
    
    def test_get_place_details(self):
        """Test the get_place_details method."""
        details = self.provider.get_place_details(self.place_id)
        
        # Test should fail if no details are returned
        self.assertIsNotNone(details, "Place details response is None")
        self.assertTrue(details, "Place details response is empty")
        self.assertEqual(details['place_id'], self.place_id)
        print(f"Got details for {details['place_name']}.")
        
        # Write the results to a JSON file 
        output_file = os.path.join(self.output_dir, f"details_{self.place_id}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(details, f, indent=4, ensure_ascii=False)
        print(f"Saved place details to {output_file}")
    
    def test_get_place_reviews(self):
        """Test the get_place_reviews method."""
        reviews = self.provider.get_place_reviews(self.place_id)
        
        # Test should fail if no reviews are returned or if structure is invalid
        self.assertIsNotNone(reviews, "Place reviews response is None")
        self.assertTrue(reviews, "Place reviews response is empty")
        self.assertEqual(reviews['place_id'], self.place_id)
        self.assertTrue('reviews_data' in reviews, "Missing reviews_data field in response")
        
        # Check if we got reviews and log the count
        if 'reviews_data' in reviews and reviews['reviews_data']:
            print(f"Got {len(reviews['reviews_data'])} reviews for {TEST_PLACE_NAME}")
            # Ensure we have at least one review
            self.assertGreater(len(reviews['reviews_data']), 0, "No reviews found")
        else:
            self.fail("No reviews_data found in response")
        
        # Write the results to a JSON file
        output_file = os.path.join(self.output_dir, f"reviews_{self.place_id}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(reviews, f, indent=4, ensure_ascii=False)
        print(f"Saved place reviews to {output_file}")
    
    def test_get_place_photos(self):
        """Test the get_place_photos method."""
        photos = self.provider.get_place_photos(self.place_id)
        
        # Test should fail if no photos are returned
        self.assertIsNotNone(photos, "Place photos response is None")
        self.assertIn('place_id', photos, "Response doesn't have place_id field")
        self.assertIn('photo_urls', photos, "Response doesn't have photo_urls field")
        
        # Get the actual photo URLs array
        photo_urls = photos.get('photo_urls', [])
        print(f"Got {len(photo_urls)} photo URLs for {TEST_PLACE_NAME}")

        # Write the results to a JSON file
        output_file = os.path.join(self.output_dir, f"photos_{self.place_id}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(photos, f, indent=4, ensure_ascii=False)
        print(f"Saved place photos to {output_file}")
    
    def test_find_place_id(self):
        """Test the find_place_id method."""
        place_id = self.provider.find_place_id(TEST_PLACE_NAME)
        
        self.assertIsNotNone(place_id)
        self.assertEqual(place_id, TEST_PLACE_ID, f"Expected place ID for {TEST_PLACE_NAME} to be 'ChIJH9S7TOcPVIgRnG5eHqW4DE0', but got '{place_id}'")
        
        print(f"Found the correct place ID for {TEST_PLACE_NAME}: {place_id}")
    
    def test_is_place_operational(self):
        """Test the is_place_operational method."""
        is_operational = self.provider.is_place_operational(self.place_id)

        self.assertIsNotNone(is_operational, "Operational status is None")
        self.assertIsInstance(is_operational, bool, "Operational status is not a boolean")

        # Report operational status
        print(f"{TEST_PLACE_NAME} is {'operational' if is_operational else 'not operational'}")
        
    def test_all_place_data(self):
        """Test the get_all_place_data method."""
        all_data = self.provider.get_all_place_data(self.place_id, self.place_name)
        
        # Ensure we got a valid response with key fields
        self.assertIsNotNone(all_data, "All place data response is None")
        self.assertEqual(all_data.get('place_id', ''), self.place_id)
        self.assertEqual(all_data.get('place_name', ''), self.place_name)
        self.assertIn('details', all_data, "Response doesn't have details field")
        self.assertIn('reviews', all_data, "Response doesn't have reviews field")
        self.assertIn('photos', all_data, "Response doesn't have photos field")
        self.assertIn('data_source', all_data, "Response doesn't have data_source field") 
        self.assertIn('last_updated', all_data, "Response doesn't have last_updated field")
        
        # Check that the data_source field correctly identifies the provider
        self.assertEqual(all_data.get('data_source', ''), 'OutscraperProvider')
        
        print(f"Got all data for {TEST_PLACE_NAME}")
        
        # Write the results to a JSON file
        output_file = os.path.join(self.output_dir, f"all_data_{self.place_id}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, indent=4, ensure_ascii=False)
        print(f"Saved all place data to {output_file}")

# This if condition ensures that the tests are only run when this script is executed directly.
# It prevents the tests from running when this module is imported elsewhere.
if __name__ == "__main__":
    # Instantiate the test class
    test_instance = TestOutscraperProvider()
    
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
    run_test('test_get_place_details', test_instance.test_get_place_details)
    run_test('test_get_place_reviews', test_instance.test_get_place_reviews)
    run_test('test_get_place_photos', test_instance.test_get_place_photos)
    run_test('test_find_place_id', test_instance.test_find_place_id)
    run_test('test_is_place_operational', test_instance.test_is_place_operational)
    run_test('test_all_place_data', test_instance.test_all_place_data)
    
    # Print summary
    print("\n==== TEST SUMMARY ====")
    for method_name, result in results.items():
        print(f"{method_name}: {result}")
