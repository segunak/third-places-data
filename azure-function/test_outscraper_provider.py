"""
Integration tests for the OutscraperDataProvider class from place_data_providers.py.
"""

import os
import json
import dotenv
import unittest
from constants import DEFAULT_REVIEWS_LIMIT
from place_data_providers import OutscraperProvider

# Sample real place to test with
TEST_PLACE_NAME = "Mattie Ruth's Coffee House"
TEST_PLACE_ID = "ChIJH9S7TOcPVIgRnG5eHqW4DE0"

class TestOutscraperProvider(unittest.TestCase):
    """Integration test suite for the OutscraperProvider class."""

    def setUp(self):
        """Set up test environment before each test."""
        # Load environment variables from .env file
        dotenv.load_dotenv()
        
        # Initialize the real OutscraperProvider
        self.provider = OutscraperProvider()
        
        self.place_id = TEST_PLACE_ID
        self.place_name = TEST_PLACE_NAME
    
    def test_init(self):
        """Test the initialization of OutscraperProvider."""
        self.assertIsNotNone(self.provider.API_KEY)
        self.assertIsNotNone(self.provider.GOOGLE_MAPS_API_KEY)
        self.assertIsNotNone(self.provider.client)
    
    def test_get_place_details(self):
        """Test the get_place_details method."""
        details = self.provider.get_place_details(self.place_id)
        
        # Test should fail if no details are returned
        self.assertIsNotNone(details, "Place details response is None")
        self.assertTrue(details, "Place details response is empty")
        self.assertEqual(details['place_id'], self.place_id)
        print(f"Got details for {details['place_name']}.")
        
        # Create the directory if it doesn't exist
        output_dir = os.path.join(".", "data", "testing", "outscraper")
        os.makedirs(output_dir, exist_ok=True)
        
        # Write the results to a JSON file in data/testing/outscraper directory
        output_file = os.path.join(output_dir, f"details_{self.place_id}.json")
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
        
        # Create the directory if it doesn't exist
        output_dir = os.path.join(".", "data", "testing", "outscraper")
        os.makedirs(output_dir, exist_ok=True)
        
        # Write the results to a JSON file in data/testing/outscraper directory
        output_file = os.path.join(output_dir, f"reviews_{self.place_id}.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(reviews, f, indent=4, ensure_ascii=False)
        print(f"Saved place reviews to {output_file}")
    
    def test_get_place_photos(self):
        """Test the get_place_photos method."""
        photos = self.provider.get_place_photos(self.place_id)
        
        # Test should fail if no photos are returned
        self.assertIsNotNone(photos, "Place photos response is None")

        # Assert that photos_data exists in photos
        self.assertTrue('photos_data' in photos, "Missing photos_data field in response")

        # Create the directory if it doesn't exist
        output_dir = os.path.join(".", "data", "testing", "outscraper")
        os.makedirs(output_dir, exist_ok=True)
        
        # Write the results to a JSON file in data/testing/outscraper directory
        output_file = os.path.join(output_dir, f"photos_{self.place_id}.json")
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
    
    # Print summary
    print("\n==== TEST SUMMARY ====")
    for method_name, result in results.items():
        print(f"{method_name}: {result}")
