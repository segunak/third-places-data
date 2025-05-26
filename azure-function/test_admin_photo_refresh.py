# Test script for photo refresh function
import os
import sys
import logging

# Add parent directory to path so we can import from parent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_admin_photo_refresh():
    """Test the administrative photo refresh function with dry run"""
    
    try:
        import helper_functions as helpers
        
        print("Testing administrative photo refresh function...")
        print("=" * 60)
          # Test with dry run and limited places
        results = helpers.refresh_all_photos(
            provider_type='outscraper',
            city='charlotte',
            dry_run=True,
            max_places=3  # Test with just 3 places
        )
        
        print("\nTest Results:")
        print(f"Status: {results.get('status')}")
        print(f"Dry Run: {results.get('dry_run')}")
        print(f"Total Places: {results.get('total_places')}")
        print(f"Processed: {results.get('processed')}")
        print(f"Would Update: {results.get('updated')}")
        print(f"Skipped: {results.get('skipped')}")
        print(f"Errors: {results.get('errors')}")
        
        if results.get('errors', 0) > 0:
            print("\nError Details:")
            for error in results.get('error_details', []):
                print(f"  - {error}")
        
        print("\nPlace Results (first 3):")
        for i, place_result in enumerate(results.get('place_results', [])[:3]):
            print(f"  {i+1}. {place_result.get('place_name', 'Unknown')}")
            print(f"     Status: {place_result.get('status')}")
            print(f"     Message: {place_result.get('message')}")
            print(f"     Photos Before: {place_result.get('photos_before', 0)}")
            print(f"     Photos After: {place_result.get('photos_after', 0)}")
            print()
        
        # Test successful
        if results.get('status') == 'completed':
            print("✅ Test PASSED - Administrative photo refresh function works correctly")
            return True
        else:
            print("❌ Test FAILED - Function did not complete successfully")
            return False
            
    except Exception as e:
        print(f"❌ Test FAILED with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Administrative Photo Refresh Function Test")
    print("This test will run the function in dry-run mode with 3 places")
    print()
    
    # Load environment variables for testing
    try:
        import dotenv
        dotenv.load_dotenv()
        print("Environment variables loaded")
    except ImportError:
        print("Warning: dotenv not available, ensure environment variables are set")
    
    success = test_admin_photo_refresh()
    
    if success:
        print("\n🎉 All tests completed successfully!")
        print("\nNext steps:")
        print("1. The Azure Function endpoint is now available at /refresh-all-photos")
        print("2. To run a real update (not dry run), set dry_run=false in the request")
        print("3. Monitor the logs for detailed processing information")
    else:
        print("\n⚠️  Tests failed - check the error messages above")
    
    sys.exit(0 if success else 1)
