import os
import re
import json
import dotenv
import base64
import logging
import requests
import unicodedata
from unidecode import unidecode
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient
from typing import Dict, Optional, Tuple
from threading import Lock
from constants import DEFAULT_CACHE_REFRESH_INTERVAL, SearchField
from place_data_providers import PlaceDataProviderFactory

def normalize_text(text: str) -> str:
    """
    Normalize the text to ensure consistent encoding, formatting, and case.
    This function will:
    1. Normalize Unicode characters to NFC form (Normalization Form C).
    2. Strip leading/trailing spaces.
    3. Remove newlines.
    4. Compress multiple spaces into one.
    5. Convert the text to lowercase for case-insensitive comparison.

    Args:
        text (str): The input text to normalize.

    Returns:
        str: The normalized string.
    """
    if isinstance(text, str):
        # Step 1: Normalize the text using NFC (Normalization Form C)
        text = unicodedata.normalize('NFC', text)

        # Step 2: Strip leading/trailing spaces and normalize whitespace
        text = re.sub(r'\s+', ' ', text.strip().lower())

    return text

def save_reviews_azure(json_data, review_file_name):
    """
    Save review data as a JSON file to Azure Data Lake Storage.

    This function connects to Azure Data Lake Storage using the connection string 
    from the environment variable 'AzureWebJobsStorage'. It uploads the provided 
    JSON data into a file under the 'reviews' directory in the 'data' filesystem.

    Args:
        json_data (str): JSON-formatted string containing the review data.
        review_file_name (str): The name of the file to save in the 'reviews' directory.
    """
    try:
        # Retrieve the Azure Data Lake connection string
        datalake_connection_string = os.environ['AzureWebJobsStorage']
        logging.info("Retrieved Azure Data Lake connection string.")

        # Initialize the Data Lake Service Client
        datalake_service_client = DataLakeServiceClient.from_connection_string(datalake_connection_string)
        logging.info("Initialized DataLakeServiceClient.")

        # Get the file system and directory clients
        file_system_client = datalake_service_client.get_file_system_client(file_system="data")
        directory_client = file_system_client.get_directory_client("reviews")

        # Get the file client and upload data
        file_client = directory_client.get_file_client(review_file_name)
        file_client.upload_data(data=json_data, overwrite=True)
        logging.info(f"Successfully uploaded {review_file_name} to Azure Data Lake.")
    except Exception as e:
        logging.error(f"An unexpected error occurred while saving '{review_file_name}'.")
        logging.exception(e)


def save_data_github(json_data: str, full_file_path: str, max_retries=3):
    """ Saves the given JSON data to the specified file path in the GitHub repository.
    Implements retry logic for handling 409 Conflict errors.

    Args:
        json_data (str): The JSON data to save
        full_file_path (str): Path to save the file, e.g., "data/places/charlotte/SomePlaceId.json"
        max_retries (int, optional): Maximum number of retry attempts for conflict errors. Defaults to 3.
    
    Returns:
        tuple: (success, message)
            - success (bool): True if the operation was successful, False otherwise
            - message (str): Success or error message
    """
    import time
    
    github_token = os.environ.get('GITHUB_PERSONAL_ACCESS_TOKEN')
    if not github_token:
        return False, "GitHub token not found in environment variables"
        
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    repo_name = "segunak/third-places-data"
    branch = "master"
    
    # Base URLs for GitHub API requests
    url_get = f"https://api.github.com/repos/{repo_name}/contents/{full_file_path}?ref={branch}"
    url_put = f"https://api.github.com/repos/{repo_name}/contents/{full_file_path}"
    
    encoded_content = base64.b64encode(json_data.encode()).decode()
    
    for attempt in range(max_retries + 1):
        try:
            # Step 1: Get current SHA if file exists
            logging.info(f"Getting SHA for {full_file_path} (attempt {attempt+1}/{max_retries+1})")
            get_response = requests.get(url_get, headers=headers)
            
            sha = None
            if get_response.status_code == 200:
                sha = get_response.json().get('sha')
                
            # Step 2: Prepare data for creating/updating file
            data = {
                "message": "Saving JSON file via save_data_github utility function",
                "content": encoded_content,
                "branch": branch
            }
            if sha:
                data['sha'] = sha  # Include SHA only if updating an existing file
            
            # Step 3: Make the request to create/update file
            put_response = requests.put(url_put, headers=headers, data=json.dumps(data, indent=4))
            
            # Step 4: Handle the response
            if put_response.status_code in {200, 201}:
                return True, f"File saved successfully to GitHub at {full_file_path}"
                
            # Handle 409 Conflict error with retries
            if put_response.status_code == 409 and attempt < max_retries:
                logging.warning(f"GitHub API returned 409 Conflict. Retrying ({attempt+1}/{max_retries})...")
                time.sleep(1 * (attempt + 1))  # Increasing backoff
                continue
                
            # Any other error
            return False, f"GitHub API returned status code {put_response.status_code}: {put_response.text}"
                
        except Exception as e:
            if attempt < max_retries:
                logging.warning(f"Error during save attempt {attempt+1}: {str(e)}. Retrying...")
                time.sleep(1 * (attempt + 1))
                continue
            return False, f"Failed to save to GitHub: {str(e)}"
    
    return False, "Maximum retries exceeded while attempting to save file to GitHub"

def fetch_data_github(full_file_path) -> Tuple[bool, Optional[Dict], str]:
    """
    Fetches JSON data from the specified file path in the GitHub repository.
    Handles both standard-sized files and large files (>1MB).
    Implements retry logic for handling network and API errors.

    Args:
        full_file_path (str): Path to the file in the GitHub repository.
            For example: "data/places/charlotte/SomePlaceId.json"

    Returns:
        tuple: (success, data, message)
            - success (bool): Whether the operation was successful
            - data (dict or None): The fetched data as a Python dictionary, or None if unsuccessful
            - message (str): A message describing the result of the operation
    """
    from requests.adapters import HTTPAdapter
    from urllib3.util import Retry
    
    # Create a session with retry mechanism
    session = requests.Session()
    retry_strategy = Retry(
        total=3,  # Total number of retries
        backoff_factor=1,  # Wait 1, 2, 4 seconds between retries
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on these status codes
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    
    try:
        # Get GitHub auth and repo details
        github_token = os.environ['GITHUB_PERSONAL_ACCESS_TOKEN']
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        repo_name = "segunak/third-places-data"
        branch = "master"
        
        # Get the file metadata
        url_get = f"https://api.github.com/repos/{repo_name}/contents/{full_file_path}?ref={branch}"
        logging.info(f"Fetching file from GitHub: {full_file_path}")
        
        # Get file metadata response
        get_response = session.get(url_get, headers=headers, timeout=30)
        
        if get_response.status_code != 200:
            if get_response.status_code == 404:
                return False, None, f"File {full_file_path} not found in repository"
            else:
                return False, None, f"Failed to fetch file: {get_response.status_code}"
        
        # Parse the response
        content_info = get_response.json()
        
        # Get file content based on size
        file_content = None
        if content_info["type"] != "file":
            return False, None, f"Path {full_file_path} does not point to a file"
            
        # For large files, GitHub uses download_url instead of content
        if content_info.get("encoding") == "none" or not content_info.get("content"):
            if "download_url" in content_info:
                download_response = session.get(content_info["download_url"], headers=headers, timeout=60)
                if download_response.status_code == 200:
                    file_content = download_response.text
                else:
                    return False, None, f"Failed to download large file: Status {download_response.status_code}"
            else:
                return False, None, "Large file detected but no download_url provided"
        else:
            # Normal case: decode the base64 content
            file_content = base64.b64decode(content_info["content"]).decode('utf-8')
        
        # Check for empty content
        if not file_content or not file_content.strip():
            return False, None, f"Empty file content received from GitHub for {full_file_path}"
            
        # Parse the JSON content
        try:
            parsed_json = json.loads(file_content)
            return True, parsed_json, "File fetched successfully"
        except json.JSONDecodeError as je:
            # Log a preview of the content to help diagnose issues
            preview = file_content[:200] + "..." if len(file_content) > 200 else file_content
            logging.error(f"JSON parsing error for {full_file_path}: {str(je)}\nContent preview: {preview}")
            return False, None, f"JSON parsing error: {str(je)}"
            
    except requests.RequestException as e:
        return False, None, f"Network error while fetching from GitHub: {str(e)}"
    except Exception as e:
        logging.error(f"Failed to fetch from GitHub: {str(e)}", exc_info=True)
        return False, None, f"Failed to fetch from GitHub: {str(e)}"

def is_cache_valid(cached_data: Dict, refresh_interval_days: int) -> bool:
    """
    Checks if the cached data is still valid based on the last updated timestamp.

    Args:
        cached_data (Dict): The cached data containing a 'last_updated' timestamp
        refresh_interval_days (int): Number of days after which the cache should be refreshed

    Returns:
        bool: True if the cache is still valid, False if it's stale and needs to be refreshed
    """
    try:
        # Get the last_updated timestamp from the cached data
        last_updated_str = cached_data.get('last_updated')
        if not last_updated_str:
            logging.warning("No last_updated timestamp found in cached data")
            return False
        
        # Parse the timestamp
        last_updated = datetime.fromisoformat(last_updated_str)
        
        # Calculate the age of the cache
        now = datetime.now()
        cache_age = now - last_updated
        
        # Check if the cache is still valid
        return cache_age.days < refresh_interval_days
    
    except Exception as e:
        logging.error(f"Error checking cache validity: {str(e)}")
        return False

def _fill_photos_from_airtable(place_data: Dict, photos_json: str) -> bool:
    """
    Fill the photos field in place_data with photos from Airtable.
    
    Args:
        place_data: The place data to update
        photos_json: JSON string containing photo URLs
        
    Returns:
        bool: True if photos were successfully filled, False otherwise
    """
    try:
        # Parse the JSON string
        photos = json.loads(photos_json)
        
        # Update the place_data with the photos
        if 'photos' not in place_data:
            place_data['photos'] = {}
            
        place_data['photos']['photo_urls'] = photos
        place_data['photos']['message'] = "Retrieved from Airtable"
        
        return True
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing photos JSON from Airtable: {e}")
        return False
    except Exception as e:
        logging.error(f"Error filling photos from Airtable: {e}")
        return False

def get_and_cache_place_data(provider_type: str, place_name: str, place_id: str = None,
                              city: str = None, force_refresh: bool = False) -> Tuple[str, Dict, str]:
    """
    Get place data, either from cache or by calling data provider, then cache the result.
    
    This function implements the photo optimization strategy where we skip retrieving photos
    if they already exist in Airtable. This helps conserve API calls and bandwidth.
    
    Args:
        provider_type: Type of data provider to use ('google' or 'outscraper')
        place_name: Name of the place to get data for
        place_id: Google Maps Place Id (optional, will be looked up if not provided)
        city: The city to use for caching (default: 'charlotte')
        force_refresh: Whether to force refresh cached data (bypass cache)
        
    Returns:
        tuple: (status, data, message) where status is one of:
            - 'succeeded': Fresh data was retrieved and cached
            - 'cached': Cached data was used
            - 'failed': Data retrieval failed
    """
    if not city:
        return 'failed', None, 'Missing required parameter: city'
    try:
        # Validate provider_type
        if not provider_type:
            error_msg = f"Cannot get place data for {place_name} - provider_type not specified"
            logging.error(error_msg)
            return 'failed', None, error_msg
            
        # Create the provider explicitly
        data_provider = PlaceDataProviderFactory.get_provider(provider_type)
        
        # If place_id wasn't provided, find it using the data provider
        if not place_id:
            logging.info(f"No place_id provided for {place_name}, looking up...")
            place_id = data_provider.find_place_id(place_name)
            if not place_id:
                return 'failed', None, f"Could not find place ID for {place_name}"
            logging.info(f"Found place_id {place_id} for {place_name}")
        
        # Define the path for the cached file
        cached_file_path = f"data/places/{city}/{place_id}.json"
        
        # Set up the data retrieval parameters
        skip_photos = False
        existing_photos_json = None
        
        try:
            from airtable_client import AirtableClient
            # Get the AirtableClient
            airtable_client = AirtableClient(provider_type)
            
            # Check if the place exists in Airtable
            record = airtable_client.get_record(SearchField.GOOGLE_MAPS_PLACE_ID, place_id)
            
            # If the record exists and has photos, skip photo retrieval
            if record and 'Photos' in record['fields'] and record['fields']['Photos']:
                logging.info(f"Place {place_id} already has photos in Airtable, skipping photo retrieval")
                skip_photos = True
                existing_photos_json = record['fields']['Photos']
        except Exception as e:
            logging.error(f"Error checking if photos should be skipped: {e}")
            # In case of error, don't skip photo retrieval
            skip_photos = False
            existing_photos_json = None
        
        # Check if we have cached data and if it's still valid
        if not force_refresh:
            success, cached_data, message = fetch_data_github(cached_file_path)
            if success and is_cache_valid(cached_data, DEFAULT_CACHE_REFRESH_INTERVAL):
                logging.info(f"Using cached data for {place_name} from {cached_file_path}")
                return 'cached', cached_data, f"Using cached data from {cached_file_path}"
        
        # If we reach here, we need to fetch fresh data, there is no valid cache or we are forcing a refresh
        logging.info(f"Getting fresh data from API provider {provider_type} for {place_name} with place_id {place_id}. The value of skip_photos is {skip_photos} and the value of force_refresh is {force_refresh}.")

        # Get place data from provider, skipping photos if we already have them
        place_data = data_provider.get_all_place_data(place_id, place_name, skip_photos=skip_photos)
        
        # If we're skipping photos but have existing photos in Airtable, use those
        if skip_photos and existing_photos_json:
            _fill_photos_from_airtable(place_data, existing_photos_json)
        
        # Add timestamp for future cache validity check
        place_data['last_updated'] = datetime.now().isoformat()
        
        # Save to GitHub
        success, message = save_data_github(json.dumps(place_data, indent=4), cached_file_path)
        if not success:
            logging.warning(f"Failed to cache data for {place_name}: {message}")
        
        return 'succeeded', place_data, f"Successfully retrieved fresh data for {place_name}"
        
    except Exception as e:
        logging.error(f"Error getting place data for {place_name}: {e}", exc_info=True)
        return 'failed', None, f"Error: {str(e)}"

def create_place_response(operation_status, target_place_name, http_response_data, operation_message):
    """
    Create a standard response for place operations.
    
    Args:
        operation_status: Status of the operation ('succeeded', 'cached', 'failed')
        target_place_name: Name of the place
        http_response_data: Any HTTP response data to include
        operation_message: A detailed message about the operation
        
    Returns:
        dict: A dictionary that includes the operation status, place name, any response data, and a detailed message.
    """
    if operation_status == 'failed':
        logging.warning(operation_message)
    else:
        logging.info(operation_message)

    return {
        'status': operation_status,
        'place_name': target_place_name,
        'response': http_response_data,
        'message': operation_message
    }

def refresh_all_photos(provider_type: str = 'outscraper', city: str = 'charlotte', dry_run: bool = True, max_places: int = None) -> dict:
    """
    Refresh photo selections across all places from cached data files.
    
    This function reads from cached GitHub data files (not live APIs) and:
    1. Gets all places from Airtable 
    2. Reads cached data files from GitHub for each place
    3. Extracts photos from existing photos.raw_data in cache
    4. Applies the improved photo selection algorithm with 30-photo limit
    5. Updates Airtable "Photos" field with overwrite=True
    6. Updates only the photos section in data files (preserves other data)
    
    Args:
        provider_type (str): Provider to use for photo selection algorithm ('google' or 'outscraper')
        city (str): City to process (default: 'charlotte')
        dry_run (bool): If True, only logs what would be done without making changes
        max_places (int): Maximum number of places to process (None for all)
        
    Returns:
        dict: Summary of processing results
    """
    from airtable_client import AirtableClient
    from place_data_providers import PlaceDataProviderFactory
    import json
    import time
    
    try:
        airtable_client = AirtableClient(provider_type)
        data_provider = PlaceDataProviderFactory.get_provider(provider_type)
        
        # Get photo selection method from OutscraperProvider (has the most advanced algorithm)
        if hasattr(data_provider, '_select_prioritized_photos'):
            photo_selector = data_provider._select_prioritized_photos
        else:
            logging.error(f"Provider {provider_type} does not have _select_prioritized_photos method")
            return {"status": "failed", "message": "Photo selection method not available"}
            
        logging.info(f"Starting photo refresh process - Provider: {provider_type}, City: {city}, Dry Run: {dry_run}")
        
    except Exception as e:
        logging.error(f"Failed to initialize components: {e}")
        return {"status": "failed", "message": f"Initialization error: {str(e)}"}
    
    # Get all places from Airtable
    try:
        all_places = airtable_client.all_third_places
        if not all_places:
            return {"status": "failed", "message": "No places found in Airtable"}
            
        # Limit processing if max_places specified
        if max_places and max_places > 0:
            all_places = all_places[:max_places]
            logging.info(f"Limited processing to {max_places} places")
            
        logging.info(f"Found {len(all_places)} places to process")
        
    except Exception as e:
        logging.error(f"Failed to get places from Airtable: {e}")
        return {"status": "failed", "message": f"Airtable error: {str(e)}"}
    
    # Processing results tracking
    results = {
        "status": "completed",
        "dry_run": dry_run,
        "total_places": len(all_places),
        "processed": 0,
        "updated": 0,
        "skipped": 0,
        "errors": 0,
        "error_details": [],
        "place_results": []
    }
    
    # Process each place
    for idx, place_record in enumerate(all_places, 1):
        place_result = {
            "place_name": "",
            "place_id": "",
            "record_id": place_record['id'],
            "status": "",
            "message": "",
            "photos_before": 0,
            "photos_after": 0
        }
        
        try:
            fields = place_record['fields']
            place_name = fields.get('Place', 'Unknown')
            place_id = fields.get('Google Maps Place Id', '')
            
            place_result["place_name"] = place_name
            place_result["place_id"] = place_id
            
            logging.info(f"Processing {idx}/{len(all_places)}: {place_name} ({place_id})")
            
            # Validate required fields
            if not place_id:
                place_result["status"] = "skipped"
                place_result["message"] = "No Google Maps Place Id"
                results["skipped"] += 1
                results["place_results"].append(place_result)
                continue
            
            # Read data file from GitHub
            data_file_path = f"data/places/{city}/{place_id}.json"
            success, place_data, message = fetch_data_github(data_file_path)
            
            if not success:
                place_result["status"] = "error"
                place_result["message"] = f"Failed to read data file: {message}"
                results["errors"] += 1
                results["error_details"].append(f"{place_name}: {message}")
                results["place_results"].append(place_result)
                continue
            # Extract raw photos data with dual-path parsing
            photos_section = place_data.get('photos', {})
            raw_data = photos_section.get('raw_data', [])
            
            if not raw_data:
                place_result["status"] = "skipped"
                place_result["message"] = "No raw photos data found"
                results["skipped"] += 1
                results["place_results"].append(place_result)
                continue

            photo_list = []
            parse_method = "unknown"
            
            try:
                # Method 1: raw_data is a direct list of photo dicts
                if isinstance(raw_data, list) and raw_data:
                    # Check if first item has photo_url_big (validates it's a photo dict)
                    if isinstance(raw_data[0], dict) and 'photo_url_big' in raw_data[0]:
                        photo_list = raw_data
                        parse_method = "direct_list"
                        logging.info(f"Using direct list parsing for {place_name}")
                    else:
                        logging.warning(f"Direct list parsing failed for {place_name} - items don't appear to be photo dicts")
                
                # Method 2: raw_data is a dict with photos_data property
                if not photo_list and isinstance(raw_data, dict):
                    photos_data = raw_data.get('photos_data', [])
                    if isinstance(photos_data, list) and photos_data:
                        # Check if first item has photo_url_big
                        if isinstance(photos_data[0], dict) and 'photo_url_big' in photos_data[0]:
                            photo_list = photos_data
                            parse_method = "nested_dict"
                            logging.info(f"Using nested dict parsing for {place_name}")
                        else:
                            logging.warning(f"Nested dict parsing failed for {place_name} - items don't appear to be photo dicts")
                
                if not photo_list:
                    place_result["status"] = "error"
                    place_result["message"] = "Could not parse raw photos data - no valid structure found"
                    results["errors"] += 1
                    results["error_details"].append(f"{place_name}: Unable to parse raw_data structure")
                    results["place_results"].append(place_result)
                    continue
                    
            except Exception as e:
                place_result["status"] = "error"
                place_result["message"] = f"Error parsing raw photos data: {str(e)}"
                results["errors"] += 1
                results["error_details"].append(f"{place_name}: Raw data parsing error - {str(e)}")
                results["place_results"].append(place_result)
                continue
            
            # Get current photo count
            current_photos = photos_section.get('photo_urls', [])
            place_result["photos_before"] = len(current_photos)
            
            logging.info(f"Found {len(photo_list)} raw photo data records for {place_name} (method: {parse_method})")
            
            # Apply photo selection algorithm (same logic as OutscraperProvider)
            try:
                # First validate and filter photos using centralized validation
                valid_photos = []
                for photo in photo_list:
                    photo_url = photo.get('photo_url_big', '')
                    if data_provider._is_valid_photo_url(photo_url):
                        valid_photos.append(photo)
                
                logging.info(f"Filtered to {len(valid_photos)} valid photos for {place_name}")
                
                # Apply selection algorithm to get up to 30 photos
                selected_photo_urls = photo_selector(valid_photos, max_photos=30)
                place_result["photos_after"] = len(selected_photo_urls)
                
                logging.info(f"Selected {len(selected_photo_urls)} photos for {place_name}")
                
                if not selected_photo_urls:
                    logging.info(f"No valid photos selected for {place_name}, skipping update")
                    place_result["status"] = "skipped"
                    place_result["message"] = "No valid photos after selection"
                    results["skipped"] += 1
                    results["place_results"].append(place_result)
                    continue
                
            except Exception as e:
                place_result["status"] = "error"
                place_result["message"] = f"Photo selection failed: {str(e)}"
                results["errors"] += 1
                results["error_details"].append(f"{place_name}: Photo selection error - {str(e)}")
                results["place_results"].append(place_result)
                continue
            
            # Update data if not dry run
            if not dry_run:
                try:
                    # Update Airtable Photos field with overwrite=True
                    photos_json = json.dumps(selected_photo_urls)
                    update_result = airtable_client.update_place_record(
                        record_id=place_record['id'],
                        field_to_update='Photos',
                        update_value=photos_json,
                        overwrite=True
                    )
                    
                    if not update_result.get('updated', False):
                        place_result["status"] = "error"
                        place_result["message"] = "Failed to update Airtable"
                        results["errors"] += 1
                        results["error_details"].append(f"{place_name}: Airtable update failed")
                        results["place_results"].append(place_result)
                        continue
                    
                    # Update photos section in data file (preserve other data)
                    place_data['photos']['photo_urls'] = selected_photo_urls
                    place_data['photos']['message'] = f"Photos refreshed by admin function using {provider_type} selection algorithm"
                    place_data['photos']['last_refreshed'] = datetime.now().isoformat()
                    
                    # Save updated data file to GitHub
                    updated_json = json.dumps(place_data, indent=4)
                    save_success, save_message = save_data_github(updated_json, data_file_path)
                    
                    if not save_success:
                        place_result["status"] = "error"
                        place_result["message"] = f"Airtable updated but GitHub save failed: {save_message}"
                        results["errors"] += 1
                        results["error_details"].append(f"{place_name}: GitHub save failed - {save_message}")
                        results["place_results"].append(place_result)
                        continue
                    
                    place_result["status"] = "updated"
                    place_result["message"] = f"Successfully updated with {len(selected_photo_urls)} photos"
                    results["updated"] += 1
                    
                    # Add delay to avoid hitting API rate limits
                    time.sleep(1)
                    
                except Exception as e:
                    place_result["status"] = "error"
                    place_result["message"] = f"Update failed: {str(e)}"
                    results["errors"] += 1
                    results["error_details"].append(f"{place_name}: Update error - {str(e)}")
                    results["place_results"].append(place_result)
                    continue
            else:
                # Dry run - just log what would be done
                place_result["status"] = "would_update"
                place_result["message"] = f"Would update with {len(selected_photo_urls)} photos"
                results["updated"] += 1
            
            results["processed"] += 1
            results["place_results"].append(place_result)
            
            logging.info(f"Completed {place_name}: {place_result['status']} - {place_result['message']}")
            
        except Exception as e:
            place_result["status"] = "error"
            place_result["message"] = f"Unexpected error: {str(e)}"
            results["errors"] += 1
            results["error_details"].append(f"{place_result.get('place_name', 'Unknown')}: {str(e)}")
            results["place_results"].append(place_result)
            logging.error(f"Error processing place {idx}: {e}", exc_info=True)
            continue
    
    # Log summary
    logging.info(f"""
        Photo Refresh Summary:
        - Total Places: {results['total_places']}
        - Processed: {results['processed']}
        - Updated: {results['updated']}
        - Skipped: {results['skipped']}
        - Errors: {results['errors']}
        - Dry Run: {dry_run}
    """)
    
    if results['errors'] > 0:
        logging.warning("Errors encountered:")
        for error in results['error_details']:
            logging.warning(f"  - {error}")
    
    return results
