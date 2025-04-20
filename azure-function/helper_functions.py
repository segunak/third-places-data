import os
import re
import json
import dotenv
import base64
import logging
import requests
import unicodedata
from unidecode import unidecode
from datetime import datetime, timedelta
from azure.storage.filedatalake import DataLakeServiceClient
from typing import Iterable, Callable, Any, List, Dict, Optional, Tuple, Union
from threading import Lock

# Global client singleton instances and locks for thread safety
_airtable_client_instance = None
_airtable_client_lock = Lock()
_place_data_provider_instances = {}
_place_data_provider_lock = Lock()

def get_airtable_client(sequential_mode=False, provider_type=None):
    """
    Returns a singleton instance of AirtableClient.
    This ensures that only one instance is created and reused across all threads.
    
    Args:
        sequential_mode (bool): When True, forces sequential execution in the AirtableClient. Default is False.
        provider_type (str): REQUIRED. Must be 'google' or 'outscraper'.
    Returns:
        AirtableClient: A singleton instance configured with the specified sequential mode and provider type.
    """
    global _airtable_client_instance
    if _airtable_client_instance is None:
        with _airtable_client_lock:
            if _airtable_client_instance is None:
                from airtable_client import AirtableClient
                if not provider_type:
                    raise ValueError("AirtableClient requires provider_type to be specified ('google' or 'outscraper').")
                _airtable_client_instance = AirtableClient(provider_type=provider_type, sequential_mode=sequential_mode)
                logging.info("Created singleton AirtableClient instance")
                if sequential_mode:
                    logging.info("AirtableClient running in with SEQUENTIAL execution")
    elif sequential_mode and not _airtable_client_instance.sequential_mode:
        logging.warning("Existing AirtableClient instance is not in sequential mode.")
    return _airtable_client_instance

def get_place_data_provider(provider_type=None):
    """
    Returns a singleton instance of a PlaceDataProvider based on the requested type.
    This ensures that only one instance of each provider type is created and reused across all threads.
    
    Args:
        provider_type (str): The type of provider to get ('google', 'outscraper').
                             This is now REQUIRED and must not be None.
    Returns:
        PlaceDataProvider: A singleton instance of the requested provider.
    """
    global _place_data_provider_instances


    if provider_type not in _place_data_provider_instances:
        with _place_data_provider_lock:
            if provider_type not in _place_data_provider_instances:
                from place_data_providers import PlaceDataProviderFactory
                _place_data_provider_instances[provider_type] = PlaceDataProviderFactory.get_provider(provider_type)
                logging.info(f"Created singleton PlaceDataProvider instance for {provider_type}")

    return _place_data_provider_instances[provider_type]

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


def save_data_github(json_data, full_file_path, max_retries=3):
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
    
    # Prepare the encoded content once outside the retry loop
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
            put_response = requests.put(url_put, headers=headers, data=json.dumps(data))
            
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

def get_and_cache_place_data(place_name: str, place_id: str, city: str = "charlotte", force_refresh: bool = False, provider_type: str = None) -> Tuple[str, Dict, str]:
    """
    Centralized function for retrieving and caching place data with a cache-first approach.
    
    This function implements the cache-first logic:
    1. Check if place has cached data in the GitHub repository
    2. If cache exists and is fresh (within configured interval), use the cached data
    3. If cache doesn't exist or is stale, fetch fresh data from the API provider
    4. Store the data in GitHub only if fresh data was retrieved or modifications were made
    
    Args:
        place_name (str): Name of the place
        place_id (str): Google Maps Place ID
        city (str): City name for the file path (defaults to "charlotte") 
        force_refresh (bool): If True, bypass the cache and always fetch fresh data
        provider_type (str): REQUIRED. Must be 'google' or 'outscraper'.
        
    Returns:
        tuple: (status, place_data, message)
            - status: 'succeeded', 'cached', 'skipped', or 'failed'
            - place_data: The retrieved place data or None if failed
            - message: A descriptive message about the operation outcome
    """
    import constants
    from place_data_providers import PlaceDataProviderFactory

    try:
        # No need to check/normalize provider_type here; let the factory handle it
        data_provider = get_place_data_provider(provider_type)
        logging.info(f"Processing data for place: {place_name}")

        # Step 1: Validate place ID
        if not place_id:
            place_id = data_provider.find_place_id(place_name)
            if not place_id:
                return ('skipped', None, f"Warning! No place_id found for {place_name}. Skipping data retrieval.")

        # Step 2: Define cache path and settings
        cache_file_path = f"data/places/{city}/{place_id}.json"
        refresh_interval = constants.DEFAULT_CACHE_REFRESH_INTERVAL
        
        # Step 3: Try to use cached data if not forcing a refresh
        place_data = None
        if not force_refresh:
            logging.info(f"Checking for cached data for {place_name} at {cache_file_path}. Force refresh disabled.")
            place_data = _try_get_cached_data(place_name, cache_file_path, refresh_interval)
            
            # If we got valid cached data, return it
            if place_data:
                logging.info(f"get_and_cache_place_data: Returning cached data for {place_name} (last updated: {place_data.get('last_updated')}). Data provider API calls and GitHub write skipped.")
                return ('cached', place_data, f"get_and_cache_place_data: Using cached data for {place_name}. Data provider API calls and GitHub write skipped. Returning cached data.")
        else:
            logging.info(f"Force refresh enabled. Skipping cache check for {place_name}")
        
        # Step 4: Get fresh data if no valid cache exists
        # First check if the place already has photos in Airtable to avoid API costs
        skip_photos, airtable_photos = _should_skip_photos_retrieval(place_id, place_name)
            
        # Retrieve fresh data
        logging.info(f"Retrieving fresh data for {place_name} with place_id {place_id}")
        place_data = data_provider.get_all_place_data(place_id, place_name, skip_photos=skip_photos)
            
        # Check for API errors
        if not place_data or 'error' in place_data:
            error_message = place_data.get('error', 'Unknown error') if place_data else 'No data retrieved'
            logging.error(f"Data retrieval failed for {place_name} with place_id {place_id}: {error_message}")
            return ('failed', None, f"Error: Data retrieval failed for place {place_name} with place_id {place_id}. {error_message}")
        
        # Step 5: Fill in photos from Airtable if we skipped photo retrieval
        modifications_made = False
        if skip_photos and airtable_photos and "photos" in place_data:
            modifications_made = _fill_photos_from_airtable(place_data, airtable_photos, place_name)
        
        # Log photo status before saving to GitHub
        if "photos" in place_data and place_data["photos"].get("photo_urls"):
            photo_count = len(place_data["photos"]["photo_urls"])
            photo_source = "Airtable" if modifications_made else "API provider"
            logging.info(f"Saving place data with {photo_count} photos from {photo_source} to GitHub for {place_name}")
        else:
            logging.info(f"Saving place data with no photos to GitHub for {place_name}")
        
        # Step 6: Save the data to GitHub
        final_json_data = json.dumps(place_data, indent=4)
        logging.info(f"Saving place data to GitHub at path {cache_file_path}")
        save_succeeded, save_message = save_data_github(final_json_data, cache_file_path)
        logging.info(f"GitHub save operation result: {save_message}")

        if save_succeeded:
            return ('succeeded', place_data, f"Data retrieved from provider and saved successfully for {place_name}.")
        else:
            return ('failed', None, f"Failed to save data to GitHub for {place_name}. {save_message}")
            
    except Exception as e:
        logging.error(f"Error in get_and_cache_place_data for {place_name}: {e}", exc_info=True)
        return ('failed', None, f"Error processing place data: {str(e)}")

def _try_get_cached_data(place_name: str, cache_file_path: str, refresh_interval: int) -> Optional[Dict]:
    """
    Try to retrieve and validate cached data from GitHub.
    
    Args:
        place_name: Name of the place for logging
        cache_file_path: Path to the cached file
        refresh_interval: Number of days before cache is considered stale
        
    Returns:
        Dict if valid cached data exists, None otherwise
    """
    success, cached_data, message = fetch_data_github(cache_file_path)
    
    if not success or not cached_data:
        logging.info(f"No cached data found for {place_name}: {message}")
        return None
        
    # Check if the cache is still valid
    if is_cache_valid(cached_data, refresh_interval):
        logging.info(f"Using cached data for {place_name} (last updated: {cached_data.get('last_updated')})")
        return cached_data
    else:
        logging.info(f"Cached data for {place_name} is stale. Fetching fresh data.")
        return None

def _should_skip_photos_retrieval(place_id: str, place_name: str) -> Tuple[bool, Optional[str]]:
    """
    Determine if photo retrieval should be skipped based on existing Airtable data.
    
    Args:
        place_id: Google Maps Place ID
        place_name: Name of the place for logging
        
    Returns:
        Tuple of (skip_photos, airtable_photos), where:
        - skip_photos is True if photos should be skipped
        - airtable_photos is the photos data from Airtable if available
    """
    import constants
    try:
        airtable = get_airtable_client()
        airtable_record = airtable.get_record(constants.SearchField.GOOGLE_MAPS_PLACE_ID, place_id)
        
        if airtable_record and 'Photos' in airtable_record['fields'] and airtable_record['fields']['Photos']:
            airtable_photos = airtable_record['fields']['Photos']
            logging.info(f"Place {place_name} already has photos in Airtable. Skipping photo retrieval to save API costs.")
            return True, airtable_photos
    except Exception as e:
        logging.warning(f"Could not check for existing photos in Airtable: {e}")
    
    return False, None

def _fill_photos_from_airtable(place_data: Dict, airtable_photos: str, place_name: str) -> bool:
    """
    Fill missing photos data from Airtable.
    
    Args:
        place_data: Place data dictionary to update
        airtable_photos: Photos data from Airtable
        place_name: Name of the place for logging
        
    Returns:
        True if modifications were made, False otherwise
    """
    photos_section = place_data["photos"]
    
    # Check if the photos array is empty
    photos_empty = not photos_section.get("photo_urls", []) or len(photos_section.get("photo_urls", [])) == 0
    
    if photos_empty:
        try:
            # Try to parse the Photos column value from Airtable
            import ast
            airtable_photo_list = ast.literal_eval(airtable_photos) if isinstance(airtable_photos, str) else airtable_photos
            
            # Update "photos" field
            photos_section["photo_urls"] = airtable_photo_list
            photos_section["message"] = f"Photos retrieved from Airtable ({len(airtable_photo_list)} photos)"
            logging.info(f"Filled photos with {len(airtable_photo_list)} photos from Airtable for {place_name}")
            return True
        except Exception as e:
            logging.warning(f"Failed to parse or use Airtable photos for {place_name}: {e}")
    
    return False

def create_place_response(operation_status, target_place_name, http_response_data, operation_message):
    """
    Constructs a structured response dictionary with details about an operation performed on a place.

    This function logs the operation message and returns a dictionary that encapsulates the status of
    the operation, the name of the place involved, the response data obtained (if any), and a descriptive
    message about the outcome.

    Args:
        operation_status (str): A custom status string indicating the outcome of the operation,
                                used by callers to determine further actions.
        target_place_name (str): The name of the place that was the focus during the data retrieval operation.
        http_response_data (dict or None): The actual data received from the HTTP call to retrieve information
                                           about the place. This can be None if no data was retrieved.
        operation_message (str): A custom message providing additional details about the operation's outcome,
                                 intended for logging and informing the caller.

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
