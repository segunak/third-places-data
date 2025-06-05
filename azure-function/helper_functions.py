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
            
            # If the record exists and has photos, we can skip photo retrieval except if force_refresh is True
            if record and 'Photos' in record['fields'] and record['fields']['Photos'] and not force_refresh:
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


