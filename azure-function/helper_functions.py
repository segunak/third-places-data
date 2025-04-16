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

def get_airtable_client(debug_mode=False):
    """
    Returns a singleton instance of AirtableClient.
    This ensures that only one instance is created and reused across all threads.
    
    Args:
        debug_mode (bool): When True, forces sequential execution in the AirtableClient
                           for easier debugging. Default is False.
    
    Returns:
        AirtableClient: A singleton instance configured with the specified debug mode.
    """
    global _airtable_client_instance
    
    if _airtable_client_instance is None:
        with _airtable_client_lock:
            # Double-check that another thread didn't initialize the client
            # while we were waiting for the lock
            if _airtable_client_instance is None:
                from airtable_client import AirtableClient
                _airtable_client_instance = AirtableClient(debug_mode=debug_mode)
                logging.info("Created singleton AirtableClient instance")
                if debug_mode:
                    logging.info("AirtableClient running in DEBUG MODE with SEQUENTIAL execution")
    elif debug_mode and not _airtable_client_instance.debug_mode:
        # If debug_mode is requested but the existing instance is not in debug mode,
        # log a warning since we can't change the mode of an existing instance
        logging.warning(
            "Existing AirtableClient instance is not in debug mode. "
            "To enable debug mode, restart your application."
        )
    
    return _airtable_client_instance

def get_place_data_provider(provider_type=None):
    """
    Returns a singleton instance of a PlaceDataProvider based on the requested type.
    This ensures that only one instance of each provider type is created and reused across all threads.
    
    Args:
        provider_type (str, optional): The type of provider to get ('google', 'outscraper', etc.).
                                      If None, uses the default from environment.
    
    Returns:
        PlaceDataProvider: A singleton instance of the requested provider.
    """
    global _place_data_provider_instances
    
    # If no provider type specified, get the default from environment
    if provider_type is None:
        dotenv.load_dotenv()
        provider_type = os.environ.get('DEFAULT_PLACE_DATA_PROVIDER', 'outscraper')
    
    provider_type = provider_type.lower()
    
    # Check if we already have this provider type instantiated
    if provider_type not in _place_data_provider_instances:
        with _place_data_provider_lock:
            # Double-check that another thread didn't initialize the provider
            # while we were waiting for the lock
            if provider_type not in _place_data_provider_instances:
                from place_data_providers import PlaceDataProviderFactory
                _place_data_provider_instances[provider_type] = PlaceDataProviderFactory.get_provider(provider_type)
                logging.info(f"Created singleton PlaceDataProvider instance for {provider_type}")
    
    return _place_data_provider_instances[provider_type]

dotenv.load_dotenv()

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


def save_data_github(json_data, full_file_path):
    """ Saves the given JSON data to the specified file path in the GitHub repository.

    full_file_path should include the folder and file name, no leading slash. For example
    "data/places/charlotte/SomePlaceId.json"
    """
    try:
        github_token = os.environ['GITHUB_PERSONAL_ACCESS_TOKEN']
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        repo_name = "segunak/third-places-data"
        branch = "master"

        # Check if the file exists to get the SHA
        # Reference https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28#get-repository-content
        url_get = f"https://api.github.com/repos/{repo_name}/contents/{full_file_path}?ref={branch}"
        get_response = requests.get(url_get, headers=headers)
        if get_response.status_code == 200:
            sha = get_response.json()['sha']
        else:
            sha = None  # If the file does not exist, we'll create a new file

        # Construct the data for the PUT request to create/update the file
        # Reference https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28#create-or-update-file-contents
        url_put = f"https://api.github.com/repos/{repo_name}/contents/{full_file_path}"
        commit_message = "Saving JSON file via save_data_github utility function"
        data = {
            "message": commit_message,
            "content": base64.b64encode(json_data.encode()).decode(),
            "branch": branch
        }
        if sha:
            data['sha'] = sha  # If updating an existing file, we need to provide the SHA

        # Make the PUT request to create/update the file
        put_response = requests.put(url_put, headers=headers, data=json.dumps(data))
        return put_response.status_code in {200, 201}

    except Exception as e:
        logging.error(f"Failed to save to GitHub: {str(e)}")
        return False

def fetch_data_github(full_file_path) -> Tuple[bool, Optional[Dict], str]:
    """
    Fetches JSON data from the specified file path in the GitHub repository.

    Args:
        full_file_path (str): Path to the file in the GitHub repository.
            Should include the folder and file name, no leading slash.
            For example: "data/places/charlotte/SomePlaceId.json"

    Returns:
        tuple: A tuple containing (success, data, message)
            - success (bool): Whether the operation was successful
            - data (dict or None): The fetched data as a Python dictionary, or None if unsuccessful
            - message (str): A message describing the result of the operation
    """
    try:
        github_token = os.environ['GITHUB_PERSONAL_ACCESS_TOKEN']
        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
        repo_name = "segunak/third-places-data"
        branch = "master"

        # Get the file content
        # Reference https://docs.github.com/en/rest/repos/contents?apiVersion=2022-11-28#get-repository-content
        url_get = f"https://api.github.com/repos/{repo_name}/contents/{full_file_path}?ref={branch}"
        get_response = requests.get(url_get, headers=headers)
        
        if get_response.status_code == 200:
            content = get_response.json()
            if content["type"] == "file":
                # Decode base64 content to JSON
                file_content = base64.b64decode(content["content"]).decode('utf-8')
                return True, json.loads(file_content), "File fetched successfully"
            else:
                return False, None, f"Path {full_file_path} does not point to a file"
        elif get_response.status_code == 404:
            return False, None, f"File {full_file_path} not found in repository"
        else:
            return False, None, f"Failed to fetch file: {get_response.status_code} - {get_response.text}"

    except Exception as e:
        logging.error(f"Failed to fetch from GitHub: {str(e)}")
        return False, None, f"Error fetching file: {str(e)}"

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

def get_and_cache_place_data(place_name: str, place_id: str, city_name: str = "charlotte") -> Tuple[str, Dict, str]:
    """
    Centralized function for retrieving and caching place data with a cache-first approach.
    
    This function implements the cache-first logic:
    1. Check if place has cached data in the GitHub repository
    2. If cache exists and is fresh (within configured interval), use the cached data
    3. If cache doesn't exist or is stale, fetch fresh data from the API provider
    4. Store the data in GitHub
    
    Args:
        place_name (str): Name of the place
        place_id (str): Google Maps Place ID
        city_name (str): City name for the file path (defaults to "charlotte")
        
    Returns:
        tuple: (status, place_data, message)
            - status: 'succeeded', 'cached', 'skipped', or 'failed'
            - place_data: The retrieved place data or None if failed
            - message: A descriptive message about the operation outcome
    """
    import constants
    from place_data_providers import PlaceDataProviderFactory

    try:
        # Get the provider type from environment or use default
        provider_type = os.environ.get('DEFAULT_PLACE_DATA_PROVIDER', 'outscraper')
        data_provider = get_place_data_provider(provider_type)
        
        logging.info(f"Processing data for place: {place_name}")

        # Validate/process the place ID
        if not place_id:
            place_id = data_provider.find_place_id(place_name)
            if not place_id:
                return ('skipped', None, f"Warning! No place_id found for {place_name}. Skipping data retrieval.")

        # Path to the cached data file
        cache_file_path = f"data/places/{city_name}/{place_id}.json"
        
        # Get refresh intervals from environment or use defaults from constants
        force_refresh = os.environ.get('FORCE_REFRESH_DATA', '').lower() == 'true'
        refresh_interval = constants.DEFAULT_CACHE_REFRESH_INTERVAL
        
        # Check if we have cached data for this place
        use_cache = False
        place_data = None
        
        if not force_refresh:
            success, cached_data, message = fetch_data_github(cache_file_path)
            
            if success and cached_data:
                # Check if the cache is still valid
                if is_cache_valid(cached_data, refresh_interval):
                    use_cache = True
                    logging.info(f"Using cached data for {place_name} (last updated: {cached_data.get('last_updated')})")
                    place_data = cached_data
                else:
                    logging.info(f"Cached data for {place_name} is stale. Fetching fresh data.")
            else:
                logging.info(f"No cached data found for {place_name}: {message}")
        else:
            logging.info(f"Force refresh enabled. Skipping cache check for {place_name}")
        
        # If we don't have valid cached data, get fresh data
        airtable = None
        airtable_record = None
        airtable_photos = None
        
        if not use_cache:
            # Check if the place already has photos in Airtable
            try:
                airtable = get_airtable_client()
                airtable_record = airtable.get_record(constants.SearchField.GOOGLE_MAPS_PLACE_ID, place_id)
                if airtable_record and 'Photos' in airtable_record['fields'] and airtable_record['fields']['Photos']:
                    airtable_photos = airtable_record['fields']['Photos Outscraper Reference']
                    logging.info(f"Place {place_name} already has photos in Airtable. Skipping photo retrieval to save API costs.")
            except Exception as e:
                logging.warning(f"Could not check for existing photos in Airtable: {e}")
            
            logging.info(f"Retrieving fresh data for {place_name} with place_id {place_id}")
            # Skip photos retrieval if we already have them in Airtable
            place_data = data_provider.get_all_place_data(place_id, place_name, skip_photos=airtable_photos is not None)
            
            # If photos were skipped and airtable photos exist, fill in the photos section with data from Airtable
            if airtable_photos and place_data and "photos" in place_data:
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
                    except Exception as e:
                        logging.warning(f"Failed to parse or use Airtable photos for {place_name}: {e}")
            
            if not place_data or 'error' in place_data:
                error_message = place_data.get('error', 'Unknown error') if place_data else 'No data retrieved'
                return ('failed', None, f"Error: Data retrieval failed for place {place_name} with place_id {place_id}. {error_message}")
        
        # Always save the data (whether fresh or from cache) to ensure GitHub is updated
        final_json_data = json.dumps(place_data, indent=4)
        logging.info(f"Saving place data (whether fresh or from the cache) to GitHub at path {cache_file_path}")
        save_succeeded = save_data_github(final_json_data, cache_file_path)

        if save_succeeded:
            status = 'succeeded' if not use_cache else 'cached'
            message = f"Data {'retrieved and' if not use_cache else ''} saved successfully for {place_name}."
            return (status, place_data, message)
        else:
            return ('failed', None, f"Failed to save data to GitHub for {place_name}. Review the logs for more details.")
    except Exception as e:
        logging.error(f"Error in get_and_cache_place_data for {place_name}: {e}", exc_info=True)
        return ('failed', None, f"Error processing place data: {str(e)}")

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
