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

def get_airtable_client(sequential_mode=False):
    """
    Returns a singleton instance of AirtableClient.
    This ensures that only one instance is created and reused across all threads.
    
    Args:
        sequential_mode (bool): When True, forces sequential execution in the AirtableClient
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
                _airtable_client_instance = AirtableClient(sequential_mode=sequential_mode)
                logging.info("Created singleton AirtableClient instance")
                if sequential_mode:
                    logging.info("AirtableClient running in DEBUG MODE with SEQUENTIAL execution")
    elif sequential_mode and not _airtable_client_instance.sequential_mode:
        # If sequential_mode is requested but the existing instance is not in debug mode,
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


def save_data_github(json_data, full_file_path, max_retries=3):
    """ Saves the given JSON data to the specified file path in the GitHub repository.
    Implements retry logic for handling 409 Conflict errors.

    Args:
        json_data (str): The JSON data to save
        full_file_path (str): Path to save the file, should include the folder and file name,
                              no leading slash. For example "data/places/charlotte/SomePlaceId.json"
        max_retries (int, optional): Maximum number of retry attempts for conflict errors. Defaults to 3.
    
    Returns:
        tuple: (success, error_message)
            - success (bool): True if the operation was successful, False otherwise
            - error_message (str): Error message if operation failed, or success message if operation succeeded
    """
    import time
    
    retry_count = 0
    
    while retry_count <= max_retries:
        try:
            github_token = os.environ['GITHUB_PERSONAL_ACCESS_TOKEN']
            headers = {
                "Authorization": f"token {github_token}",
                "Accept": "application/vnd.github.v3+json"
            }
            repo_name = "segunak/third-places-data"
            branch = "master"

            # Always get fresh SHA before attempting to update
            url_get = f"https://api.github.com/repos/{repo_name}/contents/{full_file_path}?ref={branch}"
            logging.info(f"Fetching current SHA for {full_file_path} (attempt {retry_count+1}/{max_retries+1})")
            get_response = requests.get(url_get, headers=headers)
            
            sha = None
            if get_response.status_code == 200:
                sha = get_response.json()['sha']
                logging.info(f"Current SHA for {full_file_path}: {sha}")
            
            # Construct the data for the PUT request to create/update the file
            url_put = f"https://api.github.com/repos/{repo_name}/contents/{full_file_path}"
            commit_message = "Saving JSON file via save_data_github utility function"
            data = {
                "message": commit_message,
                "content": base64.b64encode(json_data.encode()).decode(),
                "branch": branch
            }
            if sha:
                data['sha'] = sha  # If updating an existing file with fresh SHA

            # Make the PUT request to create/update the file
            put_response = requests.put(url_put, headers=headers, data=json.dumps(data))
            
            if put_response.status_code in {200, 201}:
                return True, f"File saved successfully to GitHub at {full_file_path}"
            elif put_response.status_code == 409 and retry_count < max_retries:
                # Handle conflict by retrying
                error_msg = f"GitHub API returned 409 Conflict. Retrying ({retry_count+1}/{max_retries})..."
                logging.warning(error_msg)
                retry_count += 1
                # Small delay before retry to allow potential concurrent operations to complete
                time.sleep(1 * retry_count)  # Increasing delay with each retry
                continue
            else:
                error_msg = f"GitHub API returned status code {put_response.status_code}: {put_response.text}"
                logging.error(error_msg)
                return False, error_msg

        except Exception as e:
            error_msg = f"Failed to save to GitHub: {str(e)}"
            logging.error(error_msg)
            return False, error_msg
    
    # If we've exhausted retries
    error_msg = "Maximum retries exceeded while attempting to save file to GitHub"
    logging.error(error_msg)
    return False, error_msg

def fetch_data_github(full_file_path) -> Tuple[bool, Optional[Dict], str]:
    """
    Fetches JSON data from the specified file path in the GitHub repository.
    Handles both standard-sized files and large files (>1MB) that don't include content directly.
    Implements comprehensive retry logic for handling SSL, network, and API errors.

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
    import time
    from requests.adapters import HTTPAdapter
    from urllib3.util import Retry
    from ssl import SSLError
    
    max_retries = 3
    retry_count = 0
    
    while retry_count <= max_retries:
        try:
            github_token = os.environ['GITHUB_PERSONAL_ACCESS_TOKEN']
            headers = {
                "Authorization": f"token {github_token}",
                "Accept": "application/vnd.github.v3+json"
            }
            repo_name = "segunak/third-places-data"
            branch = "master"

            # Create a session with retry mechanism for transient errors
            session = requests.Session()
            retry_strategy = Retry(
                total=3,  # Total number of retries
                backoff_factor=1,  # Wait 1, 2, 4 seconds between retries
                status_forcelist=[429, 500, 502, 503, 504],  # Retry on these status codes
                allowed_methods=["GET"]  # Only retry on GET requests
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("https://", adapter)

            # Get the file metadata with retry-enabled session
            url_get = f"https://api.github.com/repos/{repo_name}/contents/{full_file_path}?ref={branch}"
            logging.info(f"Fetching file from GitHub: {full_file_path} (attempt {retry_count+1}/{max_retries+1})")
            get_response = session.get(url_get, headers=headers, timeout=30)
            
            if get_response.status_code == 200:
                # First verify we have a valid JSON response from GitHub API
                try:
                    content_info = get_response.json()
                except json.JSONDecodeError as je:
                    error_msg = f"Failed to parse GitHub API response as JSON: {str(je)}. Response status: {get_response.status_code}"
                    logging.error(error_msg)
                    response_text = get_response.text[:200] + "..." if len(get_response.text) > 200 else get_response.text
                    logging.error(f"Response content preview: {response_text}")
                    
                    # Retry on JSON parse errors
                    retry_count += 1
                    if retry_count <= max_retries:
                        logging.warning(f"Retrying due to JSON parsing error ({retry_count}/{max_retries})")
                        time.sleep(1 * retry_count)  # Exponential backoff
                        continue
                    else:
                        return False, None, error_msg
                    
                if content_info["type"] == "file":
                    # Determine how to get the file content
                    file_content = None
                    
                    # For large files, GitHub omits content and provides download_url instead
                    if content_info.get("encoding") == "none" or not content_info.get("content"):
                        if "download_url" in content_info:
                            file_size = content_info.get('size', 'unknown')
                            logging.info(f"Large file detected (size: {file_size}). Using download_url instead of content field.")
                            # Use the download_url to fetch the raw file content
                            try:
                                download_response = session.get(content_info["download_url"], headers=headers, timeout=60)  # Longer timeout for large files
                                if download_response.status_code == 200:
                                    file_content = download_response.text
                                else:
                                    error_msg = f"Failed to download large file: Status {download_response.status_code}"
                                    logging.error(error_msg)
                                    
                                    # Retry on download failures
                                    retry_count += 1
                                    if retry_count <= max_retries:
                                        logging.warning(f"Retrying download ({retry_count}/{max_retries})")
                                        time.sleep(2 * retry_count)  # Longer delay for download failures
                                        continue
                                    else:
                                        return False, None, error_msg
                            except (requests.RequestException, SSLError) as e:
                                error_msg = f"Download error for large file: {str(e)}"
                                logging.error(error_msg)
                                
                                # Retry on network errors
                                retry_count += 1
                                if retry_count <= max_retries:
                                    logging.warning(f"Retrying download after network error ({retry_count}/{max_retries})")
                                    time.sleep(3 * retry_count)  # Even longer delay for network errors
                                    continue
                                else:
                                    return False, None, error_msg
                        else:
                            error_msg = "Large file detected but no download_url provided"
                            logging.error(error_msg)
                            return False, None, error_msg
                    else:
                        # Normal case: decode the base64 content
                        try:
                            file_content = base64.b64decode(content_info["content"]).decode('utf-8')
                        except Exception as e:
                            error_msg = f"Failed to decode base64 content: {str(e)}"
                            logging.error(error_msg)
                            
                            # Retry on decoding errors
                            retry_count += 1
                            if retry_count <= max_retries:
                                logging.warning(f"Retrying after decoding error ({retry_count}/{max_retries})")
                                time.sleep(1 * retry_count)
                                continue
                            else:
                                return False, None, error_msg
                    
                    # Check for empty content
                    if not file_content or not file_content.strip():
                        logging.error(f"Empty file content received from GitHub for {full_file_path}")
                        return False, None, f"Empty file content received from GitHub for {full_file_path}"
                        
                    # Try to parse the JSON content
                    try:
                        parsed_json = json.loads(file_content)
                        return True, parsed_json, "File fetched successfully"
                    except json.JSONDecodeError as je:
                        # Log more details about the parsing error
                        error_msg = f"Failed to parse file content as JSON: {str(je)}"
                        logging.error(error_msg)
                        
                        # Log a preview of the file content to help diagnose the issue
                        preview = file_content[:200] + "..." if len(file_content) > 200 else file_content
                        content_msg = f"Content preview for {full_file_path}: {preview}"
                        logging.error(content_msg)
                        
                        # Return detailed error information
                        return False, None, f"JSON parsing error: {str(je)}. Check logs for content preview."
                else:
                    error_msg = f"Path {full_file_path} does not point to a file"
                    logging.error(error_msg)
                    return False, None, error_msg
            elif get_response.status_code == 404:
                error_msg = f"File {full_file_path} not found in repository"
                logging.error(error_msg)
                return False, None, error_msg
            else:
                # Log more details about HTTP errors
                response_text = get_response.text[:200] + "..." if len(get_response.text) > 200 else get_response.text
                error_msg = f"Failed to fetch file: {get_response.status_code} - Response content: {response_text}"
                logging.error(error_msg)
                
                # Retry on non-404 errors
                retry_count += 1
                if retry_count <= max_retries:
                    logging.warning(f"Retrying after HTTP error {get_response.status_code} ({retry_count}/{max_retries})")
                    time.sleep(2 * retry_count)
                    continue
                else:
                    return False, None, f"Failed to fetch file: {get_response.status_code}"

        except (requests.RequestException, SSLError) as e:
            error_msg = f"Network/SSL error while fetching from GitHub: {str(e)}"
            logging.error(error_msg)
            
            # Retry on network/SSL errors
            retry_count += 1
            if retry_count <= max_retries:
                logging.warning(f"Retrying after network/SSL error ({retry_count}/{max_retries})")
                time.sleep(3 * retry_count)  # Longer delay for network errors
                continue
            else:
                return False, None, error_msg
                
        except Exception as e:
            error_msg = f"Failed to fetch from GitHub: {str(e)}"
            logging.error(error_msg, exc_info=True)  # Include stack trace
            
            # Retry on any other unexpected errors
            retry_count += 1
            if retry_count <= max_retries:
                logging.warning(f"Retrying after unexpected error ({retry_count}/{max_retries})")
                time.sleep(2 * retry_count)
                continue
            else:
                return False, None, error_msg
                
    # If we've reached here, we've exceeded our retry attempts
    error_msg = f"Maximum retries exceeded ({max_retries}) while attempting to fetch file from GitHub"
    logging.error(error_msg)
    return False, None, error_msg

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
    4. Store the data in GitHub only if fresh data was retrieved or modifications were made
    
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
        modifications_made = False
        
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
                    airtable_photos = airtable_record['fields']['Photos']
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
                        modifications_made = True
                    except Exception as e:
                        logging.warning(f"Failed to parse or use Airtable photos for {place_name}: {e}")
            
            if not place_data or 'error' in place_data:
                error_message = place_data.get('error', 'Unknown error') if place_data else 'No data retrieved'
                return ('failed', None, f"Error: Data retrieval failed for place {place_name} with place_id {place_id}. {error_message}")
        
        # Only save to GitHub if we got fresh data or made modifications to cached data
        if not use_cache or force_refresh or modifications_made:
            # Save the data to GitHub
            final_json_data = json.dumps(place_data, indent=4)
            logging.info(f"Saving place data to GitHub at path {cache_file_path}")
            save_succeeded, save_message = save_data_github(final_json_data, cache_file_path)
            logging.info(f"GitHub save operation result: {save_message}")

            if save_succeeded:
                status = 'succeeded' if not use_cache else 'cached'
                message = f"Data {'retrieved and' if not use_cache else ''} saved successfully for {place_name}."
                return (status, place_data, message)
            else:
                return ('failed', None, f"Failed to save data to GitHub for {place_name}. {save_message}")
        else:
            logging.info(f"Using cached data without modifications for {place_name} - skipping GitHub write operation")
            return ('cached', place_data, f"Using cached data for {place_name}. GitHub write skipped.")
            
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
