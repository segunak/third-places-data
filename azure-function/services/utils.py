from typing import Dict, Optional, Tuple
import os
import re
import json
import dotenv
import base64
import logging
import requests
import unicodedata
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient
from threading import Lock
from constants import SearchField
from services.place_data_service import PlaceDataProviderFactory


def normalize_text(text: str) -> str:
    if isinstance(text, str):
        text = unicodedata.normalize('NFC', text)
        text = re.sub(r'\s+', ' ', text.strip().lower())
    return text


def save_reviews_azure(json_data, review_file_name):
    try:
        datalake_connection_string = os.environ['AzureWebJobsStorage']
        logging.info("Retrieved Azure Data Lake connection string.")
        datalake_service_client = DataLakeServiceClient.from_connection_string(datalake_connection_string)
        logging.info("Initialized DataLakeServiceClient.")
        file_system_client = datalake_service_client.get_file_system_client(file_system="data")
        directory_client = file_system_client.get_directory_client("reviews")
        file_client = directory_client.get_file_client(review_file_name)
        file_client.upload_data(data=json_data, overwrite=True)
        logging.info(f"Successfully uploaded {review_file_name} to Azure Data Lake.")
    except Exception as e:
        logging.error(f"An unexpected error occurred while saving '{review_file_name}'.")
        logging.exception(e)


def save_data_github(json_data: str, full_file_path: str, max_retries=3):
    import time
    github_token = os.environ.get('GITHUB_PERSONAL_ACCESS_TOKEN')
    if not github_token:
        return False, "GitHub token not found in environment variables"
    headers = {"Authorization": f"token {github_token}", "Accept": "application/vnd.github.v3+json"}
    repo_name = "segunak/third-places-data"
    branch = "master"
    url_get = f"https://api.github.com/repos/{repo_name}/contents/{full_file_path}?ref={branch}"
    url_put = f"https://api.github.com/repos/{repo_name}/contents/{full_file_path}"
    encoded_content = base64.b64encode(json_data.encode()).decode()
    for attempt in range(max_retries + 1):
        try:
            logging.info(f"Getting SHA for {full_file_path} (attempt {attempt+1}/{max_retries+1})")
            get_response = requests.get(url_get, headers=headers)
            sha = get_response.json().get('sha') if get_response.status_code == 200 else None
            data = {"message": "Saving JSON file via save_data_github utility function", "content": encoded_content, "branch": branch}
            if sha:
                data['sha'] = sha
            put_response = requests.put(url_put, headers=headers, data=json.dumps(data, indent=4))
            if put_response.status_code in {200, 201}:
                return True, f"File saved successfully to GitHub at {full_file_path}"
            if put_response.status_code == 409 and attempt < max_retries:
                logging.warning(f"GitHub API returned 409 Conflict. Retrying ({attempt+1}/{max_retries})...")
                time.sleep(1 * (attempt + 1))
                continue
            return False, f"GitHub API returned status code {put_response.status_code}: {put_response.text}"
        except Exception as e:
            if attempt < max_retries:
                logging.warning(f"Error during save attempt {attempt+1}: {str(e)}. Retrying...")
                time.sleep(1 * (attempt + 1))
                continue
            return False, f"Failed to save to GitHub: {str(e)}"
    return False, "Maximum retries exceeded while attempting to save file to GitHub"


def fetch_data_github(full_file_path) -> Tuple[bool, Optional[Dict], str]:
    from requests.adapters import HTTPAdapter
    from urllib3.util import Retry
    session = requests.Session()
    retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    try:
        github_token = os.environ['GITHUB_PERSONAL_ACCESS_TOKEN']
        headers = {"Authorization": f"token {github_token}", "Accept": "application/vnd.github.v3+json"}
        repo_name = "segunak/third-places-data"
        branch = "master"
        url_get = f"https://api.github.com/repos/{repo_name}/contents/{full_file_path}?ref={branch}"
        logging.info(f"Fetching file from GitHub: {full_file_path}")
        get_response = session.get(url_get, headers=headers, timeout=30)
        if get_response.status_code != 200:
            if get_response.status_code == 404:
                return False, None, f"File {full_file_path} not found in repository"
            return False, None, f"Failed to fetch file: {get_response.status_code}"
        content_info = get_response.json()
        file_content = None
        if content_info["type"] != "file":
            return False, None, f"Path {full_file_path} does not point to a file"
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
            file_content = base64.b64decode(content_info["content"]).decode('utf-8')
        if not file_content or not file_content.strip():
            return False, None, f"Empty file content received from GitHub for {full_file_path}"
        try:
            parsed_json = json.loads(file_content)
            return True, parsed_json, "File fetched successfully"
        except json.JSONDecodeError as je:
            preview = file_content[:200] + "..." if len(file_content) > 200 else file_content
            logging.error(f"JSON parsing error for {full_file_path}: {str(je)}\nContent preview: {preview}")
            return False, None, f"JSON parsing error: {str(je)}"
    except requests.RequestException as e:
        return False, None, f"Network error while fetching from GitHub: {str(e)}"
    except Exception as e:
        logging.error(f"Failed to fetch from GitHub: {str(e)}", exc_info=True)
        return False, None, f"Failed to fetch from GitHub: {str(e)}"




def _fill_photos_from_airtable(place_data: Dict, photos_json: str) -> bool:
    logging.info(f"_fill_photos_from_airtable: place_data={place_data}")
    logging.info(f"_fill_photos_from_airtable: photos_json={photos_json}")
    
    try:
        photos = json.loads(photos_json)
        if 'photos' not in place_data:
            place_data['photos'] = {}
        place_data['photos']['photo_urls'] = photos
        place_data['photos']['message'] = "Retrieved from Airtable"
        logging.info(f"_fill_photos_from_airtable: Successfully filled photos from Airtable")
        return True
    except json.JSONDecodeError as e:
        logging.error(f"_fill_photos_from_airtable: Error parsing photos JSON from Airtable: {e}")
        return False
    except Exception as e:
        logging.error(f"_fill_photos_from_airtable: Error filling photos from Airtable: {e}")
        return False


def get_and_cache_place_data(provider_type: str, place_name: str, place_id: str = None,
                              city: str = None, force_refresh: bool = False, airtable_record_id: str = None) -> Tuple[str, Dict, str]:
    try:
        SENTINEL_NO_PLACE = "__NO_PLACE_FOUND__"
        if not city:
            return 'failed', None, 'Missing required parameter: city'

        if not provider_type:
            error_msg = f"Cannot get place data for {place_name} - provider_type not specified"
            logging.error(error_msg)
            return 'failed', None, error_msg
        data_provider = PlaceDataProviderFactory.get_provider(provider_type)

        did_lookup_find_new_place_id = False
        if not place_id:
            logging.info(f"No place_id provided for {place_name}, looking up...")
            place_id = data_provider.find_place_id(place_name)

            if not place_id or place_id == SENTINEL_NO_PLACE:
                return 'failed', None, f"Could not find place ID for {place_name}"
            did_lookup_find_new_place_id = True
            logging.info(f"Found place_id {place_id} for {place_name}")

        cached_file_path = f"data/places/{city}/{place_id}.json"
        skip_photos = False
        existing_photos_json = None

        from services.airtable_service import AirtableService
        airtable_client = AirtableService(provider_type)

        if did_lookup_find_new_place_id and airtable_record_id:
            try:
                airtable_client.update_place_record(airtable_record_id, 'Google Maps Place Id', place_id, overwrite=True)
            except Exception as e:
                logging.error(f"Failed to persist newly discovered place_id for {place_name} (record {airtable_record_id}): {e}")

        record = airtable_client.get_record(SearchField.GOOGLE_MAPS_PLACE_ID, place_id)
        if record and 'Photos' in record['fields'] and record['fields']['Photos'] and not force_refresh:
            logging.info(f"Place {place_id} already has photos in Airtable, skipping photo retrieval")
            skip_photos = True
            existing_photos_json = record['fields']['Photos']

        # 1. Attempt to load cached data from GitHub
        cached_file_exists, cached_json, cache_message = fetch_data_github(cached_file_path)

        if cached_file_exists and not force_refresh:
            logging.info(f"Using cached data for {place_name} (place_id={place_id}) from {cached_file_path}")
            if airtable_record_id and airtable_client:
                try:
                    airtable_client.update_place_record(airtable_record_id, 'Has Data File', 'Yes', overwrite=True)
                except Exception as e:
                    logging.error(f"Failed to update 'Has Data File' (cached path) for {place_name}: {e}")
            return 'cached', cached_json, f"Using cached data for {place_name} from {cached_file_path}"

        if cached_file_exists and force_refresh:
            logging.info(f"Cache present for {place_name} but force_refresh=True; fetching fresh data")

        if not cached_file_exists:
            logging.info(f"No cached file found for {place_name} at {cached_file_path} ({cache_message}); fetching fresh data")

        # 2. Fetch fresh data (either no cache or force_refresh requested)
        logging.info(f"Fetching fresh data from provider {provider_type} for {place_name} (place_id={place_id}) skip_photos={skip_photos} force_refresh={force_refresh}")
        place_data = data_provider.get_all_place_data(place_id, place_name, skip_photos=skip_photos)

        # If provider returned a sentinel indicating no real data, treat as failure and DO NOT save or update Airtable
        try:
            details = place_data.get('details', {}) if place_data else {}
            raw_details_place_id = details.get('place_id') or details.get('raw_data', {}).get('place_id')
            if raw_details_place_id == SENTINEL_NO_PLACE:
                logging.warning(f"Provider returned sentinel NO_PLACE_FOUND for {place_name} (place_id={place_id}); skipping save & Airtable updates.")
                return 'failed', None, f"NO_PLACE_FOUND: Provider could not find data for {place_name}"
        except Exception as sentinel_check_err:
            logging.error(f"Error while evaluating sentinel NO_PLACE_FOUND for {place_name}: {sentinel_check_err}")

        if skip_photos and existing_photos_json:
            _fill_photos_from_airtable(place_data, existing_photos_json)

        place_data['last_updated'] = datetime.now().isoformat()
        success, save_message = save_data_github(json.dumps(place_data, indent=4), cached_file_path)

        if not success:
            logging.warning(f"Failed to save fresh data for {place_name}: {save_message}")
        else:
            logging.info(f"Saved fresh data for {place_name} to {cached_file_path}")

        if airtable_record_id and airtable_client:
            try:
                airtable_client.update_place_record(airtable_record_id, 'Has Data File', 'Yes', overwrite=True)
            except Exception as e:
                logging.error(f"Failed to update 'Has Data File' (fresh path) for {place_name}: {e}")
        return 'succeeded', place_data, f"Fetched fresh data for {place_name}"
    except Exception as e:
        logging.error(f"Error getting place data for {place_name}: {e}", exc_info=True)
        return 'failed', None, f"Error: {str(e)}"


def create_place_response(operation_status, target_place_name, http_response_data, operation_message):
    if operation_status == 'failed':
        logging.warning(operation_message)
    else:
        logging.info(operation_message)
    return {'status': operation_status, 'place_name': target_place_name, 'response': http_response_data, 'message': operation_message}
