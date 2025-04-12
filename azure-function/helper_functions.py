import os
import re
import json
import dotenv
import base64
import logging
import requests
import unicodedata
from datetime import datetime
from unidecode import unidecode
from typing import Iterable, Callable, Any, List
from azure.storage.filedatalake import DataLakeServiceClient
from concurrent.futures import ThreadPoolExecutor, as_completed

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
