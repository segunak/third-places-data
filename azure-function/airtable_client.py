import os
import time
import dotenv
import logging
import pyairtable
from pyairtable import Api
from collections import Counter
from urllib.parse import urlparse
from constants import SearchField, MAX_THREAD_WORKERS
import helper_functions as helpers
from typing import Dict, Any, List
from pyairtable.formulas import match
from place_data_providers import PlaceDataProviderFactory
from concurrent.futures import ThreadPoolExecutor, as_completed

class AirtableClient:
    """Defines methods for interaction with the Charlotte Third Places Airtable database.
    """

    def __init__(self, data_provider_type=None, sequential_mode=False):
        logging.basicConfig(level=logging.INFO)

        if 'FUNCTIONS_WORKER_RUNTIME' in os.environ:
            logging.info('Airtable Client instantiated for Azure Function use.')
        else:
            logging.info('Airtable Client instantiated for local use.')
            dotenv.load_dotenv()

        # Store the debug mode flag
        self.sequential_mode = sequential_mode
        if self.sequential_mode:
            logging.info('Airtable Client running in DEBUG MODE with SEQUENTIAL execution')

        self.AIRTABLE_BASE_ID = os.environ['AIRTABLE_BASE_ID']
        self.AIRTABLE_PERSONAL_ACCESS_TOKEN = os.environ['AIRTABLE_PERSONAL_ACCESS_TOKEN']
        self.AIRTABLE_WORKSPACE_ID = os.environ['AIRTABLE_WORKSPACE_ID']
        self.charlotte_third_places = pyairtable.Table(
            self.AIRTABLE_PERSONAL_ACCESS_TOKEN, self.AIRTABLE_BASE_ID, 'Charlotte Third Places'
        )
        
        # Use the singleton pattern to get a place data provider
        import helper_functions as helpers
        self.data_provider = helpers.get_place_data_provider(data_provider_type)
        
        self.api = Api(self.AIRTABLE_PERSONAL_ACCESS_TOKEN)
        self.all_third_places = self.charlotte_third_places.all(sort=["-Created Time"])

    def update_place_record(self, record_id: str, field_to_update: str, update_value, overwrite: bool) -> Dict[str, Any]:
        """
        Attempts to update a record in the Airtable database based on given parameters.
        The function considers whether the field should be overwritten if it already exists.

        Args:
            record_id (str): The unique identifier for the record.
            field_to_update (str): The field within the record to update.
            update_value: The new value for the specified field.
            overwrite (bool): Whether to overwrite the existing value if the field is not empty.

        Returns:
            bool: True if an update occurred, False otherwise.
        """
        try:
            record = self.charlotte_third_places.get(record_id)
            place_name = record['fields']['Place']
            current_value = record['fields'].get(field_to_update)

            current_value_normalized = helpers.normalize_text(current_value)
            update_value_normalized = helpers.normalize_text(update_value)

            result = {
                "updated": False,
                "field_name": field_to_update,
                "record_id": record_id,
                "old_value": current_value,
                "new_value": update_value
            }

            # Determine whether to update the field based on these rules:
            # 1. Only proceed if we have a valid new value (not empty)
            # 2. Always update if the current value is empty or "Unsure"
            # 3. Otherwise, only update if both conditions are true:
            #    a) The overwrite parameter allows replacing existing values
            #    b) The current value is different from the new value
            if (
                update_value_normalized not in (None, '') and
                (
                    current_value_normalized in (None, 'Unsure') or
                    (
                        overwrite and
                        current_value_normalized != update_value_normalized
                    )
                )
            ):
                self.charlotte_third_places.update(record_id, {field_to_update: update_value})
                logging.info(
                    f'Successfully updated the field "{field_to_update}" for place "{place_name}". New value: "{update_value}".'
                )
                time.sleep(1)
                result["updated"] = True
            else:
                logging.info(
                    f'\n\nSkipped updating the field "{field_to_update}" for place "{place_name}".\n'
                    f'Existing value:{current_value}\n'
                    f'Provided value:{update_value}\n'
                )
            return result
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return {
                "updated": False,
                "field_name": field_to_update,
                "record_id": record_id,
                "old_value": None,
                "new_value": None
            }

    def get_base_url(self, url: str) -> str:
        """
        Extracts and returns the base URL (scheme, domain, and path) from a full URL.
        If the input URL is invalid, returns an empty string.

        Args:
            url (str): The full URL from which to extract the base.

        Returns:
            str: The base URL, or an empty string if the URL is invalid.
        """
        parsed_url = urlparse(url)

        if not parsed_url.scheme or not parsed_url.netloc:
            return ""

        return f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}".strip()

    def enrich_base_data(self) -> list:
        """
        Enriches the base data of places stored in Airtable with additional metadata fetched using the configured data provider.
        Uses threading to parallelize fetching details for all places, unless sequential_mode is enabled.
        
        This method:
        1. Gets data for each place (cached or fresh) using helper_functions.get_and_cache_place_data
        2. Updates Airtable fields with this data
        
        Returns:
            list: A list of dictionaries containing the results of each place enrichment operation.
        """
        places_updated = []
        
        def enrich_callback(record_id, place_data):
            """
            Callback function to enrich Airtable fields with place data.
            
            Args:
                record_id (str): The Airtable record ID
                place_data (dict): The place data from helper_functions.get_and_cache_place_data
                
            Returns:
                dict: Additional data to include in the result
            """
            result = {"field_updates": {}}
            
            if place_data and 'details' in place_data:
                # Mark this place as having data file
                self.update_place_record(record_id, 'Has Data File', 'Yes', overwrite=True)
            
                details = place_data['details']
                
                website = details.get('website', '')
                if website:
                    website = self.get_base_url(website)
                
                address = details.get('address', '')
                latitude = details.get('latitude')
                longitude = details.get('longitude')
                
                # Parking is returned as a list of tags like ["Free", "Street"]
                parking = details.get('parking', ['Unsure'])
                if parking and len(parking) > 0:
                    parking = parking[0]  # Use the first tag which indicates if it's free/paid/unsure
                else:
                    parking = 'Unsure'
                    
                # Purchase required is standardized as "Yes", "No", or "Unsure"
                purchase_required = details.get('purchase_required', 'Unsure')
                
                description = details.get('description', '')
                google_maps_url = details.get('google_maps_url', '')
                
                # Check if we already have photos in Airtable before considering updating them
                record = self.charlotte_third_places.get(record_id)
                has_existing_photos = 'Photos' in record['fields'] and record['fields']['Photos']
                
                photos_data = place_data.get('photos', {})
                photos_list = photos_data.get('photo_urls', []) if photos_data else []
                
                # Format is the place value and the boolean indicating if it should be overwritten
                # The boolean is used to determine if the field should be updated even if it already has a value
                fields_to_update = {
                    'Google Maps Place Id': (place_data.get('place_id'), True),
                    'Google Maps Profile URL': (google_maps_url, True),
                    'Website': (website, True),
                    'Address': (address, True),
                    'Description': (description, False),
                    'Purchase Required': (purchase_required, False),
                    'Parking': (parking, False),
                    'Latitude': (str(latitude), True) if latitude else (None, False),
                    'Longitude': (str(longitude), True) if longitude else (None, False),
                }
                
                # Only add Photos to fields_to_update if:
                # 1. We have new photos from the API AND
                # 2. The place doesn't already have photos in Airtable
                # This ensures we never overwrite existing photos
                if photos_list and not has_existing_photos:
                    fields_to_update['Photos'] = (str(photos_list), False)
                elif has_existing_photos:
                    logging.info(f"Skipping photo update as photos already exist in Airtable")

                # Process updates
                for field_name, (field_value, overwrite) in fields_to_update.items():
                    if field_value:
                        update_result = self.update_place_record(
                            record_id,
                            field_name,
                            field_value,
                            overwrite
                        )
    
                        result['field_updates'][field_name] = {
                            "updated": update_result["updated"],
                            "old_value": update_result["old_value"],
                            "new_value": update_result["new_value"]
                        }
            
            return result

        # Check if we're in debug mode to decide between sequential or parallel execution
        if self.sequential_mode:
            logging.info("Running enrich_base_data in SEQUENTIAL mode for debugging")
            for third_place in self.all_third_places:
                place_name = third_place['fields'].get('Place', 'Unknown Place')
                try:
                    # Call process_place_common directly with the enrich_callback
                    result = self._process_place(third_place, enrich_callback)
                    places_updated.append(result)
                    logging.info(f"Finished processing {place_name}")
                except Exception as e:
                    logging.error(f"Error processing {place_name}: {e}")
        else:
            # Standard parallel execution with ThreadPoolExecutor
            logging.info(f"Running enrich_base_data in PARALLEL mode with {MAX_THREAD_WORKERS} workers")
            with ThreadPoolExecutor(max_workers=MAX_THREAD_WORKERS) as executor:
                futures = {
                    # Call process_place_common directly with the enrich_callback
                    executor.submit(self._process_place, third_place, enrich_callback): 
                    third_place['fields'].get('Place', 'Unknown Place')
                    for third_place in self.all_third_places
                }

                for future in as_completed(futures):
                    place_name = futures[future]
                    try:
                        result = future.result()
                        places_updated.append(result)
                        logging.info(f"Finished processing {place_name}")
                    except Exception as e:
                        logging.error(f"Error processing {place_name}: {e}")

        return places_updated
        
    def update_cache_data(self) -> list:
        """
        Updates the cached data for all places without doing full Airtable field enrichment.
        Only updates the 'Has Data File' field in Airtable to mark places that have data files.
        
        This method:
        1. Gets data for each place (cached or fresh) using helper_functions.get_and_cache_place_data
        2. Updates only the 'Has Data File' field in Airtable
        
        Returns:
            list: A list of dictionaries containing the results of each place cache update operation.
        """
        places_updated = []
        
        def cache_update_callback(record_id, place_data):
            """
            Callback function to update just the 'Has Data File' field in Airtable.
            
            Args:
                record_id (str): The Airtable record ID
                place_data (dict): The place data from helper_functions.get_and_cache_place_data
                
            Returns:
                dict: Empty dict as no additional data needs to be included in the result
            """
            # Only update the Has Data File field to indicate we have data
            self.update_place_record(record_id, 'Has Data File', 'Yes', overwrite=True)
            return {}
            
        # Check if we're in debug mode to decide between sequential or parallel execution
        if self.sequential_mode:
            logging.info("Running update_cache_data in SEQUENTIAL mode for debugging")
            for third_place in self.all_third_places:
                place_name = third_place['fields'].get('Place', 'Unknown Place')
                try:
                    result = self._process_place(third_place, cache_update_callback)
                    places_updated.append(result)
                    logging.info(f"Finished cache update for {place_name}")
                except Exception as e:
                    logging.error(f"Error updating cache for {place_name}: {e}")
        else:
            # Standard parallel execution with ThreadPoolExecutor
            logging.info(f"Running update_cache_data in PARALLEL mode with {MAX_THREAD_WORKERS} workers")
            with ThreadPoolExecutor(max_workers=MAX_THREAD_WORKERS) as executor:
                futures = {
                    executor.submit(self._process_place, third_place, cache_update_callback): 
                    third_place['fields'].get('Place', 'Unknown Place')
                    for third_place in self.all_third_places
                }

                for future in as_completed(futures):
                    place_name = futures[future]
                    try:
                        result = future.result()
                        places_updated.append(result)
                        logging.info(f"Finished cache update for {place_name}")
                    except Exception as e:
                        logging.error(f"Error updating cache for {place_name}: {e}")

        return places_updated

    def get_record(self, search_field: SearchField, search_value: str) -> dict:
        """
        Retrieves a single record from Airtable that matches the specified search criteria.
        
        Args:
            search_field: The field to search on (from SearchField enum)
            search_value: The value to search for
            
        Returns:
            dict: The matching record, or None if no match or multiple matches found
        """
        logging.info(f"Looking up record: {search_field.value}='{search_value}'")
        
        try:
            matched_records = self.charlotte_third_places.all(
                formula=match({search_field.value: search_value})
            )
            
            # Handle no matches
            if not matched_records:
                logging.warning(f"No records found for {search_field.value}='{search_value}'")
                return None
                
            # Handle multiple matches
            if len(matched_records) > 1:
                record_ids = [record['id'] for record in matched_records]
                logging.warning(
                    f"Found {len(matched_records)} records for {search_field.value}='{search_value}'. "
                    f"IDs: {record_ids}"
                )
                return None
            
            # Success case - exactly one match
            record = matched_records[0]
            logging.info(f"Found record: {record['id']}")
            return record

        except Exception as e:
            logging.error(f"Error looking up {search_field.value}='{search_value}': {e}")
            return None

    def get_place_photos(self, place_id: str) -> list:
        """
        Retrieves photos for a place using the configured data provider.

        Args:
            place_id (str): The Google Maps Place ID of the place.

        Returns:
            list: A list of photo URLs.
        """
        try:
            photos_response = self.data_provider.get_place_photos(place_id)
            
            if photos_response and 'photos' in photos_response:
                return photos_response['photos']
            else:
                logging.warning(f"No photos found for place ID: {place_id}")
                return []
        except Exception as e:
            logging.error(f"Error retrieving photos for place ID {place_id}: {e}")
            return []

    def find_duplicate_records(self, field_name: str, third_place_records: list) -> dict:
        """
        Identifies and returns a dictionary of values that appear more than once for a specified field
        across a list of records.

        Args:
            field_name (str): The name of the field to check for duplicate values.
            third_place_records (list): A list of records (dictionaries) from the Airtable database.

        Returns:
            dict: A dictionary with each key being a value that appears more than once in the specified field,
                and each value being the count of occurrences.
        """
        field_values = [
            record['fields'].get(field_name) for record in third_place_records
            if field_name in record['fields']
        ]

        field_values_count = Counter(field_values)

        multiple_occurrences = {value: count for value,
                                count in field_values_count.items() if count > 1}

        return multiple_occurrences

    def get_places_missing_field(self, field_to_check, third_place_records):
        """For a collection of third places returned by calling pyAirtable.Table.all(), return a list of places that are missing a value in the provided field_to_check.
        """
        missing_places = []
        for third_place in third_place_records:
            if (field_to_check not in third_place['fields']):
                place_name = third_place['fields']['Place']
                missing_places.append(place_name)

        return missing_places

    def has_data_file(self, place_id: str) -> bool:
        """Checks if the place with the given Google Maps Place ID has stored Google Maps reviews.

        Args:
            place_id (str): The Google Maps Place ID of the place to check.

        Returns:
            bool: True if the place has reviews ('Has Data File' field is 'Yes'), False otherwise.
        """
        logging.info(f"Checking if place with Google Maps Place ID {place_id} has reviews.")
        record = self.get_record(SearchField.GOOGLE_MAPS_PLACE_ID, place_id)
        if record:
            has_data_file = record['fields'].get('Has Data File', 'No')
            logging.info(f"Has Data File for Place ID {place_id}: {has_data_file}")
            return has_data_file == 'Yes'
        else:
            logging.warning(f"Record with Place ID {place_id} not found.")
            return False

    def get_place_types(self) -> List[str]:
        """Retrieves a list of all distinct types from the 'Type' column in the Airtable base.

        Returns:
            List[str]: A sorted list of unique place types.
        """
        logging.info("Collecting all distinct place types from 'Type' column.")
        place_types = set()

        for record in self.all_third_places:
            type_field = record['fields'].get('Type', None)
            if type_field:
                if isinstance(type_field, list):
                    place_types.update(type_field)
                else:
                    place_types.add(type_field)

        logging.info(f"Found {len(place_types)} distinct place types.")
        return sorted(place_types)

    def refresh_operational_statuses(self) -> List[Dict[str, Any]]:
        """
        Refreshes the 'Operational' status of each place in the Airtable base by checking their current status via the configured data provider.

        For each record in the 'Charlotte Third Places' Airtable base, this method performs the following:
        - Retrieves the Google Maps Place ID.
        - Uses the configured data provider to determine if the place is operational.
        - Updates the 'Operational' field in Airtable with 'Yes' if the place is operational, 'No' otherwise.
        - Compares the new operational status with the current value in the 'Operational' field.
            - If they are the same, the update is skipped to conserve API calls.
            - If they are different, the record is updated accordingly.
        - Collects detailed information about each operation, including whether it was updated, skipped, or failed.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each containing:
                - 'place_name': The name of the place.
                - 'place_id': The Google Maps Place ID.
                - 'record_id': The Airtable record ID.
                - 'old_value': The old value of the 'Operational' field.
                - 'new_value': The new value of the 'Operational' field.
                - 'update_status': A string indicating the operation status ('updated', 'skipped', 'failed').
                - 'message': Any additional messages or error descriptions.
        """
        results = []

        def process_place(third_place: dict) -> Dict[str, Any]:
            result = {
                'place_name': '',
                'place_id': '',
                'record_id': '',
                'old_value': '',
                'new_value': '',
                'update_status': '',
                'message': ''
            }
            try:
                place_name = third_place['fields'].get('Place', '')
                result['place_name'] = place_name

                record_id = third_place.get('id', '')
                result['record_id'] = record_id

                place_id = third_place['fields'].get('Google Maps Place Id')
                result['place_id'] = place_id

                if not place_id:
                    logging.warning(f"No Google Maps Place ID for {place_name}.")
                    result['update_status'] = 'failed'
                    result['message'] = 'No Google Maps Place ID.'
                    return result

                current_operational_value = third_place['fields'].get('Operational', '')
                result['old_value'] = current_operational_value

                is_operational = self.data_provider.is_place_operational(place_id)
                new_operational_value = 'Yes' if is_operational else 'No'
                result['new_value'] = new_operational_value

                if current_operational_value == new_operational_value:
                    logging.info(f"Operational status for '{place_name}' is unchanged ({current_operational_value}). Skipping update.")
                    result['update_status'] = 'skipped'
                else:
                    update_result = self.update_place_record(
                        record_id,
                        'Operational',
                        new_operational_value,
                        overwrite=True
                    )
                    if update_result['updated']:
                        logging.info(f"Updated Operational status for '{place_name}' from '{current_operational_value}' to '{new_operational_value}'.")
                        result['update_status'] = 'updated'
                    else:
                        logging.warning(f"Failed to update Operational status for '{place_name}'.")
                        result['update_status'] = 'failed'
                        result['message'] = 'Update failed.'

            except Exception as e:
                logging.error(f"Error processing place '{place_name}': {e}")
                result['update_status'] = 'failed'
                result['message'] = str(e)

            return result

        # Check if we're in debug mode to decide between sequential or parallel execution
        if self.sequential_mode:
            logging.info("Running refresh_operational_statuses in SEQUENTIAL mode for debugging")
            for third_place in self.all_third_places:
                place_name = third_place['fields'].get('Place', 'Unknown Place')
                try:
                    result = process_place(third_place)
                    results.append(result)
                    logging.info(f"Finished operational status check for {place_name}")
                except Exception as e:
                    logging.error(f"Error checking operational status for {place_name}: {e}")
                    results.append({
                        'place_name': place_name,
                        'place_id': '',
                        'record_id': '',
                        'old_value': '',
                        'new_value': '',
                        'update_status': 'failed',
                        'message': str(e)
                    })
        else:
            # Standard parallel execution with ThreadPoolExecutor
            logging.info(f"Running refresh_operational_statuses in PARALLEL mode with {MAX_THREAD_WORKERS} workers")
            with ThreadPoolExecutor(max_workers=MAX_THREAD_WORKERS) as executor:
                futures = {
                    executor.submit(process_place, third_place): third_place['fields'].get('Place', 'Unknown Place')
                    for third_place in self.all_third_places
                }
                for future in as_completed(futures):
                    place_name = futures[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logging.error(f"Error processing place '{place_name}': {e}")
                        results.append({
                            'place_name': place_name,
                            'place_id': '',
                            'record_id': '',
                            'old_value': '',
                            'new_value': '',
                            'update_status': 'failed',
                            'message': str(e)
                        })

        return results

    def _process_place(self, third_place: dict, callback) -> dict:
        """
        Common processing logic for place data that both enrich_base_data and update_cache_data share.
        
        Args:
            third_place (dict): The place data from Airtable.
            callback (callable): A function to call with place data that handles the specific processing logic.
                                The callback should accept (record_id, place_data) and return any additional data for the result.
        
        Returns:
            dict: Contains results of the place processing operation.
        """
        import helper_functions as helpers
        
        return_data = {
            "place_name": "",
            "place_id": "",
            "record_id": "",
            "status": "",
            "message": ""
        }

        try:
            if 'Place' not in third_place['fields']:
                return_data["message"] = "Missing place name."
                return_data["status"] = "skipped"
                return return_data
            
            place_name = third_place['fields']['Place']
            logging.info(f"Processing place: {place_name}")
            return_data['place_name'] = place_name
            
            record_id = third_place['id']
            return_data['record_id'] = record_id
            
            # Get place ID
            place_id = third_place['fields'].get('Google Maps Place Id', None)
            return_data['place_id'] = place_id
            
            # Use the helper_functions module to get place data with caching
            status, place_data, message = helpers.get_and_cache_place_data(place_name, place_id, 'charlotte')
            return_data["status"] = status
            return_data["message"] = message
            
            if status == 'failed' or status == 'skipped':
                logging.warning(f"Processing skipped for {place_name}: {message}")
                return return_data
            
            # Update the place ID in case it was newly found
            if place_data and place_data.get('place_id'):
                return_data["place_id"] = place_data.get('place_id')
            
            # Run the callback to handle specific processing
            callback_result = callback(record_id, place_data)
            if callback_result:
                return_data.update(callback_result)
            
            return return_data
                
        except Exception as e:
            logging.error(f"Error processing place {place_name if 'place_name' in locals() else 'unknown'}: {e}")
            return_data["message"] = f"Error: {str(e)}"
            return_data["status"] = "failed"
            return return_data
