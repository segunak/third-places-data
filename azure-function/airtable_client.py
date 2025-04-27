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
from concurrent.futures import ThreadPoolExecutor, as_completed

class AirtableClient:
    """Defines methods for interaction with the Charlotte Third Places Airtable database.
    """

    def __init__(self, provider_type=None, sequential_mode=False, insufficient_only=False):
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

        # Store the view filter flag
        self.insufficient_only = insufficient_only
        if self.insufficient_only:
            logging.info('Airtable Client running in FILTER MODE for "Insufficient" view only')

        self.AIRTABLE_BASE_ID = os.environ['AIRTABLE_BASE_ID']
        self.AIRTABLE_PERSONAL_ACCESS_TOKEN = os.environ['AIRTABLE_PERSONAL_ACCESS_TOKEN']
        self.AIRTABLE_WORKSPACE_ID = os.environ['AIRTABLE_WORKSPACE_ID']

        self.charlotte_third_places = pyairtable.Table(
            self.AIRTABLE_PERSONAL_ACCESS_TOKEN, self.AIRTABLE_BASE_ID, 'Charlotte Third Places'
        )

        self._all_third_places = None
        
        if not provider_type:
            raise ValueError("AirtableClient requires provider_type to be specified ('google' or 'outscraper').")

        self.provider_type = provider_type

        from place_data_providers import PlaceDataProviderFactory
        self.data_provider = PlaceDataProviderFactory.get_provider(self.provider_type)
        logging.info(f"Initialized data provider of type '{self.provider_type}'")
        
        self.api = Api(self.AIRTABLE_PERSONAL_ACCESS_TOKEN)
    
    @property
    def all_third_places(self):
        """
        Gets third places from Airtable, but only when needed.
        If insufficient_only is True, filters to only records in the "Insufficient" view.
        Caches the result to avoid repeated API calls.
        
        Returns:
            list: Third places records from the Airtable database
        """
        if self._all_third_places is None:
            if self.insufficient_only:
                logging.info("Retrieving third places from 'Insufficient' view in Airtable.")
                self._all_third_places = self.charlotte_third_places.all(
                    view="Insufficient", 
                    sort=["-Created Time"]
                )
                
                if not self._all_third_places:
                    logging.info("No records found in the 'Insufficient' view. Nothing to enrich.")
                else:
                    logging.info(f"Retrieved {len(self._all_third_places)} places from 'Insufficient' view.")
            else:
                logging.info("Retrieving all third places from Airtable.")
                self._all_third_places = self.charlotte_third_places.all(
                    sort=["-Created Time"]
                )
                logging.info(f"Retrieved {len(self._all_third_places)} places total.")
        return self._all_third_places
    
    def clear_cached_places(self):
        """
        Clears the cached third places data to force a refresh on next access.
        """
        logging.info("Clearing cached third places data")
        self._all_third_places = None
    
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

    def enrich_base_data(self, resource_manager) -> list:
        """
        Enriches the base data of places stored in Airtable with additional metadata.
        
        Args:
            resource_manager: The resource_manager module for configuration access
        
        Returns:
            list: A list of dictionaries containing the results of each place enrichment
        """
        logging.info("Started enriching base data in enrich_base_data.")
        places_results = []
        force_refresh = resource_manager.get_config('force_refresh', False)
        
        def process_single_place(third_place):
            """
            Process a single place by fetching its data and updating Airtable.
            
            Args:
                third_place: A dictionary containing place data from Airtable
                
            Returns:
                dict: Result of the place processing including update status
            """
            result = {
                "place_name": "",
                "place_id": "",
                "record_id": "",
                "status": "",
                "message": "",
                "field_updates": {}
            }
            
            try:
                if 'Place' not in third_place['fields']:
                    result["message"] = "Missing place name"
                    result["status"] = "skipped"
                    return result
                
                # Extract basic place information
                place_name = third_place['fields']['Place']
                record_id = third_place['id']
                place_id = third_place['fields'].get('Google Maps Place Id', None)
                
                logging.info(f"Processing place: {place_name}")
                result['place_name'] = place_name
                result['record_id'] = record_id
                result['place_id'] = place_id
                
                # Get place data using the helper function (with caching)
                status, place_data, message = helpers.get_and_cache_place_data(
                    provider_type=resource_manager.get_config('provider_type'),
                    place_name=place_name,
                    place_id=place_id,
                    city=resource_manager.get_config('city', 'charlotte'),
                    force_refresh=force_refresh
                )
                result["status"] = status
                result["message"] = message
                
                # Stop processing if no data was retrieved
                if status == 'failed' or status == 'skipped':
                    logging.info(f"Processing skipped for {place_name}: {message}")
                    return result
                
                # Update the place ID in case it was newly found
                if place_data and place_data.get('place_id'):
                    result["place_id"] = place_data.get('place_id')
                    place_id = place_data.get('place_id')
        
                # Stop if no place data or no details available
                if not place_data or 'details' not in place_data:
                    logging.info(f"No place data found for {place_name}.")
                    return result
                
                # Update 'Has Data File' status
                update_result = self.update_place_record(record_id, 'Has Data File', 'Yes', overwrite=True)
                result['field_updates']['Has Data File'] = update_result
                
                # Extract details from place data
                details = place_data['details']
                
                # Process website URL
                website = details.get('website', '')
                if website:
                    website = self.get_base_url(website)
                
                # Extract other place details
                address = details.get('address', '')
                latitude = details.get('latitude')
                longitude = details.get('longitude')
                
                # Process parking information (first tag represents if it's free/paid/unsure)
                parking = details.get('parking', ['Unsure'])
                if parking and len(parking) > 0:
                    parking = parking[0]
                else:
                    parking = 'Unsure'
                
                # Get purchase required status
                purchase_required = details.get('purchase_required', 'Unsure')
                
                # Other details
                description = details.get('description', '')
                google_maps_url = details.get('google_maps_url', '')
                
                # Prepare field updates (field value, should_overwrite)
                fields_to_update = {
                    'Google Maps Place Id': (place_id, True),
                    'Google Maps Profile URL': (google_maps_url, True),
                    'Website': (website, True),
                    'Address': (address, True),
                    'Description': (description, False),
                    'Purchase Required': (purchase_required, False),
                    'Parking': (parking, False),
                    'Latitude': (str(latitude), True) if latitude else (None, False),
                    'Longitude': (str(longitude), True) if longitude else (None, False),
                }
                
                # Handle photos - check if we already have photos in Airtable
                record = self.charlotte_third_places.get(record_id)
                has_existing_photos = 'Photos' in record['fields'] and record['fields']['Photos']
                photos_data = place_data.get('photos', {})
                photos_list = photos_data.get('photo_urls', []) if photos_data else []
                
                # Only add Photos if we have new photos AND the place doesn't already have photos
                if photos_list and not has_existing_photos:
                    fields_to_update['Photos'] = (str(photos_list), False)
                elif has_existing_photos:
                    logging.info(f"Skipping photo update as photos already exist in Airtable")
                
                # Process each field update
                for field_name, (field_value, overwrite) in fields_to_update.items():
                    if field_value:
                        update_result = self.update_place_record(
                            record_id,
                            field_name,
                            field_value,
                            overwrite
                        )
                        
                        result['field_updates'][field_name] = update_result
                return result
                
            except Exception as e:
                logging.error(f"Error processing place {place_name if 'place_name' in locals() else 'unknown'}: {e}")
                result["message"] = f"Error: {str(e)}"
                result["status"] = "failed"
                return result
        
        # Decide between sequential and parallel execution
        if self.sequential_mode:
            # Sequential execution (for debugging)
            logging.info("Running enrich_base_data in SEQUENTIAL mode")
            for third_place in self.all_third_places:
                place_name = third_place['fields'].get('Place')
                try:
                    result = process_single_place(third_place)
                    places_results.append(result)
                    logging.info(f"Finished processing {place_name}")
                except Exception as e:
                    logging.error(f"Error processing {place_name}: {e}")
        else:
            # Parallel execution using ThreadPoolExecutor
            logging.info(f"Running enrich_base_data in PARALLEL mode with {MAX_THREAD_WORKERS} workers")
            with ThreadPoolExecutor(max_workers=MAX_THREAD_WORKERS) as executor:
                futures = {
                    executor.submit(process_single_place, third_place): 
                    third_place['fields'].get('Place')
                    for third_place in self.all_third_places
                }
                
                for future in as_completed(futures):
                    place_name = futures[future]
                    try:
                        result = future.result()
                        places_results.append(result)
                        logging.info(f"Finished processing {place_name}")
                    except Exception as e:
                        logging.error(f"Error processing {place_name}: {e}")
        
        return places_results

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
                logging.info(f"No records found for {search_field.value}='{search_value}'")
                return None
                
            # Handle multiple matches
            if len(matched_records) > 1:
                record_ids = [record['id'] for record in matched_records]
                logging.info(
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

    def get_place_photos(self, place_id: str) -> List[str]:
        """
        Retrieves photos for a place using the configured data provider.

        Args:
            place_id (str): The Google Maps Place ID of the place.

        Returns:
            list: A list of photo URLs.
        """
        try:
            photos_response = self.data_provider.get_place_photos(place_id)
            
            if photos_response and 'photo_urls' in photos_response:
                return photos_response['photo_urls']
            else:
                logging.info(f"No photos found for place ID: {place_id}")
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
            logging.info(f"Record with Place ID {place_id} not found.")
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

    def refresh_operational_statuses(self, data_provider) -> List[Dict[str, Any]]:
        """
        Refreshes the 'Operational' status of each place in the Airtable base.

        Args:
            data_provider: The data provider to use for checking operational status
        
        Returns:
            List[Dict[str, Any]]: Results for each place with update status and details
        """
        results = []
        
        def process_single_place(third_place: dict) -> Dict[str, Any]:
            """
            Process a single place by checking its operational status and updating Airtable.
            
            Args:
                third_place: A dictionary containing place data from Airtable
                
            Returns:
                dict: Result of the operation including update status
            """
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
                # Get basic place information
                place_name = third_place['fields'].get('Place', '')
                record_id = third_place.get('id', '')
                place_id = third_place['fields'].get('Google Maps Place Id')
                
                result['place_name'] = place_name
                result['record_id'] = record_id
                result['place_id'] = place_id
                
                # Check for valid place ID
                if not place_id:
                    logging.info(f"No Google Maps Place ID for {place_name}.")
                    result['update_status'] = 'failed'
                    result['message'] = 'No Google Maps Place ID.'
                    return result
                
                # Get current status from Airtable
                current_operational_value = third_place['fields'].get('Operational', '')
                result['old_value'] = current_operational_value
                
                # Check if place is operational via data provider
                is_operational = data_provider.is_place_operational(place_id)
                new_operational_value = 'Yes' if is_operational else 'No'
                result['new_value'] = new_operational_value
                
                # Skip update if status hasn't changed
                if current_operational_value == new_operational_value:
                    logging.info(f"Operational status for '{place_name}' is unchanged ({current_operational_value}). Skipping update.")
                    result['update_status'] = 'skipped'
                    return result
                
                # Update Airtable with new status
                update_result = self.update_place_record(
                    record_id,
                    'Operational',
                    new_operational_value,
                    overwrite=True
                )
                
                # Record result of update operation
                if update_result['updated']:
                    logging.info(f"Updated Operational status for '{place_name}' from '{current_operational_value}' to '{new_operational_value}'.")
                    result['update_status'] = 'updated'
                else:
                    logging.info(f"Failed to update Operational status for '{place_name}'.")
                    result['update_status'] = 'failed'
                    result['message'] = 'Update failed.'
                
                return result
                
            except Exception as e:
                logging.error(f"Error processing place '{place_name if 'place_name' in locals() else 'unknown'}': {e}")
                result['update_status'] = 'failed'
                result['message'] = str(e)
                return result
        
        # Choose between sequential and parallel execution based on mode
        if self.sequential_mode:
            # Sequential execution for debugging
            logging.info("Running refresh_operational_statuses in SEQUENTIAL mode")
            for third_place in self.all_third_places:
                place_name = third_place['fields'].get('Place', 'Unknown Place')
                try:
                    result = process_single_place(third_place)
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
            # Parallel execution for performance
            logging.info(f"Running refresh_operational_statuses in PARALLEL mode with {MAX_THREAD_WORKERS} workers")
            with ThreadPoolExecutor(max_workers=MAX_THREAD_WORKERS) as executor:
                futures = {
                    executor.submit(process_single_place, third_place): third_place['fields'].get('Place', 'Unknown Place')
                    for third_place in self.all_third_places
                }
                
                for future in as_completed(futures):
                    place_name = futures[future]
                    try:
                        result = future.result()
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
        
        return results
