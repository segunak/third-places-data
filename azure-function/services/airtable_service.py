import os
import time
import dotenv
import logging
import pyairtable
from pyairtable import Api
from collections import Counter
from urllib.parse import urlparse
from constants import SearchField, MAX_THREAD_WORKERS
from services import utils as helpers
from services.place_data_service import PlaceDataProviderFactory
from typing import Dict, Any, List
from pyairtable.formulas import match
from concurrent.futures import ThreadPoolExecutor, as_completed

class AirtableService:
    """Defines methods for interaction with the Charlotte Third Places Airtable database.
    """

    def __init__(self, provider_type=None, sequential_mode=False, view="Production"):
        logging.basicConfig(level=logging.INFO)

        if 'FUNCTIONS_WORKER_RUNTIME' in os.environ:
            logging.info('Airtable Service instantiated for Azure Function use.')
        else:
            logging.info('Airtable Service instantiated for local use.')
            dotenv.load_dotenv()

        self.sequential_mode = sequential_mode
        if self.sequential_mode:
            logging.info('Airtable Service running in DEBUG MODE with SEQUENTIAL execution')

        # The Airtable view to use when fetching records. Defaults to "Production".
        # Pass a different view name (e.g., "Insufficient") to filter records.
        self.view = view
        logging.info(f'Airtable Service configured to use view: "{self.view}"')

        self.AIRTABLE_BASE_ID = os.environ['AIRTABLE_BASE_ID']
        self.AIRTABLE_PERSONAL_ACCESS_TOKEN = os.environ['AIRTABLE_PERSONAL_ACCESS_TOKEN']
        self.AIRTABLE_WORKSPACE_ID = os.environ['AIRTABLE_WORKSPACE_ID']

        self.charlotte_third_places = pyairtable.Table(
            self.AIRTABLE_PERSONAL_ACCESS_TOKEN, self.AIRTABLE_BASE_ID, 'Charlotte Third Places'
        )

        self._all_third_places = None

        if not provider_type:
            raise ValueError("AirtableService requires provider_type to be specified ('google' or 'outscraper').")

        self.provider_type = provider_type
        self.data_provider = PlaceDataProviderFactory.get_provider(self.provider_type)
        logging.info(f"Initialized data service of type '{self.provider_type}'")
        self.api = Api(self.AIRTABLE_PERSONAL_ACCESS_TOKEN)
    
    @property
    def all_third_places(self):
        """
        Gets third places from Airtable, but only when needed.
        Uses the configured view to filter records (defaults to "Production").
        Caches the result to avoid repeated API calls.
        
        Returns:
            list: Third places records from the Airtable database
        """
        if self._all_third_places is None:
            logging.info(f"Retrieving third places from '{self.view}' view in Airtable.")
            self._all_third_places = self.charlotte_third_places.all(
                view=self.view, 
                sort=["-Created Time"]
            )
            
            if not self._all_third_places:
                logging.info(f"No records found in the '{self.view}' view. Nothing to return.")
            else:
                logging.info(f"Retrieved {len(self._all_third_places)} places from '{self.view}' view.")
        return self._all_third_places
    
    def clear_cached_places(self):
        """
        Clears the cached third places data to force a refresh on next access.
        """
        logging.info("Clearing cached third places data")
        self._all_third_places = None

    def _extract_raw_provider_values(self, raw_data: dict, data_source: str) -> Dict[str, Any]:
        """
        Extracts raw provider values from the API response based on the data source.
        Returns a mapping of Airtable field names to their raw API values.
        
        Args:
            raw_data: The raw API response from the provider
            data_source: The provider type ('GoogleMapsProvider' or 'OutscraperProvider')
            
        Returns:
            dict: Mapping of field names to raw provider values (structured objects preserved)
        """
        no_value = "No Value From Provider"
        
        if not raw_data:
            return {
                'Google Maps Place Id': no_value,
                'Google Maps Profile URL': no_value,
                'Website': no_value,
                'Address': no_value,
                'Description': no_value,
                'Purchase Required': no_value,
                'Parking': no_value,
                'Photos': no_value,
                'Latitude': no_value,
                'Longitude': no_value,
            }
        
        if data_source == 'GoogleMapsProvider':
            return {
                'Google Maps Place Id': no_value,  # Derived field
                'Google Maps Profile URL': raw_data.get('googleMapsUri', no_value),
                'Website': raw_data.get('websiteUri', no_value),
                'Address': raw_data.get('formattedAddress', no_value),
                'Description': raw_data.get('editorialSummary', no_value),
                'Purchase Required': raw_data.get('priceLevel', no_value),
                'Parking': raw_data.get('parkingOptions', no_value),
                'Photos': no_value,  # Photos come from separate API call
                'Latitude': raw_data.get('location', {}).get('latitude', no_value) if isinstance(raw_data.get('location'), dict) else no_value,
                'Longitude': raw_data.get('location', {}).get('longitude', no_value) if isinstance(raw_data.get('location'), dict) else no_value,
            }
        elif data_source == 'OutscraperProvider':
            # Outscraper stores parking info under 'about' -> 'Parking'
            about = raw_data.get('about', {})
            parking_raw = about.get('Parking', no_value) if isinstance(about, dict) else no_value
            
            return {
                'Google Maps Place Id': no_value,  # Derived field
                'Google Maps Profile URL': no_value,  # Constructed from CID
                'Website': raw_data.get('site', no_value),
                'Address': raw_data.get('full_address', no_value),
                'Description': raw_data.get('description', no_value),
                'Purchase Required': raw_data.get('range', no_value),
                'Parking': parking_raw,
                'Photos': no_value,  # Photos come from separate API call
                'Latitude': raw_data.get('latitude', no_value),
                'Longitude': raw_data.get('longitude', no_value),
            }
        else:
            # Unknown provider, return no_value for all fields
            return {
                'Google Maps Place Id': no_value,
                'Google Maps Profile URL': no_value,
                'Website': no_value,
                'Address': no_value,
                'Description': no_value,
                'Purchase Required': no_value,
                'Parking': no_value,
                'Photos': no_value,
                'Latitude': no_value,
                'Longitude': no_value,
            }
    
    def update_place_record(self, record_id: str, field_to_update: str, update_value, overwrite: bool, raw_provider_value="No Value From Provider") -> Dict[str, Any]:
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
                "new_value": update_value,
                "raw_provider_value": raw_provider_value
            }

            # Determine whether to update the field based on these rules:
            # 1. Only proceed if we have a valid new value (not empty)
            # 2. Always update if the current value is empty or "Unsure"
            # 3. Otherwise, only update if both conditions are true:
            #    a) The overwrite parameter allows replacing existing values
            #    b) The current value is different from the new value
            #
            # IMPORTANT: We compare against 'unsure' (lowercase) because current_value_normalized
            # has been processed by normalize_text(), which lowercases all text. Even though
            # Airtable stores "Unsure" with a capital U, after normalization it becomes 'unsure'.
            # Bug discovered: Previously compared against 'Unsure' which never matched since
            # normalize_text() converts to lowercase. Fixed 2024-12-03.
            if (
                update_value_normalized not in (None, '') and
                (
                    current_value_normalized in (None, 'unsure') or
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
                "new_value": None,
                "raw_provider_value": raw_provider_value
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

    def enrich_single_place(self, third_place: dict, provider_type: str, city: str, force_refresh: bool) -> dict:
        """
        Enriches a single Airtable place record.
        Args:
            third_place: A dictionary containing place data from Airtable
            provider_type: The data provider type (e.g., 'google', 'outscraper')
            city: The city name (must be provided)
            force_refresh: Whether to force refresh cached data
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

            place_name = third_place['fields']['Place']
            record_id = third_place['id']
            place_id = third_place['fields'].get('Google Maps Place Id', None)

            logging.info(f"Processing place: {place_name}")

            result['place_name'] = place_name
            result['record_id'] = record_id
            result['place_id'] = place_id

            status, place_data, message = helpers.get_and_cache_place_data(
                provider_type=provider_type,
                place_name=place_name,
                place_id=place_id,
                city=city,
                force_refresh=force_refresh,
                airtable_record_id=record_id
            )

            result["status"] = status
            result["message"] = message

            if status == 'failed' or status == 'skipped':
                logging.info(f"Processing skipped for {place_name}: {message}")
                return result

            if place_data and place_data.get('place_id'):
                result["place_id"] = place_data.get('place_id')
                place_id = place_data.get('place_id')

            if not place_data or 'details' not in place_data:
                logging.info(f"No place data found for {place_name}.")
                return result

            details = place_data['details']
            website = details.get('website', '')
            if website:
                website = self.get_base_url(website)

            address = details.get('address', '')
            latitude = details.get('latitude')
            longitude = details.get('longitude')
            parking = details.get('parking', ['Unsure'])

            if parking and len(parking) > 0:
                parking = parking[0]
            else:
                parking = 'Unsure'

            purchase_required = details.get('purchase_required', 'Unsure')
            description = details.get('description', '')
            google_maps_url = details.get('google_maps_url', '')
            photos_list = place_data.get('photos', {}).get('photo_urls', [])
            
            # Extract raw provider values from the API response
            raw_data = details.get('raw_data', {})
            data_source = place_data.get('data_source', '')
            raw_provider_values = self._extract_raw_provider_values(raw_data, data_source)
            
            # Tuple format is (field_value, overwrite)
            # Overwrite is True for fields that should be updated even if they already have a value
            fields_to_update = {
                'Google Maps Place Id': (place_id, True),
                'Google Maps Profile URL': (google_maps_url, True),
                'Website': (website, True),
                'Address': (address, True),
                'Description': (description, False),
                'Purchase Required': (purchase_required, False),
                'Parking': (parking, False),
                'Photos': (str(photos_list), True) if photos_list else (None, False),
                'Latitude': (str(latitude), True) if latitude else (None, False),
                'Longitude': (str(longitude), True) if longitude else (None, False),
            }

            for field_name, (field_value, overwrite) in fields_to_update.items():
                if field_value:
                    raw_value = raw_provider_values.get(field_name, "No Value From Provider")
                    update_result = self.update_place_record(
                        record_id,
                        field_name,
                        field_value,
                        overwrite,
                        raw_provider_value=raw_value
                    )
                    result['field_updates'][field_name] = update_result
            return result
        except Exception as e:
            logging.error(f"Error processing place {place_name if 'place_name' in locals() else 'unknown'}: {e}")
            result["message"] = f"Error: {str(e)}"
            result["status"] = "failed"
            return result

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
        Retrieves photos for a place using the configured place data service.

        Args:
            place_id (str): The Google Maps Place Id of the place.

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
        """Checks if the place with the given Google Maps Place Id has stored Google Maps reviews.

        Args:
            place_id (str): The Google Maps Place Id of the place to check.

        Returns:
            bool: True if the place has reviews ('Has Data File' field is 'Yes'), False otherwise.
        """
        logging.info(f"Checking if place with Google Maps Place Id {place_id} has reviews.")
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
        Wrapper that calls refresh_single_place_operational_status for each place in all_third_places.
        Args:
            data_provider: The place data service instance to use for checking operational status
        Returns:
            List[Dict[str, Any]]: Results for each place with update status and details
        """
        results = []
        for third_place in self.all_third_places:
            result = self.refresh_single_place_operational_status(third_place, data_provider)
            results.append(result)
        return results

    def refresh_single_place_operational_status(self, third_place: dict, data_provider) -> dict:
        """
        Checks and updates the operational status for a single place record.
        Args:
            third_place: A dictionary containing place data from Airtable
            data_provider: The place data service instance to use for checking operational status
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
            place_name = third_place['fields'].get('Place', '')
            record_id = third_place.get('id', '')
            place_id = third_place['fields'].get('Google Maps Place Id')

            result['place_name'] = place_name
            result['record_id'] = record_id
            result['place_id'] = place_id

            if not place_id:
                result['update_status'] = 'failed'
                result['message'] = 'No Google Maps Place Id.'
                return result

            current_operational_value = third_place['fields'].get('Operational', '')
            result['old_value'] = current_operational_value

            # Special case: preserve 'Opening Soon' without modification
            if current_operational_value == 'Opening Soon':
                result['new_value'] = current_operational_value
                result['update_status'] = 'success'
                result['message'] = 'Place is marked as Opening Soon; manual update required when operational. Automated refresh skipped.'
                return result

            is_operational = data_provider.is_place_operational(place_id)
            new_operational_value = 'Yes' if is_operational else 'No'
            result['new_value'] = new_operational_value

            if current_operational_value == new_operational_value:
                result['update_status'] = 'skipped'
                return result

            update_result = self.update_place_record(
                record_id,
                'Operational',
                new_operational_value,
                overwrite=True
            )

            if update_result['updated']:
                result['update_status'] = 'updated'
            else:
                result['update_status'] = 'failed'
                result['message'] = 'Update failed.'
            return result
        except Exception as e:
            result['update_status'] = 'failed'
            result['message'] = str(e)
            return result

    def enrich_base_data(self, provider_type: str = None, city: str = None, force_refresh: bool = False) -> list:
        """
        Enriches all places in the Airtable base by calling enrich_single_place for each.
        Args:
            provider_type: The place data service type (if None, uses self.provider_type)
            city: The city name (must be provided)
            force_refresh: Whether to force refresh cached data
        Returns:
            list: List of enrichment results for each place
        """
        if provider_type is None:
            provider_type = self.provider_type
        if city is None:
            raise ValueError("city must be provided to enrich_base_data")
        results = []
        for place in self.all_third_places:
            result = self.enrich_single_place(place, provider_type, city, force_refresh)
            results.append(result)
        return results
