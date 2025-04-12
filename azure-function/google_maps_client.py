import os
import json
import dotenv
import logging
import requests
from datetime import datetime
from unidecode import unidecode
from place_data_providers import GoogleMapsProvider, PlaceDataProviderFactory

class GoogleMapsClient:
    """Class for common methods for interacting with the Google Maps API, regardless of the target database for recovered data.
    
    This class now utilizes the PlaceDataProvider design pattern but maintains its original interface for backward compatibility.
    """
    def __init__(self, provider_type=None):
        logging.basicConfig(level=logging.INFO)
        
        if 'FUNCTIONS_WORKER_RUNTIME' in os.environ:
            logging.info('Google Maps Client instantiated for Azure Function use.')
        else:
            logging.info('Google Maps Client instantiated for local use.')
            dotenv.load_dotenv()
        
        self.GOOGLE_MAPS_API_KEY = os.environ['GOOGLE_MAPS_API_KEY']
        
        # Create provider based on specified type or default
        if provider_type:
            self.provider = PlaceDataProviderFactory.get_provider(provider_type)
        else:
            # Use environment variable if set, otherwise default to direct Google Maps provider
            default_provider = os.environ.get('DEFAULT_PLACE_DATA_PROVIDER', 'google')
            self.provider = PlaceDataProviderFactory.get_provider(default_provider)
        
        # For backward compatibility, if using Google directly, also instantiate the direct provider
        if isinstance(self.provider, GoogleMapsProvider):
            self._direct_provider = self.provider
        else:
            self._direct_provider = GoogleMapsProvider()

    def strip_string(self, input_string):
        """Given a string, strip all special characters, punctuation, accents and the like from it. Return an alphanumeric characters only string in lowercase. Used for turning place name's into simple strings that can be used to name files and objects.
        """
        return unidecode(''.join(char for char in input_string if char.isalnum()).lower())

    def get_google_reviews(self):
        pass
    
    def place_photo_new(self, photo_name: str, maxHeightPx :str, maxWidthPx: str):
        """
        Retrieves the photo details or the photo itself from Google Maps Places API based on the provided photo name.
        If `skipHttpRedirect` is set to True, the function returns JSON containing the photo URL details; otherwise,
        it attempts to fetch the actual photo.

        Documentation: https://developers.google.com/maps/documentation/places/web-service/place-photos

        Args:
            photo_name (str): The resource name of the photo as returned by a Place Details request.
            maxHeightPx (str): The maximum height of the photo in pixels, from 1 to 4800.
            maxWidthPx (str): The maximum width of the photo in pixels, from 1 to 4800.
            skipHttpRedirect (bool): If True, skips HTTP redirect and returns a JSON response with the photo details.

        Returns:
            dict: A JSON object containing the photo details or the actual photo, depending on `skipHttpRedirect`.
        """
        # Use the direct provider method for backward compatibility
        params = {
            'maxHeightPx': maxHeightPx,
            'maxWidthPx': maxWidthPx,
            'key': self.GOOGLE_MAPS_API_KEY,
            'skipHttpRedirect': 'true'
        }
        
        response = requests.get(f'https://places.googleapis.com/v1/{photo_name}/media', params=params)
        logging.debug(f"Received response: {response.text}")
        
        if response.status_code == requests.codes.ok:
            try:
                return response.json()
            except ValueError as e:
                logging.warning(f"Request succeeded, but there was an error parsing the JSON: {e}")
                return None
        else:
            logging.error(f"Request failed with status code: {response.status_code}. Response text: {response.text}")
            return None

    def text_search_new(self, text_query: str, fields: list) -> dict:
        """
        Performs a text search using the Google Maps Places API Text Search endpoint. Returns the API's
        JSON response containing the requested fields for the queried text.

        Documentation: https://developers.google.com/maps/documentation/places/web-service/text-search

        Args:
            text_query (str): The query text for which the search is performed.
            fields (list): Fields to be included in the API response.

        Returns:
            dict: The JSON response from the Google Maps API if the request is successful;
                None if there is an error in the request or response.
        """
        # For backward compatibility, maintain the original method implementation
        url = 'https://places.googleapis.com/v1/places:searchText'
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': self.GOOGLE_MAPS_API_KEY,
            'X-Goog-FieldMask': ','.join(fields)
        }
        params = {
            "textQuery": text_query,
            'languageCode': 'en',
            # Reference https://developers.google.com/maps/documentation/places/web-service/text-search#location-bias
            # Use https://www.mapdevelopers.com/draw-circle-tool.php to get latitude/longitude and radius.
            "locationBias": {
                "circle": {
                    "center": {"latitude": 35.23075539296459, "longitude": -80.83165532446358}, # Uptown Charlotte.
                    "radius": 50000 # Meters. Max is 50,000
                }
            }
        }

        try:
            response = requests.post(url, headers=headers, json=params)
            logging.debug(f"Received response: {response.text}")
            response.raise_for_status()  # Will raise an exception for HTTP error codes
            return response.json()
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error occurred: {e}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error during requests to Google Maps API: {e}")
        except ValueError as e:
            logging.warning(f"Request succeeded, but there was an error parsing the JSON: {e}")
        
        return None

    def place_details_new(self, place_id: str, fields: list) -> dict:
        """
        Retrieves details for a specific place using the Google Maps Places API. This method constructs a request
        with specified fields and returns the response in JSON format.

        Documentation: https://developers.google.com/maps/documentation/places/web-service/place-details
        Field Return Values: https://developers.google.com/maps/documentation/places/web-service/reference/rest/v1/places

        Args:
            place_id (str): The unique identifier for the place.
            fields (list): A list of strings representing the fields to be included in the response.

        Returns:
            dict: The JSON response from the Google Maps API if the request is successful and valid;
                None if the request fails or if the response is not JSON.
        """
        # Use the direct provider to maintain backward compatibility
        url = f'https://places.googleapis.com/v1/places/{place_id}?languageCode=en'
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': self.GOOGLE_MAPS_API_KEY,
            'X-Goog-FieldMask': ','.join(fields)
        }
        
        try:
            response = requests.get(url, headers=headers)
            logging.debug(f"Received response: {response.text}")
            response.raise_for_status()  # Raises an HTTPError for bad responses
            return response.json()
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error occurred: {e}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error during requests to Google Maps API: {e}")
        except ValueError as e:
            logging.warning(f"Request succeeded, but there was an error parsing the JSON: {e}")
        
        return None

    def find_place_id(self, place_name: str) -> str:
        """
        Retrieves the Google Maps Place Id for a given place name. Place Ids can change over time,
        thus requiring periodic verification. This function performs a text search to find the most
        current Place Id based on the place name provided.

        Read more at: https://developers.google.com/maps/documentation/places/web-service/place-id

        Args:
            place_name (str): The name of the place to find the Place Id for.

        Returns:
            str: The Google Maps Place Id if exactly one match is found; an empty string otherwise.
        """
        # Use the new provider interface
        return self.provider.find_place_id(place_name)

    def validate_place_id(self, place_id: str) -> bool:
        """
        Checks if a place ID exists in the details retrieved from Google Maps. Place IDs can change 
        over time, so it's recommended to periodically refresh them. This operation incurs no cost 
        against the Places API.

        Reference: https://developers.google.com/maps/documentation/places/web-service/place-id#refresh-id

        Args:
            place_id (str): The ID of the place to be validated.

        Returns:
            bool: True if the 'id' key exists in the retrieved place details, False otherwise.
        """
        # Use the new provider interface
        return self.provider.validate_place_id(place_id)

    def place_id_handler(self, place_name, place_id) -> str:
        """
        Handle place_id interactions. Place Id's are Google's unique identifier for a place. 
        The database stores the place_id for every place, but they can change, which means 
        they need to be validated and/or refreshed from time to time. This function either 
        confirms the validity of an existing place_id and returns it, or tries to find a 
        place_id and return it. Either way, after calling this, you'll either have a valid 
        place_id or nothing at all, and can take action as needed.
        
        https://developers.google.com/maps/documentation/places/web-service/place-id
        """
        # Use the new provider interface
        return self.provider.place_id_handler(place_name, place_id)

    def is_place_operational(self, place_id: str) -> bool:
        """
        Checks whether a place identified by its Google Maps Place ID is still operational.

        Args:
            place_id (str): The Google Maps Place ID of the location.

        Returns:
            bool: True if the place is operational or temporarily closed; False if permanently closed.

        Note:
            The function uses the 'businessStatus' field from the Google Maps API place details. The 'businessStatus' can have the following enum values:
            - BUSINESS_STATUS_UNSPECIFIED: Default value. This value is unused.
            - OPERATIONAL: The establishment is operational, not necessarily open now.
            - CLOSED_TEMPORARILY: The establishment is temporarily closed.
            - CLOSED_PERMANENTLY: The establishment is permanently closed.

            This method returns False only if the 'businessStatus' is 'CLOSED_PERMANENTLY'. In all other cases, including 
            when the status is unspecified, temporarily closed, or if the status cannot be determined (e.g., API failure or 
            missing 'businessStatus' in response), it returns True.
            
            Reference: https://developers.google.com/maps/documentation/places/web-service/reference/rest/v1/places#businessstatus
            Reference: https://developers.google.com/maps/documentation/places/web-service/place-details
        """
        # Use the new provider interface
        return self.provider.is_place_operational(place_id)
        
    def get_all_place_data(self, place_id: str, place_name: str) -> dict:
        """
        Retrieves all available data for a place using the configured data provider.
        
        Args:
            place_id (str): The unique identifier for the place.
            place_name (str): The name of the place.
            
        Returns:
            dict: A comprehensive dictionary containing all available place data.
        """
        return self.provider.get_all_place_data(place_id, place_name)
