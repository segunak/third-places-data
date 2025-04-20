import os
import json
import dotenv
import logging
import requests
import datetime
from unidecode import unidecode
from outscraper import ApiClient
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from constants import DEFAULT_REVIEWS_LIMIT, PlaceDetailsField

class PlaceDataProvider(ABC):
    """Abstract base class that defines the contract for all place data providers.

    This class establishes a common interface that all concrete data providers must implement,
    ensuring consistency in how place data is retrieved and structured regardless of the source.
    """

    def __init__(self):
        # Ensure Google Maps API key is loaded for place ID validation
        if 'FUNCTIONS_WORKER_RUNTIME' in os.environ:
            logging.info('PlaceDataProvider instantiated for Azure Function use.')
        else:
            # VS Code automatically loads local.settings.json into your environment when running/debugging Azure Functions locally, 
            # even outside the full runtime. This behavior mimics Azure Functions and populates os.environ with everything under Values.
            logging.info('PlaceDataProvider instantiated for local use.')
            dotenv.load_dotenv()

        # Ensure we have the Google Maps API key for place ID validation
        self.GOOGLE_MAPS_API_KEY = os.environ['GOOGLE_MAPS_API_KEY']

    @abstractmethod
    def get_place_details(self, place_id: str) -> Dict[str, Any]:
        """
        Retrieves comprehensive details about a place based on its unique identifier.

        Args:
            place_id (str): The unique identifier for the place.

        Returns:
            Dict[str, Any]: A standardized dictionary containing place details.
        """
        pass

    @abstractmethod
    def get_place_reviews(self, place_id: str, limit: int = DEFAULT_REVIEWS_LIMIT) -> Dict[str, Any]:
        """
        Retrieves reviews for a specific place.
        
        The returned dictionary must include the following standardized fields:
        - place_id: The Google Maps Place ID
        - message: Result message (can be empty if successful)
        - reviews_data: List of review data

        Args:
            place_id (str): The unique identifier for the place.
            limit (int): The maximum number of reviews to retrieve. Defaults to DEFAULT_REVIEWS_LIMIT.

        Returns:
            Dict[str, Any]: A standardized dictionary containing the reviews data.
        """
        pass

    @abstractmethod
    def get_place_photos(self, place_id: str) -> Dict[str, Any]:
        """
        Retrieves photo URLs for a specific place.
        
        The returned dictionary must include the following standardized fields:
        - place_id: The Google Maps Place ID
        - message: Result message (can be empty if successful)
        - photos: List of photo URLs or data

        Args:
            place_id (str): The unique identifier for the place.

        Returns:
            Dict[str, Any]: A standardized dictionary containing photo URLs and data.
        """
        pass

    @abstractmethod
    def find_place_id(self, place_name: str) -> str:
        """
        Finds the unique identifier for a place based on its name.

        Args:
            place_name (str): The name of the place.

        Returns:
            str: The unique identifier for the place, or an empty string if not found.
        """
        pass

    def validate_place_id(self, place_id: str) -> bool:
        """
        Validates whether a place ID exists and is valid using the Google Maps Places API.
        This method is implemented at the base class level to ensure all providers use
        the Google Maps API for validating place IDs, as it's the authoritative source.

        This method uses the free ID refresh approach as documented by Google:
        "You can refresh Place IDs at no charge, by making a Place Details request, 
        specifying only the place ID field in the fields parameter."

        Reference: https://developers.google.com/maps/documentation/places/web-service/place-id#refresh-id

        Args:
            place_id (str): The place ID to validate.

        Returns:
            bool: True if the place ID is valid, False otherwise.
        """
        # Use Google Maps API directly to validate place ID
        # Request only the 'id' field to ensure this is a no-cost operation
        url = f'https://places.googleapis.com/v1/places/{place_id}?fields=id&languageCode=en'
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': self.GOOGLE_MAPS_API_KEY,
            # Only request the ID field to minimize data transfer and ensure no cost
            'X-Goog-FieldMask': 'id'
        }

        try:
            response = requests.get(url, headers=headers)
            logging.debug(
                f"Received response for place ID validation: Status {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                return 'id' in data and data['id'] == place_id
            elif response.status_code == 404:
                # 404 (NOT_FOUND) indicates the place ID is obsolete or no longer valid
                logging.warning(
                    f"Place ID {place_id} is obsolete or no longer valid (status 404)")
                return False
            elif response.status_code == 400:
                # 400 (INVALID_REQUEST) indicates the place ID format is invalid
                logging.warning(
                    f"Place ID {place_id} has an invalid format (status 400)")
                return False
            else:
                logging.warning(
                    f"Place ID validation failed with status {response.status_code}: {response.text}")
                return False
        except Exception as e:
            logging.error(f"Error validating place ID {place_id}: {e}")
            return False

    @abstractmethod
    def is_place_operational(self, place_id: str) -> bool:
        """
        Checks if a place is currently operational.

        Args:
            place_id (str): The unique identifier for the place.

        Returns:
            bool: True if the place is operational, False otherwise.
        """
        pass

    def place_id_handler(self, place_name: str, place_id: Optional[str] = None) -> str:
        """
        Handles place ID validation and retrieval. If a place ID is provided and valid,
        it is returned. Otherwise, an attempt is made to find the place ID based on the place name.

        Args:
            place_name (str): The name of the place.
            place_id (Optional[str]): An existing place ID to validate, if available.

        Returns:
            str: A valid place ID, or an empty string if no valid ID could be obtained.
        """
        # Validate the place_id if it exists; otherwise, find a new one
        if place_id and self.validate_place_id(place_id):
            return place_id
        else:
            return self.find_place_id(place_name)

    def get_all_place_data(self, place_id: str, place_name: str, skip_photos: bool = True, force_refresh: bool = False) -> Dict[str, Any]:
        """
        Retrieves and combines all available data for a place, including details, reviews, and photos.

        Args:
            place_id (str): The unique identifier for the place.
            place_name (str): The name of the place.
            skip_photos (bool): If True, skip retrieving photos. Default is False.
            force_refresh (bool): If True, bypass any caching mechanism and retrieve fresh data.

        Returns:
            Dict[str, Any]: A comprehensive dictionary containing all available data about the place.
        """
        try:
            details = self.get_place_details(place_id)
            reviews = self.get_place_reviews(place_id)
            
            # Only fetch photos if not explicitly skipped
            if skip_photos:
                logging.info(f"get_all_place_data: Skipping photo retrieval for {place_name} as requested.")
                photos = {
                    "place_id": place_id,
                    "message": "Photos retrieval skipped.",
                    "photo_urls": []
                }
            else:
                photos = self.get_place_photos(place_id)

            # Combine all data into a unified structure
            combined_data = {
                "place_id": place_id,
                "place_name": place_name,
                "details": details,
                "reviews": reviews,
                "photos": photos,
                "data_source": self.__class__.__name__,
                "last_updated": self._get_current_timestamp()
            }

            return combined_data
        except Exception as e:
            logging.error(
                f"Error retrieving data for place {place_name} with ID {place_id}: {e}")
            return {
                "place_id": place_id,
                "place_name": place_name,
                "error": str(e),
                "data_source": self.__class__.__name__,
                "last_updated": self._get_current_timestamp()
            }

    def _get_current_timestamp(self) -> str:
        """
        Returns the current timestamp in ISO format.

        Returns:
            str: The current timestamp.
        """
        from datetime import datetime
        return datetime.now().isoformat()


class GoogleMapsProvider(PlaceDataProvider):
    """
    Implementation of PlaceDataProvider that directly uses the Google Maps API.
    """

    def __init__(self):
        """
        Initialize the provider with the Google Maps API key from environment variables.
        """
        super().__init__() # Call the base class constructor

        if 'FUNCTIONS_WORKER_RUNTIME' in os.environ:
            logging.info('GoogleMapsProvider instantiated for Azure Function use.')
        else:
            # VS Code automatically loads local.settings.json into your environment when running/debugging Azure Functions locally, 
            # even outside the full runtime. This behavior mimics Azure Functions and populates os.environ with everything under Values.
            logging.info('GoogleMapsProvider instantiated for local use.')
            dotenv.load_dotenv()

        self.API_KEY = os.environ['GOOGLE_MAPS_API_KEY']

    def get_place_details(self, place_id: str) -> Dict[str, Any]:
        """
        Retrieves details for a specific place using the Google Maps Places API.

        Args:
            place_id (str): The unique identifier for the place.
            
        Returns:
            Dict[str, Any]: A standardized dictionary containing place details.
        """
        try:
            fields = [
                PlaceDetailsField.ID.value,
                PlaceDetailsField.NAME.value,
                PlaceDetailsField.DISPLAY_NAME.value,
                PlaceDetailsField.GOOGLE_MAPS_URI.value,
                PlaceDetailsField.WEBSITE_URI.value,
                PlaceDetailsField.FORMATTED_ADDRESS.value,
                PlaceDetailsField.EDITORIAL_SUMMARY.value,
                PlaceDetailsField.ADDRESS_COMPONENTS.value,
                PlaceDetailsField.PARKING_OPTIONS.value,
                PlaceDetailsField.PRICE_LEVEL.value,
                PlaceDetailsField.PAYMENT_OPTIONS.value,
                PlaceDetailsField.PRIMARY_TYPE.value,
                PlaceDetailsField.TYPES.value,
                PlaceDetailsField.OUTDOOR_SEATING.value,
                PlaceDetailsField.LOCATION.value,
                PlaceDetailsField.BUSINESS_STATUS.value,
            ]
            
            url = f'https://places.googleapis.com/v1/places/{place_id}?languageCode=en'
            headers = {
                'Content-Type': 'application/json',
                'X-Goog-Api-Key': self.API_KEY,
                'X-Goog-FieldMask': ','.join(fields)
            }
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            raw_data = response.json()
            logging.debug(f"Received place details response: {raw_data}")

            details = {
                'place_name': raw_data.get('displayName', {}).get('text', ''),
                'place_id': place_id,
                'google_maps_url': raw_data.get('googleMapsUri', ''),
                'website': raw_data.get('websiteUri', ''),
                'address': raw_data.get('formattedAddress', ''),
                'description': self._extract_description(raw_data),
                'purchase_required': self._determine_purchase_requirement(raw_data),
                'parking': self._extract_parking_info(raw_data),
                'latitude': raw_data.get('location', {}).get('latitude'),
                'longitude': raw_data.get('location', {}).get('longitude'),
                'raw_data': raw_data
            }
            
            return details
        except Exception as e:
            logging.error(f"Error retrieving details for place ID {place_id}: {e}")
            return {}
    
    def _extract_description(self, data: Dict[str, Any]) -> str:
        """Extracts the editorial summary or description."""
        if 'editorialSummary' in data and 'text' in data['editorialSummary']:
            return data['editorialSummary']['text']
        return ''
    
    def _determine_purchase_requirement(self, data: Dict[str, Any]) -> str:
        """Determines if a purchase is required based on the price level."""
        price_level = data.get('priceLevel', 'PRICE_LEVEL_UNSPECIFIED')
        
        price_level_mapping = {
            'PRICE_LEVEL_UNSPECIFIED': 'Unsure',
            'PRICE_LEVEL_FREE': 'No',
            'PRICE_LEVEL_INEXPENSIVE': 'Yes',
            'PRICE_LEVEL_MODERATE': 'Yes',
            'PRICE_LEVEL_EXPENSIVE': 'Yes',
            'PRICE_LEVEL_VERY_EXPENSIVE': 'Yes'
        }
        
        return price_level_mapping.get(price_level, 'Unsure')
    
    def _extract_parking_info(self, data: Dict[str, Any]) -> List[str]:
        """Extracts parking information."""
        parking_options = data.get('parkingOptions', {})
        parking_tags = []
        
        # Determine if parking is free or paid
        if any(parking_options.get(key, False) for key in ["freeParkingLot", "freeStreetParking", "freeGarageParking"]):
            parking_tags.append("Free")
        elif any(parking_options.get(key, False) for key in ["paidParkingLot", "paidStreetParking", "paidGarageParking", "valetParking"]):
            parking_tags.append("Paid")
        else:
            parking_tags.append("Unsure")
        
        # Add modifiers
        if parking_options.get("freeGarageParking", False) or parking_options.get("paidGarageParking", False):
            parking_tags.append("Garage")
        if parking_options.get("freeStreetParking", False) or parking_options.get("paidStreetParking", False):
            parking_tags.append("Street")
        if parking_options.get("paidStreetParking", False):
            parking_tags.append("Metered")
        
        return parking_tags
    
    def get_place_reviews(self, place_id: str, limit: int = DEFAULT_REVIEWS_LIMIT) -> Dict[str, Any]:
        """
        Google Maps API doesn't directly provide reviews through their API.
        This would typically use a workaround like scraping or a third-party service.

        Args:
            place_id (str): The unique identifier for the place.
            limit (int): The maximum number of reviews to retrieve.

        Returns:
            Dict[str, Any]: A standardized dictionary with empty reviews data.
        """
        logging.warning("Direct Google Maps API doesn't provide reviews. Consider using OutscraperProvider for reviews.")
            
        # Return a standardized response with empty reviews data
        return {
            "place_id": place_id,
            "message": "Reviews are not available directly from the Google Maps API",
            "reviews_data": []
        }

    def get_place_photos(self, place_id: str) -> Dict[str, Any]:
        """
        Retrieves photo URLs for a place using the Google Maps Places API.

        Args:
            place_id (str): The unique identifier for the place.

        Returns:
            Dict[str, Any]: A standardized dictionary containing photo URLs.
        """
        try:
            # Request place details with just the photos field
            url = f'https://places.googleapis.com/v1/places/{place_id}?languageCode=en'
            headers = {
                'Content-Type': 'application/json',
                'X-Goog-Api-Key': self.API_KEY,
                'X-Goog-FieldMask': PlaceDetailsField.PHOTOS.value
            }
            
            response = requests.get(url, headers=headers)
            logging.debug(f"Received photo response: {response.text}")
            response.raise_for_status()
            
            raw_data = response.json()
            photo_references = []
            
            # Extract photo references
            if 'photos' in raw_data and isinstance(raw_data['photos'], list):
                photo_references = [photo.get('name', '') for photo in raw_data['photos'] if 'name' in photo]
            
            # Process each photo reference to get the actual URLs
            photo_urls = []
            for photo_name in photo_references:
                if photo_name:
                    photo_details = self._get_photo_details(photo_name)
                    if photo_details and 'photoUri' in photo_details:
                        photo_urls.append(photo_details['photoUri'])
            
            return {
                "place_id": place_id,
                "message": f"Retrieved {len(photo_urls)} photos",
                "photo_urls": photo_urls,
                "raw_data": raw_data
            }
        except Exception as e:
            logging.error(f"Error retrieving photos for place ID {place_id}: {e}")
            return {
                "place_id": place_id,
                "message": f"Error retrieving photos: {str(e)}",
                "photo_urls": [],
                "raw_data": {}
            }

    def _get_photo_details(self, photo_name: str) -> Dict[str, Any]:
        """
        Retrieves details for a specific photo using the Google Maps Places API.

        Args:
            photo_name (str): The resource name of the photo.

        Returns:
            Dict[str, Any]: A dictionary containing the photo details.
        """
        params = {
            'maxHeightPx': '4800',
            'maxWidthPx': '4800',
            'key': self.API_KEY,
            'skipHttpRedirect': 'true'
        }

        try:
            response = requests.get(f'https://places.googleapis.com/v1/{photo_name}/media', params=params)
            if response.status_code == 200:
                return response.json()
            else:
                logging.error(f"Failed to retrieve photo details: {response.text}")
                return {}
        except Exception as e:
            logging.error(f"Error retrieving photo details: {e}")
            return {}

    def find_place_id(self, place_name: str) -> str:
        """
        Finds the unique identifier for a place based on its name using the Google Maps Places API.

        Args:
            place_name (str): The name of the place.

        Returns:
            str: The unique identifier for the place, or an empty string if not found.
        """
        url = 'https://places.googleapis.com/v1/places:searchText'
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': self.API_KEY,
            'X-Goog-FieldMask': 'places.id'
        }
        params = {
            "textQuery": place_name,
            'languageCode': 'en',
            # Reference https://developers.google.com/maps/documentation/places/web-service/text-search#location-bias
            # Use https://www.mapdevelopers.com/draw-circle-tool.php to get latitude/longitude and radius.
            "locationBias": {
                "circle": {
                    # Uptown Charlotte
                    "center": {"latitude": 35.23075539296459, "longitude": -80.83165532446358},
                    "radius": 50000  # Meters. Max is 50,000
                }
            }
        }

        try:
            response = requests.post(url, headers=headers, json=params)
            response.raise_for_status()
            data = response.json()
            places = data.get('places', [])

            # Ensure we're only processing the first result if multiple are returned
            if len(places) > 0:
                if len(places) > 1:
                    logging.warning(f"Multiple places found for '{place_name}'. Got {len(places)} results. Using only the first result.")

                return places[0].get('id', '')
            else:
                logging.warning(f"No places found for '{place_name}'.")
                return ''
        except Exception as e:
            logging.error(f"Error finding place ID for '{place_name}': {e}")
            return ''

    def is_place_operational(self, place_id: str) -> bool:
        """
        Checks if a place is currently operational using the Google Maps Places API.

        Args:
            place_id (str): The unique identifier for the place.

        Returns:
            bool: True if the place is operational, False otherwise.
        """
        try:
            # Request place details with just the business status field
            url = f'https://places.googleapis.com/v1/places/{place_id}?languageCode=en'
            headers = {
                'Content-Type': 'application/json',
                'X-Goog-Api-Key': self.API_KEY,
                'X-Goog-FieldMask': PlaceDetailsField.BUSINESS_STATUS.value
            }
            
            response = requests.get(url, headers=headers)
            logging.debug(f"Received business status response: {response.text}")
            response.raise_for_status()
            raw_data = response.json()

            if 'businessStatus' in raw_data:
                # Check if the place is operational based on business status
                business_status = raw_data['businessStatus']
                logging.debug(f"Business status for place ID {place_id}: {business_status}")
                return business_status != 'CLOSED_PERMANENTLY'
        except Exception as e:
            logging.error(f"Error checking operational status for place ID {place_id}: {e}")
            # Default to True if we couldn't determine status
            return True

class OutscraperProvider(PlaceDataProvider):
    """
    Implementation of PlaceDataProvider using the Outscraper API.
    
    Outscraper provides a powerful interface to Google Maps data including places, reviews, and photos.
    This implementation uses Outscraper's API endpoints to provide the same functionality as the
    direct Google Maps API provider.
    
    API Reference: https://app.outscraper.com/api-docs
    """
    
    def __init__(self):
        """
        Initialize the provider with the Outscraper API key from environment variables.
        Sets up the API client and configures default parameters for Charlotte, NC.
        """
        super().__init__() # Call the base class constructor

        if 'FUNCTIONS_WORKER_RUNTIME' in os.environ:
            logging.info('OutscraperProvider instantiated for Azure Function use.')
        else:
            # VS Code automatically loads local.settings.json into your environment when running/debugging Azure Functions locally, 
            # even outside the full runtime. This behavior mimics Azure Functions and populates os.environ with everything under Values.
            logging.info('OutscraperProvider instantiated for local use.')
            dotenv.load_dotenv()

        self.API_KEY = os.environ['OUTSCRAPER_API_KEY']
        self.client = ApiClient(api_key=self.API_KEY)

        # Charlotte, NC coordinates for location bias
        # Format: "@latitude,longitude,zoom" as required by Outscraper API
        # Zoom level 9 provides appropriate coverage for a ~50,000 meter radius
        self.charlotte_coordinates = "@35.23075539296459,-80.83165532446358,9z"  # Uptown Charlotte with zoom
        
        # Default parameters for all requests
        self.default_params = {
            'language': 'en',          # English language results
            'region': 'US',            # United States region
            'async': False,            # Synchronous requests by default
        }

    def get_place_details(self, place_id: str) -> Dict[str, Any]:
        """
        Retrieves comprehensive details about a place using the Outscraper API.
        
        This method utilizes the maps/search-v3 endpoint to get detailed information about a place
        using its Google Maps place ID.
        
        API Endpoint: https://app.outscraper.com/api-docs#tag/Google/paths/~1maps~1search-v3/get
        
        Args:
            place_id (str): The unique identifier for the place.
            
        Returns:
            Dict[str, Any]: A standardized dictionary containing place details.
        """
        try:
            # Outscraper's google_maps_search can fetch place details using the place ID
            # Set region to 'US', language to 'en', and coordinates to Uptown Charlotte
            results = self.client.google_maps_search(
                place_id, 
                limit=1,
                language=self.default_params['language'],
                region=self.default_params['region'], 
                coordinates=self.charlotte_coordinates
            )

            if results and len(results) > 0 and len(results[0]) > 0:
                raw_data = results[0][0]
                
                # Process the address to remove country suffix
                full_address = raw_data.get('full_address', '')
                clean_address = self._clean_address(full_address)

                standardized_data = {
                    "place_name": raw_data.get('name', ''),
                    "place_id": raw_data.get('place_id', place_id),
                    "google_maps_url": f'https://maps.google.com/?cid={raw_data.get("cid", "")}',
                    "website": raw_data.get('site', ''),
                    "address": clean_address,
                    "description": raw_data.get('description', ''),
                    "purchase_required": self._determine_purchase_requirement(raw_data),
                    "parking": self._extract_parking_info(raw_data),
                    "latitude": raw_data.get('latitude'),
                    "longitude": raw_data.get('longitude'),
                    "raw_data": raw_data
                }

                return standardized_data
            else:
                logging.warning(f"No results found for place ID {place_id} using Outscraper.")
                return self._create_empty_details_response(place_id)
        except Exception as e:
            logging.error(f"Error retrieving place details from Outscraper for {place_id}: {e}")
            return self._create_empty_details_response(place_id, str(e))
    
    def _clean_address(self, address: str) -> str:
        """
        Cleans the address by removing country suffix.
        
        This method removes country suffixes like ", United States" or ", USA" from addresses
        to ensure consistent address formatting without the country.
        
        Args:
            address (str): The address to clean
            
        Returns:
            str: The cleaned address without country suffix
        """
        if not address:
            return ""
            
        # List of country suffixes to remove - add more patterns if needed
        country_suffixes = [
            ", United States",
            ", USA",
            ", U.S.A.",
            ", US",
            ", U.S.",
            " United States",
            " USA",
            " US"
        ]
        
        cleaned_address = address
        for suffix in country_suffixes:
            if cleaned_address.endswith(suffix):
                cleaned_address = cleaned_address[:-len(suffix)]
                logging.debug(f"Removed country suffix from address. Original: '{address}', Cleaned: '{cleaned_address}'")
                break
                
        return cleaned_address
    
    def _create_empty_details_response(self, place_id: str, error_message: str = "") -> Dict[str, Any]:
        """Creates a standardized empty response for when details can't be retrieved."""
        return {
            "place_name": "",
            "place_id": place_id,
            "google_maps_url": "",
            "website": "",
            "address": "",
            "description": "",
            "purchase_required": "Unsure",
            "parking": ["Unsure"],
            "latitude": None,
            "longitude": None,
            "raw_data": {},
            "error": error_message
        }
    
    def _determine_purchase_requirement(self, data: Dict[str, Any]) -> str:
        """Determines if a purchase is required based on price level or range."""

        price_range = data.get('range', '')

        if price_range and price_range != "$0" and price_range != "None":
            return "Yes"
        
        return "Unsure"
    
    def _extract_parking_info(self, data: Dict[str, Any]) -> List[str]:
        """Extracts parking information from Outscraper data."""
        parking_tags = []
        
        # Check 'about' section for parking info
        about = data.get('about', {})
        parking_section = about.get('Parking', {})
        
        if parking_section:
            # Determine if parking is free or paid
            if parking_section.get('Free parking lot', False) or parking_section.get('Free street parking', False):
                parking_tags.append("Free")
            elif any(parking_section.get(key, False) for key in ["Paid parking lot", "Paid street parking", "Valet parking"]):
                parking_tags.append("Paid")
            else:
                parking_tags.append("Unsure")
            
            # Add modifiers
            if parking_section.get("Garage", False):
                parking_tags.append("Garage")
            if parking_section.get("Free street parking", False) or parking_section.get("Paid street parking", False):
                parking_tags.append("Street")
            if parking_section.get("Paid street parking", False) and "Metered" not in parking_tags:
                parking_tags.append("Metered")
        else:
            # No specific parking information
            parking_tags.append("Unsure")
        
        return parking_tags
    
    def get_place_reviews(self, place_id: str, limit: int = DEFAULT_REVIEWS_LIMIT) -> Dict[str, Any]:
        """
        Retrieves reviews for a place using the Outscraper API.
        
        This method utilizes the maps/reviews-v3 endpoint to get reviews for a place using its
        Google Maps place ID. It returns not only the reviews but also basic information about
        the place itself.
        
        API Endpoint: https://app.outscraper.com/api-docs#tag/Google/paths/~1maps~1reviews-v3/get
        
        Args:
            place_id (str): The unique identifier for the place.
            limit (int): The maximum number of reviews to retrieve.
            
        Returns:
            Dict[str, Any]: A standardized dictionary containing the reviews data.
        """
        try:
            response = self.client.google_maps_reviews(
                place_id, 
                limit=1, 
                reviews_limit=limit, 
                sort='newest', 
                language=self.default_params['language'],
                ignore_empty=True
            )
            
            if response and len(response) > 0:
                raw_data = response[0]
                
                # Standardize the response format
                standardized_data = {
                    "place_id": place_id,
                    "message": "",  # Success case has no specific message
                    "reviews_data": raw_data.get('reviews_data', []),
                    "raw_data": raw_data
                }
                
                return standardized_data
            else:
                logging.warning(f"No review results found for place ID {place_id} using Outscraper.")
                return {
                    "place_id": place_id,
                    "message": "No reviews found",
                    "reviews_data": []
                }
        except Exception as e:
            logging.error(f"Error retrieving reviews from Outscraper for {place_id}: {e}")
            return {
                "place_id": place_id,
                "message": f"Error retrieving reviews: {str(e)}",
                "reviews_data": []
            }
    
    def get_place_photos(self, place_id: str) -> Dict[str, Any]:
        """
        Retrieves photo URLs for a place using the Outscraper API and applies an
        intelligent selection algorithm to prioritize photos based on tags.
        
        Selection criteria:
        1. Photos are sorted by date, newest to oldest
        2. Photos are prioritized in this order:
           - "vibe" tag: Include all vibe photos possible
           - "front" tag: Up to 5 photos (take all available if less than 5)
           - "all" and "other" tags: Fill remaining slots after vibe and front
        3. Returns a maximum of 25 photo URLs
        
        Args:
            place_id (str): The unique identifier for the place.
            
        Returns:
            Dict[str, Any]: A standardized dictionary containing the photo URLs.
        """
        try:
            response = self.client.google_maps_photos(
                place_id,
                language=self.default_params['language'],
                region=self.default_params['region']
            )
            
            if response and len(response) > 0 and len(response[0]) > 0:
                raw_data = response[0][0]
                all_photos_data = raw_data.get('photos_data', [])
                
                # Apply the photo selection algorithm
                selected_photos = self._select_prioritized_photos(all_photos_data)
                
                standardized_data = {
                    "place_id": place_id,
                    "message": f"Retrieved {len(all_photos_data)} photos, selected {len(selected_photos)}",
                    "photo_urls": selected_photos,
                    "raw_data": raw_data
                }
                
                return standardized_data
            else:
                logging.warning(f"No photo results found for place ID {place_id} using Outscraper.")
                return {
                    "place_id": place_id,
                    "message": "No photos found",
                    "photo_urls": []
                }
        except Exception as e:
            logging.error(f"Error retrieving photos from Outscraper for {place_id}: {e}")
            return {
                "place_id": place_id,
                "message": f"Error retrieving photos: {str(e)}",
                "photo_urls": []
            }
    
    def _select_prioritized_photos(self, photos_data, max_photos=25):
        """
        Selects photos based on specific criteria from the provided photos data.
        
        Selection criteria:
        1. First sort all photos by date, newest to oldest
        2. Select photos with the following priority:
           - "vibe" tag: Include all vibe photos possible
           - "front" tag: Up to 5 photos (take all available if less than 5)
           - "all" and "other" tags: Fill remaining slots after vibe and front
        3. Return a maximum of 'max_photos' photo URLs (default: 25)
        
        Args:
            photos_data (list): List of photo dictionaries from Outscraper
            max_photos (int): Maximum number of photos to select (default: 25)
        
        Returns:
            list: A list of selected photo URLs (photo_url_big values)
        """
        # Handle case where photos_data is empty or None
        if not photos_data:
            return []
        
        # Helper function to parse date strings
        def parse_date(date_str):
            try:
                # Try to parse the date format "MM/DD/YYYY HH:MM:SS"
                return datetime.datetime.strptime(date_str, "%m/%d/%Y %H:%M:%S")
            except (ValueError, AttributeError) as e:
                # If parsing fails, return the earliest possible date
                return datetime.datetime.min
        
        # Sort photos by date, newest first
        photos_data.sort(key=lambda x: parse_date(x.get('photo_date', '')), reverse=True)
        
        # Initialize collections for different photo categories
        front_photos = []
        vibe_photos = []
        all_photos = []
        other_photos = []
        remaining_photos = []
        
        # Categorize photos based on tags
        for photo in photos_data:
            # Skip photos without a photo_url_big
            if 'photo_url_big' not in photo:
                continue
            
            tags = photo.get('photo_tags', [])
            
            # Skip photos without any tags
            if not isinstance(tags, list) or not tags:
                continue
                
            # Categorize photos by priority tags
            if 'front' in tags:
                front_photos.append(photo)
            elif 'vibe' in tags:
                vibe_photos.append(photo)
            elif 'all' in tags:
                all_photos.append(photo)
            elif 'other' in tags:
                other_photos.append(photo)
            else:
                remaining_photos.append(photo)
        
        # Count the total preferred photos (front, vibe, all, other)
        total_preferred = len(front_photos) + len(vibe_photos) + len(all_photos) + len(other_photos)
        
        # If total preferred photos is less than max_photos, take all of them
        if total_preferred <= max_photos:
            selected_photos = front_photos + vibe_photos + all_photos + other_photos
        else:
            # Otherwise apply the priority rules with limits
            selected_photos = []
            
            # First priority: vibe photos (all of them, limited by max_photos)
            vibe_limit = min(len(vibe_photos), max_photos)
            selected_photos.extend(vibe_photos[:vibe_limit])
            
            # Second priority: front photos (up to 5)
            remaining_slots = max_photos - len(selected_photos)
            front_limit = min(5, len(front_photos), remaining_slots)
            selected_photos.extend(front_photos[:front_limit])
            
            # Next priority: all photos 
            remaining_slots = max_photos - len(selected_photos)
            selected_photos.extend(all_photos[:remaining_slots])
            
            # Last priority: other photos
            remaining_slots = max_photos - len(selected_photos)
            selected_photos.extend(other_photos[:remaining_slots])
        
        # If we still have room, add remaining photos
        remaining_slots = max_photos - len(selected_photos)
        if remaining_slots > 0:
            selected_photos.extend(remaining_photos[:remaining_slots])
        
        # Remove duplicates while preserving order (in case a photo has multiple tags)
        unique_photos = []
        seen_urls = set()
        
        for photo in selected_photos:
            url = photo['photo_url_big']
            if url not in seen_urls:
                unique_photos.append(photo)
                seen_urls.add(url)
        
        # Extract photo_url_big values from the selected photos
        selected_urls = [photo['photo_url_big'] for photo in unique_photos[:max_photos]]
        
        return selected_urls

    def find_place_id(self, place_name: str) -> str:
        """
        Finds the unique identifier for a place based on its name using the Outscraper API.
        
        This method searches for a place using its name and returns the place ID of the first
        matching result. It prioritizes exact name matches over partial matches.
        
        API Endpoint: https://app.outscraper.com/api-docs#tag/Google/paths/~1maps~1search-v3/get
        
        Args:
            place_name (str): The name of the place.
            
        Returns:
            str: The unique identifier for the place, or an empty string if not found.
        """
        try:
            # Search for the place by name with location bias towards Charlotte
            query = f"{place_name}"
            results = self.client.google_maps_search(
                query, 
                limit=1,  # Strictly limit to 1 result
                language=self.default_params['language'],
                region=self.default_params['region'],
                coordinates=self.charlotte_coordinates
            )
            
            if results and len(results) > 0 and len(results[0]) > 0:
                candidates = results[0]
                
                # Log a warning if more than one result was returned despite the limit=1
                if len(candidates) > 1:
                    logging.warning(f"Multiple places ({len(candidates)}) found for '{place_name}' using Outscraper despite limit=1. Using first exact match or first result.")
                
                # Look for exact matches first
                for candidate in candidates:
                    if candidate.get('name', '').lower() == place_name.lower():
                        return candidate.get('place_id', '')
                
                # If no exact match, return the first result
                if candidates[0].get('place_id'):
                    return candidates[0].get('place_id', '')
            
            logging.warning(f"No place ID found for '{place_name}' using Outscraper.")
            return ''
        except Exception as e:
            logging.error(f"Error finding place ID for '{place_name}' using Outscraper: {e}")
            return ''
    
    def is_place_operational(self, place_id: str) -> bool:
        """
        Checks if a place is currently operational using the Outscraper API.
        
        Outscraper returns a 'business_status' field in the place details that indicates the operational status of a business. Outscraper
        is just returning Google Maps data, so the business status values are the same as those in Google Maps API.

        - BUSINESS_STATUS_UNSPECIFIED: Default value. This value is unused.
        - OPERATIONAL: The establishment is operational, not necessarily open now.
        - CLOSED_TEMPORARILY: The establishment is temporarily closed.
        - CLOSED_PERMANENTLY: The establishment is permanently closed.
        
        Reference: https://developers.google.com/maps/documentation/places/web-service/reference/rest/v1/places#businessstatus
        Args:
            place_id (str): The unique identifier for the place.
            
        Returns:
            bool: True if the place is operational, False otherwise.
        """
        try:
            place_details = self.get_place_details(place_id)
            
            if place_details and 'raw_data' in place_details:
                business_status = place_details['raw_data'].get('business_status', 'BUSINESS_STATUS_UNSPECIFIED')
            
            # A place is not operational only if it's permanently closed
            operational = business_status != 'CLOSED_PERMANENTLY'
            
            if not operational:
                logging.warning(f"Place ID {place_id} is permanently closed.")
            
            return operational
        except Exception as e:
            logging.error(f"Error checking operational status for place ID {place_id}: {e}")
            # Default to True if we couldn't determine status
            return True


class PlaceDataProviderFactory:
    """
    Factory class for creating and managing place data providers.
    """

    @staticmethod
    def get_provider(provider_type: str) -> PlaceDataProvider:
        """
        Creates and returns a place data provider of the specified type.
        
        Note: This method instantiates a new provider every time it's called.
        For singleton behavior, use helper_functions.get_place_data_provider() instead,
        which ensures only one instance of each provider type is created.

        Args:
            provider_type (str): The type of provider to create ('google', 'outscraper', etc.).

        Returns:
            PlaceDataProvider: An instance of the requested provider.

        Raises:
            ValueError: If the specified provider type is not supported.
        """
        if not isinstance(provider_type, str):
            raise ValueError("provider_type must be a string and be either 'google' or 'outscraper'.")
        normalized = provider_type.strip().lower()
        if normalized == 'google':
            return GoogleMapsProvider()
        elif normalized == 'outscraper':
            return OutscraperProvider()
        else:
            raise ValueError(f"Unsupported provider type: {provider_type}. Must be 'google' or 'outscraper'.")

