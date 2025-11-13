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
from constants import DEFAULT_REVIEWS_LIMIT, PlaceDetailsField, OUTSCRAPER_BALANCE_THRESHOLD


class PlaceDataService(ABC):
    """Abstract base class that defines the contract for all place data services."""

    def __init__(self):
        if 'FUNCTIONS_WORKER_RUNTIME' in os.environ:
            logging.info('PlaceDataService instantiated for Azure Function use.')
        else:
            logging.info('PlaceDataService instantiated for local use.')
            dotenv.load_dotenv()

        self.GOOGLE_MAPS_API_KEY = os.environ['GOOGLE_MAPS_API_KEY']
        self._provider_type = None

    @property
    def provider_type(self) -> str:
        return self._provider_type

    @abstractmethod
    def get_place_details(self, place_id: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_place_reviews(self, place_id: str, limit: int = DEFAULT_REVIEWS_LIMIT) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_place_photos(self, place_id: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def find_place_id(self, place_name: str) -> str:
        pass

    def validate_place_id(self, place_id: str) -> bool:
        url = f'https://places.googleapis.com/v1/places/{place_id}?fields=id&languageCode=en'
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': self.GOOGLE_MAPS_API_KEY,
            'X-Goog-FieldMask': 'id'
        }
        try:
            response = requests.get(url, headers=headers)
            logging.debug(f"Received response for place ID validation: Status {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                return 'id' in data and data['id'] == place_id
            if response.status_code in (400, 404):
                return False
            logging.warning(f"Place ID validation failed with status {response.status_code}: {response.text}")
            return False
        except Exception as e:
            logging.error(f"Error validating place ID {place_id}: {e}")
            return False

    @abstractmethod
    def is_place_operational(self, place_id: str) -> bool:
        pass

    def place_id_handler(self, place_name: str, place_id: Optional[str] = None) -> str:
        if place_id and self.validate_place_id(place_id):
            return place_id
        return self.find_place_id(place_name)

    def get_all_place_data(self, place_id: str, place_name: str, skip_photos: bool = True) -> Dict[str, Any]:
        try:
            details = self.get_place_details(place_id)
            reviews = self.get_place_reviews(place_id)
            if skip_photos:
                photos = {"place_id": place_id, "message": "Photos retrieval skipped.", "photo_urls": []}
            else:
                photos = self.get_place_photos(place_id)
            return {
                "place_id": place_id,
                "place_name": place_name,
                "details": details,
                "reviews": reviews,
                "photos": photos,
                "data_source": self.__class__.__name__,
                "last_updated": datetime.datetime.now().isoformat()
            }
        except Exception as e:
            logging.error(f"Error retrieving data for place {place_name} with ID {place_id}: {e}")
            return {
                "place_id": place_id,
                "place_name": place_name,
                "error": str(e),
                "data_source": self.__class__.__name__,
                "last_updated": datetime.datetime.now().isoformat()
            }


class GoogleMapsProvider(PlaceDataService):
    def __init__(self):
        super().__init__()
        if 'FUNCTIONS_WORKER_RUNTIME' in os.environ:
            logging.info('GoogleMapsProvider instantiated for Azure Function use.')
        else:
            logging.info('GoogleMapsProvider instantiated for local use.')
            dotenv.load_dotenv()
        self.API_KEY = os.environ['GOOGLE_MAPS_API_KEY']
        self._provider_type = 'google'

    def get_place_details(self, place_id: str) -> Dict[str, Any]:
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
            details = {
                'place_name': raw_data.get('displayName', {}).get('text', ''),
                'place_id': place_id,
                'google_maps_url': raw_data.get('googleMapsUri', ''),
                'website': raw_data.get('websiteUri', ''),
                'address': raw_data.get('formattedAddress', ''),
                'description': raw_data.get('editorialSummary', {}).get('text', ''),
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

    def _determine_purchase_requirement(self, data: Dict[str, Any]) -> str:
        price_level = data.get('priceLevel', 'PRICE_LEVEL_UNSPECIFIED')
        mapping = {
            'PRICE_LEVEL_UNSPECIFIED': 'Unsure',
            'PRICE_LEVEL_FREE': 'No',
            'PRICE_LEVEL_INEXPENSIVE': 'Yes',
            'PRICE_LEVEL_MODERATE': 'Yes',
            'PRICE_LEVEL_EXPENSIVE': 'Yes',
            'PRICE_LEVEL_VERY_EXPENSIVE': 'Yes'
        }
        return mapping.get(price_level, 'Unsure')

    def _extract_parking_info(self, data: Dict[str, Any]) -> List[str]:
        parking_options = data.get('parkingOptions', {})
        tags = []
        if any(parking_options.get(k, False) for k in ["freeParkingLot", "freeStreetParking", "freeGarageParking"]):
            tags.append("Free")
        elif any(parking_options.get(k, False) for k in ["paidParkingLot", "paidStreetParking", "paidGarageParking", "valetParking"]):
            tags.append("Paid")
        else:
            tags.append("Unsure")
        if parking_options.get("freeGarageParking", False) or parking_options.get("paidGarageParking", False):
            tags.append("Garage")
        if parking_options.get("freeStreetParking", False) or parking_options.get("paidStreetParking", False):
            tags.append("Street")
        if parking_options.get("paidStreetParking", False):
            tags.append("Metered")
        return tags

    def get_place_reviews(self, place_id: str, limit: int = DEFAULT_REVIEWS_LIMIT) -> Dict[str, Any]:
        logging.warning("Direct Google Maps API doesn't provide reviews. Consider using OutscraperProvider for reviews.")
        return {"place_id": place_id, "message": "Reviews are not available directly from the Google Maps API", "reviews_data": []}

    def _is_valid_photo_url(self, url: str) -> bool:
        if not url or not isinstance(url, str):
            logging.debug("Invalid photo URL: empty or not a string")
            return False
        if not url.startswith('http'):
            logging.debug(f"Invalid photo URL: does not start with http - {url}")
            return False
        for pattern in ['/gps-cs-s/', '/gps-proxy/']:
            if pattern in url:
                logging.debug(f"Filtered out restricted photo URL containing '{pattern}': {url}")
                return False
        return True

    def get_place_photos(self, place_id: str) -> Dict[str, Any]:
        try:
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
            photo_refs = [p.get('name', '') for p in raw_data.get('photos', []) if 'name' in p]
            all_urls = []
            for name in photo_refs:
                if not name:
                    continue
                details = self._get_photo_details(name)
                if details and 'photoUri' in details:
                    all_urls.append(details['photoUri'])
            valid_urls = [u for u in all_urls if self._is_valid_photo_url(u)][:30]
            logging.info(f"Photo selection for {place_id}: Total={len(all_urls)}, Selected={len(valid_urls)}")
            return {"place_id": place_id, "message": f"Selected {len(valid_urls)} photos", "photo_urls": valid_urls, "raw_data": raw_data}
        except Exception as e:
            logging.error(f"Error retrieving photos for place ID {place_id}: {e}")
            return {"place_id": place_id, "message": f"Error retrieving photos: {str(e)}", "photo_urls": [], "raw_data": {}}

    def _get_photo_details(self, photo_name: str) -> Dict[str, Any]:
        params = {'maxHeightPx': '4800', 'maxWidthPx': '4800', 'key': self.API_KEY, 'skipHttpRedirect': 'true'}
        try:
            resp = requests.get(f'https://places.googleapis.com/v1/{photo_name}/media', params=params)
            if resp.status_code == 200:
                return resp.json()
            logging.error(f"Failed to retrieve photo details: {resp.text}")
            return {}
        except Exception as e:
            logging.error(f"Error retrieving photo details: {e}")
            return {}

    def find_place_id(self, place_name: str) -> str:
        url = 'https://places.googleapis.com/v1/places:searchText'
        headers = {'Content-Type': 'application/json', 'X-Goog-Api-Key': self.API_KEY, 'X-Goog-FieldMask': 'places.id'}
        params = {
            "textQuery": place_name,
            'languageCode': 'en',
            "locationBias": {"circle": {"center": {"latitude": 35.23075539296459, "longitude": -80.83165532446358}, "radius": 50000}}
        }
        try:
            response = requests.post(url, headers=headers, json=params)
            response.raise_for_status()
            data = response.json()
            places = data.get('places', [])
            if places:
                if len(places) > 1:
                    logging.warning(f"Multiple places found for '{place_name}'. Got {len(places)} results. Using only the first result.")
                return places[0].get('id', '')
            logging.warning(f"No places found for '{place_name}'.")
            return ''
        except Exception as e:
            logging.error(f"Error finding place ID for '{place_name}': {e}")
            return ''

    def is_place_operational(self, place_id: str) -> bool:
        try:
            url = f'https://places.googleapis.com/v1/places/{place_id}?languageCode=en'
            headers = {'Content-Type': 'application/json', 'X-Goog-Api-Key': self.API_KEY, 'X-Goog-FieldMask': PlaceDetailsField.BUSINESS_STATUS.value}
            response = requests.get(url, headers=headers)
            logging.debug(f"Received business status response: {response.text}")
            response.raise_for_status()
            raw = response.json()
            if 'businessStatus' in raw:
                status = raw['businessStatus']
                logging.debug(f"Business status for place ID {place_id}: {status}")
                return status != 'CLOSED_PERMANENTLY'
            return True
        except Exception as e:
            logging.error(f"Error checking operational status for place ID {place_id}: {e}")
            return True


class OutscraperProvider(PlaceDataService):
    def __init__(self):
        super().__init__()
        if 'FUNCTIONS_WORKER_RUNTIME' in os.environ:
            logging.info('OutscraperProvider instantiated for Azure Function use.')
        else:
            logging.info('OutscraperProvider instantiated for local use.')
            dotenv.load_dotenv()

        self.API_KEY = os.environ['OUTSCRAPER_API_KEY']
        self.client = ApiClient(api_key=self.API_KEY)
        self._provider_type = 'outscraper'
        self.charlotte_coordinates = "@35.23075539296459,-80.83165532446358,9z"
        self.default_params = {'language': 'en', 'region': 'US', 'async': False}
        self.balance_threshold = OUTSCRAPER_BALANCE_THRESHOLD

        try:
            balance_payload = self._fetch_outscraper_balance()
            
            if 'balance' not in balance_payload:
                raise ValueError("Response from Outscraper is missing 'balance' field")
            balance = float(balance_payload['balance'])
 
            logging.info(f"Outscraper balance retrieved: balance=${balance:.2f}")

            if balance < self.balance_threshold:
                raise Exception(
                    f"Outscraper balance ${balance:.2f} is below required minimum ${self.balance_threshold:.2f}. "
                    f"Add funds before using Outscraper features. Current state: balance=${balance:.2f}"
                )
            logging.info(
                f"Outscraper billing check passed: balance=${balance:.2f} (>= ${self.balance_threshold:.2f})"
            )
        except Exception as e:
            raise Exception(f"Failed Outscraper balance check: {e}")

    def _fetch_outscraper_balance(self) -> Dict[str, Any]:
        """Fetch current Outscraper billing snapshot. Returns full JSON including upcoming_invoice.

        Expected keys:
          - balance (float)
        Raises on any failure or malformed response.
        """
        url = 'https://api.app.outscraper.com/profile/balance'
        headers = {'X-API-KEY': self.API_KEY, 'Accept': 'application/json'}
        try:
            resp = requests.get(url, headers=headers, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            if 'balance' not in data:
                raise ValueError("'balance' field missing in response JSON")
            return data
        except Exception as e:
            raise Exception(f"Unable to retrieve balance from Outscraper endpoint '{url}': {e}")

    def get_place_details(self, place_id: str) -> Dict[str, Any]:
        try:
            results = self.client.google_maps_search(place_id, limit=1, language=self.default_params['language'], region=self.default_params['region'], coordinates=self.charlotte_coordinates)
    
            if results and len(results) > 0 and len(results[0]) > 0:
                raw = results[0][0]
                full_address = raw.get('full_address', '')
                clean_address = self._clean_address(full_address)
                cid = raw.get('cid')
                return {
                    "place_name": raw.get('name', ''),
                    "place_id": raw.get('place_id', place_id),
                    "google_maps_url": f'https://maps.google.com/?cid={cid}' if cid else '',
                    "website": raw.get('site', ''),
                    "address": clean_address,
                    "description": raw.get('description', ''),
                    "purchase_required": self._determine_purchase_requirement(raw),
                    "parking": self._extract_parking_info(raw),
                    "latitude": raw.get('latitude'),
                    "longitude": raw.get('longitude'),
                    "raw_data": raw
                }
            logging.warning(f"No results found for place ID {place_id} using Outscraper.")
            return self._create_empty_details_response(place_id)
        except Exception as e:
            logging.error(f"Error retrieving place details from Outscraper for {place_id}: {e}")
            return self._create_empty_details_response(place_id, str(e))

    def _clean_address(self, address: str) -> str:
        if not address:
            return ""
        suffixes = [", United States", ", USA", ", U.S.A.", ", US", ", U.S.", " United States", " USA", " US"]
        cleaned = address
        for s in suffixes:
            if cleaned.endswith(s):
                cleaned = cleaned[:-len(s)]
                logging.debug(f"Removed country suffix from address. Original: '{address}', Cleaned: '{cleaned}'")
                break
        title = ' '.join(word.capitalize() for word in cleaned.split())
        import re
        state_pattern = re.compile(r',\s*([A-Za-z]{2})(\s+\d|$)')
        def upper_state(m):
            return m.group(0).replace(m.group(1), m.group(1).upper())
        final = state_pattern.sub(upper_state, title)
        logging.debug(f"Formatted address: Original '{cleaned}' → Title Case '{title}' → Final '{final}'")
        return final

    def _create_empty_details_response(self, place_id: str, error_message: str = "") -> Dict[str, Any]:
        return {"place_name": "", "place_id": place_id, "google_maps_url": "", "website": "", "address": "", "description": "", "purchase_required": "Unsure", "parking": ["Unsure"], "latitude": None, "longitude": None, "raw_data": {}, "error": error_message}

    def _determine_purchase_requirement(self, data: Dict[str, Any]) -> str:
        price_range = data.get('range', '')
        if price_range and price_range != "$0" and price_range != "None":
            return "Yes"
        return "Unsure"

    def _extract_parking_info(self, data: Dict[str, Any]) -> List[str]:
        tags = []
        about = data.get('about', {})
        parking_section = about.get('Parking', {})
        if parking_section:
            if parking_section.get('Free parking lot', False) or parking_section.get('Free street parking', False):
                tags.append("Free")
            elif any(parking_section.get(k, False) for k in ["Paid parking lot", "Paid street parking", "Valet parking"]):
                tags.append("Paid")
            else:
                tags.append("Unsure")
            if parking_section.get("Garage", False):
                tags.append("Garage")
            if parking_section.get("Free street parking", False) or parking_section.get("Paid street parking", False):
                tags.append("Street")
            if parking_section.get("Paid street parking", False) and "Metered" not in tags:
                tags.append("Metered")
        else:
            tags.append("Unsure")
        return tags

    def get_place_reviews(self, place_id: str, limit: int = DEFAULT_REVIEWS_LIMIT) -> Dict[str, Any]:
        try:
            response = self.client.google_maps_reviews(place_id, limit=1, reviews_limit=limit, sort='newest', language=self.default_params['language'], ignore_empty=True)
            if response and len(response) > 0:
                raw = response[0]
                return {"place_id": place_id, "message": "", "reviews_data": raw.get('reviews_data', []), "raw_data": raw}
            logging.warning(f"No review results found for place ID {place_id} using Outscraper.")
            return {"place_id": place_id, "message": "No reviews found", "reviews_data": []}
        except Exception as e:
            logging.error(f"Error retrieving reviews from Outscraper for {place_id}: {e}")
            return {"place_id": place_id, "message": f"Error retrieving reviews: {str(e)}", "reviews_data": []}

    def _is_valid_photo_url(self, url: str) -> bool:
        if not url or not isinstance(url, str):
            logging.debug("Invalid photo URL: empty or not a string")
            return False
        if not url.startswith('http'):
            logging.debug(f"Invalid photo URL: does not start with http - {url}")
            return False
        for pattern in ['/gps-cs-s/', '/gps-proxy/']:
            if pattern in url:
                logging.debug(f"Filtered out restricted photo URL containing '{pattern}': {url}")
                return False
        return True

    def get_place_photos(self, place_id: str) -> Dict[str, Any]:
        try:
            response = self.client.google_maps_photos(place_id, language=self.default_params['language'], region=self.default_params['region'])
            if response and len(response) > 0 and len(response[0]) > 0:
                raw = response[0][0]
                all_photos = raw.get('photos_data', [])
                valid = []
                filtered = 0
                for p in all_photos:
                    url = p.get('photo_url_big', '')
                    if self._is_valid_photo_url(url):
                        valid.append(p)
                    else:
                        filtered += 1
                selected = self._select_prioritized_photos(valid, max_photos=30)
                logging.info(f"Photo selection for {place_id}: Total={len(all_photos)}, Filtered out={filtered}, Valid={len(valid)}, Selected={len(selected)}")
                return {"place_id": place_id, "message": f"Retrieved {len(all_photos)} photos, filtered out {filtered}, selected {len(selected)}", "photo_urls": selected, "raw_data": raw}
            logging.warning(f"No photo results found for place ID {place_id} using Outscraper.")
            return {"place_id": place_id, "message": "No photos found", "photo_urls": []}
        except Exception as e:
            logging.error(f"Error retrieving photos from Outscraper for {place_id}: {e}")
            return {"place_id": place_id, "message": f"Error retrieving photos: {str(e)}",                "photo_urls": []}

    def _select_prioritized_photos(self, photos_data, max_photos=30):
        if not photos_data:
            return []
        def parse_date(date_str):
            try:
                return datetime.datetime.strptime(date_str, "%m/%d/%Y %H:%M:%S")
            except (ValueError, AttributeError):
                return datetime.datetime.min
        photos_data.sort(key=lambda x: parse_date(x.get('photo_date', '')), reverse=True)
        all_valid = [p for p in photos_data if 'photo_url_big' in p]
        front, vibe, all_tag, other, tagless = [], [], [], [], []
        for p in all_valid:
            tags = p.get('photo_tags', [])
            if not isinstance(tags, list) or not tags:
                tagless.append(p)
                continue
            if 'front' in tags:
                front.append(p)
            elif 'vibe' in tags:
                vibe.append(p)
            elif 'all' in tags:
                all_tag.append(p)
            elif 'other' in tags:
                other.append(p)
            else:
                tagless.append(p)
        selected = []
        selected.extend(vibe[:min(len(vibe), max_photos)])
        remaining = max_photos - len(selected)
        selected.extend(front[:min(5, len(front), remaining)])
        remaining = max_photos - len(selected)
        selected.extend(all_tag[:remaining])
        remaining = max_photos - len(selected)
        selected.extend(other[:remaining])
        remaining = max_photos - len(selected)
        if remaining > 0:
            selected.extend(tagless[:remaining])
        unique, seen = [], set()
        for p in selected:
            url = p['photo_url_big']
            if url not in seen:
                unique.append(p)
                seen.add(url)
        return [p['photo_url_big'] for p in unique[:max_photos]]

    def find_place_id(self, place_name: str) -> str:
        try:
            query = f"{place_name}"
            results = self.client.google_maps_search(query, limit=1, language=self.default_params['language'], region=self.default_params['region'], coordinates=self.charlotte_coordinates)
            if results and len(results) > 0 and len(results[0]) > 0:
                candidates = results[0]
                if len(candidates) > 1:
                    logging.warning(f"Multiple places ({len(candidates)}) found for '{place_name}' using Outscraper despite limit=1. Using first exact match or first result.")
                for c in candidates:
                    if c.get('name', '').lower() == place_name.lower():
                        return c.get('place_id', '')
                if candidates[0].get('place_id'):
                    return candidates[0].get('place_id', '')
            logging.warning(f"No place ID found for '{place_name}' using Outscraper.")
            return ''
        except Exception as e:
            logging.error(f"Error finding place ID for '{place_name}' using Outscraper: {e}")
            return ''

    def is_place_operational(self, place_id: str) -> bool:
        try:
            details = self.get_place_details(place_id)
            if details and 'raw_data' in details:
                status = details['raw_data'].get('business_status', 'BUSINESS_STATUS_UNSPECIFIED')
                return status != 'CLOSED_PERMANENTLY'
            return True
        except Exception as e:
            logging.error(f"Error checking operational status for place ID {place_id}: {e}")
            return True


class PlaceDataProviderFactory:
    @staticmethod
    def get_provider(provider_type: str) -> PlaceDataService:
        if provider_type is None:
            raise ValueError("provider_type cannot be None - must be 'google' or 'outscraper'")
        if not isinstance(provider_type, str):
            raise ValueError(f"provider_type must be a string, got {type(provider_type).__name__}")
        normalized = provider_type.strip().lower()
        if normalized == 'google':
            logging.info("Creating new GoogleMapsProvider instance")
            return GoogleMapsProvider()
        if normalized == 'outscraper':
            logging.info("Creating new OutscraperProvider instance")
            return OutscraperProvider()
        raise ValueError(f"Unsupported provider type: '{provider_type}'. Must be 'google' or 'outscraper'.")
