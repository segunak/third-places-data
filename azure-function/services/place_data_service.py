import os
import re
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

    def _is_valid_photo_url(self, url: str) -> bool:
        if not url or not isinstance(url, str):
            logging.debug("Invalid photo URL: empty or not a string")
            return False
        if not url.startswith('http'):
            logging.debug(f"Invalid photo URL: does not start with http - {url}")
            return False
        return True

    # ======================================================
    # Operating Hours Normalization Utilities
    # ======================================================
    # Target format: "Day: H:MM AM - H:MM PM"
    # Examples: "Monday: 3:00 PM - 8:00 PM", "Tuesday: 11:00 AM - 2:00 PM, 5:00 PM - 10:00 PM"
    # Pass-through values: "Closed", "Open 24 hours"

    @staticmethod
    def _clean_google_hours_unicode(text: str) -> str:
        """Replace Google's Unicode formatting characters with regular ASCII equivalents."""
        if not text:
            return text
        text = text.replace('\u202f', ' ')   # narrow no-break space → space
        text = text.replace('\u2009', ' ')   # thin space → space
        text = text.replace('\u2013', '-')   # en dash → hyphen
        # Normalize "H:MM AM - H:MM PM" with consistent spacing around dash
        text = re.sub(r'\s*-\s*', ' - ', text)
        # Collapse any double spaces
        text = re.sub(r'  +', ' ', text)
        return text.strip()

    @staticmethod
    def _parse_compact_time(time_str: str, fallback_period: str = '') -> str:
        """Parse a compact time like '3PM', '11AM', '7:30AM', '12' into 'H AM/PM' or 'H:MM AM/PM' format.
        Drops minutes when on the hour (e.g., '3:00 PM' -> '3 PM', '3:30 PM' stays).

        Args:
            time_str: Compact time string (e.g., '3PM', '11AM', '7:30AM', '12')
            fallback_period: AM/PM to use if the time_str doesn't include one
        """
        if not time_str:
            return time_str
        time_str = time_str.strip()

        # Extract AM/PM suffix
        upper = time_str.upper()
        period = ''
        if upper.endswith('AM'):
            period = 'AM'
            time_str = time_str[:-2]
        elif upper.endswith('PM'):
            period = 'PM'
            time_str = time_str[:-2]
        else:
            period = fallback_period

        # Split hours and minutes
        if ':' in time_str:
            parts = time_str.split(':')
            hour = parts[0]
            minute = parts[1]
        else:
            hour = time_str
            minute = '00'

        try:
            hour_int = int(hour)
        except ValueError:
            return time_str + period  # Can't parse, return as-is

        # Drop :00 on the hour, keep :30 etc.
        if minute == '00':
            return f"{hour_int} {period}".strip()
        return f"{hour_int}:{minute} {period}".strip()

    @staticmethod
    def _parse_compact_time_range(range_str: str) -> str:
        """Parse a compact time range like '3-8PM' or '11AM-2PM' into '3:00 PM - 8:00 PM' format.

        Handles:
          - '3-8PM' → '3:00 PM - 8:00 PM'
          - '11AM-2PM' → '11:00 AM - 2:00 PM'
          - '7:30AM-5PM' → '7:30 AM - 5:00 PM'
          - '12-11PM' → '12:00 PM - 11:00 PM'
          - 'Closed' → 'Closed' (pass-through)
          - 'Open 24 hours' → 'Open 24 hours' (pass-through)
        """
        if not range_str or not isinstance(range_str, str):
            return range_str or ''

        range_str = range_str.strip()

        # Pass through non-time values
        lower = range_str.lower()
        if lower in ('closed', 'open 24 hours'):
            return range_str

        # Split on hyphen to get open and close times
        # Use regex to split on hyphen that's between time parts (not inside a time like 7:30)
        # Pattern: split on '-' that has a digit or AM/PM before and after
        parts = re.split(r'(?<=[APMapm0-9])-(?=[0-9])', range_str, maxsplit=1)

        if len(parts) != 2:
            return range_str  # Can't parse, return as-is

        open_str, close_str = parts

        # Determine the period (AM/PM) of the close time first
        close_upper = close_str.strip().upper()
        close_period = ''
        if close_upper.endswith('AM'):
            close_period = 'AM'
        elif close_upper.endswith('PM'):
            close_period = 'PM'

        # Determine the period of the open time
        open_upper = open_str.strip().upper()
        open_period = ''
        if open_upper.endswith('AM'):
            open_period = 'AM'
        elif open_upper.endswith('PM'):
            open_period = 'PM'
        else:
            # No AM/PM on open time — infer from close time
            open_period = close_period

        open_formatted = PlaceDataService._parse_compact_time(open_str, open_period)
        close_formatted = PlaceDataService._parse_compact_time(close_str, close_period)

        return f"{open_formatted} - {close_formatted}"

    @staticmethod
    def _strip_on_the_hour(text: str) -> str:
        """Strip :00 from on-the-hour times. '7:00 AM' -> '7 AM', '7:30 AM' stays.
        Also strips :00 before ' - ' for times without AM/PM (e.g., Google's bare noon '12:00 - 9 PM' -> '12 - 9 PM').
        The bare '12' case is then fixed by _fix_bare_noon which adds PM."""
        if not text:
            return text
        # Strip :00 before AM/PM (e.g., "7:00 AM" -> "7 AM")
        text = re.sub(r'(\d{1,2}):00(\s*(?:AM|PM))', r'\1\2', text)
        # Strip :00 before " - " (opening time without AM/PM, e.g., "12:00 - 9 PM" -> "12 - 9 PM")
        text = re.sub(r'(\d{1,2}):00(\s*-)', r'\1\2', text)
        return text

    @staticmethod
    def _fix_bare_noon(text: str) -> str:
        """Add PM to bare '12' opening times. Google sometimes sends noon without AM/PM.
        '12 - 9 PM' -> '12 PM - 9 PM'. Only applies to '12' not followed by AM/PM."""
        if not text:
            return text
        return re.sub(r'\b12(\s*-)', r'12 PM\1', text)

    @staticmethod
    def normalize_operating_hours(hours_list: List[str]) -> List[str]:
        """Normalize a list of operating hours strings to the canonical format.

        Handles both Google format (Unicode cleanup) and Outscraper format (compact time parsing).
        Strips :00 from on-the-hour times for cleaner display.
        Adds PM to bare noon times from Google.
        Target: 'Day: 3 PM - 8 PM' or 'Day: 3:30 PM - 8 PM'
        """
        if not hours_list:
            return []
        result = [PlaceDataService._clean_google_hours_unicode(line) for line in hours_list]
        result = [PlaceDataService._strip_on_the_hour(line) for line in result]
        return [PlaceDataService._fix_bare_noon(line) for line in result]

    def _select_prioritized_photos(self, photos_data: List[Dict[str, Any]], max_photos: int = 30) -> List[str]:
        if not photos_data:
            return []

        def parse_date(date_str: str):
            try:
                return datetime.datetime.strptime(date_str, "%m/%d/%Y %H:%M:%S")
            except (ValueError, AttributeError, TypeError):
                return datetime.datetime.min

        photos_data.sort(key=lambda x: parse_date(x.get('photo_date', '')), reverse=True)
        all_valid = [p for p in photos_data if isinstance(p, dict) and p.get('photo_url_big')]
        front, vibe, all_tag, other, tagless = [], [], [], [], []

        for photo in all_valid:
            tags = photo.get('photo_tags', [])
            if not isinstance(tags, list) or not tags:
                tagless.append(photo)
                continue
            if 'front' in tags:
                front.append(photo)
            elif 'vibe' in tags:
                vibe.append(photo)
            elif 'all' in tags:
                all_tag.append(photo)
            elif 'other' in tags:
                other.append(photo)
            else:
                tagless.append(photo)

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
        for photo in selected:
            url = photo['photo_url_big']
            if url not in seen:
                unique.append(photo)
                seen.add(url)

        return [photo['photo_url_big'] for photo in unique[:max_photos]]

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

    @abstractmethod
    def get_operating_hours(self, place_id: str) -> List[str]:
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
                PlaceDetailsField.REGULAR_OPENING_HOURS.value,
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
            return {
                "place_id": place_id,
                "error": str(e),
                "message": "Failed to retrieve details from Google Maps API"
            }

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

            # Place Photos (New) note:
            # Place Details/Nearby/Text Search responses can include at most 10 photo resources.
            # https://developers.google.com/maps/documentation/places/web-service/place-photos
            photo_refs = [p.get('name', '') for p in raw_data.get('photos', []) if 'name' in p]
            photo_records = []
            for name in photo_refs:
                if not name:
                    continue
                details = self._get_photo_details(name)
                if details and 'photoUri' in details:
                    photo_records.append({'photo_url_big': details['photoUri'], 'photo_tags': [], 'photo_date': ''})

            valid_records = [photo for photo in photo_records if self._is_valid_photo_url(photo.get('photo_url_big', ''))]
            selected_urls = [photo.get('photo_url_big', '') for photo in valid_records if photo.get('photo_url_big')][:30]
            raw_data['photos_data'] = valid_records
            logging.info(f"Photo selection for {place_id}: Total={len(photo_records)}, Selected={len(selected_urls)}")
            return {"place_id": place_id, "message": f"Selected {len(selected_urls)} photos", "photo_urls": selected_urls, "raw_data": raw_data}
        except Exception as e:
            logging.error(f"Error retrieving photos for place ID {place_id}: {e}")
            return {"place_id": place_id, "message": f"Error retrieving photos: {str(e)}", "photo_urls": [], "raw_data": {}}

    def _get_photo_details(self, photo_name: str) -> Dict[str, Any]:
        # Place photo resource names can expire. Always request fresh names from a recent
        # Place Details/Nearby/Text Search response before fetching media.
        # https://developers.google.com/maps/documentation/places/web-service/place-photos
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

    def get_operating_hours(self, place_id: str) -> List[str]:
        try:
            url = f'https://places.googleapis.com/v1/places/{place_id}?languageCode=en'
            headers = {
                'Content-Type': 'application/json',
                'X-Goog-Api-Key': self.API_KEY,
                'X-Goog-FieldMask': PlaceDetailsField.REGULAR_OPENING_HOURS.value
            }
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            raw = response.json()
            hours = raw.get('regularOpeningHours', {})
            weekday_descriptions = hours.get('weekdayDescriptions', [])
            normalized = self.normalize_operating_hours(weekday_descriptions)
            logging.info(f"Retrieved operating hours for {place_id}: {len(normalized)} days")
            return normalized
        except Exception as e:
            logging.error(f"Error retrieving operating hours for place ID {place_id}: {e}")
            return []


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
                # Try address first, then fallback to full_address. Outscraper API can be inconsistent.
                raw_address = raw.get('address') or raw.get('full_address', '')
                clean_address = self._clean_address(raw_address)
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

    def get_place_photos(self, place_id: str) -> Dict[str, Any]:
        try:
            response = self.client.google_maps_photos(place_id, language=self.default_params['language'], region=self.default_params['region'])
            if response and len(response) > 0 and len(response[0]) > 0:
                raw = response[0][0]
                all_photos = raw.get('photos_data', [])
                valid = []
                for p in all_photos:
                    url = p.get('photo_url_big', '')
                    if self._is_valid_photo_url(url):
                        valid.append(p)
                selected = self._select_prioritized_photos(valid, max_photos=30)
                logging.info(f"Photo selection for {place_id}: Total={len(all_photos)}, Valid={len(valid)}, Selected={len(selected)}")
                return {"place_id": place_id, "message": f"Retrieved {len(all_photos)} photos, selected {len(selected)}", "photo_urls": selected, "raw_data": raw}
            logging.warning(f"No photo results found for place ID {place_id} using Outscraper.")
            return {"place_id": place_id, "message": "No photos found", "photo_urls": []}
        except Exception as e:
            logging.error(f"Error retrieving photos from Outscraper for {place_id}: {e}")
            return {"place_id": place_id, "message": f"Error retrieving photos: {str(e)}",                "photo_urls": []}

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

    @staticmethod
    def _normalize_outscraper_hours(working_hours: Dict[str, Any]) -> List[str]:
        if not working_hours or not isinstance(working_hours, dict):
            return []
        day_order = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
        result = []
        for day in day_order:
            ranges = working_hours.get(day)
            if ranges and isinstance(ranges, list):
                formatted_ranges = [PlaceDataService._parse_compact_time_range(r) for r in ranges]
                result.append(f"{day}: {', '.join(formatted_ranges)}")
            elif ranges and isinstance(ranges, str):
                formatted = PlaceDataService._parse_compact_time_range(ranges)
                result.append(f"{day}: {formatted}")
        return result

    def get_operating_hours(self, place_id: str) -> List[str]:
        try:
            results = self.client.google_maps_search(
                place_id, limit=1,
                language=self.default_params['language'],
                region=self.default_params['region'],
                fields=['working_hours', 'name', 'place_id']
            )
            if results and len(results) > 0 and len(results[0]) > 0:
                raw = results[0][0]
                working_hours = raw.get('working_hours', {})
                normalized = self._normalize_outscraper_hours(working_hours)
                logging.info(f"Retrieved operating hours for {place_id} via Outscraper: {len(normalized)} days")
                return normalized
            return []
        except Exception as e:
            logging.error(f"Error retrieving operating hours for place ID {place_id} via Outscraper: {e}")
            return []


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
