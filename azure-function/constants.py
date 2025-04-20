import datetime
from enum import Enum

# Default number of reviews to retrieve from place data providers
DEFAULT_REVIEWS_LIMIT = 250

# Cache refresh intervals (in days)
DEFAULT_CACHE_REFRESH_INTERVAL = 90  # Default refresh interval for all data types

# Thread pool configuration
MAX_THREAD_WORKERS = 100 # Maximum number of threads for concurrent tasks

class SearchField(Enum):
    """Enumeration of unique fields for searching records in the Airtable database.

    This Enum defines the fields that are considered unique identifiers within the Airtable
    'Charlotte Third Places' table. These fields can be used to efficiently search and retrieve
    specific records.
    """
    PLACE_NAME = "Place"
    GOOGLE_MAPS_PLACE_ID = "Google Maps Place Id"

class PlaceDetailsField(Enum):
    """Enumeration of all possible fields for Google Maps Places API details.
    
    This Enum organizes all available fields by pricing tier as documented at:
    https://developers.google.com/maps/documentation/places/web-service/place-details
    
    Fields are grouped into several pricing tiers:
    - ESSENTIALS_IDS_ONLY: Basic identifying information
    - ESSENTIALS: Basic location information
    - PRO: Additional business details
    - ENTERPRISE: Opening hours and contact information
    - ENTERPRISE_ATMOSPHERE: Detailed amenities and features
    """
    # Essentials IDs Only SKU
    ATTRIBUTIONS = "attributions"
    ID = "id"
    NAME = "name"
    PHOTOS = "photos"
    
    # Essentials SKU
    ADDRESS_COMPONENTS = "addressComponents"
    ADR_FORMAT_ADDRESS = "adrFormatAddress"
    FORMATTED_ADDRESS = "formattedAddress"
    LOCATION = "location"
    PLUS_CODE = "plusCode"
    POSTAL_ADDRESS = "postalAddress"
    SHORT_FORMATTED_ADDRESS = "shortFormattedAddress"
    TYPES = "types"
    VIEWPORT = "viewport"
    
    # Pro SKU
    ACCESSIBILITY_OPTIONS = "accessibilityOptions"
    BUSINESS_STATUS = "businessStatus"
    CONTAINING_PLACES = "containingPlaces"
    DISPLAY_NAME = "displayName"
    GOOGLE_MAPS_LINKS = "googleMapsLinks"
    GOOGLE_MAPS_URI = "googleMapsUri"
    ICON_BACKGROUND_COLOR = "iconBackgroundColor"
    ICON_MASK_BASE_URI = "iconMaskBaseUri"
    PRIMARY_TYPE = "primaryType"
    PRIMARY_TYPE_DISPLAY_NAME = "primaryTypeDisplayName"
    PURE_SERVICE_AREA_BUSINESS = "pureServiceAreaBusiness"
    SUB_DESTINATIONS = "subDestinations"
    UTC_OFFSET_MINUTES = "utcOffsetMinutes"
    
    # Enterprise SKU
    CURRENT_OPENING_HOURS = "currentOpeningHours"
    CURRENT_SECONDARY_OPENING_HOURS = "currentSecondaryOpeningHours"
    INTERNATIONAL_PHONE_NUMBER = "internationalPhoneNumber"
    NATIONAL_PHONE_NUMBER = "nationalPhoneNumber"
    PRICE_LEVEL = "priceLevel"
    PRICE_RANGE = "priceRange"
    RATING = "rating"
    REGULAR_OPENING_HOURS = "regularOpeningHours"
    REGULAR_SECONDARY_OPENING_HOURS = "regularSecondaryOpeningHours"
    USER_RATING_COUNT = "userRatingCount"
    WEBSITE_URI = "websiteUri"
    
    # Enterprise + Atmosphere SKU
    ALLOWS_DOGS = "allowsDogs"
    CURBSIDE_PICKUP = "curbsidePickup"
    DELIVERY = "delivery"
    DINE_IN = "dineIn"
    EDITORIAL_SUMMARY = "editorialSummary"
    EV_CHARGE_OPTIONS = "evChargeOptions"
    FUEL_OPTIONS = "fuelOptions"
    GOOD_FOR_CHILDREN = "goodForChildren"
    GOOD_FOR_GROUPS = "goodForGroups"
    GOOD_FOR_WATCHING_SPORTS = "goodForWatchingSports"
    LIVE_MUSIC = "liveMusic"
    MENU_FOR_CHILDREN = "menuForChildren"
    PARKING_OPTIONS = "parkingOptions"
    PAYMENT_OPTIONS = "paymentOptions"
    OUTDOOR_SEATING = "outdoorSeating"
    RESERVABLE = "reservable"
    RESTROOM = "restroom"
    REVIEWS = "reviews"
    ROUTING_SUMMARIES = "routingSummaries"
    SERVES_BEER = "servesBeer"
    SERVES_BREAKFAST = "servesBreakfast"
    SERVES_BRUNCH = "servesBrunch"
    SERVES_COCKTAILS = "servesCocktails"
    SERVES_COFFEE = "servesCoffee"
    SERVES_DESSERT = "servesDessert"
    SERVES_DINNER = "servesDinner"
    SERVES_LUNCH = "servesLunch"
    SERVES_VEGETARIAN_FOOD = "servesVegetarianFood"
    SERVES_WINE = "servesWine"
    TAKEOUT = "takeout"
