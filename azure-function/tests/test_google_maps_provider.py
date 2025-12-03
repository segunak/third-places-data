"""
Unit tests for the GoogleMapsProvider class from place_data_service.py.

All tests use mocked HTTP responses - no live API calls are made.
"""

import pytest
from unittest import mock

from conftest import TEST_PLACE_ID, TEST_PLACE_NAME, load_fixture, MockResponse, create_mock_response


class TestGoogleMapsProviderInit:
    """Tests for GoogleMapsProvider initialization."""

    def test_init_sets_api_key(self, mock_env_vars):
        """Test that initialization sets the API key from environment."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests"):
            provider = GoogleMapsProvider()
        
        assert provider.API_KEY == "test-google-maps-api-key"
        assert provider.GOOGLE_MAPS_API_KEY == "test-google-maps-api-key"
        assert provider.provider_type == "google"


class TestGoogleMapsProviderGetPlaceDetails:
    """Tests for get_place_details method."""

    def test_get_place_details_success(self, mock_google_maps_requests, google_maps_place_details):
        """Test successful place details retrieval."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests.get", mock_google_maps_requests["get"]):
            provider = GoogleMapsProvider()
            details = provider.get_place_details(TEST_PLACE_ID)
        
        assert details is not None
        assert details["place_id"] == TEST_PLACE_ID
        assert details["place_name"] == "Mattie Ruth's Coffee House"
        assert details["google_maps_url"] == "https://maps.google.com/?cid=5555315679825756572"
        assert details["website"] == "https://mattieruths.com"
        assert details["address"] == "1421 Central Ave, Charlotte, NC 28205, USA"
        assert details["latitude"] == 35.220585
        assert details["longitude"] == -80.814385
        assert "raw_data" in details

    def test_get_place_details_extracts_parking(self, mock_google_maps_requests):
        """Test that parking info is correctly extracted."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests.get", mock_google_maps_requests["get"]):
            provider = GoogleMapsProvider()
            details = provider.get_place_details(TEST_PLACE_ID)
        
        assert "parking" in details
        assert "Free" in details["parking"]
        assert "Street" in details["parking"]

    def test_get_place_details_determines_purchase_required(self, mock_google_maps_requests):
        """Test that purchase requirement is determined from price level."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests.get", mock_google_maps_requests["get"]):
            provider = GoogleMapsProvider()
            details = provider.get_place_details(TEST_PLACE_ID)
        
        # PRICE_LEVEL_MODERATE in fixture should map to "Yes"
        assert details["purchase_required"] == "Yes"

    def test_get_place_details_http_error_returns_empty(self, mock_env_vars):
        """Test that HTTP errors return an empty dict."""
        from services.place_data_service import GoogleMapsProvider
        from conftest import create_mock_response
        
        mock_response = create_mock_response({}, 500)
        with mock.patch("services.place_data_service.requests.get", return_value=mock_response):
            provider = GoogleMapsProvider()
            details = provider.get_place_details(TEST_PLACE_ID)
        
        assert details == {}


class TestGoogleMapsProviderDeterminePurchaseRequirement:
    """Tests for _determine_purchase_requirement method."""

    @pytest.mark.parametrize("price_level,expected", [
        ("PRICE_LEVEL_UNSPECIFIED", "Unsure"),
        ("PRICE_LEVEL_FREE", "No"),
        ("PRICE_LEVEL_INEXPENSIVE", "Yes"),
        ("PRICE_LEVEL_MODERATE", "Yes"),
        ("PRICE_LEVEL_EXPENSIVE", "Yes"),
        ("PRICE_LEVEL_VERY_EXPENSIVE", "Yes"),
        ("UNKNOWN_LEVEL", "Unsure"),
    ])
    def test_determine_purchase_requirement(self, mock_env_vars, price_level, expected):
        """Test purchase requirement mapping for various price levels."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests"):
            provider = GoogleMapsProvider()
        
        result = provider._determine_purchase_requirement({"priceLevel": price_level})
        assert result == expected


class TestGoogleMapsProviderExtractParkingInfo:
    """Tests for _extract_parking_info method."""

    def test_extract_parking_free_lot(self, mock_env_vars):
        """Test free parking lot extraction."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests"):
            provider = GoogleMapsProvider()
        
        data = {"parkingOptions": {"freeParkingLot": True}}
        result = provider._extract_parking_info(data)
        assert "Free" in result

    def test_extract_parking_paid_street(self, mock_env_vars):
        """Test paid street parking extraction."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests"):
            provider = GoogleMapsProvider()
        
        data = {"parkingOptions": {"paidStreetParking": True}}
        result = provider._extract_parking_info(data)
        assert "Paid" in result
        assert "Street" in result
        assert "Metered" in result

    def test_extract_parking_garage(self, mock_env_vars):
        """Test garage parking extraction."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests"):
            provider = GoogleMapsProvider()
        
        data = {"parkingOptions": {"freeGarageParking": True}}
        result = provider._extract_parking_info(data)
        assert "Free" in result
        assert "Garage" in result

    def test_extract_parking_no_options_returns_unsure(self, mock_env_vars):
        """Test that missing parking options returns Unsure."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests"):
            provider = GoogleMapsProvider()
        
        result = provider._extract_parking_info({})
        assert result == ["Unsure"]


class TestGoogleMapsProviderGetPlaceReviews:
    """Tests for get_place_reviews method."""

    def test_get_place_reviews_returns_empty_with_message(self, mock_env_vars):
        """Test that Google Maps provider returns empty reviews with message."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests"):
            provider = GoogleMapsProvider()
            reviews = provider.get_place_reviews(TEST_PLACE_ID)
        
        assert reviews["place_id"] == TEST_PLACE_ID
        assert reviews["reviews_data"] == []
        assert "not available" in reviews["message"]


class TestGoogleMapsProviderGetPlacePhotos:
    """Tests for get_place_photos method."""

    def test_get_place_photos_success(self, mock_google_maps_requests, google_maps_photo_media):
        """Test successful photo retrieval."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests.get", mock_google_maps_requests["get"]):
            provider = GoogleMapsProvider()
            photos = provider.get_place_photos(TEST_PLACE_ID)
        
        assert photos["place_id"] == TEST_PLACE_ID
        assert "photo_urls" in photos
        # Should have valid photos (3 photo refs in fixture, each returns a valid URL)
        assert len(photos["photo_urls"]) == 3

    def test_get_place_photos_filters_invalid_urls(self, mock_env_vars):
        """Test that invalid photo URLs are filtered out."""
        from services.place_data_service import GoogleMapsProvider
        from conftest import create_mock_response
        
        # Response with photos that have restricted URLs
        photos_response = {
            "photos": [
                {"name": "places/test/photos/good-photo"},
                {"name": "places/test/photos/bad-photo"}
            ]
        }
        
        def mock_get(url, **kwargs):
            if "/media" in url:
                if "good-photo" in url:
                    return create_mock_response({"photoUri": "https://valid-url.com/photo"})
                else:
                    return create_mock_response({"photoUri": "https://example.com/gps-cs-s/restricted"})
            return create_mock_response(photos_response)
        
        with mock.patch("services.place_data_service.requests.get", mock_get):
            provider = GoogleMapsProvider()
            photos = provider.get_place_photos(TEST_PLACE_ID)
        
        # Only valid URL should be included
        assert len(photos["photo_urls"]) == 1
        assert "gps-cs-s" not in photos["photo_urls"][0]


class TestGoogleMapsProviderIsValidPhotoUrl:
    """Tests for _is_valid_photo_url method."""

    @pytest.mark.parametrize("url,expected", [
        ("https://valid-url.com/photo.jpg", True),
        ("http://valid-url.com/photo.jpg", True),
        ("https://example.com/gps-cs-s/restricted", False),
        ("https://example.com/gps-proxy/restricted", False),
        ("", False),
        (None, False),
        ("not-a-url", False),
    ])
    def test_is_valid_photo_url(self, mock_env_vars, url, expected):
        """Test photo URL validation."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests"):
            provider = GoogleMapsProvider()
        
        result = provider._is_valid_photo_url(url)
        assert result == expected


class TestGoogleMapsProviderFindPlaceId:
    """Tests for find_place_id method."""

    def test_find_place_id_success(self, mock_google_maps_requests, google_maps_find_place):
        """Test successful place ID lookup."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests.get", mock_google_maps_requests["get"]):
            with mock.patch("services.place_data_service.requests.post", mock_google_maps_requests["post"]):
                provider = GoogleMapsProvider()
                place_id = provider.find_place_id(TEST_PLACE_NAME)
        
        assert place_id == TEST_PLACE_ID

    def test_find_place_id_not_found_returns_empty(self, mock_env_vars):
        """Test that no results returns empty string."""
        from services.place_data_service import GoogleMapsProvider
        from conftest import create_mock_response
        
        with mock.patch("services.place_data_service.requests.get"):
            with mock.patch("services.place_data_service.requests.post", return_value=create_mock_response({"places": []})):
                provider = GoogleMapsProvider()
                place_id = provider.find_place_id("Nonexistent Place")
        
        assert place_id == ""

    def test_find_place_id_multiple_results_uses_first(self, mock_env_vars):
        """Test that multiple results uses the first one."""
        from services.place_data_service import GoogleMapsProvider
        from conftest import create_mock_response
        
        multi_result = {
            "places": [
                {"id": "first-place-id"},
                {"id": "second-place-id"}
            ]
        }
        
        with mock.patch("services.place_data_service.requests.get"):
            with mock.patch("services.place_data_service.requests.post", return_value=create_mock_response(multi_result)):
                provider = GoogleMapsProvider()
                place_id = provider.find_place_id(TEST_PLACE_NAME)
        
        assert place_id == "first-place-id"


class TestGoogleMapsProviderValidatePlaceId:
    """Tests for validate_place_id method."""

    def test_validate_place_id_valid(self, mock_env_vars):
        """Test validation of a valid place ID."""
        from services.place_data_service import GoogleMapsProvider
        from conftest import create_mock_response
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response({"id": TEST_PLACE_ID})):
            provider = GoogleMapsProvider()
            is_valid = provider.validate_place_id(TEST_PLACE_ID)
        
        assert is_valid is True

    def test_validate_place_id_invalid_404(self, mock_env_vars):
        """Test validation returns False for 404."""
        from services.place_data_service import GoogleMapsProvider
        from conftest import create_mock_response
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response({}, 404)):
            provider = GoogleMapsProvider()
            is_valid = provider.validate_place_id("invalid-id")
        
        assert is_valid is False

    def test_validate_place_id_invalid_400(self, mock_env_vars):
        """Test validation returns False for 400."""
        from services.place_data_service import GoogleMapsProvider
        from conftest import create_mock_response
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response({}, 400)):
            provider = GoogleMapsProvider()
            is_valid = provider.validate_place_id("bad-format-id")
        
        assert is_valid is False


class TestGoogleMapsProviderIsPlaceOperational:
    """Tests for is_place_operational method."""

    def test_is_place_operational_true(self, mock_env_vars):
        """Test operational place returns True."""
        from services.place_data_service import GoogleMapsProvider
        from conftest import create_mock_response
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response({"businessStatus": "OPERATIONAL"})):
            provider = GoogleMapsProvider()
            is_operational = provider.is_place_operational(TEST_PLACE_ID)
        
        assert is_operational is True

    def test_is_place_operational_closed_permanently(self, mock_env_vars):
        """Test permanently closed place returns False."""
        from services.place_data_service import GoogleMapsProvider
        from conftest import create_mock_response
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response({"businessStatus": "CLOSED_PERMANENTLY"})):
            provider = GoogleMapsProvider()
            is_operational = provider.is_place_operational(TEST_PLACE_ID)
        
        assert is_operational is False

    def test_is_place_operational_no_status_returns_true(self, mock_env_vars):
        """Test missing status returns True (assume operational)."""
        from services.place_data_service import GoogleMapsProvider
        from conftest import create_mock_response
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response({})):
            provider = GoogleMapsProvider()
            is_operational = provider.is_place_operational(TEST_PLACE_ID)
        
        assert is_operational is True


class TestGoogleMapsProviderPlaceIdHandler:
    """Tests for place_id_handler method."""

    def test_place_id_handler_valid_id_returns_same(self, mock_env_vars):
        """Test that valid place ID is returned as-is."""
        from services.place_data_service import GoogleMapsProvider
        from conftest import create_mock_response
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response({"id": TEST_PLACE_ID})):
            with mock.patch("services.place_data_service.requests.post"):
                provider = GoogleMapsProvider()
                result = provider.place_id_handler(TEST_PLACE_NAME, TEST_PLACE_ID)
        
        assert result == TEST_PLACE_ID

    def test_place_id_handler_invalid_id_looks_up(self, mock_env_vars):
        """Test that invalid place ID triggers lookup."""
        from services.place_data_service import GoogleMapsProvider
        from conftest import create_mock_response
        
        def mock_get(url, **kwargs):
            if "searchText" not in url:
                # validate_place_id returns invalid
                return create_mock_response({}, 404)
            return create_mock_response({})
        
        with mock.patch("services.place_data_service.requests.get", mock_get):
            with mock.patch("services.place_data_service.requests.post", return_value=create_mock_response({"places": [{"id": "new-place-id"}]})):
                provider = GoogleMapsProvider()
                result = provider.place_id_handler(TEST_PLACE_NAME, "invalid-id")
        
        assert result == "new-place-id"

    def test_place_id_handler_no_id_looks_up(self, mock_env_vars):
        """Test that missing place ID triggers lookup."""
        from services.place_data_service import GoogleMapsProvider
        from conftest import create_mock_response
        
        with mock.patch("services.place_data_service.requests.get"):
            with mock.patch("services.place_data_service.requests.post", return_value=create_mock_response({"places": [{"id": "found-place-id"}]})):
                provider = GoogleMapsProvider()
                result = provider.place_id_handler(TEST_PLACE_NAME, None)
        
        assert result == "found-place-id"


class TestGoogleMapsProviderGetAllPlaceData:
    """Tests for get_all_place_data method."""

    def test_get_all_place_data_success(self, mock_google_maps_requests):
        """Test successful retrieval of all place data."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests.get", mock_google_maps_requests["get"]):
            provider = GoogleMapsProvider()
            data = provider.get_all_place_data(TEST_PLACE_ID, TEST_PLACE_NAME)
        
        assert data["place_id"] == TEST_PLACE_ID
        assert data["place_name"] == TEST_PLACE_NAME
        assert "details" in data
        assert "reviews" in data
        assert "photos" in data
        assert data["data_source"] == "GoogleMapsProvider"
        assert "last_updated" in data

    def test_get_all_place_data_skip_photos_default(self, mock_google_maps_requests):
        """Test that photos are skipped by default."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests.get", mock_google_maps_requests["get"]):
            provider = GoogleMapsProvider()
            data = provider.get_all_place_data(TEST_PLACE_ID, TEST_PLACE_NAME)
        
        assert data["photos"]["message"] == "Photos retrieval skipped."
        assert data["photos"]["photo_urls"] == []

    def test_get_all_place_data_include_photos(self, mock_google_maps_requests):
        """Test that photos are included when skip_photos=False."""
        from services.place_data_service import GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests.get", mock_google_maps_requests["get"]):
            provider = GoogleMapsProvider()
            data = provider.get_all_place_data(TEST_PLACE_ID, TEST_PLACE_NAME, skip_photos=False)
        
        assert len(data["photos"]["photo_urls"]) > 0

    def test_get_all_place_data_error_handling(self, mock_env_vars):
        """Test error handling in get_all_place_data when sub-methods fail.
        
        When get_place_details fails, it returns empty dict, but get_all_place_data
        still returns a valid response structure (graceful degradation).
        """
        from services.place_data_service import GoogleMapsProvider
        from requests.exceptions import HTTPError
        
        # Make all requests fail
        def mock_get(*args, **kwargs):
            raise HTTPError("500 Server Error")
        
        with mock.patch("services.place_data_service.requests.get", side_effect=mock_get):
            provider = GoogleMapsProvider()
            data = provider.get_all_place_data(TEST_PLACE_ID, TEST_PLACE_NAME)
        
        # Even when details fail, the response structure is maintained
        assert "details" in data
        assert "data_source" in data
        assert data["data_source"] == "GoogleMapsProvider"
        # Details should be empty due to error
        assert data["details"] == {}
