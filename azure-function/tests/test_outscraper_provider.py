"""
Unit tests for the OutscraperProvider class from place_data_service.py.

All tests use mocked HTTP responses and API clients - no live API calls are made.
"""

import pytest
from unittest import mock

from conftest import TEST_PLACE_ID, TEST_PLACE_NAME, load_fixture, create_mock_response
from constants import OUTSCRAPER_BALANCE_THRESHOLD


class TestOutscraperProviderInit:
    """Tests for OutscraperProvider initialization."""

    def test_init_with_sufficient_balance(self, mock_env_vars, outscraper_balance_sufficient):
        """Test successful initialization with sufficient balance."""
        from services.place_data_service import OutscraperProvider
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient"):
                provider = OutscraperProvider()
        
        assert provider.API_KEY == "test-outscraper-api-key"
        assert provider.GOOGLE_MAPS_API_KEY == "test-google-maps-api-key"
        assert provider.provider_type == "outscraper"
        assert provider.client is not None

    def test_init_with_low_balance_raises_exception(self, mock_env_vars, outscraper_balance_low):
        """Test that initialization fails with low balance."""
        from services.place_data_service import OutscraperProvider
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_low)):
            with mock.patch("services.place_data_service.ApiClient"):
                with pytest.raises(Exception) as exc_info:
                    OutscraperProvider()
        
        assert "below required minimum" in str(exc_info.value)

    def test_init_with_missing_balance_field_raises_exception(self, mock_env_vars):
        """Test that initialization fails when balance field is missing."""
        from services.place_data_service import OutscraperProvider
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response({"currency": "USD"})):
            with mock.patch("services.place_data_service.ApiClient"):
                with pytest.raises(Exception) as exc_info:
                    OutscraperProvider()
        
        assert "balance" in str(exc_info.value).lower()

    def test_init_balance_api_failure_raises_exception(self, mock_env_vars):
        """Test that initialization fails when balance API call fails."""
        from services.place_data_service import OutscraperProvider
        
        with mock.patch("services.place_data_service.requests.get", side_effect=Exception("Connection error")):
            with mock.patch("services.place_data_service.ApiClient"):
                with pytest.raises(Exception) as exc_info:
                    OutscraperProvider()
        
        assert "Failed Outscraper balance check" in str(exc_info.value)


class TestOutscraperProviderFetchOutscraperBalance:
    """Tests for _fetch_outscraper_balance method."""

    def test_fetch_balance_success(self, mock_env_vars, outscraper_balance_sufficient):
        """Test successful balance fetch."""
        from services.place_data_service import OutscraperProvider
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient"):
                provider = OutscraperProvider()
        
        # The provider initialized successfully, which means balance was fetched
        assert provider is not None

    def test_fetch_balance_http_error(self, mock_env_vars):
        """Test balance fetch with HTTP error."""
        from services.place_data_service import OutscraperProvider
        
        mock_response = create_mock_response({}, 500)
        with mock.patch("services.place_data_service.requests.get", return_value=mock_response):
            with mock.patch("services.place_data_service.ApiClient"):
                with pytest.raises(Exception):
                    OutscraperProvider()


class TestOutscraperProviderGetPlaceDetails:
    """Tests for get_place_details method."""

    def test_get_place_details_success(self, mock_env_vars, outscraper_balance_sufficient, outscraper_place_details):
        """Test successful place details retrieval."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_search.return_value = outscraper_place_details
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                details = provider.get_place_details(TEST_PLACE_ID)
        
        assert details is not None
        assert details["place_id"] == TEST_PLACE_ID
        assert details["place_name"] == "Mattie Ruth's Coffee House"
        assert "1421 Central Ave" in details["address"]
        assert details["latitude"] == 35.220585
        assert details["longitude"] == -80.814385
        assert "raw_data" in details

    def test_get_place_details_cleans_address(self, mock_env_vars, outscraper_balance_sufficient, outscraper_place_details):
        """Test that address is cleaned properly (country suffix removed)."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_search.return_value = outscraper_place_details
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                details = provider.get_place_details(TEST_PLACE_ID)
        
        # Address should not have "United States" suffix
        assert "United States" not in details["address"]

    def test_get_place_details_no_results_returns_empty(self, mock_env_vars, outscraper_balance_sufficient):
        """Test that empty results return empty details."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_search.return_value = [[]]
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                details = provider.get_place_details(TEST_PLACE_ID)
        
        assert details["place_name"] == ""
        assert details["place_id"] == TEST_PLACE_ID

    def test_get_place_details_api_error_returns_empty(self, mock_env_vars, outscraper_balance_sufficient):
        """Test that API errors return empty details with error message."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_search.side_effect = Exception("API Error")
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                details = provider.get_place_details(TEST_PLACE_ID)
        
        assert details["place_name"] == ""
        assert "error" in details


class TestOutscraperProviderCleanAddress:
    """Tests for _clean_address method."""

    @pytest.fixture
    def provider(self, mock_env_vars, outscraper_balance_sufficient):
        """Create a provider instance for testing."""
        from services.place_data_service import OutscraperProvider
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient"):
                return OutscraperProvider()

    @pytest.mark.parametrize("input_address,expected_suffix_removed", [
        ("123 Main St, Charlotte, NC 28205, United States", True),
        ("123 Main St, Charlotte, NC 28205, USA", True),
        ("123 Main St, Charlotte, NC 28205, U.S.A.", True),
        ("123 Main St, Charlotte, NC 28205, US", True),
        ("123 Main St, Charlotte, NC 28205", False),
    ])
    def test_clean_address_removes_country_suffix(self, provider, input_address, expected_suffix_removed):
        """Test that country suffixes are removed from addresses."""
        result = provider._clean_address(input_address)
        
        assert "United States" not in result
        assert result.endswith("28205") or not expected_suffix_removed

    def test_clean_address_title_cases(self, provider):
        """Test that address is properly title-cased."""
        result = provider._clean_address("123 main st, charlotte, nc 28205")
        
        assert "Main" in result
        assert "Charlotte" in result

    def test_clean_address_preserves_state_uppercase(self, provider):
        """Test that state abbreviation is uppercase."""
        result = provider._clean_address("123 Main St, Charlotte, nc 28205")
        
        assert "NC" in result

    def test_clean_address_empty_string(self, provider):
        """Test that empty string returns empty string."""
        result = provider._clean_address("")
        assert result == ""

    def test_clean_address_none_returns_empty(self, provider):
        """Test that None returns empty string."""
        result = provider._clean_address(None)
        assert result == ""


class TestOutscraperProviderDeterminePurchaseRequirement:
    """Tests for _determine_purchase_requirement method."""

    @pytest.fixture
    def provider(self, mock_env_vars, outscraper_balance_sufficient):
        """Create a provider instance for testing."""
        from services.place_data_service import OutscraperProvider
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient"):
                return OutscraperProvider()

    @pytest.mark.parametrize("price_range,expected", [
        ("$", "Yes"),
        ("$$", "Yes"),
        ("$$$", "Yes"),
        ("$$$$", "Yes"),
        ("$0", "Unsure"),
        ("None", "Unsure"),
        ("", "Unsure"),
        (None, "Unsure"),
    ])
    def test_determine_purchase_requirement(self, provider, price_range, expected):
        """Test purchase requirement determination from price range."""
        data = {"range": price_range} if price_range is not None else {}
        result = provider._determine_purchase_requirement(data)
        assert result == expected


class TestOutscraperProviderExtractParkingInfo:
    """Tests for _extract_parking_info method."""

    @pytest.fixture
    def provider(self, mock_env_vars, outscraper_balance_sufficient):
        """Create a provider instance for testing."""
        from services.place_data_service import OutscraperProvider
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient"):
                return OutscraperProvider()

    def test_extract_parking_free_lot(self, provider):
        """Test free parking lot extraction."""
        data = {"about": {"Parking": {"Free parking lot": True}}}
        result = provider._extract_parking_info(data)
        assert "Free" in result

    def test_extract_parking_free_street(self, provider):
        """Test free street parking extraction."""
        data = {"about": {"Parking": {"Free street parking": True}}}
        result = provider._extract_parking_info(data)
        assert "Free" in result
        assert "Street" in result

    def test_extract_parking_paid_street(self, provider):
        """Test paid street parking extraction."""
        data = {"about": {"Parking": {"Paid street parking": True}}}
        result = provider._extract_parking_info(data)
        assert "Paid" in result
        assert "Street" in result
        assert "Metered" in result

    def test_extract_parking_no_info_returns_unsure(self, provider):
        """Test that missing parking info returns Unsure."""
        result = provider._extract_parking_info({})
        assert result == ["Unsure"]

    def test_extract_parking_empty_section_returns_unsure(self, provider):
        """Test that empty parking section returns Unsure."""
        data = {"about": {"Parking": {}}}
        result = provider._extract_parking_info(data)
        assert "Unsure" in result


class TestOutscraperProviderGetPlaceReviews:
    """Tests for get_place_reviews method."""

    def test_get_place_reviews_success(self, mock_env_vars, outscraper_balance_sufficient, outscraper_reviews):
        """Test successful review retrieval."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_reviews.return_value = outscraper_reviews
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                reviews = provider.get_place_reviews(TEST_PLACE_ID)
        
        assert reviews["place_id"] == TEST_PLACE_ID
        assert len(reviews["reviews_data"]) == 3
        assert reviews["reviews_data"][0]["author_title"] == "Sarah Johnson"

    def test_get_place_reviews_empty_results(self, mock_env_vars, outscraper_balance_sufficient):
        """Test empty review results."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_reviews.return_value = []
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                reviews = provider.get_place_reviews(TEST_PLACE_ID)
        
        assert reviews["place_id"] == TEST_PLACE_ID
        assert reviews["reviews_data"] == []
        assert "No reviews found" in reviews["message"]

    def test_get_place_reviews_api_error(self, mock_env_vars, outscraper_balance_sufficient):
        """Test review retrieval with API error."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_reviews.side_effect = Exception("API Error")
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                reviews = provider.get_place_reviews(TEST_PLACE_ID)
        
        assert reviews["place_id"] == TEST_PLACE_ID
        assert reviews["reviews_data"] == []
        assert "Error" in reviews["message"]


class TestOutscraperProviderGetPlacePhotos:
    """Tests for get_place_photos method."""

    def test_get_place_photos_success(self, mock_env_vars, outscraper_balance_sufficient, outscraper_photos):
        """Test successful photo retrieval."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_photos.return_value = outscraper_photos
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                photos = provider.get_place_photos(TEST_PLACE_ID)
        
        assert photos["place_id"] == TEST_PLACE_ID
        assert "photo_urls" in photos
        # Should filter out restricted URLs (gps-cs-s and gps-proxy)
        for url in photos["photo_urls"]:
            assert "gps-cs-s" not in url
            assert "gps-proxy" not in url

    def test_get_place_photos_filters_restricted_urls(self, mock_env_vars, outscraper_balance_sufficient, outscraper_photos):
        """Test that restricted photo URLs are filtered out."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_photos.return_value = outscraper_photos
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                photos = provider.get_place_photos(TEST_PLACE_ID)
        
        # Fixture has 7 photos, 2 are restricted
        assert len(photos["photo_urls"]) == 5

    def test_get_place_photos_empty_results(self, mock_env_vars, outscraper_balance_sufficient):
        """Test empty photo results."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_photos.return_value = [[]]
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                photos = provider.get_place_photos(TEST_PLACE_ID)
        
        assert photos["place_id"] == TEST_PLACE_ID
        assert photos["photo_urls"] == []


class TestOutscraperProviderSelectPrioritizedPhotos:
    """Tests for _select_prioritized_photos method."""

    @pytest.fixture
    def provider(self, mock_env_vars, outscraper_balance_sufficient):
        """Create a provider instance for testing."""
        from services.place_data_service import OutscraperProvider
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient"):
                return OutscraperProvider()

    def test_select_prioritized_photos_empty_list(self, provider):
        """Test with empty photo list."""
        result = provider._select_prioritized_photos([])
        assert result == []

    def test_select_prioritized_photos_vibe_first(self, provider):
        """Test that vibe-tagged photos are prioritized."""
        photos = [
            {"photo_url_big": "http://other.jpg", "photo_date": "01/01/2024 10:00:00", "photo_tags": ["other"]},
            {"photo_url_big": "http://vibe.jpg", "photo_date": "01/02/2024 10:00:00", "photo_tags": ["vibe"]},
        ]
        result = provider._select_prioritized_photos(photos, max_photos=2)
        assert result[0] == "http://vibe.jpg"

    def test_select_prioritized_photos_front_limited(self, provider):
        """Test that front-tagged photos are limited to 5."""
        photos = [
            {"photo_url_big": f"http://front{i}.jpg", "photo_date": f"01/{i:02d}/2024 10:00:00", "photo_tags": ["front"]}
            for i in range(1, 11)
        ]
        result = provider._select_prioritized_photos(photos, max_photos=30)
        front_count = sum(1 for url in result if "front" in url)
        assert front_count <= 5

    def test_select_prioritized_photos_respects_max(self, provider):
        """Test that max_photos limit is respected."""
        photos = [
            {"photo_url_big": f"http://photo{i}.jpg", "photo_date": f"01/{i:02d}/2024 10:00:00", "photo_tags": ["vibe"]}
            for i in range(1, 50)
        ]
        result = provider._select_prioritized_photos(photos, max_photos=10)
        assert len(result) <= 10

    def test_select_prioritized_photos_deduplicates(self, provider):
        """Test that duplicate URLs are removed."""
        photos = [
            {"photo_url_big": "http://same.jpg", "photo_date": "01/01/2024 10:00:00", "photo_tags": ["vibe"]},
            {"photo_url_big": "http://same.jpg", "photo_date": "01/02/2024 10:00:00", "photo_tags": ["front"]},
        ]
        result = provider._select_prioritized_photos(photos, max_photos=30)
        assert len(result) == 1


class TestOutscraperProviderIsValidPhotoUrl:
    """Tests for _is_valid_photo_url method."""

    @pytest.fixture
    def provider(self, mock_env_vars, outscraper_balance_sufficient):
        """Create a provider instance for testing."""
        from services.place_data_service import OutscraperProvider
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient"):
                return OutscraperProvider()

    @pytest.mark.parametrize("url,expected", [
        ("https://valid-url.com/photo.jpg", True),
        ("http://valid-url.com/photo.jpg", True),
        ("https://example.com/gps-cs-s/restricted", False),
        ("https://example.com/gps-proxy/restricted", False),
        ("", False),
        (None, False),
        ("not-a-url", False),
    ])
    def test_is_valid_photo_url(self, provider, url, expected):
        """Test photo URL validation."""
        result = provider._is_valid_photo_url(url)
        assert result == expected


class TestOutscraperProviderFindPlaceId:
    """Tests for find_place_id method."""

    def test_find_place_id_success(self, mock_env_vars, outscraper_balance_sufficient, outscraper_place_details):
        """Test successful place ID lookup."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_search.return_value = outscraper_place_details
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                place_id = provider.find_place_id(TEST_PLACE_NAME)
        
        assert place_id == TEST_PLACE_ID

    def test_find_place_id_exact_match_preferred(self, mock_env_vars, outscraper_balance_sufficient):
        """Test that exact name match is preferred."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_search.return_value = [[
            {"name": "Other Coffee Shop", "place_id": "other-id"},
            {"name": TEST_PLACE_NAME, "place_id": TEST_PLACE_ID}
        ]]
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                place_id = provider.find_place_id(TEST_PLACE_NAME)
        
        assert place_id == TEST_PLACE_ID

    def test_find_place_id_not_found(self, mock_env_vars, outscraper_balance_sufficient):
        """Test place ID not found returns empty string."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_search.return_value = [[]]
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                place_id = provider.find_place_id("Nonexistent Place")
        
        assert place_id == ""


class TestOutscraperProviderIsPlaceOperational:
    """Tests for is_place_operational method."""

    def test_is_place_operational_true(self, mock_env_vars, outscraper_balance_sufficient, outscraper_place_details):
        """Test operational place returns True."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_search.return_value = outscraper_place_details
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                is_operational = provider.is_place_operational(TEST_PLACE_ID)
        
        assert is_operational is True

    def test_is_place_operational_closed_permanently(self, mock_env_vars, outscraper_balance_sufficient):
        """Test permanently closed place returns False."""
        from services.place_data_service import OutscraperProvider
        
        mock_client = mock.MagicMock()
        mock_client.google_maps_search.return_value = [[{"place_id": TEST_PLACE_ID, "business_status": "CLOSED_PERMANENTLY"}]]
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient", return_value=mock_client):
                provider = OutscraperProvider()
                is_operational = provider.is_place_operational(TEST_PLACE_ID)
        
        assert is_operational is False


class TestPlaceDataProviderFactory:
    """Tests for PlaceDataProviderFactory."""

    def test_get_provider_google(self, mock_env_vars):
        """Test factory creates GoogleMapsProvider."""
        from services.place_data_service import PlaceDataProviderFactory, GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests"):
            provider = PlaceDataProviderFactory.get_provider("google")
        
        assert isinstance(provider, GoogleMapsProvider)

    def test_get_provider_outscraper(self, mock_env_vars, outscraper_balance_sufficient):
        """Test factory creates OutscraperProvider."""
        from services.place_data_service import PlaceDataProviderFactory, OutscraperProvider
        
        with mock.patch("services.place_data_service.requests.get", return_value=create_mock_response(outscraper_balance_sufficient)):
            with mock.patch("services.place_data_service.ApiClient"):
                provider = PlaceDataProviderFactory.get_provider("outscraper")
        
        assert isinstance(provider, OutscraperProvider)

    def test_get_provider_case_insensitive(self, mock_env_vars):
        """Test factory is case-insensitive."""
        from services.place_data_service import PlaceDataProviderFactory, GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests"):
            provider = PlaceDataProviderFactory.get_provider("GOOGLE")
        
        assert isinstance(provider, GoogleMapsProvider)

    def test_get_provider_strips_whitespace(self, mock_env_vars):
        """Test factory strips whitespace."""
        from services.place_data_service import PlaceDataProviderFactory, GoogleMapsProvider
        
        with mock.patch("services.place_data_service.requests"):
            provider = PlaceDataProviderFactory.get_provider("  google  ")
        
        assert isinstance(provider, GoogleMapsProvider)

    def test_get_provider_invalid_type_raises_error(self, mock_env_vars):
        """Test factory raises error for invalid type."""
        from services.place_data_service import PlaceDataProviderFactory
        
        with pytest.raises(ValueError) as exc_info:
            PlaceDataProviderFactory.get_provider("invalid")
        
        assert "Unsupported provider type" in str(exc_info.value)

    def test_get_provider_none_raises_error(self, mock_env_vars):
        """Test factory raises error for None type."""
        from services.place_data_service import PlaceDataProviderFactory
        
        with pytest.raises(ValueError) as exc_info:
            PlaceDataProviderFactory.get_provider(None)
        
        assert "cannot be None" in str(exc_info.value)

    def test_get_provider_non_string_raises_error(self, mock_env_vars):
        """Test factory raises error for non-string type."""
        from services.place_data_service import PlaceDataProviderFactory
        
        with pytest.raises(ValueError) as exc_info:
            PlaceDataProviderFactory.get_provider(123)
        
        assert "must be a string" in str(exc_info.value)
