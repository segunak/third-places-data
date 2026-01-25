"""
Unit tests for the AirtableService class from airtable_service.py.

All tests use mocked Airtable API calls - no live API calls are made.
"""

import pytest
from unittest import mock
from collections import Counter

from conftest import TEST_PLACE_ID, TEST_PLACE_NAME, load_fixture, create_mock_response
from constants import SearchField


class TestAirtableServiceInit:
    """Tests for AirtableService initialization."""

    def test_init_requires_provider_type(self, mock_env_vars):
        """Test that initialization requires provider_type."""
        from services.airtable_service import AirtableService
        
        with pytest.raises(ValueError) as exc_info:
            with mock.patch("services.airtable_service.pyairtable.Table"):
                with mock.patch("services.airtable_service.Api"):
                    with mock.patch("services.airtable_service.PlaceDataProviderFactory"):
                        AirtableService(provider_type=None)
        
        assert "provider_type" in str(exc_info.value)

    def test_init_with_google_provider(self, mock_env_vars):
        """Test initialization with google provider."""
        from services.airtable_service import AirtableService
        
        with mock.patch("services.airtable_service.pyairtable.Table"):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="google")
        
        assert service.provider_type == "google"
        assert service.AIRTABLE_BASE_ID == "appTestBaseId123"

    def test_init_with_outscraper_provider(self, mock_env_vars, outscraper_balance_sufficient):
        """Test initialization with outscraper provider."""
        from services.airtable_service import AirtableService
        
        with mock.patch("services.airtable_service.pyairtable.Table"):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="outscraper")
        
        assert service.provider_type == "outscraper"

    def test_init_sets_default_view(self, mock_env_vars):
        """Test that default view is 'Production'."""
        from services.airtable_service import AirtableService
        
        with mock.patch("services.airtable_service.pyairtable.Table"):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="google")
        
        assert service.view == "Production"

    def test_init_with_custom_view(self, mock_env_vars):
        """Test initialization with custom view."""
        from services.airtable_service import AirtableService
        
        with mock.patch("services.airtable_service.pyairtable.Table"):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="google", view="Insufficient")
        
        assert service.view == "Insufficient"

    def test_init_sequential_mode(self, mock_env_vars):
        """Test initialization with sequential mode."""
        from services.airtable_service import AirtableService
        
        with mock.patch("services.airtable_service.pyairtable.Table"):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="google", sequential_mode=True)
        
        assert service.sequential_mode is True


class TestAirtableServiceAllThirdPlaces:
    """Tests for all_third_places property."""

    def test_all_third_places_lazy_loads(self, mock_env_vars, airtable_records):
        """Test that all_third_places is lazy loaded."""
        from services.airtable_service import AirtableService
        
        mock_table = mock.MagicMock()
        mock_table.all.return_value = airtable_records["records"]
        
        with mock.patch("services.airtable_service.pyairtable.Table", return_value=mock_table):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="google")
        
        # Should not have called all() yet
        mock_table.all.assert_not_called()
        
        # Access the property
        places = service.all_third_places
        
        # Now it should have been called
        mock_table.all.assert_called_once()
        assert len(places) == 5

    def test_all_third_places_caches_result(self, mock_env_vars, airtable_records):
        """Test that all_third_places caches the result."""
        from services.airtable_service import AirtableService
        
        mock_table = mock.MagicMock()
        mock_table.all.return_value = airtable_records["records"]
        
        with mock.patch("services.airtable_service.pyairtable.Table", return_value=mock_table):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="google")
        
        # Access twice
        _ = service.all_third_places
        _ = service.all_third_places
        
        # Should only have been called once
        mock_table.all.assert_called_once()


class TestAirtableServiceClearCachedPlaces:
    """Tests for clear_cached_places method."""

    def test_clear_cached_places(self, mock_env_vars, airtable_records):
        """Test that clear_cached_places clears the cache."""
        from services.airtable_service import AirtableService
        
        mock_table = mock.MagicMock()
        mock_table.all.return_value = airtable_records["records"]
        
        with mock.patch("services.airtable_service.pyairtable.Table", return_value=mock_table):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="google")
        
        # Load cache
        _ = service.all_third_places
        assert mock_table.all.call_count == 1
        
        # Clear cache
        service.clear_cached_places()
        
        # Access again - should reload
        _ = service.all_third_places
        assert mock_table.all.call_count == 2


class TestAirtableServiceUpdatePlaceRecord:
    """Tests for update_place_record method."""

    @pytest.fixture
    def service_with_mock_table(self, mock_env_vars):
        """Create a service with a mocked table."""
        from services.airtable_service import AirtableService
        
        mock_table = mock.MagicMock()
        
        with mock.patch("services.airtable_service.pyairtable.Table", return_value=mock_table):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="google")
        
        return service, mock_table

    def test_update_place_record_updates_empty_field(self, service_with_mock_table):
        """Test that empty fields are updated."""
        service, mock_table = service_with_mock_table
        
        mock_table.get.return_value = {
            "id": "recABC123",
            "fields": {"Place": TEST_PLACE_NAME, "Website": None}
        }
        
        result = service.update_place_record(
            record_id="recABC123",
            field_to_update="Website",
            update_value="https://example.com",
            overwrite=False
        )
        
        assert result["updated"] is True
        mock_table.update.assert_called_once()

    def test_update_place_record_updates_unsure_field(self, service_with_mock_table):
        """Test that 'Unsure' fields are updated."""
        service, mock_table = service_with_mock_table
        
        mock_table.get.return_value = {
            "id": "recABC123",
            "fields": {"Place": TEST_PLACE_NAME, "Parking": "Unsure"}
        }
        
        result = service.update_place_record(
            record_id="recABC123",
            field_to_update="Parking",
            update_value="Free",
            overwrite=False
        )
        
        assert result["updated"] is True
        mock_table.update.assert_called_once()

    def test_update_place_record_skips_without_overwrite(self, service_with_mock_table):
        """Test that existing fields are not updated without overwrite."""
        service, mock_table = service_with_mock_table
        
        mock_table.get.return_value = {
            "id": "recABC123",
            "fields": {"Place": TEST_PLACE_NAME, "Website": "https://existing.com"}
        }
        
        result = service.update_place_record(
            record_id="recABC123",
            field_to_update="Website",
            update_value="https://new.com",
            overwrite=False
        )
        
        assert result["updated"] is False
        mock_table.update.assert_not_called()

    def test_update_place_record_updates_with_overwrite(self, service_with_mock_table):
        """Test that existing fields are updated with overwrite."""
        service, mock_table = service_with_mock_table
        
        mock_table.get.return_value = {
            "id": "recABC123",
            "fields": {"Place": TEST_PLACE_NAME, "Website": "https://existing.com"}
        }
        
        result = service.update_place_record(
            record_id="recABC123",
            field_to_update="Website",
            update_value="https://new.com",
            overwrite=True
        )
        
        assert result["updated"] is True
        mock_table.update.assert_called_once()

    def test_update_place_record_skips_empty_update_value(self, service_with_mock_table):
        """Test that empty update values are skipped."""
        service, mock_table = service_with_mock_table
        
        mock_table.get.return_value = {
            "id": "recABC123",
            "fields": {"Place": TEST_PLACE_NAME, "Website": None}
        }
        
        result = service.update_place_record(
            record_id="recABC123",
            field_to_update="Website",
            update_value="",
            overwrite=True
        )
        
        assert result["updated"] is False
        mock_table.update.assert_not_called()

    def test_update_place_record_handles_error(self, service_with_mock_table):
        """Test that errors are handled gracefully."""
        service, mock_table = service_with_mock_table
        
        mock_table.get.side_effect = Exception("API Error")
        
        result = service.update_place_record(
            record_id="recABC123",
            field_to_update="Website",
            update_value="https://example.com",
            overwrite=True
        )
        
        assert result["updated"] is False

    def test_update_place_record_includes_raw_provider_value(self, service_with_mock_table):
        """Test that raw_provider_value is included in the result."""
        service, mock_table = service_with_mock_table
        
        mock_table.get.return_value = {
            "id": "recABC123",
            "fields": {"Place": TEST_PLACE_NAME, "Website": None}
        }
        
        raw_value = "https://example.com/full/path?query=param"
        result = service.update_place_record(
            record_id="recABC123",
            field_to_update="Website",
            update_value="https://example.com",
            overwrite=False,
            raw_provider_value=raw_value
        )
        
        assert result["raw_provider_value"] == raw_value
        assert result["updated"] is True

    def test_update_place_record_default_raw_provider_value(self, service_with_mock_table):
        """Test that raw_provider_value defaults to 'No Value From Provider'."""
        service, mock_table = service_with_mock_table
        
        mock_table.get.return_value = {
            "id": "recABC123",
            "fields": {"Place": TEST_PLACE_NAME, "Website": None}
        }
        
        result = service.update_place_record(
            record_id="recABC123",
            field_to_update="Website",
            update_value="https://example.com",
            overwrite=False
        )
        
        assert result["raw_provider_value"] == "No Value From Provider"

    def test_update_place_record_raw_provider_value_preserved_on_error(self, service_with_mock_table):
        """Test that raw_provider_value is preserved even on error."""
        service, mock_table = service_with_mock_table
        
        mock_table.get.side_effect = Exception("API Error")
        
        raw_value = {"parkingOptions": {"freeParkingLot": True}}
        result = service.update_place_record(
            record_id="recABC123",
            field_to_update="Parking",
            update_value="Free",
            overwrite=True,
            raw_provider_value=raw_value
        )
        
        assert result["updated"] is False
        assert result["raw_provider_value"] == raw_value


class TestAirtableServiceExtractRawProviderValues:
    """Tests for _extract_raw_provider_values method."""

    @pytest.fixture
    def service(self, mock_env_vars):
        """Create a service instance."""
        from services.airtable_service import AirtableService
        
        with mock.patch("services.airtable_service.pyairtable.Table"):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    return AirtableService(provider_type="google")

    def test_extract_google_provider_values(self, service):
        """Test extraction of raw values from Google Maps provider response."""
        raw_data = {
            "websiteUri": "https://example.com/full/path",
            "formattedAddress": "123 Main St, Charlotte, NC 28202",
            "editorialSummary": {"text": "A great coffee shop"},
            "priceLevel": "PRICE_LEVEL_MODERATE",
            "parkingOptions": {"freeParkingLot": True, "paidStreetParking": False},
            "googleMapsUri": "https://maps.google.com/?cid=123456",
            "location": {"latitude": 35.2271, "longitude": -80.8431}
        }
        
        result = service._extract_raw_provider_values(raw_data, "GoogleMapsProvider")
        
        assert result["Website"] == "https://example.com/full/path"
        assert result["Address"] == "123 Main St, Charlotte, NC 28202"
        assert result["Description"] == {"text": "A great coffee shop"}
        assert result["Purchase Required"] == "PRICE_LEVEL_MODERATE"
        assert result["Parking"] == {"freeParkingLot": True, "paidStreetParking": False}
        assert result["Google Maps Profile URL"] == "https://maps.google.com/?cid=123456"
        assert result["Latitude"] == 35.2271
        assert result["Longitude"] == -80.8431
        # Derived fields should have no value
        assert result["Google Maps Place Id"] == "No Value From Provider"
        assert result["Photos"] == "No Value From Provider"

    def test_extract_outscraper_provider_values(self, service):
        """Test extraction of raw values from Outscraper provider response."""
        raw_data = {
            "site": "https://example.com/full/path",
            "full_address": "123 Main St, Charlotte, NC 28202, United States",
            "description": "A great coffee shop",
            "range": "$$",
            "about": {"Parking": {"Free parking lot": True, "Paid street parking": False}},
            "latitude": 35.2271,
            "longitude": -80.8431
        }
        
        result = service._extract_raw_provider_values(raw_data, "OutscraperProvider")
        
        assert result["Website"] == "https://example.com/full/path"
        assert result["Address"] == "123 Main St, Charlotte, NC 28202, United States"
        assert result["Description"] == "A great coffee shop"
        assert result["Purchase Required"] == "$$"
        assert result["Parking"] == {"Free parking lot": True, "Paid street parking": False}
        assert result["Latitude"] == 35.2271
        assert result["Longitude"] == -80.8431
        # Derived fields should have no value
        assert result["Google Maps Place Id"] == "No Value From Provider"
        assert result["Google Maps Profile URL"] == "No Value From Provider"
        assert result["Photos"] == "No Value From Provider"

    def test_extract_empty_raw_data(self, service):
        """Test extraction with empty raw data returns all 'No Value From Provider'."""
        result = service._extract_raw_provider_values({}, "GoogleMapsProvider")
        
        for field in ["Website", "Address", "Description", "Purchase Required", "Parking", "Photos", "Latitude", "Longitude"]:
            assert result[field] == "No Value From Provider"

    def test_extract_none_raw_data(self, service):
        """Test extraction with None raw data returns all 'No Value From Provider'."""
        result = service._extract_raw_provider_values(None, "GoogleMapsProvider")
        
        for field in ["Website", "Address", "Description", "Purchase Required", "Parking", "Photos", "Latitude", "Longitude"]:
            assert result[field] == "No Value From Provider"

    def test_extract_unknown_provider(self, service):
        """Test extraction with unknown provider returns all 'No Value From Provider'."""
        raw_data = {"site": "https://example.com"}
        result = service._extract_raw_provider_values(raw_data, "UnknownProvider")
        
        for field in ["Website", "Address", "Description", "Purchase Required", "Parking", "Photos", "Latitude", "Longitude"]:
            assert result[field] == "No Value From Provider"

    def test_extract_preserves_structured_objects(self, service):
        """Test that structured objects are preserved, not stringified."""
        raw_data = {
            "parkingOptions": {"freeParkingLot": True, "paidGarage": False, "valetParking": True},
            "editorialSummary": {"text": "Great place", "languageCode": "en"},
            "location": {"latitude": 35.2271, "longitude": -80.8431}
        }
        
        result = service._extract_raw_provider_values(raw_data, "GoogleMapsProvider")
        
        # Parking should be a dict, not a string
        assert isinstance(result["Parking"], dict)
        assert result["Parking"]["freeParkingLot"] is True
        
        # Description should be a dict (editorialSummary object)
        assert isinstance(result["Description"], dict)
        assert result["Description"]["text"] == "Great place"

    def test_extract_outscraper_missing_about_section(self, service):
        """Test extraction when Outscraper response has no 'about' section."""
        raw_data = {
            "site": "https://example.com",
            "full_address": "123 Main St"
        }
        
        result = service._extract_raw_provider_values(raw_data, "OutscraperProvider")
        
        assert result["Parking"] == "No Value From Provider"


class TestAirtableServiceGetBaseUrl:
    """Tests for get_base_url method."""

    @pytest.fixture
    def service(self, mock_env_vars):
        """Create a service instance."""
        from services.airtable_service import AirtableService
        
        with mock.patch("services.airtable_service.pyairtable.Table"):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    return AirtableService(provider_type="google")

    def test_get_base_url_with_query_params(self, service):
        """Test URL with query parameters."""
        result = service.get_base_url("https://example.com/path?query=value#fragment")
        assert result == "https://example.com/path"

    def test_get_base_url_without_path(self, service):
        """Test URL without path."""
        result = service.get_base_url("https://example.com")
        assert result == "https://example.com"

    def test_get_base_url_with_path(self, service):
        """Test URL with path."""
        result = service.get_base_url("https://example.com/path/to/resource")
        assert result == "https://example.com/path/to/resource"

    def test_get_base_url_invalid_url(self, service):
        """Test invalid URL returns empty string."""
        result = service.get_base_url("not-a-url")
        assert result == ""

    def test_get_base_url_empty_string(self, service):
        """Test empty string returns empty string."""
        result = service.get_base_url("")
        assert result == ""


class TestAirtableServiceGetRecord:
    """Tests for get_record method."""

    @pytest.fixture
    def service_with_mock_table(self, mock_env_vars, airtable_records):
        """Create a service with a mocked table."""
        from services.airtable_service import AirtableService
        
        mock_table = mock.MagicMock()
        
        with mock.patch("services.airtable_service.pyairtable.Table", return_value=mock_table):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="google")
        
        return service, mock_table, airtable_records

    def test_get_record_found(self, service_with_mock_table):
        """Test successful record lookup."""
        service, mock_table, airtable_records = service_with_mock_table
        
        mock_table.all.return_value = [airtable_records["records"][0]]
        
        record = service.get_record(SearchField.GOOGLE_MAPS_PLACE_ID, TEST_PLACE_ID)
        
        assert record is not None
        assert record["id"] == "recABC123"

    def test_get_record_not_found(self, service_with_mock_table):
        """Test record not found returns None."""
        service, mock_table, _ = service_with_mock_table
        
        mock_table.all.return_value = []
        
        record = service.get_record(SearchField.GOOGLE_MAPS_PLACE_ID, "nonexistent-id")
        
        assert record is None

    def test_get_record_multiple_matches_returns_none(self, service_with_mock_table):
        """Test that multiple matches returns None."""
        service, mock_table, airtable_records = service_with_mock_table
        
        # Return two records with the same place name
        mock_table.all.return_value = [
            airtable_records["records"][3],  # Duplicate Test Place
            airtable_records["records"][4]   # Duplicate Test Place
        ]
        
        record = service.get_record(SearchField.PLACE_NAME, "Duplicate Test Place")
        
        assert record is None

    def test_get_record_handles_error(self, service_with_mock_table):
        """Test that errors return None."""
        service, mock_table, _ = service_with_mock_table
        
        mock_table.all.side_effect = Exception("API Error")
        
        record = service.get_record(SearchField.GOOGLE_MAPS_PLACE_ID, TEST_PLACE_ID)
        
        assert record is None


class TestAirtableServiceGetPlacePhotos:
    """Tests for get_place_photos method."""

    def test_get_place_photos_success(self, mock_env_vars):
        """Test successful photo retrieval."""
        from services.airtable_service import AirtableService
        
        mock_provider = mock.MagicMock()
        mock_provider.get_place_photos.return_value = {
            "place_id": TEST_PLACE_ID,
            "photo_urls": ["http://photo1.jpg", "http://photo2.jpg"]
        }
        
        with mock.patch("services.airtable_service.pyairtable.Table"):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider", return_value=mock_provider):
                    service = AirtableService(provider_type="google")
                    photos = service.get_place_photos(TEST_PLACE_ID)
        
        assert len(photos) == 2
        assert "http://photo1.jpg" in photos

    def test_get_place_photos_empty(self, mock_env_vars):
        """Test empty photo response."""
        from services.airtable_service import AirtableService
        
        mock_provider = mock.MagicMock()
        mock_provider.get_place_photos.return_value = {"place_id": TEST_PLACE_ID, "photo_urls": []}
        
        with mock.patch("services.airtable_service.pyairtable.Table"):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider", return_value=mock_provider):
                    service = AirtableService(provider_type="google")
                    photos = service.get_place_photos(TEST_PLACE_ID)
        
        assert photos == []

    def test_get_place_photos_error(self, mock_env_vars):
        """Test photo retrieval error."""
        from services.airtable_service import AirtableService
        
        mock_provider = mock.MagicMock()
        mock_provider.get_place_photos.side_effect = Exception("API Error")
        
        with mock.patch("services.airtable_service.pyairtable.Table"):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider", return_value=mock_provider):
                    service = AirtableService(provider_type="google")
                    photos = service.get_place_photos(TEST_PLACE_ID)
        
        assert photos == []


class TestAirtableServiceFindDuplicateRecords:
    """Tests for find_duplicate_records method."""

    @pytest.fixture
    def service(self, mock_env_vars):
        """Create a service instance."""
        from services.airtable_service import AirtableService
        
        with mock.patch("services.airtable_service.pyairtable.Table"):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    return AirtableService(provider_type="google")

    def test_find_duplicate_records(self, service, airtable_records):
        """Test finding duplicate records."""
        duplicates = service.find_duplicate_records("Place", airtable_records["records"])
        
        assert "Duplicate Test Place" in duplicates
        assert duplicates["Duplicate Test Place"] == 2

    def test_find_duplicate_records_no_duplicates(self, service):
        """Test with no duplicates."""
        records = [
            {"id": "rec1", "fields": {"Place": "Unique Place 1"}},
            {"id": "rec2", "fields": {"Place": "Unique Place 2"}},
        ]
        
        duplicates = service.find_duplicate_records("Place", records)
        
        assert duplicates == {}

    def test_find_duplicate_records_missing_field(self, service):
        """Test with records missing the field."""
        records = [
            {"id": "rec1", "fields": {"Place": "Place 1"}},
            {"id": "rec2", "fields": {}},  # Missing field
            {"id": "rec3", "fields": {"Place": "Place 1"}},
        ]
        
        duplicates = service.find_duplicate_records("Place", records)
        
        assert "Place 1" in duplicates
        assert duplicates["Place 1"] == 2


class TestAirtableServiceGetPlacesMissingField:
    """Tests for get_places_missing_field method."""

    @pytest.fixture
    def service(self, mock_env_vars):
        """Create a service instance."""
        from services.airtable_service import AirtableService
        
        with mock.patch("services.airtable_service.pyairtable.Table"):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    return AirtableService(provider_type="google")

    def test_get_places_missing_field(self, service, airtable_records):
        """Test finding places missing a field."""
        # recDEF456 (Undercurrent Coffee) and recJKL012/recMNO345 are missing Photos
        missing = service.get_places_missing_field("Photos", airtable_records["records"])
        
        assert "Undercurrent Coffee" in missing
        assert "Duplicate Test Place" in missing

    def test_get_places_missing_field_none_missing(self, service):
        """Test when all places have the field."""
        records = [
            {"id": "rec1", "fields": {"Place": "Place 1", "Website": "http://1.com"}},
            {"id": "rec2", "fields": {"Place": "Place 2", "Website": "http://2.com"}},
        ]
        
        missing = service.get_places_missing_field("Website", records)
        
        assert missing == []


class TestAirtableServiceHasDataFile:
    """Tests for has_data_file method."""

    @pytest.fixture
    def service_with_mock_table(self, mock_env_vars):
        """Create a service with a mocked table."""
        from services.airtable_service import AirtableService
        
        mock_table = mock.MagicMock()
        
        with mock.patch("services.airtable_service.pyairtable.Table", return_value=mock_table):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="google")
        
        return service, mock_table

    def test_has_data_file_yes(self, service_with_mock_table):
        """Test place with data file."""
        service, mock_table = service_with_mock_table
        
        mock_table.all.return_value = [{
            "id": "recABC123",
            "fields": {"Place": TEST_PLACE_NAME, "Has Data File": "Yes"}
        }]
        
        result = service.has_data_file(TEST_PLACE_ID)
        
        assert result is True

    def test_has_data_file_no(self, service_with_mock_table):
        """Test place without data file."""
        service, mock_table = service_with_mock_table
        
        mock_table.all.return_value = [{
            "id": "recABC123",
            "fields": {"Place": TEST_PLACE_NAME, "Has Data File": "No"}
        }]
        
        result = service.has_data_file(TEST_PLACE_ID)
        
        assert result is False

    def test_has_data_file_record_not_found(self, service_with_mock_table):
        """Test place not found returns False."""
        service, mock_table = service_with_mock_table
        
        mock_table.all.return_value = []
        
        result = service.has_data_file("nonexistent-id")
        
        assert result is False


class TestAirtableServiceGetPlaceTypes:
    """Tests for get_place_types method."""

    def test_get_place_types(self, mock_env_vars, airtable_records):
        """Test getting all place types."""
        from services.airtable_service import AirtableService
        
        mock_table = mock.MagicMock()
        mock_table.all.return_value = airtable_records["records"]
        
        with mock.patch("services.airtable_service.pyairtable.Table", return_value=mock_table):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="google")
                    types = service.get_place_types()
        
        assert "Coffee Shop" in types
        assert "Cafe" in types
        assert "Bakery" in types
        # Should be sorted
        assert types == sorted(types)

    def test_get_place_types_with_string_type(self, mock_env_vars):
        """Test getting place types when Type field is a string (line 422)."""
        from services.airtable_service import AirtableService
        
        # Create records where Type is a string, not a list
        records_with_string_type = [
            {"id": "rec1", "fields": {"Place": "Place 1", "Type": "Coffee Shop"}},
            {"id": "rec2", "fields": {"Place": "Place 2", "Type": "Bookstore"}},
            {"id": "rec3", "fields": {"Place": "Place 3", "Type": ["Bar", "Restaurant"]}},
        ]
        
        mock_table = mock.MagicMock()
        mock_table.all.return_value = records_with_string_type
        
        with mock.patch("services.airtable_service.pyairtable.Table", return_value=mock_table):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="google")
                    types = service.get_place_types()
        
        # Should include string types
        assert "Coffee Shop" in types
        assert "Bookstore" in types
        # And list types
        assert "Bar" in types
        assert "Restaurant" in types


class TestAirtableServiceRefreshOperationalStatuses:
    """Tests for refresh_operational_statuses method."""

    def test_refresh_operational_statuses(self, mock_env_vars, airtable_records):
        """Test refreshing operational statuses for all places."""
        from services.airtable_service import AirtableService
        
        mock_table = mock.MagicMock()
        mock_table.all.return_value = airtable_records["records"]
        mock_table.get.return_value = airtable_records["records"][0]
        
        mock_provider = mock.MagicMock()
        mock_provider.is_place_operational.return_value = True
        
        with mock.patch("services.airtable_service.pyairtable.Table", return_value=mock_table):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider", return_value=mock_provider):
                    service = AirtableService(provider_type="google")
                    results = service.refresh_operational_statuses(mock_provider)
        
        assert len(results) == 5


class TestAirtableServiceRefreshSinglePlaceOperationalStatus:
    """Tests for refresh_single_place_operational_status method."""

    @pytest.fixture
    def service_with_mock_table(self, mock_env_vars):
        """Create a service with a mocked table."""
        from services.airtable_service import AirtableService
        
        mock_table = mock.MagicMock()
        
        with mock.patch("services.airtable_service.pyairtable.Table", return_value=mock_table):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="google")
        
        return service, mock_table

    def test_refresh_skips_opening_soon(self, service_with_mock_table):
        """Test that 'Opening Soon' places are skipped."""
        service, mock_table = service_with_mock_table
        
        mock_provider = mock.MagicMock()
        third_place = {
            "id": "recMNO345",
            "fields": {
                "Place": "Duplicate Test Place",
                "Google Maps Place Id": TEST_PLACE_ID,
                "Operational": "Opening Soon"
            }
        }
        
        result = service.refresh_single_place_operational_status(third_place, mock_provider)
        
        assert result["update_status"] == "success"
        assert "Opening Soon" in result["message"]
        mock_provider.is_place_operational.assert_not_called()

    def test_refresh_no_place_id(self, service_with_mock_table):
        """Test that places without place ID are failed."""
        service, mock_table = service_with_mock_table
        
        mock_provider = mock.MagicMock()
        third_place = {
            "id": "recXYZ",
            "fields": {"Place": "No Place ID Place"}
        }
        
        result = service.refresh_single_place_operational_status(third_place, mock_provider)
        
        assert result["update_status"] == "failed"
        assert "No Google Maps Place Id" in result["message"]

    def test_refresh_updates_when_changed(self, service_with_mock_table):
        """Test that status is updated when it changes."""
        service, mock_table = service_with_mock_table
        
        mock_provider = mock.MagicMock()
        mock_provider.is_place_operational.return_value = False
        
        mock_table.get.return_value = {
            "id": "recABC123",
            "fields": {"Place": TEST_PLACE_NAME, "Operational": "Yes"}
        }
        
        third_place = {
            "id": "recABC123",
            "fields": {
                "Place": TEST_PLACE_NAME,
                "Google Maps Place Id": TEST_PLACE_ID,
                "Operational": "Yes"
            }
        }
        
        result = service.refresh_single_place_operational_status(third_place, mock_provider)
        
        assert result["old_value"] == "Yes"
        assert result["new_value"] == "No"

    def test_refresh_skips_when_unchanged(self, service_with_mock_table):
        """Test that status is skipped when unchanged."""
        service, mock_table = service_with_mock_table
        
        mock_provider = mock.MagicMock()
        mock_provider.is_place_operational.return_value = True
        
        third_place = {
            "id": "recABC123",
            "fields": {
                "Place": TEST_PLACE_NAME,
                "Google Maps Place Id": TEST_PLACE_ID,
                "Operational": "Yes"
            }
        }
        
        result = service.refresh_single_place_operational_status(third_place, mock_provider)
        
        assert result["update_status"] == "skipped"

    def test_refresh_update_fails(self, service_with_mock_table):
        """Test handling when update fails (lines 501-502)."""
        service, mock_table = service_with_mock_table
        
        mock_provider = mock.MagicMock()
        mock_provider.is_place_operational.return_value = False
        
        # Mock update to fail
        mock_table.get.return_value = {
            "id": "recABC123",
            "fields": {"Place": TEST_PLACE_NAME, "Operational": "Yes"}
        }
        mock_table.update.side_effect = Exception("Update failed")
        
        third_place = {
            "id": "recABC123",
            "fields": {
                "Place": TEST_PLACE_NAME,
                "Google Maps Place Id": TEST_PLACE_ID,
                "Operational": "Yes"
            }
        }
        
        result = service.refresh_single_place_operational_status(third_place, mock_provider)
        
        # Should return failed status from exception
        assert result["update_status"] == "failed"
        assert result["message"] != ""

    def test_refresh_exception_handling(self, service_with_mock_table):
        """Test exception handling during refresh (lines 504-507)."""
        service, mock_table = service_with_mock_table
        
        mock_provider = mock.MagicMock()
        mock_provider.is_place_operational.side_effect = Exception("API Error")
        
        third_place = {
            "id": "recABC123",
            "fields": {
                "Place": TEST_PLACE_NAME,
                "Google Maps Place Id": TEST_PLACE_ID,
                "Operational": "Yes"
            }
        }
        
        result = service.refresh_single_place_operational_status(third_place, mock_provider)
        
        assert result["update_status"] == "failed"
        assert "API Error" in result["message"]


class TestAirtableServiceEnrichSinglePlace:
    """Tests for enrich_single_place method."""

    @pytest.fixture
    def service_with_mocks(self, mock_env_vars):
        """Create a service with mocked dependencies."""
        from services.airtable_service import AirtableService
        
        mock_table = mock.MagicMock()
        mock_provider = mock.MagicMock()
        
        with mock.patch("services.airtable_service.pyairtable.Table", return_value=mock_table):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider", return_value=mock_provider):
                    service = AirtableService(provider_type="google")
        
        return service, mock_table, mock_provider

    def test_enrich_single_place_missing_name(self, service_with_mocks):
        """Test that places without a name are skipped."""
        service, mock_table, _ = service_with_mocks
        
        third_place = {"id": "recXYZ", "fields": {}}
        
        result = service.enrich_single_place(third_place, "google", "Charlotte", False)
        
        assert result["status"] == "skipped"
        assert "Missing place name" in result["message"]

    def test_enrich_single_place_success(self, service_with_mocks):
        """Test successful place enrichment."""
        service, mock_table, _ = service_with_mocks
        
        mock_table.get.return_value = {
            "id": "recABC123",
            "fields": {"Place": TEST_PLACE_NAME}
        }
        
        third_place = {
            "id": "recABC123",
            "fields": {
                "Place": TEST_PLACE_NAME,
                "Google Maps Place Id": TEST_PLACE_ID
            }
        }
        
        mock_place_data = {
            "place_id": TEST_PLACE_ID,
            "details": {
                "website": "https://example.com",
                "address": "123 Main St",
                "latitude": 35.22,
                "longitude": -80.81,
                "parking": ["Free"],
                "purchase_required": "Yes",
                "description": "A nice place",
                "google_maps_url": "https://maps.google.com/?cid=123"
            },
            "photos": {"photo_urls": ["http://photo.jpg"]}
        }
        
        with mock.patch("services.airtable_service.helpers.get_and_cache_place_data") as mock_get_data:
            mock_get_data.return_value = ("success", mock_place_data, "")
            result = service.enrich_single_place(third_place, "google", "Charlotte", False)
        
        assert result["status"] == "success"
        assert result["place_name"] == TEST_PLACE_NAME

    def test_enrich_single_place_failed_status(self, service_with_mocks):
        """Test handling of failed status from data fetch."""
        service, mock_table, _ = service_with_mocks
        
        third_place = {
            "id": "recABC123",
            "fields": {
                "Place": TEST_PLACE_NAME,
                "Google Maps Place Id": TEST_PLACE_ID
            }
        }
        
        with mock.patch("services.airtable_service.helpers.get_and_cache_place_data") as mock_get_data:
            mock_get_data.return_value = ("failed", None, "API error")
            result = service.enrich_single_place(third_place, "google", "Charlotte", False)
        
        assert result["status"] == "failed"
        assert "API error" in result["message"]

    def test_enrich_single_place_no_details(self, service_with_mocks):
        """Test handling when place data has no details."""
        service, mock_table, _ = service_with_mocks
        
        third_place = {
            "id": "recABC123",
            "fields": {
                "Place": TEST_PLACE_NAME,
                "Google Maps Place Id": TEST_PLACE_ID
            }
        }
        
        # Return data without 'details' key
        mock_place_data = {
            "place_id": TEST_PLACE_ID
        }
        
        with mock.patch("services.airtable_service.helpers.get_and_cache_place_data") as mock_get_data:
            mock_get_data.return_value = ("success", mock_place_data, "")
            result = service.enrich_single_place(third_place, "google", "Charlotte", False)
        
        assert result["status"] == "success"
        # No field updates since no details
        assert result["field_updates"] == {}

    def test_enrich_single_place_exception(self, service_with_mocks):
        """Test handling of exceptions during enrichment."""
        service, mock_table, _ = service_with_mocks
        
        third_place = {
            "id": "recABC123",
            "fields": {
                "Place": TEST_PLACE_NAME,
                "Google Maps Place Id": TEST_PLACE_ID
            }
        }
        
        with mock.patch("services.airtable_service.helpers.get_and_cache_place_data") as mock_get_data:
            mock_get_data.side_effect = Exception("Unexpected error")
            result = service.enrich_single_place(third_place, "google", "Charlotte", False)
        
        assert result["status"] == "failed"
        assert "Error:" in result["message"]

    def test_enrich_single_place_updates_place_id_from_data(self, service_with_mocks):
        """Test that place_id is updated from fetched data."""
        service, mock_table, _ = service_with_mocks
        
        mock_table.get.return_value = {
            "id": "recABC123",
            "fields": {"Place": TEST_PLACE_NAME}
        }
        
        third_place = {
            "id": "recABC123",
            "fields": {
                "Place": TEST_PLACE_NAME
                # No place_id initially
            }
        }
        
        mock_place_data = {
            "place_id": "NEW_PLACE_ID_123",
            "details": {
                "google_maps_url": "https://maps.google.com/?cid=123"
            }
        }
        
        with mock.patch("services.airtable_service.helpers.get_and_cache_place_data") as mock_get_data:
            mock_get_data.return_value = ("success", mock_place_data, "")
            result = service.enrich_single_place(third_place, "google", "Charlotte", False)
        
        assert result["place_id"] == "NEW_PLACE_ID_123"

    def test_enrich_single_place_with_empty_parking_list(self, service_with_mocks):
        """Test handling of empty parking list (line 251)."""
        service, mock_table, _ = service_with_mocks
        
        mock_table.get.return_value = {
            "id": "recABC123",
            "fields": {"Place": TEST_PLACE_NAME, "Parking": ""}
        }
        
        third_place = {
            "id": "recABC123",
            "fields": {
                "Place": TEST_PLACE_NAME,
                "Google Maps Place Id": TEST_PLACE_ID
            }
        }
        
        mock_place_data = {
            "place_id": TEST_PLACE_ID,
            "details": {
                "parking": [],  # Empty list should default to 'Unsure'
                "google_maps_url": "https://maps.google.com/?cid=123"
            }
        }
        
        with mock.patch("services.airtable_service.helpers.get_and_cache_place_data") as mock_get_data:
            mock_get_data.return_value = ("success", mock_place_data, "")
            result = service.enrich_single_place(third_place, "google", "Charlotte", False)
        
        assert result["status"] == "success"

    def test_enrich_single_place_skipped_status(self, service_with_mocks):
        """Test handling of skipped status from data fetch."""
        service, mock_table, _ = service_with_mocks
        
        third_place = {
            "id": "recABC123",
            "fields": {
                "Place": TEST_PLACE_NAME,
                "Google Maps Place Id": TEST_PLACE_ID
            }
        }
        
        with mock.patch("services.airtable_service.helpers.get_and_cache_place_data") as mock_get_data:
            mock_get_data.return_value = ("skipped", None, "Already enriched")
            result = service.enrich_single_place(third_place, "google", "Charlotte", False)
        
        assert result["status"] == "skipped"


class TestAirtableServiceEnrichBaseData:
    """Tests for enrich_base_data method."""

    def test_enrich_base_data_requires_city(self, mock_env_vars):
        """Test that city parameter is required."""
        from services.airtable_service import AirtableService
        
        with mock.patch("services.airtable_service.pyairtable.Table"):
            with mock.patch("services.airtable_service.Api"):
                with mock.patch("services.airtable_service.PlaceDataProviderFactory.get_provider") as mock_factory:
                    mock_factory.return_value = mock.MagicMock()
                    service = AirtableService(provider_type="google")
        
        with pytest.raises(ValueError) as exc_info:
            service.enrich_base_data(city=None)
        
        assert "city must be provided" in str(exc_info.value)
