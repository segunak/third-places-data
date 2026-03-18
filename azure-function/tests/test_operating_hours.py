"""
Tests for the operating hours feature across providers and enrichment pipeline.
"""

import json
import pytest
from unittest.mock import patch, MagicMock
from services.place_data_service import OutscraperProvider, GoogleMapsProvider


class TestOutscraperNormalizeHours:
    """Tests for OutscraperProvider._normalize_outscraper_hours static method."""

    def test_normalizes_standard_hours(self):
        working_hours = {
            "Monday": ["3-8PM"],
            "Tuesday": ["4-10PM"],
            "Wednesday": ["4-10PM"],
            "Thursday": ["4-10PM"],
            "Friday": ["1-11PM"],
            "Saturday": ["12-11PM"],
            "Sunday": ["12-7PM"]
        }
        result = OutscraperProvider._normalize_outscraper_hours(working_hours)
        assert len(result) == 7
        assert result[0] == "Sunday: 12-7PM"
        assert result[1] == "Monday: 3-8PM"
        assert result[6] == "Saturday: 12-11PM"

    def test_handles_multi_range_days(self):
        working_hours = {
            "Monday": ["11AM-2PM", "5-10PM"],
            "Tuesday": ["11AM-2PM", "5-10PM"]
        }
        result = OutscraperProvider._normalize_outscraper_hours(working_hours)
        assert "Monday: 11AM-2PM, 5-10PM" in result
        assert "Tuesday: 11AM-2PM, 5-10PM" in result

    def test_handles_string_values(self):
        working_hours = {
            "Monday": "9AM-5PM",
            "Tuesday": "9AM-5PM"
        }
        result = OutscraperProvider._normalize_outscraper_hours(working_hours)
        assert "Monday: 9AM-5PM" in result

    def test_handles_empty_dict(self):
        assert OutscraperProvider._normalize_outscraper_hours({}) == []

    def test_handles_none(self):
        assert OutscraperProvider._normalize_outscraper_hours(None) == []

    def test_handles_non_dict(self):
        assert OutscraperProvider._normalize_outscraper_hours("not a dict") == []

    def test_day_ordering_sunday_through_saturday(self):
        working_hours = {
            "Friday": ["1-11PM"],
            "Monday": ["3-8PM"],
            "Sunday": ["12-7PM"],
            "Wednesday": ["4-10PM"],
            "Saturday": ["12-11PM"],
            "Thursday": ["4-10PM"],
            "Tuesday": ["4-10PM"]
        }
        result = OutscraperProvider._normalize_outscraper_hours(working_hours)
        days = [line.split(":")[0] for line in result]
        assert days == ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

    def test_partial_week(self):
        working_hours = {
            "Monday": ["9AM-5PM"],
            "Wednesday": ["9AM-5PM"],
            "Friday": ["9AM-5PM"]
        }
        result = OutscraperProvider._normalize_outscraper_hours(working_hours)
        assert len(result) == 3
        days = [line.split(":")[0] for line in result]
        assert days == ["Monday", "Wednesday", "Friday"]


class TestGoogleMapsProviderGetOperatingHours:
    """Tests for GoogleMapsProvider.get_operating_hours."""

    @patch('services.place_data_service.requests.get')
    def test_returns_weekday_descriptions(self, mock_get, mock_env_vars):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "regularOpeningHours": {
                "weekdayDescriptions": [
                    "Monday: 7:00 AM – 5:00 PM",
                    "Tuesday: 7:00 AM – 5:00 PM",
                    "Wednesday: 7:00 AM – 5:00 PM",
                    "Thursday: 7:00 AM – 5:00 PM",
                    "Friday: 7:00 AM – 5:00 PM",
                    "Saturday: 8:00 AM – 3:00 PM",
                    "Sunday: Closed"
                ]
            }
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        provider = GoogleMapsProvider()
        result = provider.get_operating_hours("ChIJtest123")

        assert len(result) == 7
        assert result[0] == "Monday: 7:00 AM – 5:00 PM"
        assert result[6] == "Sunday: Closed"

    @patch('services.place_data_service.requests.get')
    def test_returns_empty_list_when_no_hours(self, mock_get, mock_env_vars):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        provider = GoogleMapsProvider()
        result = provider.get_operating_hours("ChIJtest123")

        assert result == []

    @patch('services.place_data_service.requests.get')
    def test_returns_empty_list_on_api_error(self, mock_get, mock_env_vars):
        mock_get.side_effect = Exception("API error")

        provider = GoogleMapsProvider()
        result = provider.get_operating_hours("ChIJtest123")

        assert result == []


class TestOutscraperProviderGetOperatingHours:
    """Tests for OutscraperProvider.get_operating_hours."""

    @patch.object(OutscraperProvider, '__init__', lambda self: None)
    def test_extracts_and_normalizes_hours(self):
        provider = OutscraperProvider.__new__(OutscraperProvider)
        provider._provider_type = 'outscraper'

        mock_details = {
            "place_name": "Test Place",
            "place_id": "ChIJtest",
            "raw_data": {
                "working_hours": {
                    "Monday": ["9AM-5PM"],
                    "Tuesday": ["9AM-5PM"],
                    "Wednesday": ["9AM-5PM"],
                    "Thursday": ["9AM-5PM"],
                    "Friday": ["9AM-5PM"],
                    "Saturday": ["10AM-3PM"],
                    "Sunday": ["Closed"]
                }
            }
        }

        with patch.object(provider, 'get_place_details', return_value=mock_details):
            result = provider.get_operating_hours("ChIJtest")

        assert len(result) == 7
        assert result[0] == "Sunday: Closed"
        assert result[1] == "Monday: 9AM-5PM"

    @patch.object(OutscraperProvider, '__init__', lambda self: None)
    def test_returns_empty_when_no_working_hours(self):
        provider = OutscraperProvider.__new__(OutscraperProvider)
        provider._provider_type = 'outscraper'

        mock_details = {
            "place_name": "Test Place",
            "place_id": "ChIJtest",
            "raw_data": {}
        }

        with patch.object(provider, 'get_place_details', return_value=mock_details):
            result = provider.get_operating_hours("ChIJtest")

        assert result == []


class TestOperatingHoursJsonRoundTrip:
    """Test that hours survive JSON serialization/deserialization."""

    def test_google_hours_round_trip(self):
        hours = [
            "Monday: 7:00 AM – 5:00 PM",
            "Tuesday: 7:00 AM – 5:00 PM",
            "Wednesday: 7:00 AM – 5:00 PM",
            "Thursday: 7:00 AM – 5:00 PM",
            "Friday: 7:00 AM – 5:00 PM",
            "Saturday: 8:00 AM – 3:00 PM",
            "Sunday: Closed"
        ]
        json_str = json.dumps(hours)
        parsed = json.loads(json_str)
        assert parsed == hours

    def test_outscraper_normalized_hours_round_trip(self):
        working_hours = {
            "Monday": ["11AM-2PM", "5-10PM"],
            "Sunday": ["12-7PM"]
        }
        normalized = OutscraperProvider._normalize_outscraper_hours(working_hours)
        json_str = json.dumps(normalized)
        parsed = json.loads(json_str)
        assert parsed == normalized
        assert "Sunday: 12-7PM" in parsed
        assert "Monday: 11AM-2PM, 5-10PM" in parsed


class TestExtractOperatingHours:
    """Tests for AirtableService._extract_operating_hours."""

    def _create_airtable_service_mock(self):
        """Create a mock AirtableService with _extract_operating_hours accessible."""
        from services.airtable_service import AirtableService
        with patch.object(AirtableService, '__init__', lambda self, *args, **kwargs: None):
            svc = AirtableService.__new__(AirtableService)
            return svc

    def test_extracts_google_hours(self):
        svc = self._create_airtable_service_mock()
        raw_data = {
            "regularOpeningHours": {
                "weekdayDescriptions": ["Monday: 9:00 AM – 5:00 PM", "Tuesday: 9:00 AM – 5:00 PM"]
            }
        }
        result = svc._extract_operating_hours(raw_data, 'GoogleMapsProvider')
        parsed = json.loads(result)
        assert parsed == ["Monday: 9:00 AM – 5:00 PM", "Tuesday: 9:00 AM – 5:00 PM"]

    def test_extracts_outscraper_hours(self):
        svc = self._create_airtable_service_mock()
        raw_data = {
            "working_hours": {
                "Monday": ["9AM-5PM"],
                "Friday": ["9AM-9PM"]
            }
        }
        result = svc._extract_operating_hours(raw_data, 'OutscraperProvider')
        parsed = json.loads(result)
        assert "Monday: 9AM-5PM" in parsed
        assert "Friday: 9AM-9PM" in parsed

    def test_returns_empty_string_when_no_data(self):
        svc = self._create_airtable_service_mock()
        assert svc._extract_operating_hours({}, 'GoogleMapsProvider') == ''
        assert svc._extract_operating_hours(None, 'GoogleMapsProvider') == ''

    def test_returns_empty_string_for_empty_hours(self):
        svc = self._create_airtable_service_mock()
        raw_data = {"regularOpeningHours": {"weekdayDescriptions": []}}
        assert svc._extract_operating_hours(raw_data, 'GoogleMapsProvider') == ''
