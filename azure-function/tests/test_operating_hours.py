"""
Tests for the operating hours feature across providers and enrichment pipeline.

Canonical format: "Day: H:MM AM - H:MM PM"
Examples: "Monday: 3:00 PM - 8:00 PM", "Tuesday: 11:00 AM - 2:00 PM, 5:00 PM - 10:00 PM"
Pass-through values: "Closed", "Open 24 hours"
"""

import json
import pytest
from unittest.mock import patch, MagicMock
from services.place_data_service import PlaceDataService, OutscraperProvider, GoogleMapsProvider


# ======================================================
# Shared Normalization Utilities
# ======================================================


class TestCleanGoogleHoursUnicode:
    """Tests for PlaceDataService._clean_google_hours_unicode."""

    def test_replaces_narrow_no_break_space(self):
        result = PlaceDataService._clean_google_hours_unicode("7:00\u202fAM")
        assert result == "7:00 AM"

    def test_replaces_thin_space(self):
        result = PlaceDataService._clean_google_hours_unicode("7:00 AM\u2009\u2013\u20095:00 PM")
        assert result == "7:00 AM - 5:00 PM"

    def test_replaces_en_dash(self):
        result = PlaceDataService._clean_google_hours_unicode("7:00 AM \u2013 5:00 PM")
        assert result == "7:00 AM - 5:00 PM"

    def test_full_google_string_cleanup(self):
        raw = "Monday: 7:00\u202fAM\u2009\u2013\u20095:00\u202fPM"
        result = PlaceDataService._clean_google_hours_unicode(raw)
        assert result == "Monday: 7:00 AM - 5:00 PM"

    def test_preserves_closed(self):
        assert PlaceDataService._clean_google_hours_unicode("Sunday: Closed") == "Sunday: Closed"

    def test_handles_empty_string(self):
        assert PlaceDataService._clean_google_hours_unicode("") == ""

    def test_handles_none(self):
        assert PlaceDataService._clean_google_hours_unicode(None) is None


class TestParseCompactTimeRange:
    """Tests for PlaceDataService._parse_compact_time_range."""

    def test_simple_pm_range(self):
        assert PlaceDataService._parse_compact_time_range("3-8PM") == "3:00 PM - 8:00 PM"

    def test_am_pm_range(self):
        assert PlaceDataService._parse_compact_time_range("11AM-2PM") == "11:00 AM - 2:00 PM"

    def test_with_minutes(self):
        assert PlaceDataService._parse_compact_time_range("7:30AM-5PM") == "7:30 AM - 5:00 PM"

    def test_noon_range(self):
        assert PlaceDataService._parse_compact_time_range("12-11PM") == "12:00 PM - 11:00 PM"

    def test_noon_to_pm(self):
        assert PlaceDataService._parse_compact_time_range("12-7PM") == "12:00 PM - 7:00 PM"

    def test_am_only_range(self):
        assert PlaceDataService._parse_compact_time_range("6AM-11AM") == "6:00 AM - 11:00 AM"

    def test_passes_through_closed(self):
        assert PlaceDataService._parse_compact_time_range("Closed") == "Closed"

    def test_passes_through_open_24_hours(self):
        assert PlaceDataService._parse_compact_time_range("Open 24 hours") == "Open 24 hours"

    def test_handles_empty_string(self):
        assert PlaceDataService._parse_compact_time_range("") == ""

    def test_handles_none(self):
        assert PlaceDataService._parse_compact_time_range(None) == ""

    def test_both_am_pm_explicit(self):
        assert PlaceDataService._parse_compact_time_range("9AM-5PM") == "9:00 AM - 5:00 PM"

    def test_minutes_on_both_sides(self):
        assert PlaceDataService._parse_compact_time_range("7:30AM-5:30PM") == "7:30 AM - 5:30 PM"


class TestParseCompactTime:
    """Tests for PlaceDataService._parse_compact_time."""

    def test_simple_pm(self):
        assert PlaceDataService._parse_compact_time("3PM") == "3:00 PM"

    def test_simple_am(self):
        assert PlaceDataService._parse_compact_time("7AM") == "7:00 AM"

    def test_with_minutes(self):
        assert PlaceDataService._parse_compact_time("7:30AM") == "7:30 AM"

    def test_number_only_with_fallback(self):
        assert PlaceDataService._parse_compact_time("12", "PM") == "12:00 PM"

    def test_number_only_no_fallback(self):
        assert PlaceDataService._parse_compact_time("3") == "3:00"

    def test_double_digit_pm(self):
        assert PlaceDataService._parse_compact_time("11PM") == "11:00 PM"


class TestNormalizeOperatingHours:
    """Tests for PlaceDataService.normalize_operating_hours."""

    def test_cleans_google_unicode(self):
        raw = [
            "Monday: 7:00\u202fAM\u2009\u2013\u20095:00\u202fPM",
            "Tuesday: 8:00\u202fAM\u2009\u2013\u20093:00\u202fPM"
        ]
        result = PlaceDataService.normalize_operating_hours(raw)
        assert result == [
            "Monday: 7:00 AM - 5:00 PM",
            "Tuesday: 8:00 AM - 3:00 PM"
        ]

    def test_preserves_clean_strings(self):
        clean = ["Monday: 7:00 AM - 5:00 PM", "Sunday: Closed"]
        assert PlaceDataService.normalize_operating_hours(clean) == clean

    def test_handles_empty_list(self):
        assert PlaceDataService.normalize_operating_hours([]) == []

    def test_handles_none(self):
        assert PlaceDataService.normalize_operating_hours(None) == []


# ======================================================
# Outscraper Normalization (produces canonical format)
# ======================================================


class TestOutscraperNormalizeHours:
    """Tests for OutscraperProvider._normalize_outscraper_hours — outputs canonical format."""

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
        assert result[0] == "Sunday: 12:00 PM - 7:00 PM"
        assert result[1] == "Monday: 3:00 PM - 8:00 PM"
        assert result[6] == "Saturday: 12:00 PM - 11:00 PM"

    def test_handles_multi_range_days(self):
        working_hours = {
            "Monday": ["11AM-2PM", "5-10PM"],
            "Tuesday": ["11AM-2PM", "5-10PM"]
        }
        result = OutscraperProvider._normalize_outscraper_hours(working_hours)
        assert "Monday: 11:00 AM - 2:00 PM, 5:00 PM - 10:00 PM" in result
        assert "Tuesday: 11:00 AM - 2:00 PM, 5:00 PM - 10:00 PM" in result

    def test_handles_string_values(self):
        working_hours = {
            "Monday": "9AM-5PM",
            "Tuesday": "9AM-5PM"
        }
        result = OutscraperProvider._normalize_outscraper_hours(working_hours)
        assert "Monday: 9:00 AM - 5:00 PM" in result

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
        assert result[0] == "Monday: 9:00 AM - 5:00 PM"


# ======================================================
# Both providers produce the same canonical format
# ======================================================


class TestFormatConsistency:
    """Verify Google and Outscraper produce identical format for equivalent hours."""

    def test_same_format_simple_hours(self):
        google_raw = ["Monday: 3:00\u202fPM\u2009\u2013\u20098:00\u202fPM"]
        google_result = PlaceDataService.normalize_operating_hours(google_raw)

        outscraper_raw = {"Monday": ["3-8PM"]}
        outscraper_result = OutscraperProvider._normalize_outscraper_hours(outscraper_raw)

        assert google_result[0] == outscraper_result[0] == "Monday: 3:00 PM - 8:00 PM"

    def test_same_format_am_pm(self):
        google_raw = ["Tuesday: 9:00\u202fAM\u2009\u2013\u20095:00\u202fPM"]
        google_result = PlaceDataService.normalize_operating_hours(google_raw)

        outscraper_raw = {"Tuesday": ["9AM-5PM"]}
        outscraper_result = OutscraperProvider._normalize_outscraper_hours(outscraper_raw)

        assert google_result[0] == outscraper_result[0] == "Tuesday: 9:00 AM - 5:00 PM"

    def test_same_format_multi_range(self):
        google_raw = ["Monday: 11:00\u202fAM\u2009\u2013\u20092:00\u202fPM, 5:00\u202fPM\u2009\u2013\u200910:00\u202fPM"]
        google_result = PlaceDataService.normalize_operating_hours(google_raw)

        outscraper_raw = {"Monday": ["11AM-2PM", "5-10PM"]}
        outscraper_result = OutscraperProvider._normalize_outscraper_hours(outscraper_raw)

        assert google_result[0] == outscraper_result[0] == "Monday: 11:00 AM - 2:00 PM, 5:00 PM - 10:00 PM"


# ======================================================
# Provider Integration Tests
# ======================================================


class TestGoogleMapsProviderGetOperatingHours:
    """Tests for GoogleMapsProvider.get_operating_hours — returns normalized output."""

    @patch('services.place_data_service.requests.get')
    def test_returns_normalized_hours(self, mock_get, mock_env_vars):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "regularOpeningHours": {
                "weekdayDescriptions": [
                    "Monday: 7:00\u202fAM\u2009\u2013\u20095:00\u202fPM",
                    "Sunday: Closed"
                ]
            }
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        provider = GoogleMapsProvider()
        result = provider.get_operating_hours("ChIJtest123")

        assert result[0] == "Monday: 7:00 AM - 5:00 PM"
        assert result[1] == "Sunday: Closed"

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
    """Tests for OutscraperProvider.get_operating_hours — uses fields param for lighter requests."""

    @patch.object(OutscraperProvider, '__init__', lambda self: None)
    def test_extracts_and_normalizes_hours(self):
        provider = OutscraperProvider.__new__(OutscraperProvider)
        provider._provider_type = 'outscraper'
        provider.client = MagicMock()
        provider.default_params = {'language': 'en', 'region': 'US'}

        provider.client.google_maps_search.return_value = [[{
            "name": "Test Place",
            "place_id": "ChIJtest",
            "working_hours": {
                "Monday": ["9AM-5PM"],
                "Tuesday": ["9AM-5PM"],
                "Sunday": ["Closed"]
            }
        }]]

        result = provider.get_operating_hours("ChIJtest")

        assert result[0] == "Sunday: Closed"
        assert result[1] == "Monday: 9:00 AM - 5:00 PM"
        assert result[2] == "Tuesday: 9:00 AM - 5:00 PM"

        # Verify fields param was passed for lighter request
        call_kwargs = provider.client.google_maps_search.call_args
        assert call_kwargs[1].get('fields') == ['working_hours', 'name', 'place_id']

    @patch.object(OutscraperProvider, '__init__', lambda self: None)
    def test_returns_empty_when_no_working_hours(self):
        provider = OutscraperProvider.__new__(OutscraperProvider)
        provider._provider_type = 'outscraper'
        provider.client = MagicMock()
        provider.default_params = {'language': 'en', 'region': 'US'}

        provider.client.google_maps_search.return_value = [[{
            "name": "Test Place",
            "place_id": "ChIJtest"
        }]]

        result = provider.get_operating_hours("ChIJtest")
        assert result == []

    @patch.object(OutscraperProvider, '__init__', lambda self: None)
    def test_returns_empty_on_api_error(self):
        provider = OutscraperProvider.__new__(OutscraperProvider)
        provider._provider_type = 'outscraper'
        provider.client = MagicMock()
        provider.default_params = {'language': 'en', 'region': 'US'}

        provider.client.google_maps_search.side_effect = Exception("API error")

        result = provider.get_operating_hours("ChIJtest")
        assert result == []


# ======================================================
# JSON Round-Trip Tests
# ======================================================


class TestOperatingHoursJsonRoundTrip:
    """Test that normalized hours survive JSON serialization."""

    def test_canonical_format_round_trip(self):
        hours = [
            "Monday: 7:00 AM - 5:00 PM",
            "Tuesday: 7:00 AM - 5:00 PM",
            "Sunday: Closed"
        ]
        json_str = json.dumps(hours, ensure_ascii=False)
        parsed = json.loads(json_str)
        assert parsed == hours

    def test_outscraper_normalized_round_trip(self):
        working_hours = {
            "Monday": ["11AM-2PM", "5-10PM"],
            "Sunday": ["12-7PM"]
        }
        normalized = OutscraperProvider._normalize_outscraper_hours(working_hours)
        json_str = json.dumps(normalized, ensure_ascii=False)
        parsed = json.loads(json_str)
        assert parsed == normalized
        assert "Sunday: 12:00 PM - 7:00 PM" in parsed
        assert "Monday: 11:00 AM - 2:00 PM, 5:00 PM - 10:00 PM" in parsed


# ======================================================
# Enrichment Pipeline Tests
# ======================================================


class TestExtractOperatingHours:
    """Tests for AirtableService._extract_operating_hours — produces canonical format."""

    def _create_airtable_service_mock(self):
        from services.airtable_service import AirtableService
        with patch.object(AirtableService, '__init__', lambda self, *args, **kwargs: None):
            svc = AirtableService.__new__(AirtableService)
            return svc

    def test_extracts_and_normalizes_google_hours(self):
        svc = self._create_airtable_service_mock()
        raw_data = {
            "regularOpeningHours": {
                "weekdayDescriptions": [
                    "Monday: 9:00\u202fAM\u2009\u2013\u20095:00\u202fPM",
                    "Tuesday: 9:00\u202fAM\u2009\u2013\u20095:00\u202fPM"
                ]
            }
        }
        result = svc._extract_operating_hours(raw_data, 'GoogleMapsProvider')
        parsed = json.loads(result)
        assert parsed == ["Monday: 9:00 AM - 5:00 PM", "Tuesday: 9:00 AM - 5:00 PM"]

    def test_extracts_and_normalizes_outscraper_hours(self):
        svc = self._create_airtable_service_mock()
        raw_data = {
            "working_hours": {
                "Monday": ["9AM-5PM"],
                "Friday": ["9AM-9PM"]
            }
        }
        result = svc._extract_operating_hours(raw_data, 'OutscraperProvider')
        parsed = json.loads(result)
        assert "Monday: 9:00 AM - 5:00 PM" in parsed
        assert "Friday: 9:00 AM - 9:00 PM" in parsed

    def test_returns_empty_string_when_no_data(self):
        svc = self._create_airtable_service_mock()
        assert svc._extract_operating_hours({}, 'GoogleMapsProvider') == ''
        assert svc._extract_operating_hours(None, 'GoogleMapsProvider') == ''

    def test_returns_empty_string_for_empty_hours(self):
        svc = self._create_airtable_service_mock()
        raw_data = {"regularOpeningHours": {"weekdayDescriptions": []}}
        assert svc._extract_operating_hours(raw_data, 'GoogleMapsProvider') == ''

    def test_google_and_outscraper_enrichment_produce_same_format(self):
        """Key test: both providers produce identical format through the enrichment pipeline."""
        svc = self._create_airtable_service_mock()

        google_raw = {
            "regularOpeningHours": {
                "weekdayDescriptions": ["Monday: 3:00\u202fPM\u2009\u2013\u20098:00\u202fPM"]
            }
        }
        outscraper_raw = {"working_hours": {"Monday": ["3-8PM"]}}

        google_result = json.loads(svc._extract_operating_hours(google_raw, 'GoogleMapsProvider'))
        outscraper_result = json.loads(svc._extract_operating_hours(outscraper_raw, 'OutscraperProvider'))

        assert google_result[0] == outscraper_result[0] == "Monday: 3:00 PM - 8:00 PM"
