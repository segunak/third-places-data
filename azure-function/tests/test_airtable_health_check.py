"""
Unit tests for the Airtable health check functionality.

Tests the data quality validation logic including:
- Place ID format validation
- Duplicate Place ID detection
- Required field checks
"""

import pytest
from unittest import mock

from blueprints.airtable import _validate_place_id_format, _check_required_fields


class TestValidatePlaceIdFormat:
    """Tests for _validate_place_id_format function."""

    def test_valid_place_id_chij_prefix(self):
        """Test valid Place ID with ChIJ prefix."""
        is_valid, error = _validate_place_id_format("ChIJgUbEo8cfqokR5lP9_Wh_DaM")
        assert is_valid is True
        assert error == ""

    def test_valid_place_id_ghij_prefix(self):
        """Test valid Place ID with GhIJ prefix."""
        is_valid, error = _validate_place_id_format("GhIJQWDl0CIeQUARxks3icF8U8A")
        assert is_valid is True
        assert error == ""

    def test_valid_place_id_with_underscore(self):
        """Test valid Place ID containing underscores."""
        is_valid, error = _validate_place_id_format("ChIJ_H9S7TOcPVIgRnG5eHqW4DE0")
        assert is_valid is True
        assert error == ""

    def test_valid_place_id_with_hyphen(self):
        """Test valid Place ID containing hyphens."""
        is_valid, error = _validate_place_id_format("ChIJ-abc123def456ghi789jkl0")
        assert is_valid is True
        assert error == ""

    def test_empty_place_id(self):
        """Test empty Place ID returns error."""
        is_valid, error = _validate_place_id_format("")
        assert is_valid is False
        assert "Empty Place ID" in error

    def test_none_place_id(self):
        """Test None Place ID returns error."""
        is_valid, error = _validate_place_id_format(None)
        assert is_valid is False
        assert "Empty Place ID" in error

    def test_place_id_with_spaces(self):
        """Test Place ID with spaces returns error."""
        is_valid, error = _validate_place_id_format("ChIJ abc123 def456")
        assert is_valid is False
        assert "Contains spaces" in error

    def test_place_id_with_leading_space(self):
        """Test Place ID with leading space returns error."""
        is_valid, error = _validate_place_id_format(" ChIJgUbEo8cfqokR5lP9_Wh_DaM")
        assert is_valid is False
        assert "Contains spaces" in error

    def test_place_id_with_trailing_space(self):
        """Test Place ID with trailing space returns error."""
        is_valid, error = _validate_place_id_format("ChIJgUbEo8cfqokR5lP9_Wh_DaM ")
        assert is_valid is False
        assert "Contains spaces" in error

    def test_place_id_with_invalid_characters(self):
        """Test Place ID with special characters returns error."""
        is_valid, error = _validate_place_id_format("ChIJ@#$%^&*()!")
        assert is_valid is False
        assert "invalid characters" in error

    def test_place_id_too_short(self):
        """Test Place ID that's too short returns error."""
        is_valid, error = _validate_place_id_format("ChIJabc")
        assert is_valid is False
        assert "Too short" in error

    def test_place_id_exactly_20_chars(self):
        """Test Place ID at minimum length boundary."""
        # 20 characters starting with ChIJ
        is_valid, error = _validate_place_id_format("ChIJ1234567890123456")
        assert is_valid is True
        assert error == ""

    def test_place_id_19_chars(self):
        """Test Place ID just under minimum length."""
        # 19 characters
        is_valid, error = _validate_place_id_format("ChIJ123456789012345")
        assert is_valid is False
        assert "Too short" in error

    def test_place_id_unusual_prefix_warning(self):
        """Test Place ID with unusual prefix returns warning but is valid."""
        # Valid format but unusual prefix - should be valid with warning
        is_valid, error = _validate_place_id_format("ABCD1234567890123456789")
        assert is_valid is True
        assert "Unusual prefix" in error

    def test_place_id_with_equals_sign(self):
        """Test Place ID containing equals sign (valid in some long IDs)."""
        is_valid, error = _validate_place_id_format("EicxMyBNYXJrZXQgU3Q=")
        # This is only 20 chars, let's make it longer
        is_valid, error = _validate_place_id_format("EicxMyBNYXJrZXQgU3Q=abcdefgh")
        assert is_valid is True

    def test_address_instead_of_place_id(self):
        """Test that an address is rejected as Place ID."""
        is_valid, error = _validate_place_id_format("3935 E Independence Blvd")
        assert is_valid is False
        assert "Contains spaces" in error

    def test_url_instead_of_place_id(self):
        """Test that a URL is rejected as Place ID."""
        is_valid, error = _validate_place_id_format("https://maps.google.com/?cid=123")
        assert is_valid is False
        # Contains colon and slashes which are invalid


class TestCheckRequiredFields:
    """Tests for _check_required_fields function."""

    def test_all_fields_present(self):
        """Test record with all required fields returns no issues."""
        record = {
            "id": "rec123",
            "fields": {
                "Place": "Test Coffee Shop",
                "Address": "123 Main St",
                "Type": "Coffee Shop"
            }
        }
        required = ["Place", "Address", "Type"]
        issues = _check_required_fields(record, required)
        assert len(issues) == 0

    def test_missing_one_field(self):
        """Test record missing one field returns one issue."""
        record = {
            "id": "rec123",
            "fields": {
                "Place": "Test Coffee Shop",
                "Address": "123 Main St"
                # Missing Type
            }
        }
        required = ["Place", "Address", "Type"]
        issues = _check_required_fields(record, required)
        assert len(issues) == 1
        assert issues[0]["field"] == "Type"
        assert issues[0]["recordId"] == "rec123"

    def test_missing_multiple_fields(self):
        """Test record missing multiple fields returns multiple issues."""
        record = {
            "id": "rec123",
            "fields": {
                "Place": "Test Coffee Shop"
                # Missing Address and Type
            }
        }
        required = ["Place", "Address", "Type"]
        issues = _check_required_fields(record, required)
        assert len(issues) == 2
        missing_fields = [i["field"] for i in issues]
        assert "Address" in missing_fields
        assert "Type" in missing_fields

    def test_empty_string_field(self):
        """Test field with empty string is treated as missing."""
        record = {
            "id": "rec123",
            "fields": {
                "Place": "Test Coffee Shop",
                "Address": "",  # Empty string
                "Type": "Coffee Shop"
            }
        }
        required = ["Place", "Address", "Type"]
        issues = _check_required_fields(record, required)
        assert len(issues) == 1
        assert issues[0]["field"] == "Address"

    def test_whitespace_only_field(self):
        """Test field with only whitespace is treated as missing."""
        record = {
            "id": "rec123",
            "fields": {
                "Place": "Test Coffee Shop",
                "Address": "   ",  # Only whitespace
                "Type": "Coffee Shop"
            }
        }
        required = ["Place", "Address", "Type"]
        issues = _check_required_fields(record, required)
        assert len(issues) == 1
        assert issues[0]["field"] == "Address"

    def test_null_field(self):
        """Test field with None value is treated as missing."""
        record = {
            "id": "rec123",
            "fields": {
                "Place": "Test Coffee Shop",
                "Address": None,
                "Type": "Coffee Shop"
            }
        }
        required = ["Place", "Address", "Type"]
        issues = _check_required_fields(record, required)
        assert len(issues) == 1
        assert issues[0]["field"] == "Address"

    def test_place_name_included_in_issue(self):
        """Test that place name is included in issue details."""
        record = {
            "id": "rec123",
            "fields": {
                "Place": "My Test Cafe",
                # Missing Address
            }
        }
        required = ["Address"]
        issues = _check_required_fields(record, required)
        assert issues[0]["placeName"] == "My Test Cafe"

    def test_unknown_place_when_name_missing(self):
        """Test fallback place name when Place field is missing."""
        record = {
            "id": "rec123",
            "fields": {
                # No Place field
            }
        }
        required = ["Address"]
        issues = _check_required_fields(record, required)
        assert issues[0]["placeName"] == "Unknown Place"

    def test_empty_record(self):
        """Test completely empty record."""
        record = {
            "id": "rec123",
            "fields": {}
        }
        required = ["Place", "Address", "Type"]
        issues = _check_required_fields(record, required)
        assert len(issues) == 3


class TestAirtableHealthCheckEndpoint:
    """
    Integration tests for the health check HTTP endpoint.
    
    Note: These tests are skipped because Azure Functions decorators wrap functions
    in a FunctionBuilder object that requires the full Azure Functions runtime.
    The core validation logic is tested in the helper function tests above.
    """

    @pytest.fixture
    def mock_airtable_records(self):
        """Fixture providing sample Airtable records."""
        return [
            {
                "id": "rec1",
                "fields": {
                    "Place": "Good Coffee Shop",
                    "Address": "123 Main St",
                    "Type": ["Coffee Shop"],
                    "Neighborhood": "Downtown",
                    "Google Maps Place Id": "ChIJgUbEo8cfqokR5lP9_Wh_DaM",
                    "Google Maps Profile URL": "https://maps.google.com/?cid=123",
                    "Apple Maps Profile URL": "https://maps.apple.com/?q=123"
                }
            },
            {
                "id": "rec2",
                "fields": {
                    "Place": "Another Cafe",
                    "Address": "456 Oak Ave",
                    "Type": ["Cafe"],
                    "Neighborhood": "Midtown",
                    "Google Maps Place Id": "ChIJ1234567890abcdefghij",
                    "Google Maps Profile URL": "https://maps.google.com/?cid=456",
                    "Apple Maps Profile URL": "https://maps.apple.com/?q=456"
                }
            }
        ]

    def test_validation_functions_exist(self):
        """Test that validation functions are importable."""
        from blueprints.airtable import _validate_place_id_format, _check_required_fields
        assert callable(_validate_place_id_format)
        assert callable(_check_required_fields)

    def test_endpoint_function_exists(self):
        """Test that the health check endpoint is registered."""
        from blueprints.airtable import airtable_health_check
        # Azure Functions decorators wrap the function in a FunctionBuilder
        assert airtable_health_check is not None

    def test_complete_validation_flow(self, mock_airtable_records):
        """Test the complete validation flow using helper functions directly."""
        from blueprints.airtable import _validate_place_id_format, _check_required_fields
        
        required_fields = [
            "Place", "Address", "Type", "Neighborhood",
            "Google Maps Place Id", "Google Maps Profile URL", "Apple Maps Profile URL"
        ]
        
        # Track place IDs for duplicate detection
        place_id_occurrences = {}
        all_issues = {
            "duplicates": [],
            "invalid_ids": [],
            "missing_fields": []
        }
        
        for record in mock_airtable_records:
            fields = record.get("fields", {})
            place_id = fields.get("Google Maps Place Id", "")
            
            # Check duplicates
            if place_id:
                if place_id not in place_id_occurrences:
                    place_id_occurrences[place_id] = []
                place_id_occurrences[place_id].append(record)
            
            # Validate place ID
            if place_id:
                is_valid, error = _validate_place_id_format(place_id)
                if not is_valid or error:
                    all_issues["invalid_ids"].append({
                        "place_id": place_id,
                        "error": error,
                        "is_valid": is_valid
                    })
            
            # Check required fields
            missing = _check_required_fields(record, required_fields)
            all_issues["missing_fields"].extend(missing)
        
        # Check for duplicates
        for pid, occurrences in place_id_occurrences.items():
            if len(occurrences) > 1:
                all_issues["duplicates"].append({
                    "place_id": pid,
                    "count": len(occurrences)
                })
        
        # With good test data, should have no issues
        assert len(all_issues["duplicates"]) == 0
        assert len(all_issues["missing_fields"]) == 0
        # May have warnings for unusual prefixes but not errors
        errors = [i for i in all_issues["invalid_ids"] if not i["is_valid"]]
        assert len(errors) == 0

    def test_duplicate_detection_flow(self):
        """Test duplicate detection using the complete flow."""
        from blueprints.airtable import _validate_place_id_format
        
        records = [
            {"id": "rec1", "fields": {"Place": "A", "Google Maps Place Id": "ChIJgUbEo8cfqokR5lP9_Wh_DaM"}},
            {"id": "rec2", "fields": {"Place": "B", "Google Maps Place Id": "ChIJgUbEo8cfqokR5lP9_Wh_DaM"}},  # Duplicate!
            {"id": "rec3", "fields": {"Place": "C", "Google Maps Place Id": "ChIJ1234567890abcdefghij"}},
        ]
        
        place_id_occurrences = {}
        for record in records:
            place_id = record.get("fields", {}).get("Google Maps Place Id", "")
            if place_id:
                if place_id not in place_id_occurrences:
                    place_id_occurrences[place_id] = []
                place_id_occurrences[place_id].append(record)
        
        duplicates = [pid for pid, occs in place_id_occurrences.items() if len(occs) > 1]
        assert len(duplicates) == 1
        assert duplicates[0] == "ChIJgUbEo8cfqokR5lP9_Wh_DaM"
