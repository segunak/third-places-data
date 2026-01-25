"""
Unit tests for the Airtable enrichment orchestrator result categorization logic.

These tests verify that places are correctly categorized into:
- enriched: Places where at least one field was updated
- unchanged: Places successfully processed but no fields needed updating
- not_found: Places where the provider couldn't find a match
- skipped: Places intentionally skipped
- failed: Places that encountered errors
"""

import pytest


class TestOrchestratorResultCategorization:
    """Tests for the result categorization logic in the orchestrator."""

    def categorize_results(self, results):
        """
        Helper function that replicates the categorization logic from the orchestrator.
        This allows us to test the logic in isolation without mocking Durable Functions.
        """
        actually_updated_places = []
        not_found_places = []
        skipped_places = []
        failed_places = []
        unchanged_places = []
        
        for place in results:
            if not place:
                continue
                
            # Check if place was enriched (has updates with updated=True)
            if place.get('field_updates') and any(updates.get("updated") for updates in place.get('field_updates', {}).values()):
                actually_updated_places.append(place)
            # Check if place was not found (sentinel case)
            elif place.get('status') == 'failed' and 'NO_PLACE_FOUND' in place.get('message', ''):
                not_found_places.append({
                    'place_name': place.get('place_name'),
                    'place_id': place.get('place_id'),
                    'record_id': place.get('record_id'),
                    'message': place.get('message', '')
                })
            # Skipped places (intentionally skipped, not a failure)
            elif place.get('status') == 'skipped':
                skipped_places.append({
                    'place_name': place.get('place_name'),
                    'place_id': place.get('place_id'),
                    'record_id': place.get('record_id'),
                    'message': place.get('message', 'Place skipped')
                })
            # Other failures (actual errors)
            elif place.get('status') == 'failed':
                failed_places.append({
                    'place_name': place.get('place_name'),
                    'place_id': place.get('place_id'),
                    'record_id': place.get('record_id'),
                    'message': place.get('message', 'Unknown error')
                })
            # Unchanged places - successfully processed but no fields needed updating
            elif place.get('status') in ('succeeded', 'cached'):
                field_comparison = {}
                for field_name, field_update in place.get('field_updates', {}).items():
                    field_comparison[field_name] = {
                        'current_value': field_update.get('old_value'),
                        'provider_value': field_update.get('new_value'),
                        'raw_provider_value': field_update.get('raw_provider_value', 'No Value From Provider'),
                        'reason': 'Values match' if field_update.get('old_value') == field_update.get('new_value') else 'Overwrite disabled and field has value'
                    }
                unchanged_places.append({
                    'place_name': place.get('place_name'),
                    'place_id': place.get('place_id'),
                    'record_id': place.get('record_id'),
                    'status': place.get('status'),
                    'message': 'All fields already up to date or overwrite not allowed',
                    'field_comparison': field_comparison
                })
        
        return {
            'places_enriched': actually_updated_places,
            'places_unchanged': unchanged_places,
            'places_not_found': not_found_places,
            'places_skipped': skipped_places,
            'places_failed': failed_places,
            'total_places_enriched': len(actually_updated_places),
            'total_places_unchanged': len(unchanged_places),
            'total_places_not_found': len(not_found_places),
            'total_places_skipped': len(skipped_places),
            'total_places_failed': len(failed_places),
        }

    def test_place_with_updated_field_is_enriched(self):
        """Test that a place with at least one updated field is categorized as enriched."""
        results = [{
            'place_name': 'Test Coffee Shop',
            'place_id': 'ChIJ123',
            'record_id': 'recABC',
            'status': 'succeeded',
            'field_updates': {
                'Website': {'updated': True, 'old_value': None, 'new_value': 'https://example.com'},
                'Address': {'updated': False, 'old_value': '123 Main St', 'new_value': '123 Main St'}
            }
        }]
        
        categorized = self.categorize_results(results)
        
        assert categorized['total_places_enriched'] == 1
        assert categorized['total_places_unchanged'] == 0
        assert categorized['places_enriched'][0]['place_name'] == 'Test Coffee Shop'

    def test_place_with_no_updated_fields_is_unchanged(self):
        """Test that a place with status 'succeeded' but no updated fields is categorized as unchanged."""
        results = [{
            'place_name': 'Test Coffee Shop',
            'place_id': 'ChIJ123',
            'record_id': 'recABC',
            'status': 'succeeded',
            'field_updates': {
                'Website': {'updated': False, 'old_value': 'https://example.com', 'new_value': 'https://example.com', 'raw_provider_value': 'https://example.com'},
                'Address': {'updated': False, 'old_value': '123 Main St', 'new_value': '123 Main St', 'raw_provider_value': '123 Main St, Charlotte, NC'}
            }
        }]
        
        categorized = self.categorize_results(results)
        
        assert categorized['total_places_enriched'] == 0
        assert categorized['total_places_unchanged'] == 1
        assert categorized['places_unchanged'][0]['place_name'] == 'Test Coffee Shop'
        assert categorized['places_unchanged'][0]['status'] == 'succeeded'
        assert 'field_comparison' in categorized['places_unchanged'][0]

    def test_cached_place_with_no_updates_is_unchanged(self):
        """Test that a place with status 'cached' but no updated fields is categorized as unchanged."""
        results = [{
            'place_name': 'Test Coffee Shop',
            'place_id': 'ChIJ123',
            'record_id': 'recABC',
            'status': 'cached',
            'field_updates': {
                'Website': {'updated': False, 'old_value': 'https://example.com', 'new_value': 'https://example.com'}
            }
        }]
        
        categorized = self.categorize_results(results)
        
        assert categorized['total_places_unchanged'] == 1
        assert categorized['places_unchanged'][0]['status'] == 'cached'

    def test_unchanged_place_includes_field_comparison(self):
        """Test that unchanged places include detailed field comparison with reasons."""
        results = [{
            'place_name': 'Test Coffee Shop',
            'place_id': 'ChIJ123',
            'record_id': 'recABC',
            'status': 'succeeded',
            'field_updates': {
                'Website': {
                    'updated': False, 
                    'old_value': 'https://example.com', 
                    'new_value': 'https://example.com',
                    'raw_provider_value': 'https://example.com/full/path'
                },
                'Parking': {
                    'updated': False, 
                    'old_value': 'Free', 
                    'new_value': 'Paid',
                    'raw_provider_value': {'paidStreetParking': True}
                }
            }
        }]
        
        categorized = self.categorize_results(results)
        
        unchanged = categorized['places_unchanged'][0]
        assert 'field_comparison' in unchanged
        
        # Website values match
        website_comparison = unchanged['field_comparison']['Website']
        assert website_comparison['current_value'] == 'https://example.com'
        assert website_comparison['provider_value'] == 'https://example.com'
        assert website_comparison['raw_provider_value'] == 'https://example.com/full/path'
        assert website_comparison['reason'] == 'Values match'
        
        # Parking values differ but overwrite was disabled
        parking_comparison = unchanged['field_comparison']['Parking']
        assert parking_comparison['current_value'] == 'Free'
        assert parking_comparison['provider_value'] == 'Paid'
        assert parking_comparison['raw_provider_value'] == {'paidStreetParking': True}
        assert parking_comparison['reason'] == 'Overwrite disabled and field has value'

    def test_not_found_place_categorization(self):
        """Test that places with NO_PLACE_FOUND in message are categorized as not found."""
        results = [{
            'place_name': 'Unknown Coffee Shop',
            'place_id': None,
            'record_id': 'recXYZ',
            'status': 'failed',
            'message': 'NO_PLACE_FOUND: Could not find place in provider',
            'field_updates': {}
        }]
        
        categorized = self.categorize_results(results)
        
        assert categorized['total_places_not_found'] == 1
        assert categorized['total_places_failed'] == 0
        assert categorized['places_not_found'][0]['place_name'] == 'Unknown Coffee Shop'

    def test_skipped_place_categorization(self):
        """Test that places with status 'skipped' are categorized correctly."""
        results = [{
            'place_name': 'Skipped Coffee Shop',
            'place_id': 'ChIJ456',
            'record_id': 'recDEF',
            'status': 'skipped',
            'message': 'Missing place name',
            'field_updates': {}
        }]
        
        categorized = self.categorize_results(results)
        
        assert categorized['total_places_skipped'] == 1
        assert categorized['places_skipped'][0]['place_name'] == 'Skipped Coffee Shop'

    def test_failed_place_categorization(self):
        """Test that places with status 'failed' (not NO_PLACE_FOUND) are categorized as failed."""
        results = [{
            'place_name': 'Error Coffee Shop',
            'place_id': 'ChIJ789',
            'record_id': 'recGHI',
            'status': 'failed',
            'message': 'API rate limit exceeded',
            'field_updates': {}
        }]
        
        categorized = self.categorize_results(results)
        
        assert categorized['total_places_failed'] == 1
        assert categorized['total_places_not_found'] == 0
        assert categorized['places_failed'][0]['message'] == 'API rate limit exceeded'

    def test_mixed_results_categorization(self):
        """Test categorization with a mix of all result types."""
        results = [
            # Enriched - has updated field
            {
                'place_name': 'Updated Cafe',
                'place_id': 'ChIJ1',
                'record_id': 'rec1',
                'status': 'succeeded',
                'field_updates': {'Website': {'updated': True, 'old_value': None, 'new_value': 'https://new.com'}}
            },
            # Unchanged - succeeded but nothing updated
            {
                'place_name': 'Same Cafe',
                'place_id': 'ChIJ2',
                'record_id': 'rec2',
                'status': 'succeeded',
                'field_updates': {'Website': {'updated': False, 'old_value': 'https://same.com', 'new_value': 'https://same.com'}}
            },
            # Not found
            {
                'place_name': 'Missing Cafe',
                'place_id': None,
                'record_id': 'rec3',
                'status': 'failed',
                'message': 'NO_PLACE_FOUND: Not in database',
                'field_updates': {}
            },
            # Skipped
            {
                'place_name': 'Skip Cafe',
                'place_id': 'ChIJ4',
                'record_id': 'rec4',
                'status': 'skipped',
                'message': 'Intentionally skipped',
                'field_updates': {}
            },
            # Failed
            {
                'place_name': 'Error Cafe',
                'place_id': 'ChIJ5',
                'record_id': 'rec5',
                'status': 'failed',
                'message': 'Connection timeout',
                'field_updates': {}
            },
        ]
        
        categorized = self.categorize_results(results)
        
        assert categorized['total_places_enriched'] == 1
        assert categorized['total_places_unchanged'] == 1
        assert categorized['total_places_not_found'] == 1
        assert categorized['total_places_skipped'] == 1
        assert categorized['total_places_failed'] == 1
        
        # Verify all places accounted for
        total = (
            categorized['total_places_enriched'] +
            categorized['total_places_unchanged'] +
            categorized['total_places_not_found'] +
            categorized['total_places_skipped'] +
            categorized['total_places_failed']
        )
        assert total == 5

    def test_null_results_are_skipped(self):
        """Test that None values in results are safely skipped."""
        results = [
            None,
            {
                'place_name': 'Valid Cafe',
                'place_id': 'ChIJ1',
                'record_id': 'rec1',
                'status': 'succeeded',
                'field_updates': {'Website': {'updated': True, 'old_value': None, 'new_value': 'https://example.com'}}
            },
            None
        ]
        
        categorized = self.categorize_results(results)
        
        assert categorized['total_places_enriched'] == 1

    def test_empty_field_updates_with_succeeded_status_is_unchanged(self):
        """Test that a succeeded place with empty field_updates is categorized as unchanged."""
        results = [{
            'place_name': 'Empty Updates Cafe',
            'place_id': 'ChIJ123',
            'record_id': 'recABC',
            'status': 'succeeded',
            'field_updates': {}
        }]
        
        categorized = self.categorize_results(results)
        
        assert categorized['total_places_unchanged'] == 1
        assert categorized['places_unchanged'][0]['field_comparison'] == {}

    def test_raw_provider_value_default_when_missing(self):
        """Test that raw_provider_value defaults to 'No Value From Provider' when not present."""
        results = [{
            'place_name': 'Test Cafe',
            'place_id': 'ChIJ123',
            'record_id': 'recABC',
            'status': 'succeeded',
            'field_updates': {
                'Website': {'updated': False, 'old_value': 'https://example.com', 'new_value': 'https://example.com'}
                # Note: raw_provider_value is missing
            }
        }]
        
        categorized = self.categorize_results(results)
        
        unchanged = categorized['places_unchanged'][0]
        assert unchanged['field_comparison']['Website']['raw_provider_value'] == 'No Value From Provider'

    def test_structured_raw_provider_value_preserved(self):
        """Test that structured objects in raw_provider_value are preserved."""
        raw_parking = {'freeParkingLot': True, 'paidStreetParking': False}
        results = [{
            'place_name': 'Test Cafe',
            'place_id': 'ChIJ123',
            'record_id': 'recABC',
            'status': 'succeeded',
            'field_updates': {
                'Parking': {
                    'updated': False, 
                    'old_value': 'Free', 
                    'new_value': 'Free',
                    'raw_provider_value': raw_parking
                }
            }
        }]
        
        categorized = self.categorize_results(results)
        
        unchanged = categorized['places_unchanged'][0]
        assert unchanged['field_comparison']['Parking']['raw_provider_value'] == raw_parking
        assert isinstance(unchanged['field_comparison']['Parking']['raw_provider_value'], dict)
