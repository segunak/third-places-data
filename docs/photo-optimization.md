# Photo Optimization Strategy

## Overview

This document explains the photo optimization strategy implemented in the Third Places Data application to minimize API costs associated with retrieving place photos.

## The Problem

Retrieving photos from external APIs like Google Maps or Outscraper can be expensive:

1. Each photo retrieval operation counts as an API call
2. Most places have multiple photos, requiring multiple API calls
3. Photos rarely change, making repeated retrieval wasteful
4. Airtable already stores photos that have been previously retrieved

## The Solution

The application now follows a "photos-first" optimization strategy:

1. **Never retrieve photos for places that already have photos in Airtable**
2. Preserve existing photos in Airtable and never overwrite them
3. Only fetch photos for places that have no photos in the Airtable "Photos" field

## Implementation Details

This strategy is implemented in several layers of the application:

### 1. Base `PlaceDataProvider` Class

The `get_all_place_data()` method now accepts a `skip_photos` parameter:

```python
def get_all_place_data(self, place_id: str, place_name: str, skip_photos: bool = True, force_refresh: bool = False):
    # ...
    if skip_photos:
        logging.info(f"get_all_place_data: Skipping photo retrieval for {place_name} as requested.")
        photos = {
            "place_id": place_id,
            "message": "Photos retrieval skipped.",
            "photo_urls": []
        }
    else:
        photos = self.get_place_photos(place_id)
    # ...
```

### 2. Central Helper Function

The `get_and_cache_place_data()` function in `helper_functions.py` now:

1. Checks if photos exist in Airtable for the place
2. Sets the `skip_photos` parameter accordingly when calling the data provider

```python
# First check if the place already has photos in Airtable to avoid API costs
skip_photos, airtable_photos = _should_skip_photos_retrieval(place_id, place_name)
    
# Retrieve fresh data
logging.info(f"Retrieving fresh data for {place_name} with place_id {place_id}")
place_data = data_provider.get_all_place_data(place_id, place_name, skip_photos=skip_photos)
```

The `_should_skip_photos_retrieval()` helper function determines whether photos should be skipped:

```python
def _should_skip_photos_retrieval(place_id: str, place_name: str) -> Tuple[bool, Optional[str]]:
    """
    Determine if photo retrieval should be skipped based on existing Airtable data.
    
    Returns:
        Tuple of (skip_photos, airtable_photos), where:
        - skip_photos is True if photos should be skipped
        - airtable_photos is the photos data from Airtable if available
    """
    try:
        airtable = get_airtable_client()
        airtable_record = airtable.get_record(constants.SearchField.GOOGLE_MAPS_PLACE_ID, place_id)
        
        if airtable_record and 'Photos' in airtable_record['fields'] and airtable_record['fields']['Photos']:
            airtable_photos = airtable_record['fields']['Photos']
            logging.info(f"Place {place_name} already has photos in Airtable. Skipping photo retrieval to save API costs.")
            return True, airtable_photos
    except Exception as e:
        logging.warning(f"Could not check for existing photos in Airtable: {e}")
    
    return False, None
```

### 3. Airtable Client

The `enrich_base_data()` method in `AirtableClient` has been modified to:

1. Check if photos already exist in Airtable before considering updating them
2. Only add photos to fields to update if there are no existing photos
3. Skip updating the Photos field when photos already exist

```python
# Handle photos - check if we already have photos in Airtable
record = self.charlotte_third_places.get(record_id)
has_existing_photos = 'Photos' in record['fields'] and record['fields']['Photos']
photos_data = place_data.get('photos', {})
photos_list = photos_data.get('photo_urls', []) if photos_data else []

# Only add Photos if we have new photos AND the place doesn't already have photos
if photos_list and not has_existing_photos:
    fields_to_update['Photos'] = (str(photos_list), False)
elif has_existing_photos:
    logging.info(f"Skipping photo update as photos already exist in Airtable")
```

## Cost Savings

This optimization significantly reduces API costs:

1. **Eliminated Redundant Calls**: Places with existing photos avoid making photo API calls entirely
2. **Reduced Data Transfer**: No photo URLs are transferred in responses when skipped
3. **Cache Efficiency**: Cached data retains knowledge of which places have existing photos

## Notes for Developers

When working with the codebase:

1. **Never Remove This Optimization**: Removing these checks could significantly increase API costs
2. **Check Logging**: The application logs when photo retrieval is skipped to help track savings
3. **Force Photo Updates**: If you must update photos for a place with existing photos:
   - Either manually clear the "Photos" field in Airtable first
   - Or modify the code to temporarily bypass this check

## Future Improvements

Potential enhancements to this strategy:

1. Add a scheduled job to detect and fill missing photos for places that have none
2. Implement a "photo staleness" check that could update photos after a very long period (e.g., 1 year)
3. Add an option to force photo refresh on a per-place basis