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
def get_all_place_data(self, place_id: str, place_name: str, skip_photos: bool = False):
    # ...
    if skip_photos:
        logging.info(f"Skipping photo retrieval for {place_name} as requested (photos already exist in Airtable)")
        photos = {
            "place_id": place_id,
            "message": "Photos retrieval skipped - photos already exist in Airtable",
            "photos_data": []
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
# Check if the place already has photos in Airtable
has_existing_photos = False
try:
    # Create a temporary AirtableClient
    airtable = AirtableClient()
    record = airtable.get_record(constants.SearchField.GOOGLE_MAPS_PLACE_ID, place_id)
    if record and 'Photos' in record['fields'] and record['fields']['Photos']:
        has_existing_photos = True
        logging.info(f"Place {place_name} already has photos in Airtable. Skipping photo retrieval to save API costs.")
except Exception as e:
    logging.warning(f"Could not check for existing photos in Airtable: {e}")

# Pass skip_photos=True if photos already exist
place_data = data_provider.get_all_place_data(place_id, place_name, skip_photos=has_existing_photos)
```

### 3. Airtable Client

The `enrich_base_data()` method in `AirtableClient` has been modified to:

1. Check if photos already exist in Airtable before considering updating them
2. Only add photos to fields to update if there are no existing photos
3. Skip updating the Photos field when photos already exist

```python
# Check if we already have photos in Airtable before considering updating them
has_existing_photos = 'Photos' in third_place['fields'] and third_place['fields']['Photos']

photos_data = place_data.get('photos', {})
photos_list = photos_data.get('photos_data', []) if photos_data else []

# Only add Photos to fields_to_update if:
# 1. We have new photos from the API AND
# 2. The place doesn't already have photos in Airtable
if photos_list and not has_existing_photos:
    fields_to_update['Photos'] = (str(photos_list), False)
elif has_existing_photos:
    logging.info(f"Skipping photo update for {place_name} as photos already exist in Airtable")
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