# Outscraper

Details about [Outscraper](https://outscraper.com/) and its role in the Third Places Data project.

## Overview

Outscraper is used as a data provider for Google Maps place information. Unlike the direct Google Maps API, Outscraper can retrieve:

1. Place details including address, website, business status, and description
2. Place reviews which are not directly available through Google's API
3. Photos with tags that enable intelligent photo selection

## Photo Selection Algorithm

The OutscraperProvider implementation includes an advanced photo selection algorithm that:

1. First sorts all photos by date (newest first)
2. Categorizes photos based on tags:
   - "front" - Photos of the storefront
   - "vibe" - Photos showing the interior atmosphere or vibe
   - "all" - General photos
   - "other" - Miscellaneous tagged photos
3. Applies a selection priority:
   - All "vibe" photos (top priority)
   - Up to 5 "front" photos (second priority)
   - "all" tagged photos (third priority)
   - "other" photos (fourth priority)
   - Any remaining untagged photos
4. Returns a maximum of 25 photos

This algorithm ensures we get the most useful and representative photos for each place.

## Place ID Lookup

When looking up places by name, Outscraper's API tries to find exact matches first:

```python
# Look for exact matches first
for candidate in candidates:
    if candidate.get('name', '').lower() == place_name.lower():
        return candidate.get('place_id', '')

# If no exact match, return the first result
if candidates[0].get('place_id'):
    return candidates[0].get('place_id', '')
```

## Webhooks

Outscraper allows users to write a custom webhook and have them hit that endpoint with the response of requesting reviews. You can view the result of webhook calls [here](https://app.outscraper.com/webhook-calls).

Visit the [Integrations](https://app.outscraper.com/integrations) page to set the webhook URL. Read through the [Access Keys](https://learn.microsoft.com/en-us/azure/azure-functions/function-keys-how-to?tabs=azure-portal) page for Azure Functions to understand how authentication works. In summary, provide an endpoint that grants access to 1 function (use a Function Key), rather than all of them.

## Response Formats

### Webhook Response Format

For testing Outscraper webhooks locally. The `results_location` expires after 24 hours or so. To get a new one, go to the Outscraper portal, make a Google Maps API reviews request, and then go to <https://app.outscraper.com/api-usage> to get the `results_location`.

```json
{
    "id": "your-request-id",
    "user_id": "your-user-id",
    "status": "SUCCESS",
    "api_task": true,
    "results_location": "https://api.app.outscraper.com/requests/YXV0aDB8NjNhMzRkZGRjNmRmNDM5MGJmM2ZkMzZjLDIwMjQwODE3MjA1OTM1eHM0YQ",
    "quota_usage": [
        {
            "product_name": "Google Maps Data",
            "quantity": 1
        }
    ]
}
```

### Standardized Response Format

The OutscraperProvider standardizes responses to ensure consistency across all providers. Here are the standardized response formats:

#### Place Details

```json
{
  "place_name": "Example Coffee",
  "place_id": "ChIJxxxxxxxx",
  "google_maps_url": "https://maps.google.com/?cid=1234567890",
  "website": "https://example-coffee.com",
  "address": "123 Main St, Charlotte, NC 28201",
  "description": "Cozy coffee shop with free wifi and workspace.",
  "purchase_required": "Yes",
  "parking": ["Free", "Street"],
  "latitude": 35.2271,
  "longitude": -80.8431,
  "raw_data": { /* Full raw response from Outscraper */ }
}
```

#### Reviews

```json
{
  "place_id": "ChIJxxxxxxxx",
  "message": "",
  "reviews_data": [ /* Array of review objects */ ],
  "raw_data": { /* Full raw response from Outscraper */ }
}
```

#### Photos

```json
{
  "place_id": "ChIJxxxxxxxx",
  "message": "Retrieved 50 photos, selected 25",
  "photo_urls": [
    "https://lh5.googleusercontent.com/p/example1",
    "https://lh5.googleusercontent.com/p/example2",
    /* ... more URLs ... */
  ],
  "raw_data": { /* Full raw response from Outscraper */ }
}
```

## Configuration

The OutscraperProvider is configured with location bias for Charlotte, NC:

```python
# Charlotte, NC coordinates for location bias
# Format: "@latitude,longitude,zoom" as required by Outscraper API
# Zoom level 9 provides appropriate coverage for a ~50,000 meter radius
self.charlotte_coordinates = "@35.23075539296459,-80.83165532446358,9z"  # Uptown Charlotte with zoom

# Default parameters for all requests
self.default_params = {
    'language': 'en',          # English language results
    'region': 'US',            # United States region
    'async': False,            # Synchronous requests by default
}
```

## Additional Information

- See [this page](https://outscraper.com/place-id-feature-id-cid/) for a `google_id` explainer. This field is returned by the Outscraper API but not currently used in our application.
- The Outscraper API reference documentation is available at [https://app.outscraper.com/api-docs](https://app.outscraper.com/api-docs)
