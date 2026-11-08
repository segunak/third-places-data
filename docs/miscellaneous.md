
# Miscellaneous

Stuff and thangs that don't fit into any specific category related to the project.

## Random Notes

* Go to <https://developers.google.com/maps/documentation/places/web-service/place-details> to understand Google Maps Billing in terms of which fields trigger which SKU's.

## Google Maps MCP Server Setup

The `google-maps` MCP server requires a Google Maps API key. The [MCP configuration](../.mcp.json) reads the key from the `GOOGLE_MAPS_API_KEY` environment variable. It sends the value in the `X-Goog-Api-Key` header.

1. In Google Cloud, select the project that contains the API key.
2. Enable [Maps Experience agent tools](https://console.cloud.google.com/apis/library/mapstools.googleapis.com). Its service name is `mapstools.googleapis.com`.
3. Open **APIs & Services > Credentials** and select the API key. Under **API restrictions**, include **Maps Experience agent tools** in the permitted APIs.
4. On Windows, open **Edit environment variables for your account**. Under **User variables**, add `GOOGLE_MAPS_API_KEY`. Set its value to the Google Maps API key.
5. Fully close and reopen VS Code. Then restart the `google-maps` MCP server to load the updated Windows environment.

The API must be enabled in the project and permitted by the key's API restrictions. API key restriction changes can take up to five minutes to apply. If requests return **HTTP 403 Forbidden**, check the key restrictions and try again after this delay.

Do not commit the API key or replace the environment variable reference with the key.

## Parking Notes

The details behind the `Parking` field.

### Google Options

{
  "freeParkingLot": boolean,
  "paidParkingLot": boolean,
  "freeStreetParking": boolean,
  "paidStreetParking": boolean,
  "valetParking": boolean,
  "freeGarageParking": boolean,
  "paidGarageParking": boolean
}

### My Approach

The `Parking` field, by way of Airtable automation script validation, must begin with one of these three.

* Free
* Paid
* Unsure

After that, any of these modifiers can be added.

* Garage
* Street
* Metered
* Limited
* Plentiful
* Time Limited
* Validation Available
