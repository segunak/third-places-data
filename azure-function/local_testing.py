import os
import json
import helper_functions as helpers
from outscraper import ApiClient
from airtable_client import AirtableClient


# A file for adhoc testing of Azure Function related code.
# This file is not part of the main codebase and is not used in production.

OUTSCRAPER_API_KEY = os.environ['OUTSCRAPER_API_KEY']
outscraper = ApiClient(api_key=OUTSCRAPER_API_KEY)

# Example of using Outscraper to get Google Maps reviews. The resulting data structure can be found in the 
# example file  example-raw-outscraper-reviews-response.json
outscraper_response = outscraper.google_maps_reviews(
    'ChIJH9S7TOcPVIgRnG5eHqW4DE0', limit=1, reviews_limit=250, sort='newest', language='en', ignore_empty=True
)

# Example of using Outscraper to get Google Maps search results. For a place. It has way more data,
# basically getting the place profile. The resulting data structure can be found in the example file data example-outscraper-google-places-v3-result.json
# results = outscraper.google_maps_search('ChIJH9S7TOcPVIgRnG5eHqW4DE0', limit=1, region='US')

# single_result = results[0][0]
#print(single_result)
#structured_outscraper_data = helpers.structure_outscraper_data(outscraper_response[0], "Mattie Ruth's Coffee House", 'ChIJH9S7TOcPVIgRnG5eHqW4DE0')

final_json_data = json.dumps(outscraper_response[0], indent=4)
print(final_json_data)

print("Done")