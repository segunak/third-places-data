import os
import json
import helper_functions as helpers
from outscraper import ApiClient
from airtable_client import AirtableClient


# A file for adhoc testing of Azure Function related code.
# This file is not part of the main codebase and is not used in production.

OUTSCRAPER_API_KEY = os.environ['OUTSCRAPER_API_KEY']
outscraper = ApiClient(api_key=OUTSCRAPER_API_KEY)


print("Done")