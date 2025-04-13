import os
import json
import helper_functions as helpers
from outscraper import ApiClient
from airtable_client import AirtableClient

airtable = AirtableClient()

result = airtable.enrich_base_data()

print(result)

print("Done")