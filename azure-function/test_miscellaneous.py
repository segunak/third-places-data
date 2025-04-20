# A file for testing the miscellaneous functions in the Azure Function project.
import os
import json
import csv
import datetime
import ast
import helper_functions as helpers
from collections import Counter
from constants import SearchField
from airtable_client import AirtableClient


airtable = helpers.get_airtable_client(sequential_mode=True, provider_type='outscraper')

airtable.enrich_base_data(force_refresh=False)

print("Enrichment complete.")