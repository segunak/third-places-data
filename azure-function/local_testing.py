from airtable_client import AirtableClient

# A file for adhoc testing of Azure Function related code.
# This file is not part of the main codebase and is not used in production.

airtable = AirtableClient()

airtable.enrich_base_data()

print("Done")