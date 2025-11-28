import azure.functions as func
import azure.durable_functions as df

# Functions are defined inside blueprints to keep this file focused on app wiring.

# NOTE: To avoid excessive Airtable API calls and rate limiting, only call all_third_places
# in this get_all_third_places activity. Do NOT call all_third_places in per-place activities
# such as enrich_single_place or get_place_data. Always pass the required place data from the orchestrator.
# get_all_third_places is provided by the airtable blueprint

# Create the shared Durable Functions app
app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

# Import blueprints to register their functions with the app
from blueprints.admin import bp as admin_bp 
from blueprints.places import bp as places_bp
from blueprints.airtable import bp as airtable_bp 
from blueprints.photos import bp as photos_bp
from blueprints.cosmos import bp as cosmos_bp

# Register blueprints on the app (explicitly)
app.register_functions(admin_bp)
app.register_functions(places_bp)
app.register_functions(airtable_bp)
app.register_functions(photos_bp)
app.register_functions(cosmos_bp)
