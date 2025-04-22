import json
import logging
import datetime
import azure.functions as func
import helper_functions as helpers
import azure.durable_functions as df
import resource_manager as rm
from azure.durable_functions.models.DurableOrchestrationStatus import OrchestrationRuntimeStatus

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

# ======================================================
# Place Data Refresh Functions
# ======================================================

@app.function_name(name="RefreshPlaceData")
@app.route(route="refresh-place-data")
@app.durable_client_input(client_name="client")
async def refresh_place_data(req: func.HttpRequest, client) -> func.HttpResponse:
    """
    HTTP-triggered function that initiates the refresh of place data for all places.
    
    This function is exposed as a public endpoint at /api/refresh-place-data.
    It starts a new orchestration to retrieve and cache place data for all places
    in the Airtable base. Authorization is handled via the Azure Function key.

    Optional query parameters:
    - force_refresh: If "true", bypasses the cache and always fetches fresh data
    - sequential_mode: If "true", processes places sequentially rather than in parallel
    - city: City to use for caching (defaults to "charlotte")
    - provider_type: Type of data provider to use (REQUIRED: 'google' or 'outscraper')
    
    Returns:
        func.HttpResponse: A JSON response with the orchestration instance ID and status URL
    """
    logging.info("Received request for place data refresh.")

    try:
        # Initialize configuration from request
        rm.from_request(req)
        
        # Validate required parameter
        if not rm.get_config('provider_type'):
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Missing required parameter: provider_type",
                    "data": None,
                    "error": "The provider_type parameter is required"
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        logging.info(f"Starting place data refresh with parameters: force_refresh={rm.get_config('force_refresh', False)}, "
                    f"sequential_mode={rm.get_config('sequential_mode', False)}, city={rm.get_config('city', 'charlotte')}, "
                    f"provider_type={rm.get_config('provider_type')}")
        
        # Start the orchestrator with the parameters
        orchestration_input = {
            "force_refresh": rm.get_config('force_refresh', False),
            "sequential_mode": rm.get_config('sequential_mode', False),
            "city": rm.get_config('city', 'charlotte'),
            "provider_type": rm.get_config('provider_type')
        }
        
        instance_id = await client.start_new("get_place_data_orchestrator", client_input=orchestration_input)
        logging.info(f"Started orchestration with ID: {instance_id}")
        
        # Return a response with status check URL
        response = client.create_check_status_response(req, instance_id)
        return response
        
    except Exception as ex:
        logging.error(f"Error encountered while starting the place data refresh orchestration: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred while starting the place data refresh orchestration.",
                "data": None,
                "error": str(ex)
            }),
            status_code=500,
            mimetype="application/json"
        )

@app.orchestration_trigger(context_name="context")
def get_place_data_orchestrator(context: df.DurableOrchestrationContext):
    """
    Orchestrator function that coordinates retrieving place data for all places in Airtable.
    
    This orchestrator manages the execution of the place data retrieval process with controlled concurrency.
    It retrieves all third places from Airtable and then schedules activity functions
    to fetch data for each place. It tracks the overall status of the operation.
    
    Args:
        context (df.DurableOrchestrationContext): The durable orchestration context
        
    Returns:
        dict: Results of all place data retrieval operations
    """
    try:
        logging.info("get_place_data_orchestrator started.")
        
        # Get input parameters
        orchestration_input = context.get_input() or {}
        force_refresh = orchestration_input.get("force_refresh", False)
        sequential_mode = orchestration_input.get("sequential_mode", False)
        city = orchestration_input.get("city", "charlotte")
        provider_type = orchestration_input.get("provider_type", None)

        # Initialize the global config dictionary
        config_dict = {
            "provider_type": provider_type,
            "sequential_mode": sequential_mode,
            "city": city,
            "force_refresh": force_refresh
        }
        
        # Get all third places using the config dictionary
        all_third_places = yield context.call_activity(
            'get_all_third_places', 
            {"config": config_dict}
        )

        # Set up the processing tasks
        tasks = []
        results = []
        
        # If sequential_mode mode requested, process one place at a time
        if sequential_mode:
            logging.info(f"Running place data retrieval in sequential_mode mode for {len(all_third_places)} places")
            for place in all_third_places:
                activity_input = {
                    "place": place,
                    "config": config_dict,
                    "orchestration_input": orchestration_input  # Include original input for fallback
                }
                # Process each place sequentially
                result = yield context.call_activity("get_place_data", activity_input)
                results.append(result)
        else:
            # Process places in parallel with controlled concurrency
            from constants import MAX_THREAD_WORKERS
            # Use a smaller concurrency limit than MAX_THREAD_WORKERS to avoid rate limits
            concurrency_limit = MAX_THREAD_WORKERS
            
            logging.info(f"Running place data retrieval in parallel mode with concurrency={MAX_THREAD_WORKERS} for {len(all_third_places)} places")
            
            # Process places in batches based on the concurrency limit
            for i in range(0, len(all_third_places), concurrency_limit):
                batch = all_third_places[i:i+concurrency_limit]
                batch_tasks = []
                
                for place in batch:
                    activity_input = {
                        "place": place,
                        "config": config_dict,
                        "orchestration_input": orchestration_input  # Include original input for fallback
                    }
                    batch_tasks.append(context.call_activity("get_place_data", activity_input))
                
                # Wait for this batch to complete before processing the next batch
                batch_results = yield context.task_all(batch_tasks)
                results.extend(batch_results)

        # Determine overall success
        all_successful = all(result['status'] != 'failed' for result in results)

        result = {
            "success": all_successful,
            "message": "Place data refresh processed successfully." if all_successful else "Some place data refreshes failed.",
            "data": {
                "total_places_processed": len(all_third_places),
                "places_results": results
            },
            "error": None if all_successful else "One or more place data refreshes failed."
        }
        
        logging.info(f"get_place_data_orchestrator completed. Processed {len(all_third_places)} places.")

        custom_status = 'Succeeded' if all_successful else 'Failed'
        context.set_custom_status(custom_status)

        return result
    except Exception as ex:
        logging.error(f"Critical error in get_place_data_orchestrator: {ex}", exc_info=True)
        error_response = {
            "success": False,
            "message": "Error occurred during the place data refresh orchestration.",
            "data": None,
            "error": str(ex)
        }
        context.set_custom_status('Failed')
        return error_response

@app.activity_trigger(input_name="activityInput")
@app.function_name("get_place_data")  # Add explicit function name registration
def get_place_data(activityInput):
    """
    Activity function that retrieves data for a single place.
    
    This function uses the resource_manager module to efficiently share resources
    across multiple invocations, ensuring we reuse clients and providers rather than
    creating new ones for each place.
    
    Args:
        activityInput: A dictionary containing place information and configuration
    
    Returns:
        dict: The result of the place data retrieval operation
    """
    try:
        # Extract inputs
        place = activityInput.get("place")
        config_dict = activityInput.get("config", {})
        
        # Extract place details early to help with error reporting
        place_name = place['fields']['Place'] if place and 'fields' in place and 'Place' in place['fields'] else "Unknown Place"
        
        # Log the config data we're receiving to help diagnose issues
        provider_type = config_dict.get('provider_type')
        logging.info(f"get_place_data: Received config for {place_name} with provider_type={provider_type}")
        
        # Initialize the resource manager with the config
        rm.from_dict(config_dict)
        
        # Validate that required provider_type is present
        if not rm.get_config('provider_type'):
            error_msg = f"Missing provider_type for place '{place_name}'. Attempting to use orchestrator's provider_type."
            logging.warning(error_msg)
            
            # Try to get the provider_type from the original orchestration input if possible
            orchestration_input = activityInput.get("orchestration_input", {})
            provider_type = orchestration_input.get("provider_type")
            
            if provider_type:
                logging.info(f"Recovered provider_type={provider_type} from orchestration input for {place_name}")
                rm.set_config('provider_type', provider_type)
            else:
                # If we still don't have a provider_type, we need to fail
                error_msg = f"Error processing place data: provider_type cannot be None - must be 'google' or 'outscraper'"
                logging.error(error_msg)
                return helpers.create_place_response('failed', place_name, None, error_msg)
        
        # Now extract place details
        record_id = place['id']
        place_id = place['fields'].get('Google Maps Place Id', None)
        
        provider_type = rm.get_config('provider_type')
        city = rm.get_config('city', 'charlotte')
        force_refresh = rm.get_config('force_refresh', False)
        
        logging.info(f"get_place_data: Processing {place_name} with place_id {place_id} using provider_type={provider_type}")
        
        # Call helper function to get and cache place data - using module-level functions from resource manager
        status, place_data, message = helpers.get_and_cache_place_data(
            provider_type=provider_type,
            place_name=place_name,
            place_id=place_id,
            city=city,
            force_refresh=force_refresh
        )
        
        # Update Airtable record to indicate data file exists if succeeded/cached
        if status == 'succeeded' or status == 'cached':
            record_id = place['id']
            # Get AirtableClient from resource manager 
            airtable_client = rm.get_airtable_client(provider_type)
            airtable_client.update_place_record(record_id, 'Has Data File', 'Yes', overwrite=True)

        # Format the response for the orchestrator
        if status == 'succeeded' or status == 'cached':
            place_id = place_data.get('place_id', place_id)
            github_url = f'https://github.com/segunak/third-places-data/blob/master/data/places/{city}/{place_id}.json'
            return helpers.create_place_response(status, place_name, github_url, message)
        else:
            return helpers.create_place_response(status, place_name, None, message)
    except Exception as ex:
        logging.error(f"Error getting data for place {place_name if 'place_name' in locals() else 'unknown'}: {ex}", exc_info=True)
        return helpers.create_place_response('failed', place_name if 'place_name' in locals() else 'unknown', None, f"Error processing place data: {str(ex)}")

# ======================================================
# Airtable Enrichment Functions
# ======================================================

@app.function_name(name="EnrichAirtableBase")
@app.route(route="enrich-airtable-base")
@app.durable_client_input(client_name="client")
async def enrich_airtable_base(req: func.HttpRequest, client) -> func.HttpResponse:
    """
    This function initiates the Airtable base enrichment orchestration.
    Authorization is handled via the Azure Function key.
    
    Optional query parameters:
    - force_refresh: If "true", bypasses the cache and always fetches fresh data
    - sequential_mode: If "true", processes places sequentially rather than in parallel
    - provider_type: The type of data provider to use (e.g., "google", "outscraper")
    
    Returns:
        func.HttpResponse: A JSON response with the orchestration instance ID and status URL
    """
    logging.info("Received request for Airtable base enrichment.")

    try:
        # Initialize configuration from request
        rm.from_request(req)
        
        # Validate required parameter
        if not rm.get_config('provider_type'):
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Missing required parameter: provider_type",
                    "data": None,
                    "error": "The provider_type parameter is required"
                }),
                status_code=400
            )
        
        logging.info(f"Starting enrichment with parameters: city={rm.get_config('city')}, force_refresh={rm.get_config('force_refresh', False)}, "
                     f"sequential_mode={rm.get_config('sequential_mode', False)}, provider_type={rm.get_config('provider_type')}")
        
        # Start the orchestrator with the parameters
        orchestration_input = {
            "force_refresh": rm.get_config('force_refresh', False),
            "sequential_mode": rm.get_config('sequential_mode', False),
            "provider_type": rm.get_config('provider_type'),
            "city": rm.get_config('city', 'charlotte')
        }
        
        instance_id = await client.start_new("enrich_airtable_base_orchestrator", client_input=orchestration_input)
        logging.info(f"Started orchestration with ID: {instance_id}")
        
        # Return a response with status check URL
        response = client.create_check_status_response(req, instance_id)
        return response
        
    except Exception as ex:
        logging.error(f"Error encountered while starting the enrichment orchestration: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred while starting the enrichment orchestration.",
                "data": None,
                "error": str(ex)
            }),
            status_code=500
        )

@app.orchestration_trigger(context_name="context")
def enrich_airtable_base_orchestrator(context: df.DurableOrchestrationContext):
    """
    Orchestrator function for enriching Airtable base data.
    
    This orchestrator manages the enrichment of Airtable data by retrieving fresh data
    from the configured provider and updating Airtable fields. It can run in either
    sequential or parallel mode.
    
    Args:
        context: The durable orchestration context
        
    Returns:
        dict: Results of the enrichment operation
    """
    try:
        logging.info("enrich_airtable_base_orchestrator started.")
        
        # Get input parameters
        orchestration_input = context.get_input() or {}
        force_refresh = orchestration_input.get("force_refresh", False)
        sequential_mode = orchestration_input.get("sequential_mode", False)
        provider_type = orchestration_input.get("provider_type", None)
        city = orchestration_input.get("city", "charlotte")

        # Create config dictionary
        config_dict = {
            "provider_type": provider_type,
            "force_refresh": force_refresh,
            "sequential_mode": sequential_mode,
            "city": city
        }
        
        # Call the activity function to handle the enrichment using config dictionary
        enrichment_results = yield context.call_activity("enrich_airtable_batch", {
            "config": config_dict
        })
        
        # Filter to only include places that had at least one field updated
        actually_updated_places = [
            place for place in enrichment_results 
            if place and place.get('field_updates') and any(updates["updated"] for updates in place.get('field_updates', {}).values())
        ]
        
        result = {
            "success": True,
            "message": "Airtable base enrichment processed successfully.",
            "data": {
                "total_places_processed": len(enrichment_results),
                "total_places_enriched": len(actually_updated_places),
                "places_enriched": actually_updated_places
            },
            "error": None
        }
        
        logging.info(f"enrich_airtable_base_orchestrator completed. Updated {len(actually_updated_places)} of {len(enrichment_results)} places.")
        
        context.set_custom_status('Succeeded')
        return result
        
    except Exception as ex:
        logging.error(f"Critical error in enrich_airtable_base_orchestrator: {ex}", exc_info=True)
        error_response = {
            "success": False,
            "message": "Error occurred during the enrichment orchestration.",
            "data": None,
            "error": str(ex)
        }
        context.set_custom_status('Failed')
        return error_response

@app.activity_trigger(input_name="activityInput")
@app.function_name("enrich_airtable_batch")  # Add explicit function name registration
def enrich_airtable_batch(activityInput):
    """
    Activity function that handles enriching Airtable data.
    
    This function uses the resource_manager singleton pattern
    to efficiently share resources across multiple invocations.
    
    Args:
        activityInput: Dictionary with configuration
        
    Returns:
        list: Results of the enrichment operation for each place
    """
    try:
        # Extract inputs from the config dictionary
        config_dict = activityInput.get("config", {})
        
        # Initialize the resource manager with the config
        rm.from_dict(config_dict)
        
        # Extract the provider_type directly
        provider_type = rm.get_config('provider_type')
        sequential_mode = rm.get_config('sequential_mode', False)
        
        if not provider_type:
            logging.error("Cannot get AirtableClient - provider_type is not set")
            return []
        
        # Get the AirtableClient from the resource manager
        airtable_client = rm.get_airtable_client(provider_type, sequential_mode)
        
        # Call the enrichment method with the resource manager for accessing config values
        enrichment_results = airtable_client.enrich_base_data(rm)
        
        return enrichment_results
        
    except Exception as ex:
        logging.error(f"Error in enrich_airtable_batch: {ex}", exc_info=True)
        return []

# ======================================================
# Utility Functions
# ======================================================

@app.activity_trigger(input_name="activityInput")
@app.function_name("get_all_third_places")  # Add explicit function name registration
def get_all_third_places(activityInput):
    """
    Activity function that retrieves all third places from Airtable.
    
    This function uses the resource_manager singleton pattern.
    
    Args:
        activityInput: Dictionary with configuration
        
    Returns:
        list: All third places from Airtable
    """
    try:
        # Extract inputs from the config dictionary
        config_dict = activityInput.get("config", {})
        
        # Initialize the resource manager with the config
        rm.from_dict(config_dict)
        
        # Extract the provider_type directly
        provider_type = rm.get_config('provider_type')
        sequential_mode = rm.get_config('sequential_mode', False)
        
        if not provider_type:
            logging.error("Cannot get AirtableClient - provider_type is not set")
            return []
        
        # Get the AirtableClient from the resource manager
        airtable_client = rm.get_airtable_client(provider_type, sequential_mode)
        
        return airtable_client.all_third_places
        
    except Exception as ex:
        logging.error(f"Error in get_all_third_places: {ex}", exc_info=True)
        return []

@app.function_name(name="PurgeOrchestrations")
@app.route(route="purge-orchestrations")
@app.durable_client_input(client_name="client")
async def purge_orchestrations(req: func.HttpRequest, client) -> func.HttpResponse:
    """
    HTTP-triggered function that purges the history of all completed orchestration instances.
    
    This function is exposed as a public endpoint at /api/purge-orchestrations.
    It deletes the history of all orchestration instances that have completed, failed,
    or terminated, from the beginning of time until now. This is useful for cleaning up
    the storage associated with durable functions.
    
    Args:
        req (func.HttpRequest): The HTTP request object
        client: The durable functions client provided by the runtime
        
    Returns:
        func.HttpResponse: A JSON response indicating success or failure with the count of deleted instances
    """
    logging.info("Received request to purge orchestration instances.")

    try:
        # Purge the history of all orchestration instances ever
        runtime_statuses = [
            OrchestrationRuntimeStatus.Failed,
            OrchestrationRuntimeStatus.Completed,
            OrchestrationRuntimeStatus.Terminated,
        ]

        purge_result = await client.purge_instance_history_by(
            created_time_from=datetime.datetime(1900, 1, 1),
            created_time_to=datetime.datetime.now(datetime.timezone.utc),
            runtime_status=runtime_statuses
        )

        logging.info(f"Successfully purged orchestration instances. Instances deleted: {purge_result.instances_deleted}")

        # Return a JSON response with the number of instances deleted
        return func.HttpResponse(
            json.dumps({
                "message": "Purged orchestration instances.",
                "instancesDeleted": purge_result.instances_deleted
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as ex:
        logging.error(f"Error occurred while purging orchestrations: {str(ex)}", exc_info=True)
        # If the exception contains a response, log additional details
        if hasattr(ex, 'response') and ex.response is not None:
            logging.error(f"HTTP Status Code: {ex.response.status_code}")
            logging.error(f"Response Content: {ex.response.content.decode()}")
        return func.HttpResponse(
            json.dumps({
                "message": "Failed to purge orchestration instances.",
                "error": str(ex)
            }),
            status_code=500,
            mimetype="application/json"
        )

@app.function_name(name="SmokeTest")
@app.route(route="smoke-test")
def smoke_test(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered function to verify the Azure Function is operational.
    
    This function is exposed as a public endpoint at /api/smoke-test.
    It expects a JSON body with the property "House" set to "Martell" and returns a
    success message if the body is valid. This is a diagnostic endpoint to check
    if the Azure Function is running correctly.
    
    Args:
        req (func.HttpRequest): The HTTP request object
        
    Returns:
        func.HttpResponse: A JSON response indicating success or failure
    """
    logging.info("Received smoke test request.")

    try:
        req_body = req.get_json()
        logging.info(f"Request body: {req_body}")

        expected_key = "House"
        expected_value = "Martell"

        if req_body.get(expected_key, None) == expected_value:
            logging.info("Request body contains the correct allegiance.")
            return func.HttpResponse(
                json.dumps({"message": "The Azure Function is operational and recognizes Dorne. Unbowed. Unbent. Unbroken."}),
                status_code=200,
                mimetype="application/json"
            )
        else:
            logging.info(
                f"Incorrect allegiance provided. Expected {expected_value}, but got {req_body.get(expected_key, None)}")
            return func.HttpResponse(
                json.dumps({"message": "Unexpected or incorrect allegiance provided."}),
                status_code=400,
                mimetype="application/json"
            )

    except Exception as ex:
        logging.error(f"Failed to parse request body as JSON. {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"message": "Invalid or missing JSON body. Are you sure you should be hitting this endpoint?"}),
                status_code=400,
                mimetype="application/json"
        )

@app.function_name(name="RefreshAirtableOperationalStatuses")
@app.route(route="refresh-airtable-operational-statuses")
def refresh_airtable_operational_statuses(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered function to refresh the operational statuses of all places in Airtable.
    
    This function uses the resource_manager singleton pattern.
    
    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response containing the operation results.
    """
    logging.info("Received request to refresh Airtable operational statuses.")

    try:
        # Initialize configuration from request
        rm.from_request(req)
        
        # Validate required parameter
        provider_type = rm.get_config('provider_type')
        sequential_mode = rm.get_config('sequential_mode', False)
        
        if not provider_type:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Missing required parameter: provider_type",
                    "data": None,
                    "error": "The provider_type parameter is required"
                }),
                status_code=400,
                mimetype="application/json"
            )
            
        logging.info(f"Provider type: {provider_type}")
        if sequential_mode:
            logging.info("Using sequential mode for operational status refresh")
        else:
            logging.info("Using parallel mode for operational status refresh")
            
        # Use resource manager to get AirtableClient
        airtable_client = rm.get_airtable_client(provider_type, sequential_mode)
        logging.info("AirtableClient instance retrieved from resource manager, starting to refresh operational statuses.")

        # Get the data provider from resource manager to pass to refresh_operational_statuses
        data_provider = rm.get_data_provider(provider_type)
        
        # Use the new method that takes a data provider directly
        results = airtable_client.refresh_operational_statuses(data_provider)
        logging.info("Operational statuses refreshed, processing results.")

        failed_updates = [res for res in results if res.get('update_status') == 'failed']

        if failed_updates:
            logging.error(f"Operational status updates failed for {len(failed_updates)} places.")
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "One or more operational status updates failed.",
                    "data": failed_updates,
                    "error": None
                }),
                status_code=500,
                mimetype="application/json"
            )
        else:
            logging.info("Operational statuses refreshed successfully for all places.")
            return func.HttpResponse(
                json.dumps({
                    "success": True,
                    "message": "Operational statuses refreshed successfully.",
                    "data": results,
                    "error": None
                }),
                status_code=200,
                mimetype="application/json"
            )
    except Exception as ex:
        logging.error(f"Error encountered during the refresh operation: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred during the refresh operation.",
                "data": None,
                "error": str(ex)
            }),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="place_lookup")
def place_lookup(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP Trigger that looks up place details from either Google Maps or Outscraper
    
    This endpoint accepts a query parameter called place_id which contains the Google Maps Place ID for the location.
    It will return cached data if available, unless force_refresh=true is specified in the query string.
    
    Example: place_lookup?place_id=ChIJO4rApCM0VIgR5e-aCNr_zps&force_refresh=true
    """

    place_id = req.params.get('place_id', '')
    if not place_id:
        try:
            req_body = req.get_json()
        except ValueError:
            req_body = {}
        place_id = req_body.get('place_id', '')

    if not place_id:
        return func.HttpResponse(
             "Please provide a place_id parameter.",
             status_code=400
        )

    # Get request parameters
    provider_type = req.params.get('provider_type', 'outscraper')
    force_refresh = req.params.get('force_refresh', 'false').lower() == 'true'

    # Configure the resource manager directly
    rm.set_config('provider_type', provider_type)
    rm.set_config('force_refresh', force_refresh)
    rm.set_config('city', 'charlotte')
    
    # Create a place data provider
    from place_data_providers import PlaceDataProviderFactory
    provider = PlaceDataProviderFactory.get_provider(provider_type)

    # Attempt to get place data (first check cache unless force_refresh=true)
    status, place_data, message = helpers.get_and_cache_place_data(
        provider_type=provider_type,
        place_id=place_id,
        force_refresh=force_refresh,
        city='charlotte'
    )

    if status == 'success':
        return func.HttpResponse(
            json.dumps(place_data),
            mimetype="application/json",
            status_code=200
        )
    else:
        return func.HttpResponse(
            message,
            status_code=404
        )
