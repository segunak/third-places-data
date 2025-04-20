import json
import logging
import datetime
import azure.functions as func
import helper_functions as helpers
import azure.durable_functions as df
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
    
    Returns:
        func.HttpResponse: A JSON response with the orchestration instance ID and status URL
    """
    logging.info("Received request for place data refresh.")

    try:
        # Extract optional parameters with defaults
        force_refresh = req.params.get('force_refresh', '').lower() == 'true' 
        sequential_mode = req.params.get('sequential_mode', '').lower() == 'true'
        city = req.params.get('city', 'charlotte').lower()
        provider_type = req.params.get('provider_type', None)
        
        logging.info(f"Starting place data refresh with parameters: force_refresh={force_refresh}, sequential_mode={sequential_mode}, city={city}, provider_type={provider_type}")
        
        # Start the orchestrator with the parameters
        orchestration_input = {
            "force_refresh": force_refresh,
            "sequential_mode": sequential_mode,
            "city": city,
            "provider_type": provider_type
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

        all_third_places = yield context.call_activity('get_all_third_places', {"sequential_mode": sequential_mode, "provider_type": provider_type})
        
        # Set up the processing tasks
        tasks = []
        results = []
        
        # If sequential_mode mode requested, process one place at a time
        if sequential_mode:
            logging.info(f"Running place data retrieval in sequential_mode mode for {len(all_third_places)} places")
            for place in all_third_places:
                activity_input = {
                    "place": place,
                    "force_refresh": force_refresh,
                    "city": city,
                    "provider_type": provider_type
                }
                # Process each place sequentially
                result = yield context.call_activity("get_place_data", activity_input)
                results.append(result)
        else:
            # Process places in parallel with controlled concurrency
            from constants import MAX_THREAD_WORKERS
            # Use a smaller concurrency limit than MAX_THREAD_WORKERS to avoid rate limits
            concurrency_limit = min(MAX_THREAD_WORKERS, 10)
            
            logging.info(f"Running place data retrieval in parallel mode with concurrency={concurrency_limit} for {len(all_third_places)} places")
            
            # Process places in batches based on the concurrency limit
            for i in range(0, len(all_third_places), concurrency_limit):
                batch = all_third_places[i:i+concurrency_limit]
                batch_tasks = []
                
                for place in batch:
                    activity_input = {
                        "place": place,
                        "force_refresh": force_refresh,
                        "city": city,
                        "provider_type": provider_type
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
def get_place_data(activityInput):
    """
    Activity function that retrieves all data for a specific place.
    
    This is an internal Azure Function activity triggered by orchestrator functions.
    It fetches detailed place data using the configured place data provider and 
    implements a cache-first approach via the get_and_cache_place_data helper function.
    If successful, it also updates the Airtable record to indicate that data is available.
    
    Args:
        activityInput (dict): Input parameters containing the place record from Airtable,
                              force_refresh flag, city, and sequential_mode flag
        
    Returns:
        dict: A response object indicating success/failure status, place name, and a link to the data file
    """
    place = activityInput['place']
    force_refresh = activityInput.get('force_refresh', False)
    city = activityInput.get('city', 'charlotte')
    sequential_mode = activityInput.get('sequential_mode', False)
    provider_type = activityInput.get('provider_type', None)
    
    place_name = place['fields']['Place']
    place_id = place['fields'].get('Google Maps Place Id', None)
    
    logging.info(f"Getting place data for: {place_name} (force_refresh={force_refresh}, city={city}, provider_type={provider_type})")
    
    try:
        # Call the centralized helper function with force_refresh parameter
        status, place_data, message = helpers.get_and_cache_place_data(
            place_name, 
            place_id, 
            city, 
            force_refresh=force_refresh,
            provider_type=provider_type
        )
        
        # If data was successfully retrieved, update Airtable record with "Has Data File" = "Yes"
        if (status == 'succeeded' or status == 'cached') and place_data:
            record_id = place['id']
            airtable = helpers.get_airtable_client(sequential_mode=sequential_mode)
            airtable.update_place_record(record_id, 'Has Data File', 'Yes', overwrite=True)

        # Format the response for the orchestrator
        if (status == 'succeeded' or status == 'cached'):
            place_id = place_data.get('place_id', place_id)
            github_url = f'https://github.com/segunak/third-places-data/blob/master/data/places/{city}/{place_id}.json'
            return helpers.create_place_response(status, place_name, github_url, message)
        else:
            return helpers.create_place_response(status, place_name, None, message)
    except Exception as ex:
        logging.error(f"Error getting data for place {place_name}: {ex}", exc_info=True)
        return helpers.create_place_response('failed', place_name, None, str(ex))

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
        # Extract optional parameters with defaults
        force_refresh = req.params.get('force_refresh', '').lower() == 'true' 
        sequential_mode = req.params.get('sequential_mode', '').lower() == 'true'
        provider_type = req.params.get('provider_type', None)
        
        if not provider_type:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Missing required parameter: provider_type",
                    "data": None,
                    "error": "The provider_type parameter is required"
                }),
                status_code=400
            )
        
        logging.info(f"Starting enrichment with parameters: force_refresh={force_refresh}, sequential_mode={sequential_mode}, provider_type={provider_type}")
        
        # Start the orchestrator with the parameters
        orchestration_input = {
            "force_refresh": force_refresh,
            "sequential_mode": sequential_mode,
            "provider_type": provider_type
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
    This orchestrator initiates the batch enrichment of all Airtable data.
    
    Args:
        context: The orchestration context
        
    Returns:
        A dictionary with the results of the enrichment operation
    """
    try:
        logging.info("enrich_airtable_base_orchestrator started.")  # Note: We use PascalCase in logs for consistency
        
        # Get input parameters
        orchestration_input = context.get_input() or {}
        sequential_mode = orchestration_input.get("sequential_mode", False)
        provider_type = orchestration_input.get("provider_type", None)
        force_refresh = orchestration_input.get("force_refresh", False)
        
        # Call the batch enrichment activity, which invokes enrich_base_data properly
        enrichment_results = yield context.call_activity("enrich_airtable_batch", {
            "sequential_mode": sequential_mode,
            "provider_type": provider_type,
            "force_refresh": force_refresh
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
def enrich_airtable_batch(activityInput):
    """
    Activity function to enrich all places in Airtable using enrich_base_data.
    
    This function is called by the enrich_airtable_base_orchestrator to process all places
    in a single batch operation using the AirtableClient.enrich_base_data method.
    
    Args:
        activityInput: A dictionary containing:
            - sequential_mode: Boolean indicating whether to use sequential processing mode
            - provider_type: The data provider type to use ('google' or 'outscraper')
            - force_refresh: Boolean indicating whether to force refresh cached data
    
    Returns:
        The result of the enrichment operation for all places
    """
    sequential_mode = activityInput.get('sequential_mode', False)
    provider_type = activityInput.get('provider_type', None)
    force_refresh = activityInput.get('force_refresh', False)
    
    logging.info(f"Enriching all places in batch mode (sequential_mode={sequential_mode}, provider_type={provider_type}, force_refresh={force_refresh})")
    
    try:
        # Get AirtableClient with appropriate sequential_mode and provider_type setting
        airtable = helpers.get_airtable_client(sequential_mode=sequential_mode, provider_type=provider_type)
        
        # Call the shared enrich_base_data method designed for batch processing
        return airtable.enrich_base_data(force_refresh=force_refresh)
    except Exception as ex:
        logging.error(f"Error in batch enrichment: {ex}", exc_info=True)
        return {
            "status": "failed",
            "error": str(ex)
        }

# ======================================================
# Utility Functions
# ======================================================

@app.activity_trigger(input_name="activityInput")
def get_all_third_places(activityInput):
    """
    Activity function that retrieves all third places from Airtable.
    
    This is an internal Azure Function activity triggered by orchestrator functions.
    It fetches all third places from the Airtable base using the AirtableClient.
    
    Args:
        activityInput (dict): Input parameters for the activity, including:
            - sequential_mode: Boolean indicating whether to use sequential processing mode
            - provider_type: The data provider type ('google' or 'outscraper')
        
    Returns:
        list: All third places records from Airtable
    """
    sequential_mode = activityInput.get('sequential_mode', False)
    provider_type = activityInput.get('provider_type', None)
    airtable = helpers.get_airtable_client(sequential_mode=sequential_mode, provider_type=provider_type)
    return airtable.all_third_places

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
    HTTP-triggered function that provides a simple health check endpoint.
    
    This function is exposed as a public endpoint at /api/smoke-test.
    It verifies that the Azure Function is operational by validating a specific 
    test payload in the request body. The function expects a JSON body with 
    the key "House" and value "Martell". This serves as both a health check
    and a simple authentication mechanism to ensure accidental calls don't 
    trigger the endpoint.
    
    Args:
        req (func.HttpRequest): The HTTP request object with JSON body
        
    Returns:
        func.HttpResponse: A JSON response indicating whether the function is operational
    """
    logging.info("Received request at SmokeTest endpoint.")

    try:
        req_body = req.get_json()
        logging.info(f"Request body parsed successfully: {req_body}")

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
    This function calls `airtable.refresh_operational_statuses()`, which returns a detailed list of dictionaries with the status of each update.

    Optional query parameters:
    - sequential_mode: If "true", processes places sequentially rather than in parallel

    The function returns:
    - 200 OK if the function call completes and there are no return values in the list of dicts where `update_status` is 'failed'.
    - If there's a return value in the list of dicts with `update_status` 'failed', then it returns 500 Internal Server Error and includes every single record that had a 'failed' status in the return value.
    - Else, if all return values are 'updated' or 'skipped', it returns 200 OK and returns the entire return value for the caller to parse if they want to.
    - If there's an exception or big error, it returns an HTTP status that clearly indicates failure to the caller.

    Note: Callers of this Azure Function don't need to provide any input; security is handled via the `x-functions-key` header.

    Args:
        req (func.HttpRequest): The HTTP request object.

    Returns:
        func.HttpResponse: The HTTP response containing the operation results.
    """
    logging.info("Received request to refresh Airtable operational statuses.")

    try:
        # Extract optional parameters with defaults
        sequential_mode = req.params.get('sequential_mode', '').lower() == 'true'
        provider_type = req.params.get('provider_type', None)
        
        if sequential_mode:
            logging.info("Using sequential mode for operational status refresh")
        else:
            logging.info("Using parallel mode for operational status refresh")
            
        airtable = helpers.get_airtable_client(sequential_mode=sequential_mode, provider_type=provider_type)
        logging.info("AirtableClient instance retrieved, starting to refresh operational statuses.")

        results = airtable.refresh_operational_statuses()
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
