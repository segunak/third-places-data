import json
import logging
import datetime
import azure.functions as func
import helper_functions as helpers
import azure.durable_functions as df
from place_data_providers import PlaceDataProviderFactory
from airtable_client import AirtableClient
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
        # Parse parameters directly from request
        provider_type = req.params.get('provider_type')
        force_refresh = req.params.get('force_refresh', '').lower() == 'true'
        sequential_mode = req.params.get('sequential_mode', '').lower() == 'true'
        city = req.params.get('city')
        
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
        
        if not city:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Missing required parameter: city",
                    "data": None,
                    "error": "The city parameter is required"
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        logging.info(f"Starting place data refresh with parameters: force_refresh={force_refresh}, "
                    f"sequential_mode={sequential_mode}, city={city}, provider_type={provider_type}")
        
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
        city = orchestration_input.get("city")
        provider_type = orchestration_input.get("provider_type", None)

        if not city:
            raise ValueError("Missing required parameter: city")

        if not provider_type:
            raise ValueError("Missing required parameter: provider_type")

        config_dict = {
            "provider_type": provider_type,
            "sequential_mode": sequential_mode,
            "city": city,
            "force_refresh": force_refresh
        }

        all_third_places = yield context.call_activity(
            'get_all_third_places', 
            {"config": config_dict}
        )

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
                        "config": config_dict
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
@app.function_name("get_place_data")
def get_place_data(activityInput):
    """
    Activity function that retrieves data for a single place.
    
    This function uses a stateless, explicit resource creation system.
    
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
        
        provider_type = config_dict.get('provider_type')

        if not provider_type:
            error_msg = f"Error processing place data: provider_type cannot be None - must be 'google' or 'outscraper'"
            logging.error(error_msg)
            return helpers.create_place_response('failed', place_name, None, error_msg)
        


        # Now extract place details
        record_id = place['id']
        place_id = place['fields'].get('Google Maps Place Id', None)
        
        city = config_dict.get('city')

        if not city:
            error_msg = f"Error processing place data: city cannot be None. It is a required parameter."
            logging.error(error_msg)
            return helpers.create_place_response('failed', place_name, None, error_msg)
        
        force_refresh = config_dict.get('force_refresh', False)
        logging.info(f"get_place_data: Processing {place_name} with place_id {place_id} using provider_type={provider_type}")
        
        # Call helper function to get and cache place data
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
            airtable_client = AirtableClient(provider_type)
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
    - insufficient_only: If "true", only processes records from the "Insufficient" view
    
    Returns:
        func.HttpResponse: A JSON response with the orchestration instance ID and status URL
    """
    logging.info("Received request for Airtable base enrichment.")

    try:
        # Parse parameters directly from request
        provider_type = req.params.get('provider_type')
        force_refresh = req.params.get('force_refresh', '').lower() == 'true'
        sequential_mode = req.params.get('sequential_mode', '').lower() == 'true'
        city = req.params.get('city')
        insufficient_only = req.params.get('insufficient_only', '').lower() == 'true'
        
        logging.info(f"Starting enrichment with parameters: city={city}, force_refresh={force_refresh}, "
                     f"sequential_mode={sequential_mode}, provider_type={provider_type}, "
                     f"insufficient_only={insufficient_only}")
        
        config_dict = {
            "force_refresh": force_refresh,
            "sequential_mode": sequential_mode,
            "provider_type": provider_type,
            "city": city,
            "insufficient_only": insufficient_only
        }
        
        instance_id = await client.start_new("enrich_airtable_base_orchestrator", client_input=config_dict)
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
    Orchestrator function for enriching Airtable base data. Schedules one activity function per place.
    """
    try:
        logging.info("enrich_airtable_base_orchestrator started.")
        config_dict = context.get_input() or {}
        force_refresh = config_dict.get("force_refresh", False)
        sequential_mode = config_dict.get("sequential_mode", False)
        provider_type = config_dict.get("provider_type", None)
        city = config_dict.get("city")
        insufficient_only = config_dict.get("insufficient_only", False)

        if not city:
            raise ValueError("Missing required parameter: city")

        # Get all third places to enrich
        all_third_places = yield context.call_activity(
            'get_all_third_places',
            {"config": config_dict}
        )

        if insufficient_only and not all_third_places:
            logging.info("No records were found in the 'Insufficient' view to enrich.")
            result = {
                "success": True,
                "message": "No records were found in the 'Insufficient' view to enrich.",
                "data": {
                    "total_places_processed": 0,
                    "total_places_enriched": 0,
                    "places_enriched": []
                },
                "error": None
            }
            context.set_custom_status('Succeeded')
            return result
        # Schedule enrichment activities per place
        results = []
        from constants import MAX_THREAD_WORKERS
        concurrency_limit = MAX_THREAD_WORKERS

        if sequential_mode:
            logging.info(f"Running enrichment in SEQUENTIAL mode for {len(all_third_places)} places")
            for place in all_third_places:
                activity_input = {
                    "place": place,
                    "provider_type": provider_type,
                    "city": city,
                    "force_refresh": force_refresh,
                    "sequential_mode": sequential_mode,
                    "insufficient_only": insufficient_only
                }
                result = yield context.call_activity("enrich_single_place", activity_input)
                results.append(result)
        else:
            logging.info(f"Running enrichment in PARALLEL mode with concurrency={concurrency_limit} for {len(all_third_places)} places")
            for i in range(0, len(all_third_places), concurrency_limit):
                batch = all_third_places[i:i+concurrency_limit]
                batch_tasks = []
                for place in batch:
                    activity_input = {
                        "place": place,
                        "provider_type": provider_type,
                        "city": city,
                        "force_refresh": force_refresh,
                        "sequential_mode": sequential_mode,
                        "insufficient_only": insufficient_only
                    }
                    batch_tasks.append(context.call_activity("enrich_single_place", activity_input))
                batch_results = yield context.task_all(batch_tasks)
                results.extend(batch_results)
        # Filter to only include places that had at least one field updated
        actually_updated_places = [
            place for place in results
            if place and place.get('field_updates') and any(updates["updated"] for updates in place.get('field_updates', {}).values())
        ]
        message = "Airtable base enrichment processed successfully."
        if insufficient_only:
            message = f"Airtable base enrichment processed {len(results)} records from 'Insufficient' view."
        result = {
            "success": True,
            "message": message,
            "data": {
                "total_places_processed": len(results),
                "total_places_enriched": len(actually_updated_places),
                "places_enriched": actually_updated_places
            },
            "error": None
        }
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
@app.function_name("enrich_single_place")
def enrich_single_place(activityInput):
    """
    Activity function that enriches a single Airtable place record.
    """
    try:
        place = activityInput.get("place")
        provider_type = activityInput.get("provider_type")
        city = activityInput.get("city")
        force_refresh = activityInput.get("force_refresh", False)
        sequential_mode = activityInput.get("sequential_mode", False)
        insufficient_only = activityInput.get("insufficient_only", False)
        place_name = place['fields']['Place'] if place and 'fields' in place and 'Place' in place['fields'] else "Unknown Place"

        if not provider_type or not city:
            return {
                "place_name": place_name,
                "status": "failed",
                "message": "Missing required parameter: provider_type or city",
                "field_updates": {}
            }

        airtable_client = AirtableClient(provider_type, sequential_mode, insufficient_only)
        result = airtable_client.enrich_single_place(place, provider_type, city, force_refresh)
        return result
    except Exception as ex:
        logging.error(f"Error enriching place {place_name}: {ex}", exc_info=True)
        return {
            "place_name": place_name,
            "status": "failed",
            "message": f"Error processing enrichment: {str(ex)}",
            "field_updates": {}
        }

# NOTE: To avoid excessive Airtable API calls and rate limiting, only call all_third_places
# in this get_all_third_places activity. Do NOT call all_third_places in per-place activities
# such as enrich_single_place or get_place_data. Always pass the required place data from the orchestrator.
@app.activity_trigger(input_name="activityInput")
@app.function_name("get_all_third_places")
def get_all_third_places(activityInput):
    """
    Activity function that retrieves all third places from Airtable.
    
    This function uses a stateless, explicit resource creation system.
    
    Args:
        activityInput: Dictionary with configuration
        
    Returns:
        list: All third places from Airtable
    """
    try:
        # Extract inputs from the config dictionary
        config_dict = activityInput.get("config", {})
        
        # Extract the provider_type directly
        provider_type = config_dict.get('provider_type')
        sequential_mode = config_dict.get('sequential_mode', False)
        city = config_dict.get('city')
        insufficient_only = config_dict.get('insufficient_only', False)

        if not city:
            logging.error("Cannot get AirtableClient - city is not set")
            return []

        if not provider_type:
            logging.error("Cannot get AirtableClient - provider_type is not set")
            return []

        airtable_client = AirtableClient(provider_type, sequential_mode, insufficient_only)
        
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
@app.durable_client_input(client_name="client")
async def refresh_airtable_operational_statuses(req: func.HttpRequest, client) -> func.HttpResponse:
    """
    HTTP-triggered function to refresh the operational statuses of all places in Airtable.
    """
    logging.info("Received request to refresh Airtable operational statuses.")

    try:
        provider_type = req.params.get('provider_type')
        sequential_mode = req.params.get('sequential_mode', '').lower() == 'true'
        city = req.params.get('city')

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
        if not city:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Missing required parameter: city",
                    "data": None,
                    "error": "The city parameter is required"
                }),
                status_code=400,
                mimetype="application/json"
            )

        config_dict = {
            "provider_type": provider_type,
            "sequential_mode": sequential_mode,
            "city": city
        }
        instance_id = await client.start_new("refresh_airtable_operational_statuses_orchestrator", client_input=config_dict)
        logging.info(f"Started orchestration with ID: {instance_id}")
        response = client.create_check_status_response(req, instance_id)
        return response
    except Exception as ex:
        logging.error(f"Error encountered while starting the operational status refresh orchestration: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred while starting the operational status refresh orchestration.",
                "data": None,
                "error": str(ex)
            }),
            status_code=500,
            mimetype="application/json"
        )

@app.orchestration_trigger(context_name="context")
def refresh_airtable_operational_statuses_orchestrator(context: df.DurableOrchestrationContext):
    """
    Orchestrator function for refreshing operational statuses. Schedules one activity per place.
    """
    try:
        logging.info("refresh_airtable_operational_statuses_orchestrator started.")
        config_dict = context.get_input() or {}
        sequential_mode = config_dict.get("sequential_mode", False)
        provider_type = config_dict.get("provider_type", None)
        city = config_dict.get("city")
        if not city:
            raise ValueError("Missing required parameter: city")
        if not provider_type:
            raise ValueError("Missing required parameter: provider_type")
        all_third_places = yield context.call_activity(
            'get_all_third_places',
            {"config": config_dict}
        )
        results = []
        from constants import MAX_THREAD_WORKERS
        concurrency_limit = MAX_THREAD_WORKERS
        if sequential_mode:
            logging.info(f"Running operational status refresh in SEQUENTIAL mode for {len(all_third_places)} places")
            for place in all_third_places:
                activity_input = {
                    "place": place,
                    "provider_type": provider_type,
                    "city": city
                }
                result = yield context.call_activity("refresh_single_operational_status", activity_input)
                results.append(result)
        else:
            logging.info(f"Running operational status refresh in PARALLEL mode with concurrency={concurrency_limit} for {len(all_third_places)} places")
            for i in range(0, len(all_third_places), concurrency_limit):
                batch = all_third_places[i:i+concurrency_limit]
                batch_tasks = []
                for place in batch:
                    activity_input = {
                        "place": place,
                        "provider_type": provider_type,
                        "city": city
                    }
                    batch_tasks.append(context.call_activity("refresh_single_operational_status", activity_input))
                batch_results = yield context.task_all(batch_tasks)
                results.extend(batch_results)
        failed_updates = [res for res in results if res.get('update_status') == 'failed']
        result = {
            "success": len(failed_updates) == 0,
            "message": "Operational statuses refreshed successfully." if not failed_updates else "One or more operational status updates failed.",
            "data": results,
            "error": None if not failed_updates else f"{len(failed_updates)} failed updates"
        }
        context.set_custom_status('Succeeded' if not failed_updates else 'Failed')
        return result
    except Exception as ex:
        logging.error(f"Critical error in refresh_airtable_operational_statuses_orchestrator: {ex}", exc_info=True)
        error_response = {
            "success": False,
            "message": "Error occurred during the operational status refresh orchestration.",
            "data": None,
            "error": str(ex)
        }
        context.set_custom_status('Failed')
        return error_response

@app.activity_trigger(input_name="activityInput")
@app.function_name("refresh_single_operational_status")
def refresh_single_operational_status(activityInput):
    """
    Activity function that refreshes the operational status for a single place.
    """
    try:
        place = activityInput.get("place")
        provider_type = activityInput.get("provider_type")
        city = activityInput.get("city")
        place_name = place['fields']['Place'] if place and 'fields' in place and 'Place' in place['fields'] else "Unknown Place"
        if not provider_type or not city:
            return {
                "place_name": place_name,
                "update_status": "failed",
                "message": "Missing required parameter: provider_type or city"
            }
        airtable_client = AirtableClient(provider_type)
        data_provider = PlaceDataProviderFactory.get_provider(provider_type)
        result = airtable_client.refresh_single_operational_status(place, data_provider)
        return result
    except Exception as ex:
        logging.error(f"Error refreshing operational status for {place_name}: {ex}", exc_info=True)
        return {
            "place_name": place_name,
            "update_status": "failed",
            "message": f"Error processing operational status refresh: {str(ex)}"
        }

@app.function_name(name="RefreshAllPhotos")
@app.route(route="refresh-all-photos")
def refresh_all_photos(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered function for photo refresh across all places from cached data.
    
    This function refreshes photos using cached data files (not live APIs) that:
    1. Gets all places from Airtable
    2. Reads cached data files from GitHub for each place
    3. Extracts photos from existing photos.raw_data in cache
    4. Applies the photo selection algorithm with 30-photo limit
    5. Updates Airtable "Photos" field with overwrite=True
    6. Updates only the photos section in data files (preserves other data)
    
    Query Parameters:
    - provider_type: Provider to use for photo selection algorithm (REQUIRED: 'google' or 'outscraper')
    - city: City to process (default: 'charlotte')
    - dry_run: If 'true', only logs what would be done without making changes (default: 'true')
    - max_places: Maximum number of places to process (optional)
    
    Authentication: Requires the Azure Function key in the x-functions-key header.
    
    Returns:
        func.HttpResponse: JSON response with processing results
    """
    logging.info("Received request for administrative photo refresh.")
    
    try:
        # Parse parameters
        provider_type = req.params.get('provider_type')
        city = req.params.get('city', 'charlotte')
        dry_run = req.params.get('dry_run', 'true').lower() == 'true'
        max_places_param = req.params.get('max_places')
        
        # Validate required parameters
        if not provider_type:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Missing required parameter: provider_type",
                    "data": None,
                    "error": "The provider_type parameter is required ('google' or 'outscraper')"
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        if provider_type not in ['google', 'outscraper']:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Invalid provider_type",
                    "data": None,
                    "error": "provider_type must be 'google' or 'outscraper'"
                }),
                status_code=400,
                mimetype="application/json"
            )
        
        # Parse max_places if provided
        max_places = None
        if max_places_param:
            try:
                max_places = int(max_places_param)
                if max_places <= 0:
                    return func.HttpResponse(
                        json.dumps({
                            "success": False,
                            "message": "Invalid max_places value",
                            "data": None,
                            "error": "max_places must be a positive integer"
                        }),
                        status_code=400,
                        mimetype="application/json"
                    )
            except ValueError:
                return func.HttpResponse(
                    json.dumps({
                        "success": False,
                        "message": "Invalid max_places value",
                        "data": None,
                        "error": "max_places must be a valid integer"
                    }),
                    status_code=400,
                    mimetype="application/json"
                )
        
        logging.info(f"Starting administrative photo refresh with parameters: "
                    f"provider_type={provider_type}, city={city}, dry_run={dry_run}, max_places={max_places}")
        
        # Call the administrative function
        results = helpers.refresh_all_photos(
            provider_type=provider_type,
            city=city,
            dry_run=dry_run,
            max_places=max_places
        )
        
        # Determine HTTP status based on results
        http_status = 200
        if results.get("status") == "failed":
            http_status = 500
        elif results.get("errors", 0) > 0:
            http_status = 207  # Multi-status for partial success
        
        # Format response
        response_data = {
            "success": results.get("status") != "failed",
            "message": f"Photo refresh {'dry run ' if dry_run else ''}completed",
            "data": results,
            "error": None if results.get("status") != "failed" else results.get("message")
        }
        
        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=http_status,
            mimetype="application/json"
        )
        
    except Exception as ex:
        logging.error(f"Error in administrative photo refresh: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred during administrative photo refresh",
                "data": None,
                "error": str(ex)
            }),
            status_code=500,
            mimetype="application/json"
        )

