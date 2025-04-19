import json
import logging
import datetime
import azure.functions as func
import helper_functions as helpers
import azure.durable_functions as df
from airtable_client import AirtableClient
from place_data_providers import PlaceDataProviderFactory
from azure.durable_functions.models.DurableOrchestrationStatus import OrchestrationRuntimeStatus

app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.function_name(name="StartOrchestrator")
@app.route(route="orchestrators/{functionName}")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    """
    HTTP-triggered function that serves as the client and starts the orchestrator function. This is the entry point for the orchestration, and it's publicly accessible.
    """
    function_name = req.route_params.get('functionName')
    instance_id = await client.start_new(function_name)
    # This creates and sends a response that includes a URL to query the orchestration status
    response = client.create_check_status_response(req, instance_id)
    return response

@app.activity_trigger(input_name="activityInput")
def get_all_third_places(activityInput):
    airtable = helpers.get_airtable_client()
    return airtable.all_third_places

@app.function_name(name="PurgeOrchestrations")
@app.route(route="purge-orchestrations")
@app.durable_client_input(client_name="client")
async def purge_orchestrations(req: func.HttpRequest, client) -> func.HttpResponse:
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

@app.orchestration_trigger(context_name="context")
def get_place_data_orchestrator(context: df.DurableOrchestrationContext):
    try:
        logging.info("get_place_data_orchestrator started.")

        tasks = []
        activity_input = {}
        all_third_places = yield context.call_activity('get_all_third_places', {})

        for place in all_third_places:
            # Schedule activity functions for each place
            activity_input["place"] = place
            tasks.append(context.call_activity("get_place_data", activity_input))

        # Run all tasks in parallel
        results = yield context.task_all(tasks)
        logging.info("get_place_data_orchestrator completed.")

        # Determine overall success
        all_successful = all(result['status'] != 'failed' for result in results)
        custom_status = 'Succeeded' if all_successful else 'Failed'
        context.set_custom_status(custom_status)

        return results
    except Exception as ex:
        logging.error(f"Critical error in get_place_data_orchestrator processing: {ex}", exc_info=True)
        error_response = json.dumps({"error": str(ex)}, indent=4)
        context.set_custom_status('Failed')
        return error_response


@app.activity_trigger(input_name="activityInput")
def get_place_data(activityInput):
    """
    Retrieves all data for a place using the configured place data provider.
    Uses the shared get_and_cache_place_data function to implement a cache-first approach.
    """
    place = activityInput['place']
    place_name = place['fields']['Place']
    place_id = place['fields'].get('Google Maps Place Id', None)
    
    # Call the centralized helper function
    status, place_data, message = helpers.get_and_cache_place_data(place_name, place_id, 'charlotte')
    
    # If data was successfully retrieved, update Airtable record with "Has Data File" = "Yes"
    if (status == 'succeeded' or status == 'cached') and place_data:
        record_id = place['id']
        airtable = helpers.get_airtable_client()
        airtable.update_place_record(record_id, 'Has Data File', 'Yes', overwrite=True)
    
    # Format the response for the orchestrator
    if status == 'succeeded' or status == 'cached':
        place_id = place_data.get('place_id', place_id)
        github_url = f'https://github.com/segunak/third-places-data/blob/master/data/places/charlotte/{place_id}.json'
        return helpers.create_place_response(status, place_name, github_url, message)
    else:
        return helpers.create_place_response(status, place_name, None, message)

@app.function_name(name="SmokeTest")
@app.route(route="smoke-test")
def smoke_test(req: func.HttpRequest) -> func.HttpResponse:
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


@app.function_name(name="EnrichAirtableBase")
@app.route(route="enrich-airtable-base")
def enrich_airtable_base(req: func.HttpRequest) -> func.HttpResponse:
    """
    This function enriches the Airtable base data for all places.
    Authorization is handled via the Azure Function key.
    
    Returns:
        func.HttpResponse: A JSON response with the results of the operation
    """
    logging.info("Received request for Airtable base enrichment.")

    try:
        airtable = helpers.get_airtable_client()
        logging.info("AirtableClient instance retrieved, starting the base data enrichment process.")

        enriched_places = airtable.enrich_base_data()
        
        # Filter to only include places that had at least one field updated
        actually_updated_places = [
            {
                "place_name": place["place_name"],
                "place_id": place["place_id"],
                "record_id": place["record_id"],
                "field_updates": {
                    field: {
                        "old_value": updates["old_value"],
                        "new_value": updates["new_value"]
                    }
                    for field, updates in place.get('field_updates', {}).items() if updates["updated"]
                }
            }
            for place in enriched_places if any(updates["updated"] for updates in place.get('field_updates', {}).values())
        ]

        if actually_updated_places:
            logging.info(f"Enrichment process completed successfully. The following places had at least one field updated: {actually_updated_places}")
        else:
            logging.info("Enrichment process completed successfully. No places required field updates.")

        return func.HttpResponse(
            json.dumps({
                "success": True,
                "message": "Airtable base enrichment processed successfully.",
                "data": {
                    "total_places_enriched": len(actually_updated_places),
                    "places_enriched": actually_updated_places
                },
                "error": None
            }),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as ex:
        logging.error(f"Error encountered during the enrichment process: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred during the enrichment process.",
                "data": None,
                "error": str(ex)
            }),
            status_code=500,
            mimetype="application/json"
        )


@app.function_name(name="RefreshAirtableOperationalStatuses")
@app.route(route="refresh-airtable-operational-statuses")
def refresh_airtable_operational_statuses(req: func.HttpRequest) -> func.HttpResponse:
    """
    This function calls `airtable.refresh_operational_statuses()`, which returns a detailed list of dictionaries with the status of each update.

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
        airtable = helpers.get_airtable_client()
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


@app.function_name(name="RefreshDataCache")
@app.route(route="refresh-data-cache")
def refresh_data_cache(req: func.HttpRequest) -> func.HttpResponse:
    """
    This function refreshes the cached data for all places without doing full Airtable field enrichment.
    It only updates the 'Has Data File' field in Airtable to mark places that have data files.
    
    This endpoint is useful for:
    1. Pre-warming the cache before running enrichment
    2. Ensuring all place data is up-to-date
    3. Filling in missing data files
    
    Authorization is handled via the Azure Function key.
    
    Returns:
        func.HttpResponse: A JSON response with the results of the operation
    """
    logging.info("Received request to refresh all place data caches.")

    try:
        airtable = helpers.get_airtable_client()
        logging.info("AirtableClient instance retrieved, starting the data cache refresh process.")

        # Call the update_cache_data method to refresh the cache
        updated_places = airtable.update_cache_data()
        
        # Summarize the results by status
        summary = {}
        for place in updated_places:
            status = place.get('status', 'unknown')
            if status not in summary:
                summary[status] = 0
            summary[status] += 1
        
        # Log the summary
        for status, count in summary.items():
            logging.info(f"Places with status '{status}': {count}")

        return func.HttpResponse(
            json.dumps({
                "success": True,
                "message": "Data cache refresh completed successfully.",
                "data": updated_places,
                "error": None
            }),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as ex:
        logging.error(f"Error encountered during the data cache refresh process: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred during the data cache refresh process.",
                "data": None,
                "error": str(ex)
            }),
            status_code=500,
            mimetype="application/json"
        )
