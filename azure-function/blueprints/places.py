import json
import logging
import azure.functions as func
import azure.durable_functions as df
import helper_functions as helpers
from airtable_client import AirtableClient, SearchField

bp = df.Blueprint()


# ======================================================
# Place Data Refresh Functions
# ======================================================


@bp.function_name(name="RefreshPlaceData")
@bp.route(route="refresh-place-data")
@bp.durable_client_input(client_name="client")
async def refresh_place_data(req: func.HttpRequest, client) -> func.HttpResponse:
    logging.info("Received request for place data refresh.")

    try:
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

        orchestration_input = {
            "force_refresh": force_refresh,
            "sequential_mode": sequential_mode,
            "city": city,
            "provider_type": provider_type
        }

        instance_id = await client.start_new("get_place_data_orchestrator", client_input=orchestration_input)
        logging.info(f"Started orchestration with ID: {instance_id}")

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


@bp.orchestration_trigger(context_name="context")
def get_place_data_orchestrator(context: df.DurableOrchestrationContext):
    try:
        logging.info("get_place_data_orchestrator started.")

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
        if sequential_mode:
            logging.info(f"Running place data retrieval in sequential_mode mode for {len(all_third_places)} places")
            for place in all_third_places:
                activity_input = {
                    "place": place,
                    "config": config_dict,
                    "orchestration_input": orchestration_input
                }
                result = yield context.call_activity("get_place_data", activity_input)
                results.append(result)
        else:
            from constants import MAX_THREAD_WORKERS
            concurrency_limit = MAX_THREAD_WORKERS
            logging.info(f"Running place data retrieval in parallel mode with concurrency={MAX_THREAD_WORKERS} for {len(all_third_places)} places")

            for i in range(0, len(all_third_places), concurrency_limit):
                batch = all_third_places[i:i+concurrency_limit]
                batch_tasks = []

                for place in batch:
                    activity_input = {
                        "place": place,
                        "config": config_dict
                    }
                    batch_tasks.append(context.call_activity("get_place_data", activity_input))

                batch_results = yield context.task_all(batch_tasks)
                results.extend(batch_results)

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


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("get_place_data")
def get_place_data(activityInput):
    try:
        place = activityInput.get("place")
        config_dict = activityInput.get("config", {})

        place_name = place['fields']['Place'] if place and 'fields' in place and 'Place' in place['fields'] else "Unknown Place"

        provider_type = config_dict.get('provider_type')

        if not provider_type:
            error_msg = f"Error processing place data: provider_type cannot be None - must be 'google' or 'outscraper'"
            logging.error(error_msg)
            return helpers.create_place_response('failed', place_name, None, error_msg)

        record_id = place['id']
        place_id = place['fields'].get('Google Maps Place Id', None)

        city = config_dict.get('city')

        if not city:
            error_msg = f"Error processing place data: city cannot be None. It is a required parameter."
            logging.error(error_msg)
            return helpers.create_place_response('failed', place_name, None, error_msg)

        force_refresh = config_dict.get('force_refresh', False)
        logging.info(f"get_place_data: Processing {place_name} with place_id {place_id} using provider_type={provider_type}")

        status, place_data, message = helpers.get_and_cache_place_data(
            provider_type=provider_type,
            place_name=place_name,
            place_id=place_id,
            city=city,
            force_refresh=force_refresh
        )

        if status == 'succeeded' or status == 'cached':
            record_id = place['id']
            airtable_client = AirtableClient(provider_type)
            airtable_client.update_place_record(record_id, 'Has Data File', 'Yes', overwrite=True)

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
# Single Place Refresh Functions
# ======================================================


@bp.function_name(name="RefreshSinglePlace")
@bp.route(route="refresh-single-place")
@bp.durable_client_input(client_name="client")
async def refresh_single_place(req: func.HttpRequest, client) -> func.HttpResponse:
    logging.info("Received request to refresh single place.")

    try:
        place_id = req.params.get('place_id')
        provider_type = req.params.get('provider_type')
        city = req.params.get('city')

        if not place_id:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Missing required parameter: place_id",
                    "data": None,
                    "error": "The place_id parameter is required (Google Maps Place Id)"
                }),
                status_code=400,
                mimetype="application/json"
            )

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

        if not city:
            return func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Missing required parameter: city",
                    "data": None,
                    "error": "The city parameter is required for caching"
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

        logging.info(f"Starting single place refresh for place_id={place_id}, "
                     f"provider_type={provider_type}, city={city}")

        orchestration_input = {
            "place_id": place_id,
            "provider_type": provider_type,
            "city": city,
            "force_refresh": True
        }

        instance_id = await client.start_new("refresh_single_place_orchestrator", client_input=orchestration_input)
        logging.info(f"Started single place refresh orchestration with ID: {instance_id}")

        response = client.create_check_status_response(req, instance_id)
        return response

    except Exception as ex:
        logging.error(f"Error encountered while starting single place refresh: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred while starting single place refresh.",
                "data": None,
                "error": str(ex)
            }),
            status_code=500,
            mimetype="application/json"
        )


@bp.orchestration_trigger(context_name="context")
def refresh_single_place_orchestrator(context: df.DurableOrchestrationContext):
    try:
        logging.info("refresh_single_place_orchestrator started.")

        orchestration_input = context.get_input() or {}
        place_id = orchestration_input.get("place_id")
        provider_type = orchestration_input.get("provider_type")
        city = orchestration_input.get("city")

        if not place_id or not provider_type or not city:
            raise ValueError("Missing required parameters: place_id, provider_type, or city")

        logging.info(f"Processing single place refresh for place_id: {place_id}")

        place_record = yield context.call_activity(
            "find_place_by_id",
            {
                "place_id": place_id,
                "provider_type": provider_type
            }
        )

        if not place_record:
            logging.warning(f"Place with Google Maps Place Id '{place_id}' not found in Airtable")
            return {
                "success": False,
                "message": f"Place with Google Maps Place Id '{place_id}' not found in Airtable",
                "data": None,
                "error": "Place not found in Airtable database"
            }

        place_name = place_record.get('fields', {}).get('Place', 'Unknown')
        logging.info(f"Found place in Airtable: {place_name}")

        refresh_result = yield context.call_activity(
            "get_place_data",
            {
                "place": place_record,
                "config": {
                    "provider_type": provider_type,
                    "city": city,
                    "force_refresh": True
                }
            }
        )

        enrich_result = yield context.call_activity(
            "enrich_single_place",
            {
                "place": place_record,
                "provider_type": provider_type,
                "city": city,
                "force_refresh": False
            }
        )

        refresh_success = refresh_result.get('status') not in ['failed', 'error']
        enrich_success = enrich_result.get('status') not in ['failed', 'error']
        overall_success = refresh_success and enrich_success

        result = {
            "success": overall_success,
            "message": f"Single place refresh completed for '{refresh_result.get('place_name', place_name)}'",
            "data": {
                "place_id": place_id,
                "place_name": refresh_result.get('place_name', place_name),
                "refresh_status": refresh_result.get('status'),
                "refresh_message": refresh_result.get('message'),
                "enrich_status": enrich_result.get('status'),
                "enrich_message": enrich_result.get('message'),
                "field_updates": enrich_result.get('field_updates', {}),
                "provider_type": provider_type,
                "city": city
            },
            "error": None if overall_success else f"Refresh: {refresh_result.get('message', 'Unknown error')}; Enrich: {enrich_result.get('message', 'Unknown error')}"
        }
        context.set_custom_status('Succeeded' if overall_success else 'Failed')

        logging.info(f"Single place refresh orchestrator completed with status: {refresh_result.get('status')}")
        return result

    except Exception as ex:
        logging.error(f"Critical error in refresh_single_place_orchestrator: {ex}", exc_info=True)
        error_response = {
            "success": False,
            "message": "Error occurred during single place refresh.",
            "data": None,
            "error": str(ex)
        }
        context.set_custom_status('Failed')
        return error_response


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("find_place_by_id")
def find_place_by_id(activityInput):
    try:
        place_id = activityInput.get("place_id")
        provider_type = activityInput.get("provider_type")

        if not place_id or not provider_type:
            logging.error("Missing required parameters for find_place_by_id: place_id or provider_type")
            return None

        logging.info(f"Searching for place with Google Maps Place Id: {place_id}")

        airtable_client = AirtableClient(provider_type)
        record = airtable_client.get_record(SearchField.GOOGLE_MAPS_PLACE_ID, place_id)

        if record:
            place_name = record.get('fields', {}).get('Place', 'Unknown')
            logging.info(f"Successfully found place record: {place_name} (ID: {place_id})")
        else:
            logging.warning(f"No place record found with Google Maps Place Id: {place_id}")

        return record

    except Exception as ex:
        logging.error(f"Error finding place by ID {place_id}: {ex}", exc_info=True)
        return None
