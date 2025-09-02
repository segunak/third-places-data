import json
import logging
import azure.functions as func
import azure.durable_functions as df
from airtable_client import AirtableClient

bp = df.Blueprint()


# ======================================================
# Airtable Enrichment Functions
# ======================================================


@bp.function_name(name="EnrichAirtableBase")
@bp.route(route="enrich-airtable-base")
@bp.durable_client_input(client_name="client")
async def enrich_airtable_base(req: func.HttpRequest, client) -> func.HttpResponse:
    logging.info("Received request for Airtable base enrichment.")

    try:
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


@bp.orchestration_trigger(context_name="context")
def enrich_airtable_base_orchestrator(context: df.DurableOrchestrationContext):
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


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("enrich_single_place")
def enrich_single_place(activityInput):
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


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("get_all_third_places")
def get_all_third_places(activityInput):
    try:
        config_dict = activityInput.get("config", {})
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


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("refresh_single_place_operational_status")
def refresh_single_place_operational_status(activityInput):
    try:
        from place_data_providers import PlaceDataProviderFactory
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
        result = airtable_client.refresh_single_place_operational_status(place, data_provider)
        return result
    except Exception as ex:
        logging.error(f"Error refreshing operational status for {place_name}: {ex}", exc_info=True)
        return {
            "place_name": place_name,
            "update_status": "failed",
            "message": f"Error processing operational status refresh: {str(ex)}"
        }


@bp.function_name(name="RefreshAirtableOperationalStatuses")
@bp.route(route="refresh-airtable-operational-statuses")
@bp.durable_client_input(client_name="client")
async def refresh_airtable_operational_statuses(req: func.HttpRequest, client) -> func.HttpResponse:
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


@bp.orchestration_trigger(context_name="context")
def refresh_airtable_operational_statuses_orchestrator(context: df.DurableOrchestrationContext):
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
                result = yield context.call_activity("refresh_single_place_operational_status", activity_input)
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
                    batch_tasks.append(context.call_activity("refresh_single_place_operational_status", activity_input))

                batch_results = yield context.task_all(batch_tasks)
                total_batches = (len(all_third_places) + concurrency_limit - 1) // concurrency_limit
                current_batch = i // concurrency_limit + 1
                logging.info(f"Processed batch {current_batch} of {total_batches} with {len(batch)} places")

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
