import json
import logging
import azure.functions as func
import azure.durable_functions as df
from services.airtable_service import AirtableService
from services.place_data_service import PlaceDataProviderFactory

bp = df.Blueprint()


# ======================================================
# Operating Hours Refresh Functions
# ======================================================


@bp.function_name(name="RefreshOperatingHours")
@bp.route(route="refresh-operating-hours")
@bp.durable_client_input(client_name="client")
async def refresh_operating_hours(req: func.HttpRequest, client) -> func.HttpResponse:
    logging.info("Received request to refresh operating hours.")

    try:
        provider_type = req.params.get('provider_type')
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
            "city": city
        }
        instance_id = await client.start_new("refresh_operating_hours_orchestrator", client_input=config_dict)
        logging.info(f"Started operating hours refresh orchestration with ID: {instance_id}")
        response = client.create_check_status_response(req, instance_id)
        return response
    except Exception as ex:
        logging.error(f"Error encountered while starting the operating hours refresh orchestration: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred while starting the operating hours refresh orchestration.",
                "data": None,
                "error": str(ex)
            }),
            status_code=500,
            mimetype="application/json"
        )


@bp.orchestration_trigger(context_name="context")
def refresh_operating_hours_orchestrator(context: df.DurableOrchestrationContext):
    try:
        logging.info("refresh_operating_hours_orchestrator started.")
        config_dict = context.get_input() or {}
        provider_type = config_dict.get("provider_type")
        city = config_dict.get("city")

        if not city:
            raise ValueError("Missing required parameter: city")
        if not provider_type:
            raise ValueError("Missing required parameter: provider_type")

        all_third_places = yield context.call_activity(
            'get_all_third_places',
            {"config": config_dict}
        )

        # Filter to only places with a Google Maps Place Id
        places_with_id = [
            p for p in all_third_places
            if p.get('fields', {}).get('Google Maps Place Id')
        ]
        skipped_count = len(all_third_places) - len(places_with_id)
        if skipped_count > 0:
            logging.info(f"Skipped {skipped_count} places without Google Maps Place Id.")

        results = []
        from constants import MAX_THREAD_WORKERS
        concurrency_limit = MAX_THREAD_WORKERS

        logging.info(f"Refreshing operating hours for {len(places_with_id)} places using provider '{provider_type}'")

        for i in range(0, len(places_with_id), concurrency_limit):
            batch = places_with_id[i:i + concurrency_limit]
            batch_tasks = []
            for place in batch:
                activity_input = {
                    "place": place,
                    "provider_type": provider_type,
                    "city": city
                }
                batch_tasks.append(context.call_activity("refresh_single_place_operating_hours", activity_input))

            batch_results = yield context.task_all(batch_tasks)
            total_batches = (len(places_with_id) + concurrency_limit - 1) // concurrency_limit
            current_batch = i // concurrency_limit + 1
            logging.info(f"Processed batch {current_batch} of {total_batches} with {len(batch)} places")
            results.extend(batch_results)

        updated = [r for r in results if r.get('update_status') == 'updated']
        skipped = [r for r in results if r.get('update_status') == 'skipped']
        failed = [r for r in results if r.get('update_status') == 'failed']

        result = {
            "success": len(failed) == 0,
            "message": f"Operating hours refresh complete. {len(updated)} updated, {len(skipped)} skipped, {len(failed)} failed.",
            "data": {
                "total_processed": len(results),
                "updated": len(updated),
                "skipped": len(skipped),
                "failed": len(failed),
                "results": results
            },
            "error": None if not failed else f"{len(failed)} failed updates"
        }
        return result
    except Exception as ex:
        logging.error(f"Critical error in refresh_operating_hours_orchestrator: {ex}", exc_info=True)
        return {
            "success": False,
            "message": "Error occurred during the operating hours refresh orchestration.",
            "data": None,
            "error": str(ex)
        }


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("refresh_single_place_operating_hours")
def refresh_single_place_operating_hours(activityInput):
    place_name = "Unknown Place"
    try:
        place = activityInput.get("place")
        provider_type = activityInput.get("provider_type")
        city = activityInput.get("city")
        place_name = place['fields']['Place'] if place and 'fields' in place and 'Place' in place['fields'] else "Unknown Place"
        place_id = place['fields'].get('Google Maps Place Id')
        record_id = place.get('id', '')

        if not place_id:
            return {
                "place_name": place_name,
                "place_id": None,
                "record_id": record_id,
                "update_status": "skipped",
                "message": "No Google Maps Place Id"
            }

        data_provider = PlaceDataProviderFactory.get_provider(provider_type)
        hours_list = data_provider.get_operating_hours(place_id)

        if not hours_list:
            return {
                "place_name": place_name,
                "place_id": place_id,
                "record_id": record_id,
                "update_status": "skipped",
                "message": "No operating hours returned by provider"
            }

        hours_json = json.dumps(hours_list, ensure_ascii=False)

        airtable_client = AirtableService(provider_type)
        update_result = airtable_client.update_place_record(
            record_id,
            'Operating Hours',
            hours_json,
            overwrite=True
        )

        return {
            "place_name": place_name,
            "place_id": place_id,
            "record_id": record_id,
            "update_status": "updated" if update_result.get('updated') else "skipped",
            "message": f"Operating hours {'updated' if update_result.get('updated') else 'unchanged'}",
            "hours": hours_list
        }
    except Exception as ex:
        logging.error(f"Error refreshing operating hours for {place_name}: {ex}", exc_info=True)
        return {
            "place_name": place_name,
            "update_status": "failed",
            "message": f"Error: {str(ex)}"
        }
