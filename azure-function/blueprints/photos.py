import json
import logging
import azure.functions as func
import azure.durable_functions as df
from datetime import datetime
from services.airtable_service import AirtableService
from services.place_data_service import PlaceDataProviderFactory
from services.utils import fetch_data_github, save_data_github

bp = df.Blueprint()


def validate_refresh_all_photos_request(req: func.HttpRequest):
    provider_type = req.params.get('provider_type')
    city = req.params.get('city', 'charlotte')
    dry_run = req.params.get('dry_run', 'true').lower() == 'true'
    sequential_mode = req.params.get('sequential_mode', 'false').lower() == 'true'
    max_places_param = req.params.get('max_places')
    photo_source_mode = req.params.get('photo_source_mode', 'refresh_from_data_file_raw_data')
    valid_photo_source_modes = {
        'refresh_from_data_provider',
        'refresh_from_data_file_raw_data',
        'refresh_from_data_file_photo_urls'
    }

    if not provider_type:
        return None, func.HttpResponse(
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
        return None, func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Invalid provider_type",
                "data": None,
                "error": "provider_type must be 'google' or 'outscraper'"
            }),
            status_code=400,
            mimetype="application/json"
        )

    if photo_source_mode not in valid_photo_source_modes:
        return None, func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Invalid photo_source_mode",
                "data": None,
                "error": (
                    "photo_source_mode must be one of: "
                    "refresh_from_data_provider, "
                    "refresh_from_data_file_raw_data, "
                    "refresh_from_data_file_photo_urls"
                )
            }),
            status_code=400,
            mimetype="application/json"
        )

    max_places = None
    if max_places_param:
        try:
            max_places = int(max_places_param)
            if max_places <= 0:
                return None, func.HttpResponse(
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
            return None, func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Invalid max_places value",
                    "data": None,
                    "error": "max_places must be a valid integer"
                }),
                status_code=400,
                mimetype="application/json"
            )

    parsed = {
        "provider_type": provider_type,
        "city": city,
        "dry_run": dry_run,
        "sequential_mode": sequential_mode,
        "max_places": max_places,
        "photo_source_mode": photo_source_mode,
    }
    return parsed, None


@bp.function_name(name="RefreshAllPhotos")
@bp.route(route="refresh-all-photos")
@bp.durable_client_input(client_name="client")
async def refresh_all_photos(req: func.HttpRequest, client) -> func.HttpResponse:
    logging.info("Received request for administrative photo refresh.")

    try:
        parsed_request, validation_error_response = validate_refresh_all_photos_request(req)
        if validation_error_response:
            return validation_error_response

        provider_type = parsed_request["provider_type"]
        city = parsed_request["city"]
        dry_run = parsed_request["dry_run"]
        sequential_mode = parsed_request["sequential_mode"]
        max_places = parsed_request["max_places"]
        photo_source_mode = parsed_request["photo_source_mode"]

        logging.info(f"Starting administrative photo refresh with parameters: "
                     f"provider_type={provider_type}, city={city}, dry_run={dry_run}, "
                     f"sequential_mode={sequential_mode}, max_places={max_places}, "
                     f"photo_source_mode={photo_source_mode}")

        orchestration_input = {
            "provider_type": provider_type,
            "city": city,
            "dry_run": dry_run,
            "sequential_mode": sequential_mode,
            "max_places": max_places,
            "photo_source_mode": photo_source_mode
        }

        instance_id = await client.start_new("refresh_all_photos_orchestrator", client_input=orchestration_input)
        logging.info(f"Started photo refresh orchestration with ID: {instance_id}")

        response = client.create_check_status_response(req, instance_id)
        return response

    except Exception as ex:
        logging.error(f"Error encountered while starting the photo refresh orchestration: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred while starting the photo refresh orchestration.",
                "data": None,
                "error": str(ex)
            }),
            status_code=500,
            mimetype="application/json"
        )


@bp.orchestration_trigger(context_name="context")
def refresh_all_photos_orchestrator(context: df.DurableOrchestrationContext):
    try:
        logging.info("refresh_all_photos_orchestrator started.")

        orchestration_input = context.get_input() or {}
        provider_type = orchestration_input.get("provider_type")
        city = orchestration_input.get("city", "charlotte")
        dry_run = orchestration_input.get("dry_run", True)
        sequential_mode = orchestration_input.get("sequential_mode", False)
        max_places = orchestration_input.get("max_places")
        photo_source_mode = orchestration_input.get("photo_source_mode", "refresh_from_data_file_raw_data")

        if not provider_type:
            raise ValueError("Missing required parameter: provider_type")

        config_dict = {
            "provider_type": provider_type,
            "city": city,
            "dry_run": dry_run,
            "sequential_mode": sequential_mode,
            "max_places": max_places,
            "photo_source_mode": photo_source_mode
        }

        all_third_places = yield context.call_activity(
            'get_all_third_places',
            {"config": config_dict}
        )

        if max_places and max_places > 0:
            all_third_places = all_third_places[:max_places]
            logging.info(f"Limited processing to {max_places} places for photo refresh")

        results = []

        if sequential_mode:
            logging.info(f"Running photo refresh in sequential mode for {len(all_third_places)} places")
            for place in all_third_places:
                activity_input = {
                    "place": place,
                    "config": config_dict
                }
                result = yield context.call_activity("refresh_single_place_photos", activity_input)
                results.append(result)
        else:
            concurrency_limit = 20
            logging.info(f"Running photo refresh in parallel mode with concurrency={concurrency_limit} for {len(all_third_places)} places")

            for i in range(0, len(all_third_places), concurrency_limit):
                batch = all_third_places[i:i+concurrency_limit]
                batch_tasks = []

                for place in batch:
                    activity_input = {
                        "place": place,
                        "config": config_dict
                    }
                    batch_tasks.append(context.call_activity("refresh_single_place_photos", activity_input))

                batch_results = yield context.task_all(batch_tasks)
                results.extend(batch_results)

        total_places = len(all_third_places)
        processed = len([r for r in results if r.get('status') not in ['failed', 'error']])
        updated = len([r for r in results if r.get('status') in ['updated', 'would_update']])
        skipped = len([r for r in results if r.get('status') == 'skipped'])
        no_change = len([r for r in results if r.get('status') == 'no_change'])
        errors = len([r for r in results if r.get('status') in ['failed', 'error']])

        all_successful = errors == 0

        result = {
            "success": all_successful,
            "message": f"Photo refresh {'dry run ' if dry_run else ''}processed successfully." if all_successful else "Some photo refreshes failed.",
            "data": {
                "status": "completed" if all_successful else "completed_with_errors",
                "dry_run": dry_run,
                "total_places": total_places,
                "processed": processed,
                "updated": updated,
                "skipped": skipped,
                "no_change": no_change,
                "errors": errors,
                "error_details": [r.get('message', '') for r in results if r.get('status') in ['failed', 'error']],
                "place_results": results
            },
            "error": None if all_successful else f"{errors} places failed to process"
        }

        logging.info(f"refresh_all_photos_orchestrator completed. Processed {total_places} places, {updated} updated, {skipped} skipped, {no_change} no change needed, {errors} errors.")

        return result
    except Exception as ex:
        logging.error(f"Critical error in refresh_all_photos_orchestrator: {ex}", exc_info=True)
        error_response = {
            "success": False,
            "message": "Error occurred during the photo refresh orchestration.",
            "data": None,
            "error": str(ex)
        }
        return error_response


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("refresh_single_place_photos")
def refresh_single_place_photos(activityInput):
    try:
        place = activityInput.get("place")
        config = activityInput.get("config", {})

        provider_type = config.get("provider_type")
        city = config.get("city", "charlotte")
        dry_run = config.get("dry_run", True)
        photo_source_mode = config.get("photo_source_mode", "refresh_from_data_file_raw_data")

        place_result = {
            "place_name": "",
            "place_id": "",
            "record_id": place['id'] if place else "",
            "status": "",
            "message": "",
            "photos_before": 0,
            "photos_after": 0
        }

        if not place or 'fields' not in place:
            place_result["status"] = "error"
            place_result["message"] = "Invalid place record"
            return place_result

        fields = place['fields']
        place_name = fields.get('Place', 'Unknown')
        place_id = fields.get('Google Maps Place Id', '')

        place_result["place_name"] = place_name
        place_result["place_id"] = place_id

        logging.info(f"Processing photo refresh for: {place_name} ({place_id})")

        if not place_id:
            place_result["status"] = "skipped"
            place_result["message"] = "No Google Maps Place Id"
            return place_result

        try:
            airtable_client = AirtableService(provider_type)
            data_provider = PlaceDataProviderFactory.get_provider(provider_type)
            photo_selector = data_provider._select_prioritized_photos

        except Exception as e:
            place_result["status"] = "error"
            place_result["message"] = f"Failed to initialize components: {str(e)}"
            return place_result

        data_file_path = f"data/places/{city}/{place_id}.json"
        success, place_data, message = fetch_data_github(data_file_path)

        if not success:
            place_result["status"] = "error"
            place_result["message"] = f"Failed to read data file: {message}"
            return place_result

        photos_section = place_data.get('photos', {})
        current_photos = photos_section.get('photo_urls', [])
        place_result["photos_before"] = len(current_photos)

        selected_photo_urls = []

        try:
            if photo_source_mode == "refresh_from_data_provider":
                provider_photos = data_provider.get_place_photos(place_id)
                selected_photo_urls = provider_photos.get('photo_urls', [])

                provider_raw_data = provider_photos.get('raw_data')
                if provider_raw_data:
                    place_data.setdefault('photos', {})['raw_data'] = provider_raw_data

                logging.info(f"Selected {len(selected_photo_urls)} provider photos for {place_name}")

            elif photo_source_mode == "refresh_from_data_file_photo_urls":
                selected_photo_urls = current_photos if isinstance(current_photos, list) else []
                logging.info(f"Using {len(selected_photo_urls)} existing cached photo_urls for {place_name}")

            else:
                raw_data = photos_section.get('raw_data', [])
                if not raw_data:
                    place_result["status"] = "skipped"
                    place_result["message"] = "No raw photos data found"
                    return place_result

                photo_list = []
                parse_method = "unknown"

                if isinstance(raw_data, list) and raw_data:
                    if isinstance(raw_data[0], dict) and 'photo_url_big' in raw_data[0]:
                        photo_list = raw_data
                        parse_method = "direct_list"

                if not photo_list and isinstance(raw_data, dict):
                    photos_data = raw_data.get('photos_data', [])
                    if isinstance(photos_data, list) and photos_data:
                        if isinstance(photos_data[0], dict) and 'photo_url_big' in photos_data[0]:
                            photo_list = photos_data
                            parse_method = "nested_dict"

                if not photo_list:
                    place_result["status"] = "error"
                    place_result["message"] = "Could not parse raw photos data - no valid structure found"
                    return place_result

                logging.info(f"Found {len(photo_list)} raw photo data records for {place_name} (method: {parse_method})")

                valid_photos = []
                for photo in photo_list:
                    photo_url = photo.get('photo_url_big', '')
                    if data_provider._is_valid_photo_url(photo_url):
                        valid_photos.append(photo)

                selected_photo_urls = photo_selector(valid_photos, max_photos=30)
                logging.info(f"Selected {len(selected_photo_urls)} photos from cached raw_data for {place_name}")

            place_result["photos_after"] = len(selected_photo_urls)
            if not selected_photo_urls:
                place_result["status"] = "skipped"
                if photo_source_mode == "refresh_from_data_file_photo_urls":
                    place_result["message"] = "No cached photo_urls found"
                else:
                    place_result["message"] = "No valid photos after selection"
                return place_result

        except Exception as e:
            place_result["status"] = "error"
            place_result["message"] = f"Photo selection failed: {str(e)}"
            return place_result

        if not dry_run:
            try:
                photos_json = json.dumps(selected_photo_urls)
                update_result = airtable_client.update_place_record(
                    record_id=place['id'],
                    field_to_update='Photos',
                    update_value=photos_json,
                    overwrite=True
                )
                if not update_result.get('updated', False):
                    if update_result.get('old_value') is None and update_result.get('new_value') is None:
                        place_result["status"] = "error"
                        place_result["message"] = "Failed to update Airtable due to error"
                        return place_result
                    else:
                        logging.info(f"Photos for {place_name} are already up to date - no changes needed to Airtable or data file")
                        place_result["status"] = "no_change"
                        place_result["message"] = f"Photos already up to date - no changes needed"
                        return place_result
                else:
                    logging.info(f"Airtable was updated for {place_name}, updating data file cache")

                    place_data['photos']['photo_urls'] = selected_photo_urls
                    place_data['photos']['message'] = (
                        f"Photos refreshed by admin function using {provider_type} "
                        f"and mode {photo_source_mode}"
                    )
                    place_data['photos']['last_refreshed'] = datetime.now().isoformat()

                    updated_json = json.dumps(place_data, indent=4)
                    save_success, save_message = save_data_github(updated_json, data_file_path)

                    if not save_success:
                        place_result["status"] = "error"
                        place_result["message"] = f"Airtable updated but GitHub save failed: {save_message}"
                        return place_result

                    place_result["status"] = "updated"
                    place_result["message"] = f"Successfully updated with {len(selected_photo_urls)} photos"

            except Exception as e:
                place_result["status"] = "error"
                place_result["message"] = f"Update failed: {str(e)}"
                return place_result
        else:
            place_result["status"] = "would_update"
            place_result["message"] = f"Would update with {len(selected_photo_urls)} photos"

        logging.info(f"Completed photo refresh for {place_name}: {place_result['status']} - {place_result['message']}")
        return place_result

    except Exception as ex:
        place_name = activityInput.get("place", {}).get("fields", {}).get("Place", "Unknown")
        logging.error(f"Error refreshing photos for {place_name}: {ex}", exc_info=True)
        return {
            "place_name": place_name,
            "place_id": "",
            "record_id": "",
            "status": "error",
            "message": f"Unexpected error: {str(ex)}",
            "photos_before": 0,
            "photos_after": 0
        }
