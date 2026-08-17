import json
import logging
import azure.functions as func
import azure.durable_functions as df
from datetime import datetime
from constants import MAX_SELECTED_PHOTOS, PHOTO_REFRESH_BATCH_SIZE
from services.airtable_service import AirtableService
from services.photo_asset_service import PhotoAssetConfig, PhotoAssetService, is_curator_photo_azure_url, parse_photo_manifest_list, parse_url_list, remove_photo_manifest_fields
from services.place_data_service import PlaceDataProviderFactory, PlaceDataService
from services.utils import fetch_data_github, save_data_github

bp = df.Blueprint()


def validate_refresh_all_photos_request(req: func.HttpRequest):
    provider_type = req.params.get('provider_type')
    city = req.params.get('city', 'charlotte')
    view = req.params.get('view')
    place_id = req.params.get('place_id', '').strip()
    dry_run = req.params.get('dry_run', 'true').lower() == 'true'
    upload = req.params.get('upload', 'true').lower() == 'true'
    write_airtable = req.params.get('write_airtable', 'true').lower() == 'true'
    try_url_variants = req.params.get('try_url_variants', 'true').lower() == 'true'
    max_places_param = req.params.get('max_places')
    refresh_below_param = req.params.get('refresh_below')
    photo_source_mode = req.params.get('photo_source_mode', 'cache_first')
    valid_photo_source_modes = {
        'cache_first',
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

    if not place_id and not view:
        return None, func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Missing view value",
                "data": None,
                "error": "view is required"
            }),
            status_code=400,
            mimetype="application/json"
        )

    if not place_id and refresh_below_param in (None, ''):
        return None, func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Missing refresh_below value",
                "data": None,
                "error": "refresh_below is required"
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
                    "cache_first, "
                    "refresh_from_data_provider, "
                    "refresh_from_data_file_raw_data, "
                    "refresh_from_data_file_photo_urls"
                )
            }),
            status_code=400,
            mimetype="application/json"
        )

    refresh_below = None
    if not place_id:
        try:
            refresh_below = int(refresh_below_param)
            if refresh_below <= 0:
                raise ValueError
        except ValueError:
            return None, func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "Invalid refresh_below value",
                    "data": None,
                    "error": "refresh_below must be a positive integer"
                }),
                status_code=400,
                mimetype="application/json"
            )
        if refresh_below > MAX_SELECTED_PHOTOS:
            return None, func.HttpResponse(
                json.dumps({
                    "success": False,
                    "message": "refresh_below exceeds the safe maximum",
                    "data": None,
                    "error": (
                        f"refresh_below must be between 1 and MAX_SELECTED_PHOTOS "
                        f"({MAX_SELECTED_PHOTOS}); received {refresh_below}. "
                        "Higher values could refresh every eligible place."
                    )
                }),
                status_code=400,
                mimetype="application/json"
            )

    if dry_run:
        upload = False
        write_airtable = False
    force_provider = bool(place_id) or photo_source_mode == 'refresh_from_data_provider'

    max_places = None
    if not place_id and max_places_param:
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
        "place_id": place_id,
        "dry_run": dry_run,
        "upload": upload,
        "write_airtable": write_airtable,
        "try_url_variants": try_url_variants,
        "force_provider": force_provider,
        "photo_source_mode": photo_source_mode,
    }
    if not place_id:
        parsed.update({
            "view": view,
            "refresh_below": refresh_below,
            "max_places": max_places,
        })
    return parsed, None


def _photo_refresh_result(
    place,
    status,
    message,
    photos_before=0,
    curator_photos_before=0,
    provider_photos_before=0,
):
    fields = place.get("fields", {}) if isinstance(place, dict) else {}
    return {
        "place_name": fields.get("Place", "Unknown"),
        "place_id": fields.get("Google Maps Place Id", ""),
        "record_id": place.get("id", "") if isinstance(place, dict) else "",
        "status": status,
        "message": message,
        "photos_before": photos_before,
        "photos_after": photos_before,
        "curator_photos_before": curator_photos_before,
        "provider_photos_before": provider_photos_before,
        "provider_called": False,
        "selection_source": "none",
    }


def plan_places_for_photo_refresh(places, config):
    refresh_below = int(config["refresh_below"])
    max_places = config.get("max_places")
    eligible = []
    skipped = []

    for place in places or []:
        fields = place.get("fields", {}) if isinstance(place, dict) else {}
        place_id = str(fields.get("Google Maps Place Id") or "").strip()
        if not place_id:
            skipped.append(_photo_refresh_result(
                place,
                "skipped_missing_place_id",
                "No Google Maps Place Id; photo refresh skipped.",
            ))
            continue

        try:
            manifests = parse_photo_manifest_list(fields.get("Photos"))
        except ValueError as exc:
            skipped.append(_photo_refresh_result(
                place,
                "failed_invalid_airtable_photos",
                str(exc),
            ))
            continue

        photos_before = len(manifests)
        curator_manifests, provider_manifests = _split_airtable_photo_manifests(manifests)
        if photos_before >= refresh_below:
            skipped.append(_photo_refresh_result(
                place,
                "skipped_sufficient",
                f"Airtable already has {photos_before} photos (threshold={refresh_below}).",
                photos_before,
                len(curator_manifests),
                len(provider_manifests),
            ))
            continue

        eligible.append({
            "place": place,
            "photos_before": photos_before,
            "curator_photos_before": len(curator_manifests),
            "provider_photos_before": len(provider_manifests),
        })

    eligible.sort(key=lambda item: (
        item["photos_before"],
        str(item["place"].get("fields", {}).get("Place", "")).lower(),
    ))

    selected = eligible
    if max_places and max_places > 0:
        selected = eligible[:max_places]
        for item in eligible[max_places:]:
            skipped.append(_photo_refresh_result(
                item["place"],
                "skipped_max_places",
                f"Eligible but excluded by max_places={max_places}.",
                item["photos_before"],
                item["curator_photos_before"],
                item["provider_photos_before"],
            ))

    return selected, skipped


def _split_airtable_photo_manifests(manifests):
    curator = []
    provider = []
    for manifest in manifests:
        if is_curator_photo_azure_url(manifest.get("display", "")):
            curator.append(manifest)
        else:
            provider.append(manifest)
    return curator, provider


def _cached_photos_match_provider(place_data, provider_type):
    photos_section = place_data.get("photos", {}) if isinstance(place_data, dict) else {}
    cached_provider = (
        place_data.get("photos_provider_type")
        or photos_section.get("provider_type")
        or ""
    )
    if cached_provider:
        return str(cached_provider).strip().lower() == provider_type

    data_source = str(place_data.get("data_source") or "").strip().lower()
    if data_source == "outscraperprovider":
        return provider_type == "outscraper"
    if data_source == "googlemapsprovider":
        return provider_type == "google"
    return True


def _cached_photos_are_sufficient(cache_selection, provider_type):
    selected_urls = cache_selection.get('photo_urls', [])
    if provider_type == 'google':
        return bool(selected_urls)
    return len(selected_urls) >= MAX_SELECTED_PHOTOS


def _curator_manifests_are_preserved(original, proposed):
    proposed_curator, _ = _split_airtable_photo_manifests(proposed)
    return proposed_curator == original and proposed[:len(original)] == original


def _merge_curator_manifests(*manifest_groups):
    merged = []
    seen_display_urls = set()
    for manifests in manifest_groups:
        for manifest in manifests:
            display_url = manifest.get("display", "")
            if display_url in seen_display_urls:
                continue
            merged.append(manifest)
            seen_display_urls.add(display_url)
    return merged


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
        place_id = parsed_request["place_id"]
        dry_run = parsed_request["dry_run"]
        force_provider = parsed_request["force_provider"]
        photo_source_mode = parsed_request["photo_source_mode"]
        upload = parsed_request["upload"]
        write_airtable = parsed_request["write_airtable"]
        try_url_variants = parsed_request["try_url_variants"]

        logging.info(f"Starting administrative photo refresh with parameters: "
                     f"provider_type={provider_type}, city={city}, dry_run={dry_run}, "
                     f"upload={upload}, write_airtable={write_airtable}, "
                     f"force_provider={force_provider}, "
                     f"photo_source_mode={photo_source_mode}, place_id={place_id}")

        orchestration_input = {
            "provider_type": provider_type,
            "city": city,
            "place_id": place_id,
            "dry_run": dry_run,
            "upload": upload,
            "write_airtable": write_airtable,
            "try_url_variants": try_url_variants,
            "force_provider": force_provider,
            "photo_source_mode": photo_source_mode
        }
        if not place_id:
            orchestration_input.update({
                "view": parsed_request["view"],
                "refresh_below": parsed_request["refresh_below"],
                "max_places": parsed_request["max_places"],
            })

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
        place_id = orchestration_input.get("place_id", "")
        dry_run = orchestration_input.get("dry_run", True)
        force_provider = bool(place_id) or orchestration_input.get("force_provider", False)
        photo_source_mode = orchestration_input.get("photo_source_mode", "cache_first")
        upload = orchestration_input.get("upload", not dry_run)
        write_airtable = orchestration_input.get("write_airtable", not dry_run)
        try_url_variants = orchestration_input.get("try_url_variants", True)

        if not provider_type:
            raise ValueError("Missing required parameter: provider_type")

        config_dict = {
            "provider_type": provider_type,
            "city": city,
            "place_id": place_id,
            "dry_run": dry_run,
            "force_provider": force_provider,
            "photo_source_mode": photo_source_mode,
            "upload": upload,
            "write_airtable": write_airtable,
            "try_url_variants": try_url_variants,
        }

        if place_id:
            place = yield context.call_activity(
                "find_place_by_id",
                {"place_id": place_id, "provider_type": provider_type},
            )
            if not place:
                raise ValueError(f"place_id not found: {place_id}")
            all_third_places = [place]
            selected_places = [{
                "place": place,
                "photos_before": len(parse_photo_manifest_list(place.get("fields", {}).get("Photos"))),
            }]
            place_result = yield context.call_activity(
                "refresh_single_place_photos",
                {"place": place, "config": config_dict},
            )
            results = [place_result]
        else:
            view = orchestration_input["view"]
            refresh_below = orchestration_input["refresh_below"]
            max_places = orchestration_input.get("max_places")
            config_dict.update({
                "view": view,
                "refresh_below": refresh_below,
                "max_places": max_places,
            })
            all_third_places = yield context.call_activity(
                'get_all_third_places',
                {"config": config_dict}
            )
            selected_places, pre_results = plan_places_for_photo_refresh(all_third_places, config_dict)
            results = list(pre_results)
            logging.info(
                f"Running photo refresh in sequential batches of {PHOTO_REFRESH_BATCH_SIZE} "
                f"for {len(selected_places)} selected places"
            )
            for index in range(0, len(selected_places), PHOTO_REFRESH_BATCH_SIZE):
                batch = selected_places[index:index + PHOTO_REFRESH_BATCH_SIZE]
                tasks = [
                    context.call_activity(
                        "refresh_single_place_photos",
                        {"place": item["place"], "config": config_dict},
                    )
                    for item in batch
                ]
                batch_results = yield context.task_all(tasks)
                results.extend(batch_results)

        total_places = len(all_third_places)
        processed = len(selected_places)
        updated = len([r for r in results if str(r.get('status', '')).startswith('updated_')])
        would_use_cache = len([r for r in results if r.get('status') == 'would_use_cache'])
        would_fetch_provider = len([r for r in results if r.get('status') == 'would_fetch_provider'])
        skipped = len([r for r in results if str(r.get('status', '')).startswith('skipped_')])
        no_change = len([r for r in results if r.get('status') == 'no_change'])
        conflicts = len([r for r in results if r.get('status') == 'conflict'])
        errors = len([
            r for r in results
            if r.get('status') == 'error' or str(r.get('status', '')).startswith('failed_')
        ])
        provider_calls = len([r for r in results if r.get('provider_called')])

        all_successful = errors == 0 and conflicts == 0

        result = {
            "success": all_successful,
            "message": f"Photo refresh {'dry run ' if dry_run else ''}processed successfully." if all_successful else "Some photo refreshes failed.",
            "data": {
                "status": "completed" if all_successful else "completed_with_errors",
                "dry_run": dry_run,
                "total_places": total_places,
                "selected_places": len(selected_places),
                "processed": processed,
                "updated": updated,
                "would_use_cache": would_use_cache,
                "would_fetch_provider": would_fetch_provider,
                "skipped": skipped,
                "no_change": no_change,
                "conflicts": conflicts,
                "errors": errors,
                "provider_calls": provider_calls,
                "error_details": [
                    r.get('message', '')
                    for r in results
                    if r.get('status') in {'error', 'conflict'}
                    or str(r.get('status', '')).startswith('failed_')
                ],
                "place_results": results
            },
            "error": None if all_successful else f"{errors} places failed and {conflicts} conflicted"
        }

        logging.info(f"refresh_all_photos_orchestrator completed. Considered {total_places} places, {updated} updated, {skipped} skipped, {no_change} no change needed, {conflicts} conflicts, {errors} errors.")

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
        upload = config.get("upload", not dry_run)
        write_airtable = config.get("write_airtable", not dry_run)
        try_url_variants = config.get("try_url_variants", True)
        photo_source_mode = config.get("photo_source_mode", "cache_first")
        force_provider = bool(config.get("force_provider", False)) or photo_source_mode == "refresh_from_data_provider"

        place_result = {
            "place_name": "",
            "place_id": "",
            "record_id": place['id'] if place else "",
            "status": "",
            "message": "",
            "photos_before": 0,
            "photos_after": 0,
            "cached_photo_urls_before": 0,
            "curator_photos_before": 0,
            "provider_photos_before": 0,
            "curator_photos_after": 0,
            "provider_photos_after": 0,
            "cached_raw_photo_count": 0,
            "cached_valid_photo_count": 0,
            "cached_selected_photo_count": 0,
            "provider_raw_photo_count": 0,
            "provider_selected_photo_count": 0,
            "provider_called": False,
            "selection_source": "none",
            "data_file_status": "not_started",
            "airtable_status": "not_started",
            "failed_uploads": 0,
            "uploaded_assets": 0,
            "reused_assets": 0,
            "canonical_assets": 0,
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
            place_result["status"] = "skipped_missing_place_id"
            place_result["skip_reason"] = "ignored_missing_place_id"
            place_result["message"] = "No Google Maps Place Id; photo refresh ignored."
            return place_result

        data_file_path = f"data/places/{city}/{place_id}.json"
        success, place_data, message = fetch_data_github(data_file_path)

        if not success:
            if "not found in repository" in message:
                place_data = {}
                place_result["data_file_status"] = "missing"
            else:
                place_result["status"] = "error"
                place_result["message"] = f"Failed to read data file: {message}"
                return place_result

        if not isinstance(place_data, dict):
            place_result["status"] = "error"
            place_result["message"] = "Data file did not contain a JSON object."
            return place_result

        original_place_data_json = json.dumps(place_data, sort_keys=True)
        place_data = remove_photo_manifest_fields(place_data)
        photos_section = place_data.get('photos', {})
        if not isinstance(photos_section, dict):
            photos_section = {}
            place_data['photos'] = photos_section

        current_photos = parse_url_list(photos_section.get('photo_urls', []))
        initial_airtable_photos = parse_photo_manifest_list(fields.get('Photos'))
        original_curator_photos, original_provider_photos = _split_airtable_photo_manifests(initial_airtable_photos)
        place_result["photos_before"] = len(initial_airtable_photos)
        place_result["curator_photos_before"] = len(original_curator_photos)
        place_result["provider_photos_before"] = len(original_provider_photos)
        place_result["cached_photo_urls_before"] = len(current_photos)

        raw_data = photos_section.get('raw_data', {})
        cache_selection = PlaceDataService.select_photos_from_raw_data(raw_data)
        place_result["cached_raw_photo_count"] = cache_selection['raw_photo_count']
        place_result["cached_valid_photo_count"] = cache_selection['valid_photo_count']
        place_result["cached_selected_photo_count"] = len(cache_selection['photo_urls'])

        selected_source_photo_urls = []
        provider_required = force_provider
        if force_provider:
            place_result["selection_source"] = "provider"
        elif photo_source_mode == "refresh_from_data_file_photo_urls":
            selected_source_photo_urls = current_photos
            place_result["selection_source"] = "cached_photo_urls"
            provider_required = False
        elif photo_source_mode == "refresh_from_data_file_raw_data":
            selected_source_photo_urls = cache_selection['photo_urls']
            place_result["selection_source"] = "cache"
            provider_required = False
        elif (
            not force_provider
            and _cached_photos_match_provider(place_data, provider_type)
            and _cached_photos_are_sufficient(cache_selection, provider_type)
        ):
            selected_source_photo_urls = cache_selection['photo_urls']
            place_result["selection_source"] = "cache"
            provider_required = False
        else:
            place_result["selection_source"] = "provider"
            provider_required = True

        if dry_run:
            if provider_required:
                place_result["status"] = "would_fetch_provider"
                place_result["message"] = (
                    f"Cache selected {len(cache_selection['photo_urls'])} photos; "
                    f"would fetch {provider_type}."
                )
            elif selected_source_photo_urls:
                place_result["status"] = "would_use_cache"
                place_result["photos_after"] = len(original_curator_photos) + len(selected_source_photo_urls)
                place_result["curator_photos_after"] = len(original_curator_photos)
                place_result["provider_photos_after"] = len(selected_source_photo_urls)
                place_result["message"] = f"Would use {len(selected_source_photo_urls)} cached provider photos."
            else:
                place_result["status"] = "skipped_no_photos"
                place_result["message"] = "No selectable cached photos."
            return place_result

        airtable_client = None
        if write_airtable:
            try:
                airtable_client = AirtableService(provider_type, initialize_provider=False)
            except Exception as e:
                place_result["status"] = "error"
                place_result["message"] = f"Failed to initialize Airtable: {str(e)}"
                return place_result

        try:
            if provider_required:
                data_provider = PlaceDataProviderFactory.get_provider(provider_type)
                provider_photos = data_provider.get_place_photos(place_id)
                place_result["provider_called"] = True
                if not isinstance(provider_photos, dict):
                    raise ValueError(f"{provider_type} returned an invalid photo response")
                if provider_photos.get('error'):
                    place_result["status"] = "failed_provider_fetch"
                    place_result["message"] = (
                        f"{provider_type} photo fetch failed: {provider_photos['error']}"
                    )
                    return place_result
                provider_raw_data = provider_photos.get('raw_data', {})
                provider_selection = PlaceDataService.select_photos_from_raw_data(provider_raw_data)
                place_result["provider_raw_photo_count"] = provider_selection['raw_photo_count']
                selected_source_photo_urls = provider_photos.get('photo_urls', [])
                place_result["provider_selected_photo_count"] = len(selected_source_photo_urls)
                photos_section['raw_data'] = provider_raw_data
            else:
                place_result["provider_selected_photo_count"] = len(selected_source_photo_urls)

            if not selected_source_photo_urls:
                place_result["status"] = "skipped_no_photos"
                place_result["message"] = "No valid provider photos after selection."
                return place_result

            photos_section_for_save = place_data.setdefault('photos', {})
            photos_section_for_save['photo_urls'] = selected_source_photo_urls
            photos_section_for_save['provider_type'] = provider_type
            photos_section_for_save['selection_source'] = place_result['selection_source']
            photos_section_for_save['selection_limit'] = MAX_SELECTED_PHOTOS
            photos_section_for_save['raw_photo_count'] = (
                place_result['provider_raw_photo_count']
                if provider_required
                else place_result['cached_raw_photo_count']
            )
            photos_section_for_save['selected_photo_count'] = len(selected_source_photo_urls)
            if provider_required:
                photos_section_for_save['last_refreshed'] = datetime.now().isoformat()
            photos_section_for_save['message'] = (
                f"Photos reconciled using {provider_type} from {place_result['selection_source']}"
            )
            place_data['photos_provider_type'] = provider_type

            data_file_changed = json.dumps(place_data, sort_keys=True) != original_place_data_json
            if data_file_changed:
                save_success, save_message = save_data_github(
                    json.dumps(place_data, indent=4),
                    data_file_path,
                )
                if not save_success:
                    place_result["status"] = "failed_data_file_save"
                    place_result["data_file_status"] = "failed"
                    place_result["message"] = f"GitHub save failed: {save_message}"
                    return place_result
                place_result["data_file_status"] = "updated"
            else:
                place_result["data_file_status"] = "no_change"

            asset_place = place
            if force_provider:
                asset_place = {
                    **place,
                    "fields": {
                        **fields,
                        "Photos": json.dumps(original_curator_photos),
                    },
                }

            asset_service = PhotoAssetService()
            asset_result = asset_service.process_place(
                asset_place,
                place_data,
                PhotoAssetConfig(
                    city=city,
                    dry_run=False,
                    upload=upload,
                    try_url_variants=try_url_variants,
                ),
            )
            place_result["photo_asset_summary"] = asset_result.get("summary", {})
            place_result["failed_uploads"] = len(asset_result.get("failures", []))
            assets = asset_result.get("assets", [])
            place_result["canonical_assets"] = len(assets)
            place_result["reused_assets"] = len([
                asset for asset in assets if asset.get("status") == "existing_azure"
            ])
            place_result["uploaded_assets"] = len([
                asset for asset in assets if asset.get("status") in {"uploaded", "would_upload"}
            ])

            selected_airtable_photos = parse_photo_manifest_list(
                asset_result.get("selected_airtable_photos") or [],
                "selected_airtable_photos",
            )
            if not selected_airtable_photos and asset_result.get("selected_airtable_urls"):
                raise ValueError("Photo asset processing returned display URLs without thumbnail manifests")
            if not _curator_manifests_are_preserved(original_curator_photos, selected_airtable_photos):
                place_result["status"] = "failed_curator_validation"
                place_result["message"] = "Proposed Airtable Photos did not preserve curator manifests."
                return place_result

            proposed_curator_photos, proposed_provider_photos = _split_airtable_photo_manifests(selected_airtable_photos)
            if selected_source_photo_urls and not proposed_provider_photos:
                place_result["status"] = "failed_photo_publish"
                place_result["message"] = (
                    f"Failed to publish all {len(selected_source_photo_urls)} selected provider photos."
                )
                return place_result

            if not force_provider:
                protected_provider_count = len({
                    photo["display"] for photo in original_provider_photos
                })
                proposed_provider_count = len({
                    photo["display"] for photo in proposed_provider_photos
                })
                if proposed_provider_count < protected_provider_count:
                    place_result["status"] = "failed_provider_regression"
                    place_result["message"] = (
                        "Proposed Airtable Photos would reduce the protected provider photo count "
                        f"below {protected_provider_count}."
                    )
                    return place_result

            place_result["photos_after"] = len(selected_airtable_photos)
            place_result["curator_photos_after"] = len(proposed_curator_photos)
            place_result["provider_photos_after"] = len(proposed_provider_photos)
            if not selected_airtable_photos:
                place_result["status"] = "skipped_no_photos"
                place_result["message"] = "No valid photos after publishing."
                return place_result

        except Exception as e:
            place_result["status"] = "error"
            place_result["message"] = f"Photo selection failed: {str(e)}"
            return place_result

        try:
            if write_airtable:
                latest_record = airtable_client.get_place_record_by_id(place['id'])
                latest_airtable_photos = parse_photo_manifest_list(
                    latest_record.get('fields', {}).get('Photos') if latest_record else None
                )
                if force_provider:
                    latest_curator_photos, _ = _split_airtable_photo_manifests(latest_airtable_photos)
                    retained_curator_photos = _merge_curator_manifests(
                        original_curator_photos,
                        latest_curator_photos,
                    )
                    selected_airtable_photos = [
                        *retained_curator_photos,
                        *proposed_provider_photos,
                    ]
                    place_result["photos_after"] = len(selected_airtable_photos)
                    place_result["curator_photos_after"] = len(retained_curator_photos)

                if latest_airtable_photos == selected_airtable_photos:
                    if place_result["data_file_status"] == "updated":
                        place_result["status"] = "updated_data_file"
                        place_result["message"] = "Updated the data file; Airtable Photos already matched."
                    else:
                        place_result["status"] = "no_change"
                        place_result["message"] = "Data file and Airtable Photos already matched."
                    place_result["airtable_status"] = "no_change"
                    return place_result

                if not force_provider and latest_airtable_photos != initial_airtable_photos:
                    place_result["status"] = "conflict"
                    place_result["airtable_status"] = "conflict"
                    place_result["message"] = "Airtable Photos changed during processing; update aborted."
                    return place_result

                update_result = airtable_client.update_place_record(
                    record_id=place['id'],
                    field_to_update='Photos',
                    update_value=json.dumps(selected_airtable_photos),
                    overwrite=True,
                )
                if not update_result.get('updated', False):
                    if update_result.get('old_value') is None and update_result.get('new_value') is None:
                        place_result["status"] = "failed_airtable_update"
                        place_result["airtable_status"] = "failed"
                        place_result["message"] = "Failed to update Airtable."
                        return place_result
                    place_result["status"] = (
                        "updated_data_file"
                        if place_result["data_file_status"] == "updated"
                        else "no_change"
                    )
                    place_result["airtable_status"] = "no_change"
                    place_result["message"] = (
                        "Updated the data file; Airtable Photos already matched."
                        if place_result["data_file_status"] == "updated"
                        else "Data file and Airtable Photos already matched."
                    )
                    return place_result

                place_result["airtable_status"] = "updated"
                place_result["status"] = (
                    "updated_from_provider"
                    if place_result['selection_source'] == 'provider'
                    else "updated_from_cache"
                )
                place_result["message"] = f"Updated Airtable with {len(selected_airtable_photos)} photos."
            else:
                place_result["airtable_status"] = "disabled"
                if place_result["data_file_status"] == "updated":
                    place_result["status"] = "updated_data_file"
                    place_result["message"] = "Updated the data file; Airtable writes were disabled."
                else:
                    place_result["status"] = "no_change"
                    place_result["message"] = "Data file was unchanged; Airtable writes were disabled."
        except Exception as e:
            place_result["status"] = "failed_airtable_update"
            place_result["airtable_status"] = "failed"
            place_result["message"] = f"Airtable update failed: {str(e)}"
            return place_result

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
