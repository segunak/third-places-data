import json
import logging
import azure.functions as func
import azure.durable_functions as df
from pyairtable import Api as AirtableApi
from services.photo_asset_service import (
    AZURE_ACCOUNT_HOST,
    CURATOR_PHOTOS_CONTAINER,
    CURATOR_PHOTO_URLS_FIELD,
    build_display_photo_urls,
    is_curator_photo_azure_url,
    parse_url_list,
)
from services.utils import upload_blob, delete_blob, list_blobs, download_image

bp = df.Blueprint()

CURATOR_PHOTOS_FIELD = "Curator Photos"
PHOTOS_FIELD = "Photos"
MAX_PHOTOS_FIELD_URLS = 30


def validate_sync_curator_photos_request(req: func.HttpRequest):
    city = req.params.get('city', 'charlotte')

    parsed = {
        "city": city,
    }
    return parsed, None


@bp.function_name(name="SyncCuratorPhotos")
@bp.route(route="sync-curator-photos")
@bp.durable_client_input(client_name="client")
async def sync_curator_photos(req: func.HttpRequest, client) -> func.HttpResponse:
    logging.info("Received request for curator photo sync.")

    try:
        parsed_request, validation_error_response = validate_sync_curator_photos_request(req)
        if validation_error_response:
            return validation_error_response

        city = parsed_request["city"]

        logging.info(f"Starting curator photo sync: city={city}")

        orchestration_input = {
            "city": city,
        }

        instance_id = await client.start_new("sync_curator_photos_orchestrator", client_input=orchestration_input)
        logging.info(f"Started curator photo sync orchestration with ID: {instance_id}")

        response = client.create_check_status_response(req, instance_id)
        return response

    except Exception as ex:
        logging.error(f"Error starting curator photo sync orchestration: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred while starting the curator photo sync orchestration.",
                "data": None,
                "error": str(ex)
            }),
            status_code=500,
            mimetype="application/json"
        )


@bp.orchestration_trigger(context_name="context")
def sync_curator_photos_orchestrator(context: df.DurableOrchestrationContext):
    try:
        logging.info("sync_curator_photos_orchestrator started.")

        orchestration_input = context.get_input() or {}
        city = orchestration_input.get("city", "charlotte")

        config_dict = {
            "city": city,
            "provider_type": "google",  # Required by get_all_third_places activity
        }

        all_third_places = yield context.call_activity(
            'get_all_third_places',
            {"config": config_dict}
        )

        results = []
        concurrency_limit = 20
        logging.info(f"Running curator photo sync in parallel mode with concurrency={concurrency_limit} for {len(all_third_places)} places")

        for i in range(0, len(all_third_places), concurrency_limit):
            batch = all_third_places[i:i + concurrency_limit]
            batch_tasks = []

            for place in batch:
                activity_input = {
                    "place": place,
                    "config": config_dict
                }
                batch_tasks.append(context.call_activity("sync_single_place_curator_photos", activity_input))

            batch_results = yield context.task_all(batch_tasks)
            results.extend(batch_results)

        total_places = len(all_third_places)
        updated = len([r for r in results if r.get('status') in ['updated', 'would_update']])
        skipped = len([r for r in results if r.get('status') == 'skipped'])
        no_change = len([r for r in results if r.get('status') == 'no_change'])
        errors = len([r for r in results if r.get('status') in ['failed', 'error']])

        all_successful = errors == 0

        result = {
            "success": all_successful,
            "message": "Curator photo sync completed." if all_successful else "Some curator photo syncs failed.",
            "data": {
                "status": "completed" if all_successful else "completed_with_errors",
                "total_places": total_places,
                "updated": updated,
                "skipped": skipped,
                "no_change": no_change,
                "errors": errors,
                "error_details": [r.get('message', '') for r in results if r.get('status') in ['failed', 'error']],
                "place_results": results
            },
            "error": None if all_successful else f"{errors} places failed to process"
        }

        logging.info(f"sync_curator_photos_orchestrator completed. {total_places} places, {updated} updated, {skipped} skipped, {no_change} no change, {errors} errors.")
        return result

    except Exception as ex:
        logging.error(f"Critical error in sync_curator_photos_orchestrator: {ex}", exc_info=True)
        return {
            "success": False,
            "message": "Error occurred during the curator photo sync orchestration.",
            "data": None,
            "error": str(ex)
        }


def _build_blob_path(record_id: str, attachment_id: str, filename: str) -> str:
    """Build the blob storage path for a curator photo."""
    safe_filename = filename.replace(" ", "_")
    return f"{record_id}/{attachment_id}_{safe_filename}"


def _curator_blob_url(blob_path: str) -> str:
    return f"https://{AZURE_ACCOUNT_HOST}/{CURATOR_PHOTOS_CONTAINER}/{blob_path}"


def _merge_curator_urls_into_photos(curator_urls: list[str], existing_photo_urls: list[str]) -> list[str]:
    non_curator_photo_urls = [url for url in existing_photo_urls if not is_curator_photo_azure_url(url)]
    return build_display_photo_urls(curator_urls, non_curator_photo_urls, max_photos=MAX_PHOTOS_FIELD_URLS)


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("sync_single_place_curator_photos")
def sync_single_place_curator_photos(activityInput):
    import os

    try:
        place = activityInput.get("place")
        config = activityInput.get("config", {})

        place_result = {
            "place_name": "",
            "record_id": place['id'] if place else "",
            "status": "",
            "message": "",
            "photos_synced": 0,
            "photos_deleted": 0,
            "photos_failed": 0,
            "curator_photo_urls_count": 0,
            "photos_field_count": 0,
            "airtable_fields_updated": [],
        }

        if not place or 'fields' not in place:
            place_result["status"] = "error"
            place_result["message"] = "Invalid place record"
            return place_result

        fields = place['fields']
        place_name = fields.get('Place', 'Unknown')
        record_id = place['id']

        place_result["place_name"] = place_name

        curator_attachments = fields.get(CURATOR_PHOTOS_FIELD) or []
        existing_curator_urls = parse_url_list(fields.get(CURATOR_PHOTO_URLS_FIELD))
        existing_photo_urls = parse_url_list(fields.get(PHOTOS_FIELD))
        existing_display_curator_urls = [url for url in existing_photo_urls if is_curator_photo_azure_url(url)]

        if not curator_attachments and not existing_curator_urls and not existing_display_curator_urls:
            place_result["status"] = "skipped"
            place_result["message"] = "No curator photos"
            return place_result

        # Build expected blob paths from current attachments
        expected_blob_paths = {}
        for att in curator_attachments:
            att_id = att.get("id", "")
            att_filename = att.get("filename", "photo.jpg")
            blob_path = _build_blob_path(record_id, att_id, att_filename)
            expected_blob_paths[blob_path] = att

        # Check what blobs already exist for this record
        existing_blobs = set(list_blobs(prefix=f"{record_id}/"))

        # Determine which attachments are new (not yet in blob storage)
        new_attachments = {
            path: att for path, att in expected_blob_paths.items()
            if path not in existing_blobs
        }

        # Determine orphaned blobs (in storage but not in current attachments)
        expected_paths_set = set(expected_blob_paths.keys())
        orphaned_blobs = existing_blobs - expected_paths_set

        # Upload new attachments
        uploaded_count = 0
        failed_count = 0
        uploaded_paths = set()
        for blob_path, att in new_attachments.items():
            att_url = att.get("url", "")
            if not att_url:
                logging.warning(f"No URL for attachment {att.get('id')} on {place_name}")
                failed_count += 1
                continue
            try:
                image_data, content_type = download_image(att_url)
                upload_blob(blob_path, image_data, content_type)
                uploaded_count += 1
                uploaded_paths.add(blob_path)
            except Exception as e:
                logging.error(f"Failed to download/upload curator photo for {place_name}: {e}")
                failed_count += 1

        # Delete orphaned blobs
        deleted_count = 0
        for orphan_path in orphaned_blobs:
            if delete_blob(orphan_path):
                deleted_count += 1

        # Build the complete list of permanent URLs for blobs that are actually present
        available_blob_paths = (existing_blobs & expected_paths_set) | uploaded_paths
        all_blob_urls = []
        for blob_path in expected_blob_paths.keys():
            if blob_path in available_blob_paths:
                all_blob_urls.append(_curator_blob_url(blob_path))

        merged_photo_urls = _merge_curator_urls_into_photos(all_blob_urls, existing_photo_urls)

        airtable_updates = {}
        if existing_curator_urls != all_blob_urls:
            airtable_updates[CURATOR_PHOTO_URLS_FIELD] = json.dumps(all_blob_urls)
        if existing_photo_urls != merged_photo_urls:
            airtable_updates[PHOTOS_FIELD] = json.dumps(merged_photo_urls)

        if airtable_updates:
            api = AirtableApi(os.environ['AIRTABLE_PERSONAL_ACCESS_TOKEN'])
            table = api.table(os.environ['AIRTABLE_BASE_ID'], "Charlotte Third Places")
            table.update(record_id, airtable_updates)

        if not airtable_updates and uploaded_count == 0 and deleted_count == 0 and failed_count == 0:
            place_result["status"] = "no_change"
            place_result["message"] = "All curator photos already synced"
        else:
            place_result["status"] = "updated"
            place_result["message"] = (
                f"Uploaded {uploaded_count} new, deleted {deleted_count} orphaned, "
                f"failed {failed_count}, total {len(all_blob_urls)} curator photo URLs"
            )
        place_result["photos_synced"] = uploaded_count
        place_result["photos_deleted"] = deleted_count
        place_result["photos_failed"] = failed_count
        place_result["curator_photo_urls_count"] = len(all_blob_urls)
        place_result["photos_field_count"] = len(merged_photo_urls)
        place_result["airtable_fields_updated"] = list(airtable_updates.keys())
        return place_result

    except Exception as ex:
        logging.error(f"Error syncing curator photos for place: {ex}", exc_info=True)
        return {
            "place_name": activityInput.get("place", {}).get("fields", {}).get("Place", "Unknown"),
            "record_id": activityInput.get("place", {}).get("id", ""),
            "status": "error",
            "message": str(ex),
            "photos_synced": 0,
            "photos_deleted": 0,
            "photos_failed": 0,
        }
