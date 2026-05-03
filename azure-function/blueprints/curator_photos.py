import json
import logging
import os

import azure.durable_functions as df
import azure.functions as func
from pyairtable import Api as AirtableApi

from services.photo_asset_service import (
    build_display_photo_urls,
    is_curator_photo_azure_url,
    is_photo_ready_place,
    parse_url_list,
)
from services.photo_publisher_service import PHOTOS_CONTAINER, PhotoPublisherService
from services.utils import delete_blob_from_container, list_blobs_in_container


bp = df.Blueprint()

CURATOR_PHOTOS_FIELD = "Curator Photos"
PHOTOS_FIELD = "Photos"
MAX_PHOTOS_FIELD_URLS = 30


def validate_sync_curator_photos_request(req: func.HttpRequest):
    return {"city": req.params.get("city", "charlotte")}, None


@bp.function_name(name="SyncCuratorPhotos")
@bp.route(route="sync-curator-photos")
@bp.durable_client_input(client_name="client")
async def sync_curator_photos(req: func.HttpRequest, client) -> func.HttpResponse:
    logging.info("Received request for curator photo sync.")
    try:
        parsed_request, validation_error_response = validate_sync_curator_photos_request(req)
        if validation_error_response:
            return validation_error_response

        orchestration_input = {"city": parsed_request["city"]}
        instance_id = await client.start_new("sync_curator_photos_orchestrator", client_input=orchestration_input)
        return client.create_check_status_response(req, instance_id)
    except Exception as ex:
        logging.error(f"Error starting curator photo sync orchestration: {ex}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "success": False,
                "message": "Server error occurred while starting the curator photo sync orchestration.",
                "data": None,
                "error": str(ex),
            }),
            status_code=500,
            mimetype="application/json",
        )


@bp.orchestration_trigger(context_name="context")
def sync_curator_photos_orchestrator(context: df.DurableOrchestrationContext):
    try:
        orchestration_input = context.get_input() or {}
        city = orchestration_input.get("city", "charlotte")
        config_dict = {
            "city": city,
            "provider_type": "google",
            "dry_run": False,
            "upload": True,
            "write_airtable": True,
        }

        all_third_places = yield context.call_activity("get_all_third_places", {"config": config_dict})
        results = []
        concurrency_limit = 20
        for index in range(0, len(all_third_places), concurrency_limit):
            batch = all_third_places[index:index + concurrency_limit]
            tasks = [
                context.call_activity("sync_single_place_curator_photos", {"place": place, "config": config_dict})
                for place in batch
            ]
            batch_results = yield context.task_all(tasks)
            results.extend(batch_results)

        total_places = len(all_third_places)
        updated = len([result for result in results if result.get("status") in {"updated", "would_update"}])
        skipped = len([result for result in results if result.get("status") == "skipped"])
        no_change = len([result for result in results if result.get("status") == "no_change"])
        errors = len([result for result in results if result.get("status") in {"failed", "error"}])
        all_successful = errors == 0
        return {
            "success": all_successful,
            "message": "Curator photo sync completed." if all_successful else "Some curator photo syncs failed.",
            "data": {
                "status": "completed" if all_successful else "completed_with_errors",
                "total_places": total_places,
                "updated": updated,
                "skipped": skipped,
                "no_change": no_change,
                "errors": errors,
                "error_details": [result.get("message", "") for result in results if result.get("status") in {"failed", "error"}],
                "place_results": results,
            },
            "error": None if all_successful else f"{errors} places failed to process",
        }
    except Exception as ex:
        logging.error(f"Critical error in sync_curator_photos_orchestrator: {ex}", exc_info=True)
        return {
            "success": False,
            "message": "Error occurred during the curator photo sync orchestration.",
            "data": None,
            "error": str(ex),
        }


def _merge_curator_urls_into_photos(curator_urls: list[str], existing_photo_urls: list[str]) -> list[str]:
    non_curator_photo_urls = [url for url in existing_photo_urls if not is_curator_photo_azure_url(url)]
    return build_display_photo_urls(curator_urls, non_curator_photo_urls, max_photos=MAX_PHOTOS_FIELD_URLS)


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("sync_single_place_curator_photos")
def sync_single_place_curator_photos(activityInput):
    try:
        place = activityInput.get("place")
        config = activityInput.get("config", {})
        dry_run = bool(config.get("dry_run", False))
        upload = bool(config.get("upload", not dry_run))
        write_airtable = bool(config.get("write_airtable", not dry_run))
        try_url_variants = bool(config.get("try_url_variants", True))

        place_result = {
            "place_name": "",
            "record_id": place["id"] if place else "",
            "status": "",
            "message": "",
            "photos_synced": 0,
            "photos_deleted": 0,
            "photos_failed": 0,
            "curator_photo_urls_count": 0,
            "photos_field_count": 0,
            "airtable_fields_updated": [],
        }

        if not place or "fields" not in place:
            place_result["status"] = "error"
            place_result["message"] = "Invalid place record"
            return place_result

        fields = place["fields"]
        place_name = fields.get("Place", "Unknown")
        record_id = place["id"]
        place_id = str(fields.get("Google Maps Place Id") or "").strip()
        place_result["place_name"] = place_name
        place_result["place_id"] = place_id

        if not is_photo_ready_place(fields):
            place_result["status"] = "skipped"
            place_result["skip_reason"] = "ignored_missing_place_id"
            place_result["message"] = "No Google Maps Place Id; curator photos ignored."
            return place_result

        curator_attachments = fields.get(CURATOR_PHOTOS_FIELD) or []
        existing_photo_urls = parse_url_list(fields.get(PHOTOS_FIELD))

        publisher = PhotoPublisherService()
        published_urls = []
        expected_blob_paths = set()
        failed_count = 0
        for attachment in curator_attachments:
            publish_result = publisher.publish_curator_attachment(
                attachment,
                place_id=place_id,
                record_id=record_id,
                place_name=place_name,
                dry_run=dry_run,
                upload=upload,
                try_url_variants=try_url_variants,
            )
            if not publish_result.get("success"):
                failed_count += 1
                logging.error("Failed to publish curator attachment for %s: %s", place_name, publish_result.get("error"))
                continue
            published_urls.append(publish_result["azure_url"])
            expected_blob_paths.add(publish_result["blob_path"])

        existing_curator_blobs = set()
        try:
            existing_curator_blobs = set(list_blobs_in_container(PHOTOS_CONTAINER, prefix=f"{place_id}/curator-"))
        except Exception as exc:
            logging.warning("Failed to list curator blobs for %s (%s): %s", place_name, place_id, exc)

        deleted_count = 0
        if not dry_run and failed_count == 0:
            for orphan_path in existing_curator_blobs - expected_blob_paths:
                if delete_blob_from_container(PHOTOS_CONTAINER, orphan_path):
                    deleted_count += 1

        merged_photo_urls = existing_photo_urls
        if failed_count == 0:
            merged_photo_urls = _merge_curator_urls_into_photos(published_urls, existing_photo_urls)
        airtable_updates = {}
        if existing_photo_urls != merged_photo_urls:
            airtable_updates[PHOTOS_FIELD] = json.dumps(merged_photo_urls)

        if airtable_updates and write_airtable and not dry_run:
            api = AirtableApi(os.environ["AIRTABLE_PERSONAL_ACCESS_TOKEN"])
            table = api.table(os.environ["AIRTABLE_BASE_ID"], "Charlotte Third Places")
            table.update(record_id, airtable_updates)

        if failed_count > 0:
            place_result["status"] = "failed"
            place_result["message"] = f"Failed to sync {failed_count} current curator photos; synced {len(published_urls)}."
        elif dry_run and (published_urls or airtable_updates or deleted_count):
            place_result["status"] = "would_update"
            place_result["message"] = f"Would sync {len(published_urls)} current curator photos; failed {failed_count}."
        elif not airtable_updates and deleted_count == 0 and failed_count == 0:
            place_result["status"] = "no_change"
            place_result["message"] = "All curator photos already synced"
        else:
            place_result["status"] = "updated"
            place_result["message"] = f"Synced {len(published_urls)} current curator photos, deleted {deleted_count} orphaned, failed {failed_count}."

        place_result["photos_synced"] = len(published_urls)
        place_result["photos_deleted"] = deleted_count
        place_result["photos_failed"] = failed_count
        place_result["curator_photo_urls_count"] = len(published_urls)
        place_result["photos_field_count"] = len(merged_photo_urls)
        place_result["airtable_fields_updated"] = list(airtable_updates.keys()) if write_airtable and not dry_run else []
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
