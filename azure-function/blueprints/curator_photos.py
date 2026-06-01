import json
import logging
import os

import azure.durable_functions as df
import azure.functions as func
from pyairtable import Api as AirtableApi

from services.photo_asset_service import (
    build_display_photo_manifests,
    is_curator_photo_azure_url,
    is_photo_ready_place,
    parse_photo_manifest_list,
)
from services.photo_publisher_service import PhotoPublisherService


bp = df.Blueprint()

CURATOR_PHOTOS_FIELD = "Curator Photos"
PHOTOS_FIELD = "Photos"


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


def _merge_curator_photos_into_photos(curator_photos: list[dict], existing_photos: list[dict]) -> list[dict]:
    non_curator_photos = [photo for photo in existing_photos if not is_curator_photo_azure_url(photo.get("display", ""))]
    return build_display_photo_manifests(curator_photos, non_curator_photos)


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
            "photos_uploaded": 0,
            "photos_reused": 0,
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
        existing_photos = parse_photo_manifest_list(fields.get(PHOTOS_FIELD))

        publisher = PhotoPublisherService()
        published_urls = []
        published_photos = []
        uploaded_count = 0
        reused_count = 0
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
            try:
                photo_manifest = parse_photo_manifest_list([publish_result.get("photo_manifest")], "photo_manifest")[0]
            except (IndexError, ValueError) as exc:
                failed_count += 1
                logging.error("Publisher returned invalid curator manifest for %s: %s", place_name, exc)
                continue
            published_photos.append(photo_manifest)
            published_urls.append(photo_manifest["display"])
            publish_status = publish_result.get("status")
            if publish_status == "already_exists":
                reused_count += 1
            elif publish_status == "uploaded":
                uploaded_count += 1

        deleted_count = 0

        merged_photos = existing_photos
        if failed_count == 0:
            merged_photos = _merge_curator_photos_into_photos(published_photos, existing_photos)
        airtable_updates = {}
        if existing_photos != merged_photos:
            airtable_updates[PHOTOS_FIELD] = json.dumps(merged_photos)

        if airtable_updates and write_airtable and not dry_run:
            api = AirtableApi(os.environ["AIRTABLE_PERSONAL_ACCESS_TOKEN"])
            table = api.table(os.environ["AIRTABLE_BASE_ID"], "Charlotte Third Places")
            table.update(record_id, airtable_updates)

        if failed_count > 0:
            place_result["status"] = "failed"
            place_result["message"] = (
                f"Failed to sync {failed_count} current curator photos; "
                f"uploaded {uploaded_count}, reused {reused_count}."
            )
        elif dry_run and (published_urls or airtable_updates or deleted_count):
            place_result["status"] = "would_update"
            place_result["message"] = f"Would sync {len(published_urls)} current curator photos; failed {failed_count}."
        elif not airtable_updates and deleted_count == 0 and failed_count == 0:
            place_result["status"] = "no_change"
            place_result["message"] = "All curator photos already synced"
        else:
            place_result["status"] = "updated"
            place_result["message"] = (
                f"Synced {len(published_urls)} current curator photos "
                f"({uploaded_count} uploaded, {reused_count} reused), "
                f"deleted {deleted_count} orphaned, failed {failed_count}."
            )

        place_result["photos_synced"] = len(published_urls)
        place_result["photos_uploaded"] = uploaded_count
        place_result["photos_reused"] = reused_count
        place_result["photos_deleted"] = deleted_count
        place_result["photos_failed"] = failed_count
        place_result["curator_photo_urls_count"] = len(published_urls)
        place_result["photos_field_count"] = len(merged_photos)
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
            "photos_uploaded": 0,
            "photos_reused": 0,
            "photos_deleted": 0,
            "photos_failed": 0,
        }
