import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import azure.functions as func
import azure.durable_functions as df

from services.airtable_service import AirtableService
from services.photo_asset_service import (
    PhotoAssetConfig,
    PhotoAssetService,
    curator_photo_urls_field_from_airtable,
    parse_url_list,
)
from services.utils import (
    delete_blob_from_container,
    delete_container,
    ensure_container_exists,
    fetch_data_github,
    save_data_github,
    upload_blob_to_container,
)


bp = df.Blueprint()
DEFAULT_PROVIDER_TYPE = "outscraper"


def _parse_bool(value: Optional[str], default: bool) -> bool:
    if value is None or value == "":
        return default
    return str(value).lower() == "true"


def _parse_int(value: Optional[str], default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Invalid integer value: {value}") from exc


def _base_config_from_params(params) -> Dict[str, Any]:
    return {
        "provider_type": params.get("provider_type", DEFAULT_PROVIDER_TYPE),
        "city": params.get("city", "charlotte"),
        "record_id": params.get("record_id", ""),
        "place_id": params.get("place_id", ""),
        "max_places": _parse_int(params.get("max_places"), 0),
        "dry_run": _parse_bool(params.get("dry_run"), True),
        "upload": _parse_bool(params.get("upload"), False),
        "write_airtable": _parse_bool(params.get("write_airtable"), False),
        "overwrite": _parse_bool(params.get("overwrite"), False),
        "retry_failures": _parse_bool(params.get("retry_failures"), False),
        "failure_ttl_hours": _parse_int(params.get("failure_ttl_hours"), 168),
        "try_url_variants": _parse_bool(params.get("try_url_variants"), True),
    }


def _json_response(body: Dict[str, Any], status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(json.dumps(body, indent=2), status_code=status_code, mimetype="application/json")


def _photo_asset_config(config: Dict[str, Any]) -> PhotoAssetConfig:
    return PhotoAssetConfig(
        city=config.get("city", "charlotte"),
        dry_run=bool(config.get("dry_run", True)),
        upload=bool(config.get("upload", False)),
        write_airtable=bool(config.get("write_airtable", False)),
        overwrite=bool(config.get("overwrite", False)),
        retry_failures=bool(config.get("retry_failures", False)),
        failure_ttl_hours=int(config.get("failure_ttl_hours", 168)),
        try_url_variants=bool(config.get("try_url_variants", True)),
    )


def _filter_places(places: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    record_id = config.get("record_id", "")
    place_id = config.get("place_id", "")
    filtered = places

    if record_id:
        filtered = [place for place in filtered if place.get("id") == record_id]
    if place_id:
        filtered = [place for place in filtered if place.get("fields", {}).get("Google Maps Place Id") == place_id]

    if record_id and place_id:
        matched_by_record = [place for place in places if place.get("id") == record_id]
        if not matched_by_record:
            raise ValueError(f"record_id not found: {record_id}")
        resolved_place_id = matched_by_record[0].get("fields", {}).get("Google Maps Place Id", "")
        if resolved_place_id != place_id:
            raise ValueError("record_id and place_id resolve to different Airtable records")

    max_places = int(config.get("max_places", 0) or 0)
    if max_places > 0:
        filtered = filtered[:max_places]
    return filtered


def _aggregate_results(results: List[Dict[str, Any]], dry_run: bool) -> Dict[str, Any]:
    totals = {
        "total_places": len(results),
        "dry_run": dry_run,
        "updated": len([result for result in results if result.get("status") == "updated"]),
        "would_update": len([result for result in results if result.get("status") == "would_update"]),
        "skipped": len([result for result in results if result.get("status") == "skipped"]),
        "errors": len([result for result in results if result.get("status") == "error"]),
        "data_file_source_urls": sum(result.get("summary", {}).get("candidate_count", 0) for result in results),
        "azure_available_assets": sum(result.get("summary", {}).get("azure_assets_count", 0) for result in results),
        "selected_airtable_urls": sum(result.get("summary", {}).get("selected_airtable_count", 0) for result in results),
        "curator_photo_urls_field_urls": sum(result.get("summary", {}).get("curator_photo_urls_field_count", 0) for result in results),
        "uncopied_curator_photo_urls_field_urls": sum(result.get("summary", {}).get("unselected_curator_photo_urls_field_count", 0) for result in results),
        "failed_uploads": sum(result.get("summary", {}).get("failed_upload_count", 0) for result in results),
        "successful_but_unserved": sum(result.get("summary", {}).get("successful_but_unserved_count", 0) for result in results),
        "duplicate_hashes": sum(result.get("summary", {}).get("duplicate_count", 0) for result in results),
        "blob_bytes": sum(sum(asset.get("bytes", 0) for asset in result.get("assets", [])) for result in results),
        "records_with_fewer_than_30_selected_assets": len([
            result for result in results
            if 0 < result.get("summary", {}).get("selected_airtable_count", 0) < 30
        ]),
    }
    totals["success"] = totals["errors"] == 0
    return totals


def _count_by_key(items: List[Dict[str, Any]], key: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in items:
        value = str(item.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return counts


def _diagnostic_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
    keys = [
        "candidate_count",
        "azure_assets_count",
        "selected_airtable_count",
        "failed_upload_count",
        "pending_upload_count",
        "curator_photo_urls_field_count",
        "unselected_curator_photo_urls_field_count",
        "unsupported_curator_photo_urls_field_count",
        "non_azure_airtable_photos_count",
    ]
    diagnostic = {key: summary.get(key) for key in keys if key in summary}

    unselected_curator_urls = summary.get("unselected_curator_photo_urls_field_urls") or []
    if unselected_curator_urls:
        diagnostic["unselected_curator_photo_urls_field_url_samples"] = unselected_curator_urls[:3]

    unsupported_curator_urls = summary.get("unsupported_curator_photo_urls_field_urls") or []
    if unsupported_curator_urls:
        diagnostic["unsupported_curator_photo_urls_field_url_samples"] = unsupported_curator_urls[:3]

    return diagnostic


def _diagnostic_place_result(result: Dict[str, Any]) -> Dict[str, Any]:
    diagnostic = {
        "status": result.get("status"),
        "skip_reason": result.get("skip_reason"),
        "error_reason": result.get("error_reason"),
        "message": result.get("message"),
        "place_name": result.get("place_name"),
        "place_id": result.get("place_id"),
        "record_id": result.get("record_id"),
        "summary": _diagnostic_summary(result.get("summary", {})),
    }

    failures = [failure for failure in result.get("failures", []) if isinstance(failure, dict)]
    if failures:
        diagnostic["failure_count"] = len(failures)
        diagnostic["failure_reason_counts"] = _count_by_key(failures, "reason")
        diagnostic["failure_error_samples"] = [failure.get("error") for failure in failures[:3]]

    return {key: value for key, value in diagnostic.items() if value not in (None, {}, [])}


def _migration_progress_status(
    config: Dict[str, Any],
    phase: str,
    processed_places: int,
    total_places: int,
    batch_index: int,
    total_batches: int,
    results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    dry_run = bool(config.get("dry_run", True))
    totals = _aggregate_results(results, dry_run)
    error_results = [result for result in results if result.get("status") == "error"]
    recent_problem_results = [
        result for result in results
        if result.get("status") in {"error", "skipped"}
    ][-10:]

    status = {
        "phase": phase,
        "provider_type": config.get("provider_type", DEFAULT_PROVIDER_TYPE),
        "city": config.get("city", "charlotte"),
        "dry_run": dry_run,
        "upload": bool(config.get("upload", False)),
        "write_airtable": bool(config.get("write_airtable", False)),
        "processed_places": processed_places,
        "total_places": total_places,
        "batch_index": batch_index,
        "total_batches": total_batches,
        "totals_so_far": totals,
    }

    if error_results:
        status["error_count"] = len(error_results)
        status["recent_error_results"] = [_diagnostic_place_result(result) for result in error_results[-10:]]
    elif recent_problem_results:
        status["recent_problem_results"] = [_diagnostic_place_result(result) for result in recent_problem_results]

    return status


def _compact_place_result(result: Dict[str, Any]) -> Dict[str, Any]:
    compact_keys = [
        "status",
        "skip_reason",
        "error_reason",
        "message",
        "warnings",
        "place_name",
        "place_id",
        "record_id",
        "summary",
    ]
    compact = {key: result.get(key) for key in compact_keys if key in result}

    selected_urls = result.get("selected_airtable_urls") or []
    if selected_urls:
        compact["selected_airtable_count"] = len(selected_urls)
        compact["selected_airtable_url_samples"] = selected_urls[:3]

    assets = [asset for asset in result.get("assets", []) if isinstance(asset, dict)]
    if assets:
        compact["asset_count"] = len(assets)
        compact["asset_status_counts"] = _count_by_key(assets, "status")

    failures = [failure for failure in result.get("failures", []) if isinstance(failure, dict)]
    if failures:
        compact["failure_count"] = len(failures)
        compact["failure_reason_counts"] = _count_by_key(failures, "reason")
        compact["failure_error_samples"] = [failure.get("error") for failure in failures[:3]]

    return compact


@bp.function_name(name="PhotoAssetsMigrate")
@bp.route(route="photo-assets/migrate")
@bp.durable_client_input(client_name="client")
async def photo_assets_migrate(req: func.HttpRequest, client) -> func.HttpResponse:
    try:
        config = _base_config_from_params(req.params)
        instance_id = await client.start_new("photo_assets_migration_orchestrator", client_input=config)
        return client.create_check_status_response(req, instance_id)
    except ValueError as exc:
        return _json_response({"success": False, "message": "Invalid request parameter", "error": str(exc)}, 400)
    except Exception as exc:
        logging.error(f"Failed to start photo asset migration: {exc}", exc_info=True)
        return _json_response({"success": False, "message": "Failed to start migration", "error": str(exc)}, 500)


@bp.orchestration_trigger(context_name="context")
def photo_assets_migration_orchestrator(context: df.DurableOrchestrationContext):
    try:
        config = context.get_input() or {}
        context.set_custom_status(_migration_progress_status(config, "loading_places", 0, 0, 0, 0, []))
        all_places = yield context.call_activity("get_all_third_places", {"config": config})
        filtered_places = _filter_places(all_places, config)

        results = []
        concurrency_limit = 20
        total_batches = (len(filtered_places) + concurrency_limit - 1) // concurrency_limit if filtered_places else 0
        context.set_custom_status(_migration_progress_status(config, "places_loaded", 0, len(filtered_places), 0, total_batches, results))

        for batch_index, index in enumerate(range(0, len(filtered_places), concurrency_limit), start=1):
            batch = filtered_places[index:index + concurrency_limit]
            context.set_custom_status(_migration_progress_status(
                config,
                "batch_running",
                len(results),
                len(filtered_places),
                batch_index,
                total_batches,
                results,
            ))
            tasks = [
                context.call_activity("migrate_single_place_photo_assets", {"place": place, "config": config})
                for place in batch
            ]
            batch_results = yield context.task_all(tasks)
            results.extend(batch_results)
            context.set_custom_status(_migration_progress_status(
                config,
                "batch_completed",
                len(results),
                len(filtered_places),
                batch_index,
                total_batches,
                results,
            ))

        totals = _aggregate_results(results, bool(config.get("dry_run", True)))
        compact_results = [_compact_place_result(result) for result in results]
        context.set_custom_status(_migration_progress_status(
            config,
            "completed",
            len(results),
            len(filtered_places),
            total_batches,
            total_batches,
            results,
        ))
        return {
            "success": totals["success"],
            "message": "Photo asset migration completed" if totals["success"] else "Photo asset migration completed with errors",
            "data": {"totals": totals, "place_results": compact_results},
            "error": None if totals["success"] else f"{totals['errors']} places failed",
        }
    except Exception as exc:
        logging.error(f"Critical error in photo asset migration: {exc}", exc_info=True)
        return {"success": False, "message": "Photo asset migration failed", "data": None, "error": str(exc)}


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("migrate_single_place_photo_assets")
def migrate_single_place_photo_assets(activityInput):
    place = activityInput.get("place")
    config = activityInput.get("config", {})
    try:
        if not place or "fields" not in place:
            return {"status": "error", "message": "Invalid place record", "summary": {}}

        fields = place["fields"]
        place_name = fields.get("Place", "Unknown")
        place_id = fields.get("Google Maps Place Id", "")
        city = config.get("city", "charlotte")
        if not place_id:
            curator_photo_urls_field_urls = curator_photo_urls_field_from_airtable(fields)
            existing_photos = parse_url_list(fields.get("Photos"))
            uncopied_curator_urls = [
                url for url in curator_photo_urls_field_urls
                if url not in existing_photos
            ]
            if uncopied_curator_urls:
                message = (
                    "Migration cannot prove Curator Photo URLs are safe to delete: "
                    "record is missing Google Maps Place Id and has Curator Photo URLs "
                    "that are not present in Photos."
                )
                logging.error(
                    "%s place=%s record_id=%s uncopied_curator_photo_urls=%s",
                    message,
                    place_name,
                    place.get("id", ""),
                    uncopied_curator_urls,
                )
                return {
                    "status": "error",
                    "error_reason": "curator_photo_urls_not_copied_missing_place_id",
                    "message": message,
                    "place_name": place_name,
                    "record_id": place.get("id", ""),
                    "summary": {
                        "curator_photo_urls_field_count": len(curator_photo_urls_field_urls),
                        "unselected_curator_photo_urls_field_count": len(uncopied_curator_urls),
                        "unselected_curator_photo_urls_field_urls": uncopied_curator_urls,
                    },
                }

            message = "Skipped: no Google Maps Place Id; Azure photo blobs require a place_id in their path."
            logging.info(
                "Skipping photo asset migration for %s (record_id=%s): missing Google Maps Place Id.",
                place_name,
                place.get("id", ""),
            )
            return {
                "status": "skipped",
                "skip_reason": "missing_google_maps_place_id",
                "message": message,
                "place_name": place_name,
                "record_id": place.get("id", ""),
                "summary": {},
            }

        data_file_path = f"data/places/{city}/{place_id}.json"
        fetch_success, place_data, fetch_message = fetch_data_github(data_file_path)
        warnings: List[str] = []
        if not fetch_success:
            warnings.append(f"data_file_missing: {fetch_message}")
            place_data = {"place_id": place_id, "place_name": place_name, "photos": {}}

        service = PhotoAssetService()
        asset_result = service.process_place(place, place_data, _photo_asset_config(config))
        selected_urls = asset_result["selected_airtable_urls"]
        dry_run = bool(config.get("dry_run", True))
        write_airtable = bool(config.get("write_airtable", False))
        candidate_count = asset_result["summary"].get("candidate_count", 0)
        uncopied_curator_urls_count = asset_result["summary"].get("unselected_curator_photo_urls_field_count", 0)

        if uncopied_curator_urls_count:
            summary = asset_result["summary"]
            message = (
                "Migration cannot prove Curator Photo URLs are safe to delete: "
                "one or more Curator Photo URLs would not be present in Photos."
            )
            logging.error(
                "%s place=%s place_id=%s record_id=%s uncopied_count=%s uncopied_urls=%s",
                message,
                place_name,
                place_id,
                place.get("id", ""),
                uncopied_curator_urls_count,
                summary.get("unselected_curator_photo_urls_field_urls", []),
            )
            return {
                "status": "error",
                "error_reason": "curator_photo_urls_not_copied",
                "message": message,
                "warnings": warnings,
                "place_name": place_name,
                "place_id": place_id,
                "record_id": place.get("id", ""),
                "summary": summary,
                "selected_airtable_urls": selected_urls,
                "assets": asset_result["assets"],
                "failures": asset_result["failures"],
            }

        if candidate_count == 0 and not selected_urls:
            summary = asset_result["summary"]
            message = (
                "Skipped: no migratable photo URLs found after checking Airtable Photos, "
                "data file photos.photo_urls, and raw provider photo_url_big sources."
            )
            logging.info(
                "Skipping photo asset migration for %s (%s, record_id=%s): no migratable photo URLs found. "
                "candidate_count=%s airtable_photos_count=%s data_file_photo_urls_count=%s "
                "provider_raw_photo_url_big_count=%s warnings=%s",
                place_name,
                place_id,
                place.get("id", ""),
                summary.get("candidate_count", 0),
                summary.get("airtable_photos_count", 0),
                summary.get("data_file_photo_urls_count", 0),
                summary.get("provider_raw_photo_url_big_count", 0),
                warnings,
            )
            return {
                "status": "skipped",
                "skip_reason": "no_migratable_photo_urls",
                "message": message,
                "warnings": warnings,
                "place_name": place_name,
                "place_id": place_id,
                "record_id": place.get("id", ""),
                "summary": summary,
                "selected_airtable_urls": selected_urls,
                "assets": asset_result["assets"],
                "failures": asset_result["failures"],
            }

        if not dry_run and not selected_urls:
            summary = asset_result["summary"]
            message = (
                "Migration found photo candidates but selected zero Azure Photos URLs; "
                "refusing to overwrite Airtable Photos with an empty list."
            )
            logging.error(
                "Refusing empty Photos update for %s (%s, record_id=%s): "
                "candidate_count=%s azure_assets_count=%s failed_upload_count=%s warnings=%s",
                place_name,
                place_id,
                place.get("id", ""),
                summary.get("candidate_count", 0),
                summary.get("azure_assets_count", 0),
                summary.get("failed_upload_count", 0),
                warnings,
            )
            return {
                "status": "error",
                "error_reason": "no_selected_azure_urls",
                "message": message,
                "warnings": warnings,
                "place_name": place_name,
                "place_id": place_id,
                "record_id": place.get("id", ""),
                "summary": summary,
                "selected_airtable_urls": selected_urls,
                "assets": asset_result["assets"],
                "failures": asset_result["failures"],
            }

        status = "would_update" if dry_run else "updated"
        message = f"Processed {candidate_count} candidates"

        if not dry_run:
            updated_json = json.dumps(asset_result["place_data"], indent=4)
            save_success, save_message = save_data_github(updated_json, data_file_path)
            if not save_success:
                return {
                    "status": "error",
                    "message": f"GitHub save failed: {save_message}",
                    "place_name": place_name,
                    "place_id": place_id,
                    "record_id": place.get("id", ""),
                    "summary": asset_result["summary"],
                }

            if write_airtable:
                airtable_service = AirtableService(config.get("provider_type", DEFAULT_PROVIDER_TYPE))
                update_result = airtable_service.update_place_record(
                    record_id=place["id"],
                    field_to_update="Photos",
                    update_value=json.dumps(selected_urls),
                    overwrite=True,
                )
                if not update_result.get("updated", False) and update_result.get("old_value") is None and update_result.get("new_value") is None:
                    return {
                        "status": "error",
                        "message": "Airtable update failed",
                        "place_name": place_name,
                        "place_id": place_id,
                        "record_id": place.get("id", ""),
                        "summary": asset_result["summary"],
                    }

        return {
            "status": status,
            "message": message,
            "warnings": warnings,
            "place_name": place_name,
            "place_id": place_id,
            "record_id": place.get("id", ""),
            "summary": asset_result["summary"],
            "selected_airtable_urls": selected_urls,
            "assets": asset_result["assets"],
            "failures": asset_result["failures"],
        }
    except Exception as exc:
        logging.error(f"Failed to migrate photo assets for place: {exc}", exc_info=True)
        return {"status": "error", "message": str(exc), "summary": {}}


@bp.function_name(name="PhotoAssetsReport")
@bp.route(route="photo-assets/report")
@bp.durable_client_input(client_name="client")
async def photo_assets_report(req: func.HttpRequest, client) -> func.HttpResponse:
    try:
        config = _base_config_from_params(req.params)
        config["dry_run"] = True
        config["upload"] = False
        config["write_airtable"] = False
        instance_id = await client.start_new("photo_assets_migration_orchestrator", client_input=config)
        return client.create_check_status_response(req, instance_id)
    except ValueError as exc:
        return _json_response({"success": False, "message": "Invalid request parameter", "error": str(exc)}, 400)
    except Exception as exc:
        logging.error(f"Failed to start photo asset report: {exc}", exc_info=True)
        return _json_response({"success": False, "message": "Failed to start report", "error": str(exc)}, 500)


@bp.function_name(name="PhotoHealthCheck")
@bp.route(route="photo-assets/health-check", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def photo_health_check(req: func.HttpRequest) -> func.HttpResponse:
    try:
        city = req.params.get("city", "charlotte")
        place_id = req.params.get("place_id", "")
        if not place_id:
            return _json_response({"success": False, "message": "Missing required parameter: place_id"}, 400)

        airtable_service = AirtableService(DEFAULT_PROVIDER_TYPE)
        matches = [
            place for place in airtable_service.all_third_places
            if place.get("fields", {}).get("Google Maps Place Id") == place_id
        ]
        if not matches:
            return _json_response({"success": False, "message": f"Place not found for place_id {place_id}"}, 404)

        place = matches[0]
        fetch_success, place_data, fetch_message = fetch_data_github(f"data/places/{city}/{place_id}.json")
        if not fetch_success:
            place_data = {"place_id": place_id, "photos": {}}

        report = PhotoAssetService().build_health_report(place, place_data, city)
        report["warnings"] = [] if fetch_success else [f"data_file_missing: {fetch_message}"]
        return _json_response({"success": True, "message": "Photo health check completed", "data": report})
    except Exception as exc:
        logging.error(f"Photo health check failed: {exc}", exc_info=True)
        return _json_response({"success": False, "message": "Photo health check failed", "error": str(exc)}, 500)


@bp.function_name(name="PhotoAssetsCanary")
@bp.route(route="photo-assets/canary", methods=["POST", "GET"], auth_level=func.AuthLevel.FUNCTION)
def photo_assets_canary(req: func.HttpRequest) -> func.HttpResponse:
    run_id = uuid.uuid4().hex[:12]
    temp_container = f"photo-canary-{run_id}"
    test_blob_path = "canary/test.txt"
    try:
        ensure_container_exists(temp_container, public_access="blob")
        test_url = upload_blob_to_container(
            temp_container,
            test_blob_path,
            b"photo asset canary",
            content_type="text/plain",
            public_access="blob",
            overwrite=True,
        )
        deleted_blob = delete_blob_from_container(temp_container, test_blob_path)
        deleted_container = delete_container(temp_container)
        ensure_container_exists("place-photos", public_access="blob")
        return _json_response({
            "success": True,
            "message": "Photo asset canary completed",
            "data": {
                "run_id": run_id,
                "temp_container": temp_container,
                "test_blob_url": test_url,
                "deleted_blob": deleted_blob,
                "deleted_container": deleted_container,
                "place_photos_container_ready": True,
                "checked_at": datetime.now(timezone.utc).isoformat(),
            },
        })
    except Exception as exc:
        logging.error(f"Photo asset canary failed: {exc}", exc_info=True)
        return _json_response({"success": False, "message": "Photo asset canary failed", "error": str(exc)}, 500)