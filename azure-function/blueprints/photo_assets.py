import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import azure.functions as func
import azure.durable_functions as df

from services.airtable_service import AirtableService
from services.photo_asset_service import (
    AZURE_ACCOUNT_HOST,
    PhotoAssetConfig,
    PhotoAssetService,
    classify_azure_photo_url,
    is_photo_ready_place,
    parse_url_list,
)
from services.image_conversion_service import webp_encoder_available
from services.photo_publisher_service import (
    LEGACY_CURATOR_PHOTOS_CONTAINER,
    LEGACY_PLACE_PHOTOS_CONTAINER,
    PHOTOS_CONTAINER,
)
from services.utils import (
    delete_blob_from_container,
    ensure_container_exists,
    fetch_data_github,
    list_blobs_in_container,
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
    dry_run = _parse_bool(params.get("dry_run"), True)
    return {
        "provider_type": params.get("provider_type", DEFAULT_PROVIDER_TYPE),
        "city": params.get("city", "charlotte"),
        "record_id": params.get("record_id", ""),
        "place_id": params.get("place_id", ""),
        "max_places": _parse_int(params.get("max_places"), 0),
        "dry_run": dry_run,
        "upload": _parse_bool(params.get("upload"), not dry_run),
        "write_airtable": _parse_bool(params.get("write_airtable"), not dry_run),
        "try_url_variants": _parse_bool(params.get("try_url_variants"), True),
    }


def _json_response(body: Dict[str, Any], status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(json.dumps(body, indent=2), status_code=status_code, mimetype="application/json")


def _photo_asset_config(config: Dict[str, Any]) -> PhotoAssetConfig:
    return PhotoAssetConfig(
        city=config.get("city", "charlotte"),
        dry_run=bool(config.get("dry_run", True)),
        upload=bool(config.get("upload", False)),
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
        "github_data_file_save_attempts": sum(1 for result in results if result.get("summary", {}).get("github_data_file_save_attempted")),
        "github_data_files_saved": sum(1 for result in results if result.get("summary", {}).get("github_data_file_saved")),
        "github_data_file_save_failures": sum(1 for result in results if result.get("summary", {}).get("github_data_file_save_failed")),
        "airtable_write_requested": sum(1 for result in results if result.get("summary", {}).get("airtable_write_requested")),
        "airtable_write_attempts": sum(1 for result in results if result.get("summary", {}).get("airtable_write_attempted")),
        "airtable_updates_applied": sum(1 for result in results if result.get("summary", {}).get("airtable_update_applied")),
        "airtable_updates_skipped_no_change": sum(1 for result in results if result.get("summary", {}).get("airtable_update_skipped_no_change")),
        "airtable_update_failures": sum(1 for result in results if result.get("summary", {}).get("airtable_update_failed")),
    }
    totals["success"] = totals["errors"] == 0
    return totals


def _aggregate_curator_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    errors = len([result for result in results if result.get("status") in {"failed", "error"}])
    return {
        "total_places": len(results),
        "updated": len([result for result in results if result.get("status") in {"updated", "would_update"}]),
        "skipped": len([result for result in results if result.get("status") == "skipped"]),
        "no_change": len([result for result in results if result.get("status") == "no_change"]),
        "errors": errors,
        "photos_synced": sum(int(result.get("photos_synced", 0) or 0) for result in results),
        "photos_failed": sum(int(result.get("photos_failed", 0) or 0) for result in results),
        "success": errors == 0,
    }


def _aggregate_audit_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    mappable_legacy_left = sum(int(result.get("mappable_legacy_blob_count", 0) or 0) for result in results)
    legacy_urls_left = sum(int(result.get("legacy_airtable_url_count", 0) or 0) for result in results)
    non_azure_urls_left = sum(int(result.get("non_azure_airtable_url_count", 0) or 0) for result in results)
    errors = len([result for result in results if result.get("status") == "error"])
    return {
        "total_places": len(results),
        "ignored_missing_place_id": len([result for result in results if result.get("status") == "skipped" and result.get("skip_reason") == "ignored_missing_place_id"]),
        "canonical_photo_url_count": sum(int(result.get("canonical_photo_url_count", 0) or 0) for result in results),
        "canonical_curator_url_count": sum(int(result.get("canonical_curator_url_count", 0) or 0) for result in results),
        "canonical_standard_url_count": sum(int(result.get("canonical_standard_url_count", 0) or 0) for result in results),
        "legacy_airtable_url_count": legacy_urls_left,
        "non_azure_airtable_url_count": non_azure_urls_left,
        "mappable_legacy_blob_count": mappable_legacy_left,
        "unmappable_legacy_blob_count": sum(int(result.get("unmappable_legacy_blob_count", 0) or 0) for result in results),
        "new_container_blob_count": sum(int(result.get("new_container_blob_count", 0) or 0) for result in results),
        "unserved_blob_count": sum(int(result.get("unserved_blob_count", 0) or 0) for result in results),
        "errors": errors,
        "success": errors == 0 and mappable_legacy_left == 0 and legacy_urls_left == 0 and non_azure_urls_left == 0,
    }


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
        "github_data_file_save_attempted",
        "github_data_file_saved",
        "github_data_file_save_failed",
        "airtable_write_requested",
        "airtable_write_attempted",
        "airtable_update_applied",
        "airtable_update_skipped_no_change",
        "airtable_update_failed",
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


def _all_candidate_downloads_failed(summary: Dict[str, Any], failures: List[Dict[str, Any]]) -> bool:
    candidate_count = int(summary.get("candidate_count", 0) or 0)
    failed_upload_count = int(summary.get("failed_upload_count", 0) or 0)
    if candidate_count <= 0 or failed_upload_count < candidate_count or not failures:
        return False
    return all(failure.get("reason") == "download_failed" for failure in failures if isinstance(failure, dict))


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


def _set_migration_progress_status(
    context: df.DurableOrchestrationContext,
    config: Dict[str, Any],
    phase: str,
    processed_places: int,
    total_places: int,
    batch_index: int,
    total_batches: int,
    results: List[Dict[str, Any]],
) -> None:
    if getattr(context, "is_replaying", False):
        return
    context.set_custom_status(_migration_progress_status(
        config,
        phase,
        processed_places,
        total_places,
        batch_index,
        total_batches,
        results,
    ))


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
        all_places = yield context.call_activity("get_all_third_places", {"config": config})
        filtered_places = _filter_places(all_places, config)

        results = []
        concurrency_limit = 20
        total_batches = (len(filtered_places) + concurrency_limit - 1) // concurrency_limit if filtered_places else 0
        _set_migration_progress_status(
            context,
            config,
            "places_loaded",
            0,
            len(filtered_places),
            0,
            total_batches,
            results,
        )

        for batch_index, index in enumerate(range(0, len(filtered_places), concurrency_limit), start=1):
            batch = filtered_places[index:index + concurrency_limit]
            tasks = [
                context.call_activity("migrate_single_place_photo_assets", {"place": place, "config": config})
                for place in batch
            ]
            batch_results = yield context.task_all(tasks)
            results.extend(batch_results)
            _set_migration_progress_status(
                context,
                config,
                "batch_completed",
                len(results),
                len(filtered_places),
                batch_index,
                total_batches,
                results,
            )

        curator_results = []
        for batch_index, index in enumerate(range(0, len(filtered_places), concurrency_limit), start=1):
            batch = filtered_places[index:index + concurrency_limit]
            tasks = [
                context.call_activity("sync_single_place_curator_photos", {"place": place, "config": config})
                for place in batch
            ]
            batch_results = yield context.task_all(tasks)
            curator_results.extend(batch_results)
            _set_migration_progress_status(
                context,
                config,
                "curator_batch_completed",
                len(curator_results),
                len(filtered_places),
                batch_index,
                total_batches,
                results,
            )

        audit_results = []
        for batch_index, index in enumerate(range(0, len(filtered_places), concurrency_limit), start=1):
            batch = filtered_places[index:index + concurrency_limit]
            tasks = [
                context.call_activity("audit_single_place_photo_assets", {"place": place, "config": config})
                for place in batch
            ]
            batch_results = yield context.task_all(tasks)
            audit_results.extend(batch_results)

        totals = _aggregate_results(results, bool(config.get("dry_run", True)))
        audit_totals = _aggregate_audit_results(audit_results)
        curator_errors = len([result for result in curator_results if result.get("status") in {"failed", "error"}])
        migration_success = totals["success"] and curator_errors == 0 and audit_totals["success"]
        compact_results = [_compact_place_result(result) for result in results]
        _set_migration_progress_status(
            context,
            config,
            "completed",
            len(results),
            len(filtered_places),
            total_batches,
            total_batches,
            results,
        )
        return {
            "success": migration_success,
            "message": "Photo asset migration completed" if migration_success else "Photo asset migration completed with errors or audit findings",
            "data": {
                "totals": totals,
                "curator_totals": _aggregate_curator_results(curator_results),
                "audit_totals": audit_totals,
                "place_results": compact_results,
                "curator_results": curator_results,
                "audit_results": audit_results,
            },
            "error": None if migration_success else "Migration errors, curator errors, or final audit findings remain",
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
        if not is_photo_ready_place(fields):
            message = "Skipped: no Google Maps Place Id; photo paths ignore non-photo-ready records."
            logging.info(
                "Skipping photo asset migration for %s (record_id=%s): missing Google Maps Place Id.",
                place_name,
                place.get("id", ""),
            )
            return {
                "status": "skipped",
                "skip_reason": "ignored_missing_place_id",
                "message": message,
                "place_name": place_name,
                "record_id": place.get("id", ""),
                "summary": {"ignored_missing_place_id": True},
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
        asset_result["summary"].update({
            "github_data_file_save_attempted": False,
            "github_data_file_saved": False,
            "github_data_file_save_failed": False,
            "airtable_write_requested": write_airtable,
            "airtable_write_attempted": False,
            "airtable_update_applied": False,
            "airtable_update_skipped_no_change": False,
            "airtable_update_failed": False,
        })

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
            failures = [failure for failure in asset_result.get("failures", []) if isinstance(failure, dict)]
            if _all_candidate_downloads_failed(summary, failures):
                source_hosts = sorted({failure.get("source_host", "unknown") for failure in failures})
                message = (
                    "Skipped: all photo candidate downloads failed, so Airtable Photos was left unchanged."
                )
                logging.warning(
                    "Skipping photo asset migration for %s (%s, record_id=%s): all candidate downloads failed. "
                    "candidate_count=%s failed_upload_count=%s source_hosts=%s warnings=%s",
                    place_name,
                    place_id,
                    place.get("id", ""),
                    summary.get("candidate_count", 0),
                    summary.get("failed_upload_count", 0),
                    source_hosts,
                    warnings,
                )
                return {
                    "status": "skipped",
                    "skip_reason": "all_photo_downloads_failed",
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
            if write_airtable:
                airtable_service = AirtableService(config.get("provider_type", DEFAULT_PROVIDER_TYPE))
                asset_result["summary"]["airtable_write_attempted"] = True
                update_result = airtable_service.update_place_record(
                    record_id=place["id"],
                    field_to_update="Photos",
                    update_value=json.dumps(selected_urls),
                    overwrite=True,
                )
                airtable_update_failed = (
                    not update_result.get("updated", False)
                    and update_result.get("old_value") is None
                    and update_result.get("new_value") is None
                )
                asset_result["summary"]["airtable_update_applied"] = bool(update_result.get("updated", False))
                asset_result["summary"]["airtable_update_failed"] = airtable_update_failed
                asset_result["summary"]["airtable_update_skipped_no_change"] = (
                    not update_result.get("updated", False)
                    and not airtable_update_failed
                )
                if airtable_update_failed:
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


@bp.function_name(name="PhotoAssetsAudit")
@bp.route(route="photo-assets/audit")
@bp.durable_client_input(client_name="client")
async def photo_assets_audit(req: func.HttpRequest, client) -> func.HttpResponse:
    try:
        config = _base_config_from_params(req.params)
        config["dry_run"] = True
        config["upload"] = False
        config["write_airtable"] = False
        instance_id = await client.start_new("photo_assets_audit_orchestrator", client_input=config)
        return client.create_check_status_response(req, instance_id)
    except ValueError as exc:
        return _json_response({"success": False, "message": "Invalid request parameter", "error": str(exc)}, 400)
    except Exception as exc:
        logging.error(f"Failed to start photo asset audit: {exc}", exc_info=True)
        return _json_response({"success": False, "message": "Failed to start audit", "error": str(exc)}, 500)


@bp.orchestration_trigger(context_name="context")
def photo_assets_audit_orchestrator(context: df.DurableOrchestrationContext):
    try:
        config = context.get_input() or {}
        all_places = yield context.call_activity("get_all_third_places", {"config": config})
        filtered_places = _filter_places(all_places, config)
        results = []
        concurrency_limit = 20
        for index in range(0, len(filtered_places), concurrency_limit):
            batch = filtered_places[index:index + concurrency_limit]
            tasks = [
                context.call_activity("audit_single_place_photo_assets", {"place": place, "config": config})
                for place in batch
            ]
            batch_results = yield context.task_all(tasks)
            results.extend(batch_results)

        totals = _aggregate_audit_results(results)
        return {
            "success": totals["success"],
            "message": "Photo asset audit completed" if totals["success"] else "Photo asset audit found migration leftovers",
            "data": {"totals": totals, "place_results": results},
            "error": None if totals["success"] else "Mappable legacy blobs or legacy/non-Azure Airtable Photos remain",
        }
    except Exception as exc:
        logging.error(f"Critical error in photo asset audit: {exc}", exc_info=True)
        return {"success": False, "message": "Photo asset audit failed", "data": None, "error": str(exc)}


@bp.activity_trigger(input_name="activityInput")
@bp.function_name("audit_single_place_photo_assets")
def audit_single_place_photo_assets(activityInput):
    place = activityInput.get("place")
    config = activityInput.get("config", {})
    try:
        if not place or "fields" not in place:
            return {"status": "error", "message": "Invalid place record"}
        fields = place["fields"]
        city = config.get("city", "charlotte")
        place_name = fields.get("Place", "Unknown")
        place_id = str(fields.get("Google Maps Place Id") or "").strip()
        record_id = place.get("id", "")
        if not is_photo_ready_place(fields):
            return {
                "status": "skipped",
                "skip_reason": "ignored_missing_place_id",
                "place_name": place_name,
                "record_id": record_id,
            }

        airtable_urls = parse_url_list(fields.get("Photos"))
        canonical_photo_urls = []
        canonical_curator_urls = []
        canonical_standard_urls = []
        legacy_urls = []
        non_azure_urls = []
        for url in airtable_urls:
            classification = classify_azure_photo_url(url, city, place_id)
            category = classification["category"]
            if category in {"new_curator", "new_standard"} and classification["reason"] == "valid":
                canonical_photo_urls.append(url)
                if category == "new_curator":
                    canonical_curator_urls.append(url)
                else:
                    canonical_standard_urls.append(url)
            elif category in {"legacy_curator", "legacy_place_photo"}:
                legacy_urls.append(url)
            elif urlparse(url).netloc.lower() != AZURE_ACCOUNT_HOST:
                non_azure_urls.append(url)

        new_blobs = []
        legacy_place_blobs = []
        legacy_curator_blobs = []
        blob_warnings = []
        try:
            new_blobs = list_blobs_in_container(PHOTOS_CONTAINER, prefix=f"{place_id}/")
        except Exception as exc:
            blob_warnings.append(f"new_container_list_failed: {exc}")
        try:
            legacy_place_blobs = list_blobs_in_container(LEGACY_PLACE_PHOTOS_CONTAINER, prefix=f"{city}/{place_id}/")
        except Exception as exc:
            blob_warnings.append(f"legacy_place_list_failed: {exc}")
        try:
            legacy_curator_blobs = list_blobs_in_container(LEGACY_CURATOR_PHOTOS_CONTAINER, prefix=f"{record_id}/") if record_id else []
        except Exception as exc:
            blob_warnings.append(f"legacy_curator_list_failed: {exc}")

        airtable_url_set = set(canonical_photo_urls)
        blob_url_set = {f"https://{AZURE_ACCOUNT_HOST}/{PHOTOS_CONTAINER}/{blob}" for blob in new_blobs}
        missing_blob_urls = sorted(airtable_url_set - blob_url_set)
        unserved_blobs = sorted(blob_url_set - airtable_url_set)
        curator_blob_count = len([blob for blob in new_blobs if blob.split("/", 1)[-1].startswith("curator-")])
        standard_blob_count = len(new_blobs) - curator_blob_count
        webp_count = len([blob for blob in new_blobs if blob.lower().endswith(".webp")])

        return {
            "status": "ok" if not missing_blob_urls else "error",
            "place_name": place_name,
            "place_id": place_id,
            "record_id": record_id,
            "airtable_photo_url_count": len(airtable_urls),
            "canonical_photo_url_count": len(canonical_photo_urls),
            "canonical_curator_url_count": len(canonical_curator_urls),
            "canonical_standard_url_count": len(canonical_standard_urls),
            "legacy_airtable_url_count": len(legacy_urls),
            "non_azure_airtable_url_count": len(non_azure_urls),
            "new_container_blob_count": len(new_blobs),
            "new_container_curator_blob_count": curator_blob_count,
            "new_container_standard_blob_count": standard_blob_count,
            "new_container_webp_blob_count": webp_count,
            "new_container_non_webp_blob_count": len(new_blobs) - webp_count,
            "mappable_legacy_blob_count": len(legacy_place_blobs) + len(legacy_curator_blobs),
            "unmappable_legacy_blob_count": 0,
            "missing_blob_reference_count": len(missing_blob_urls),
            "missing_blob_reference_samples": missing_blob_urls[:3],
            "unserved_blob_count": len(unserved_blobs),
            "unserved_blob_samples": unserved_blobs[:3],
            "warnings": blob_warnings,
        }
    except Exception as exc:
        logging.error(f"Failed to audit photo assets for place: {exc}", exc_info=True)
        return {"status": "error", "message": str(exc)}


@bp.function_name(name="PhotoAssetsReport")
@bp.route(route="photo-assets/report")
@bp.durable_client_input(client_name="client")
async def photo_assets_report(req: func.HttpRequest, client) -> func.HttpResponse:
    try:
        config = _base_config_from_params(req.params)
        config["dry_run"] = True
        config["upload"] = False
        config["write_airtable"] = False
        instance_id = await client.start_new("photo_assets_audit_orchestrator", client_input=config)
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
        report = audit_single_place_photo_assets({"place": place, "config": {"city": city}})
        return _json_response({"success": True, "message": "Photo health check completed", "data": report})
    except Exception as exc:
        logging.error(f"Photo health check failed: {exc}", exc_info=True)
        return _json_response({"success": False, "message": "Photo health check failed", "error": str(exc)}, 500)


@bp.function_name(name="PhotoAssetsCanary")
@bp.route(route="photo-assets/canary", methods=["POST", "GET"], auth_level=func.AuthLevel.FUNCTION)
def photo_assets_canary(req: func.HttpRequest) -> func.HttpResponse:
    run_id = uuid.uuid4().hex[:12]
    test_blob_path = f"canary/{run_id}.webp"
    try:
        ensure_container_exists(PHOTOS_CONTAINER, public_access="blob")
        test_url = upload_blob_to_container(
            PHOTOS_CONTAINER,
            test_blob_path,
            b"RIFF\x1a\x00\x00\x00WEBPVP8 \x0e\x00\x00\x00\x10\x01\x00\x9d\x01*\x01\x00\x01\x00\x01@&%\xa4\x00\x03p\x00\xfe\xfb\xfdP\x00",
            content_type="image/webp",
            public_access="blob",
            overwrite=True,
        )
        deleted_blob = delete_blob_from_container(PHOTOS_CONTAINER, test_blob_path)
        return _json_response({
            "success": True,
            "message": "Photo asset canary completed",
            "data": {
                "run_id": run_id,
                "container": PHOTOS_CONTAINER,
                "test_blob_url": test_url,
                "deleted_blob": deleted_blob,
                "photos_container_ready": True,
                "webp_encoder_available": webp_encoder_available(),
                "checked_at": datetime.now(timezone.utc).isoformat(),
            },
        })
    except Exception as exc:
        logging.error(f"Photo asset canary failed: {exc}", exc_info=True)
        return _json_response({"success": False, "message": "Photo asset canary failed", "error": str(exc)}, 500)