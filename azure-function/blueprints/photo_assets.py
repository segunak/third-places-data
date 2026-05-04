import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import azure.functions as func
import azure.durable_functions as df

from services.airtable_service import AirtableService
from services.photo_asset_service import (
    AZURE_ACCOUNT_HOST,
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
    list_blobs_in_container,
    upload_blob_to_container,
)


bp = df.Blueprint()
DEFAULT_PROVIDER_TYPE = "outscraper"
EXPECTED_SKIP_SAMPLE_LIMIT = 5
EXPECTED_SKIP_REASONS = {"ignored_missing_place_id", "no_migratable_photo_urls"}


def _parse_int(value: Optional[str], default: int) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Invalid integer value: {value}") from exc


def _base_config_from_params(params) -> Dict[str, Any]:
    return {
        "city": params.get("city", "charlotte"),
        "record_id": params.get("record_id", ""),
        "place_id": params.get("place_id", ""),
        "max_places": _parse_int(params.get("max_places"), 0),
        "dry_run": True,
        "upload": False,
        "write_airtable": False,
    }


def _json_response(body: Dict[str, Any], status_code: int = 200) -> func.HttpResponse:
    return func.HttpResponse(json.dumps(body, indent=2), status_code=status_code, mimetype="application/json")


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


def _compact_audit_result(result: Dict[str, Any]) -> Dict[str, Any]:
    keys = [
        "status",
        "skip_reason",
        "message",
        "place_name",
        "place_id",
        "record_id",
        "canonical_photo_url_count",
        "canonical_curator_url_count",
        "canonical_standard_url_count",
        "legacy_airtable_url_count",
        "non_azure_airtable_url_count",
        "mappable_legacy_blob_count",
        "unmappable_legacy_blob_count",
        "missing_blob_reference_count",
        "missing_blob_reference_samples",
        "unserved_blob_count",
        "unserved_blob_samples",
        "warnings",
    ]
    return {key: result.get(key) for key in keys if result.get(key) not in (None, {}, [])}


def _compact_results_by_status(
    results: List[Dict[str, Any]],
    compact_fn,
    statuses: set[str],
) -> List[Dict[str, Any]]:
    return [compact_fn(result) for result in results if result.get("status") in statuses]


def _skip_reason_counts(results: List[Dict[str, Any]]) -> Dict[str, int]:
    return _count_by_key([result for result in results if result.get("status") == "skipped"], "skip_reason")


def _unexpected_skip_results(results: List[Dict[str, Any]], compact_fn) -> List[Dict[str, Any]]:
    return [
        compact_fn(result)
        for result in results
        if result.get("status") == "skipped" and result.get("skip_reason") not in EXPECTED_SKIP_REASONS
    ]


def _expected_skip_samples(results: List[Dict[str, Any]], compact_fn) -> Dict[str, List[Dict[str, Any]]]:
    samples: Dict[str, List[Dict[str, Any]]] = {}
    for reason in sorted(EXPECTED_SKIP_REASONS):
        reason_results = [
            result
            for result in results
            if result.get("status") == "skipped" and result.get("skip_reason") == reason
        ]
        if reason_results:
            samples[reason] = [compact_fn(result) for result in reason_results[-EXPECTED_SKIP_SAMPLE_LIMIT:]]
    return samples


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
            "message": "Photo asset audit completed" if totals["success"] else "Photo asset audit found unresolved photo asset findings",
            "data": {
                "totals": totals,
                "result_count": len(results),
                "status_counts": _count_by_key(results, "status"),
                "skip_reason_counts": _skip_reason_counts(results),
                "error_results": _compact_results_by_status(results, _compact_audit_result, {"error"}),
                "unexpected_skip_results": _unexpected_skip_results(results, _compact_audit_result),
                "expected_skip_samples": _expected_skip_samples(results, _compact_audit_result),
            },
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