from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
AZURE_FUNCTION_DIR = REPO_ROOT / "azure-function"
if str(AZURE_FUNCTION_DIR) not in sys.path:
    sys.path.insert(0, str(AZURE_FUNCTION_DIR))

from pyairtable import Api, Table  # noqa: E402

from blueprints.photo_assets import _aggregate_audit_results, audit_single_place_photo_assets  # noqa: E402
from services.photo_asset_service import (  # noqa: E402
    PhotoAssetConfig,
    PhotoAssetService,
    is_photo_ready_place,
    parse_url_list,
)


TABLE_NAME = "Charlotte Third Places"
PHOTOS_FIELD = "Photos"
AIRTABLE_FIELDS = [
    "Place",
    "Google Maps Place Id",
    PHOTOS_FIELD,
    "Curator Photos",
]
REQUIRED_ENV_VARS = [
    "AIRTABLE_BASE_ID",
    "AIRTABLE_PERSONAL_ACCESS_TOKEN",
    "AIRTABLE_WORKSPACE_ID",
    "AzureWebJobsStorage",
]
GOOGLE_HOST_TEXT_PATTERN = re.compile(
    r"(lh\d+\.googleusercontent\.com|googleusercontent\.com|googleapis\.com|maps\.google\.com|google\.com)",
    re.IGNORECASE,
)
LEGACY_PLACE_PHOTOS_TEXT_PATTERN = re.compile(r"thirdplacesdata\.blob\.core\.windows\.net/place-photos/", re.IGNORECASE)


@dataclass
class MigrationRunConfig:
    city: str = "charlotte"
    dry_run: bool = True
    upload: bool = False
    write_airtable: bool = False
    try_url_variants: bool = True
    download_timeout_seconds: int = 20
    include_legacy_blob_candidates: bool = False
    refresh_google_photos_on_download_failure: bool = True
    recovery_entries_by_record_id: Optional[Dict[str, Dict[str, Any]]] = None
    recovery_max_source_urls: int = 10

    def to_photo_asset_config(self) -> PhotoAssetConfig:
        return PhotoAssetConfig(
            city=self.city,
            dry_run=self.dry_run,
            upload=self.upload,
            try_url_variants=self.try_url_variants,
            download_timeout_seconds=self.download_timeout_seconds,
            include_legacy_blob_candidates=self.include_legacy_blob_candidates,
        )


class AirtablePhotoClient:
    def __init__(self, table: Optional[Table] = None):
        self.table = table or Api(os.environ["AIRTABLE_PERSONAL_ACCESS_TOKEN"]).table(
            os.environ["AIRTABLE_BASE_ID"],
            TABLE_NAME,
        )

    def fetch_records(self, view: str) -> List[Dict[str, Any]]:
        return self.table.all(view=view, fields=AIRTABLE_FIELDS, sort=["-Created Time"])

    def update_photos(self, record: Dict[str, Any], selected_urls: List[str]) -> Dict[str, Any]:
        record_id = record.get("id", "")
        current_record = self.table.get(record_id)
        current_value = current_record.get("fields", {}).get(PHOTOS_FIELD)
        update_value = json.dumps(selected_urls)
        result = {
            "updated": False,
            "field_name": PHOTOS_FIELD,
            "record_id": record_id,
            "old_value": current_value,
            "new_value": update_value,
        }
        if parse_url_list(current_value) == selected_urls:
            return result

        self.table.update(record_id, {PHOTOS_FIELD: update_value})
        time.sleep(1)
        result["updated"] = True
        return result


def load_dotenv_if_present() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(AZURE_FUNCTION_DIR / ".env", override=False)


def load_local_settings(settings_file: Path) -> List[str]:
    if not settings_file.exists():
        return []
    with settings_file.open("r", encoding="utf-8") as settings_handle:
        payload = json.load(settings_handle)
    values = payload.get("Values", {})
    loaded_keys: List[str] = []
    for key, value in values.items():
        if value is None or key in os.environ:
            continue
        os.environ[key] = str(value)
        loaded_keys.append(key)
    return loaded_keys


def validate_required_env() -> None:
    missing = [key for key in REQUIRED_ENV_VARS if not os.environ.get(key)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")


def is_google_hosted_photo_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return (
        host == "google.com"
        or host.endswith(".google.com")
        or host == "googleusercontent.com"
        or host.endswith(".googleusercontent.com")
        or host == "googleapis.com"
        or host.endswith(".googleapis.com")
    )


def is_legacy_place_photo_url(url: str) -> bool:
    parsed = urlparse(url)
    return (
        parsed.scheme == "https"
        and parsed.netloc.lower() == "thirdplacesdata.blob.core.windows.net"
        and parsed.path.startswith("/place-photos/")
    )


def google_photo_occurrences(record: Dict[str, Any]) -> int:
    photos_value = record.get("fields", {}).get(PHOTOS_FIELD)
    urls = parse_url_list(photos_value)
    if urls:
        return len([url for url in urls if is_google_hosted_photo_url(url)])
    if photos_value is None:
        return 0
    return len(GOOGLE_HOST_TEXT_PATTERN.findall(json.dumps(photos_value, ensure_ascii=False)))


def legacy_place_photo_occurrences(record: Dict[str, Any]) -> int:
    photos_value = record.get("fields", {}).get(PHOTOS_FIELD)
    urls = parse_url_list(photos_value)
    if urls:
        return len([url for url in urls if is_legacy_place_photo_url(url)])
    if photos_value is None:
        return 0
    return len(LEGACY_PLACE_PHOTOS_TEXT_PATTERN.findall(json.dumps(photos_value, ensure_ascii=False)))


def count_google_photo_rows(records: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    total_rows = 0
    rows_with_google = 0
    occurrences = 0
    for record in records:
        total_rows += 1
        count = google_photo_occurrences(record)
        if count:
            rows_with_google += 1
            occurrences += count
    return {
        "total_rows_checked": total_rows,
        "rows_with_google_urls_in_photos": rows_with_google,
        "google_url_occurrences_in_photos": occurrences,
    }


def count_legacy_place_photo_rows(records: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    total_rows = 0
    rows_with_legacy = 0
    occurrences = 0
    for record in records:
        total_rows += 1
        count = legacy_place_photo_occurrences(record)
        if count:
            rows_with_legacy += 1
            occurrences += count
    return {
        "total_rows_checked": total_rows,
        "rows_with_legacy_place_photo_urls_in_photos": rows_with_legacy,
        "legacy_place_photo_url_occurrences_in_photos": occurrences,
    }


def load_recovery_manifest(manifest_path: Optional[Path]) -> Dict[str, Any]:
    if not manifest_path:
        return {}
    with manifest_path.open("r", encoding="utf-8") as manifest_handle:
        manifest = json.load(manifest_handle)
    if not isinstance(manifest, dict):
        raise ValueError("Recovery manifest must be a JSON object.")
    records = manifest.get("records", [])
    if not isinstance(records, list):
        raise ValueError("Recovery manifest records must be a list.")
    return manifest


def recovery_entries_by_record_id(manifest: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    entries: Dict[str, Dict[str, Any]] = {}
    for entry in manifest.get("records", []):
        if not isinstance(entry, dict):
            continue
        record_id = str(entry.get("record_id") or "").strip()
        if not record_id:
            raise ValueError("Every recovery manifest record needs a record_id.")
        if record_id in entries:
            raise ValueError(f"Duplicate record_id in recovery manifest: {record_id}")
        entries[record_id] = entry
    return entries


def select_target_records(records: List[Dict[str, Any]], args: argparse.Namespace) -> List[Dict[str, Any]]:
    record_id = args.record_id.strip()
    place_id = args.place_id.strip()
    recovery_record_ids: Set[str] = set(getattr(args, "recovery_record_ids", set()) or set())
    filtered = records

    if record_id:
        matched_by_record = [record for record in records if record.get("id") == record_id]
        if not matched_by_record:
            raise ValueError(f"record_id not found: {record_id}")
        filtered = matched_by_record

    if place_id:
        matched_by_place = [
            record for record in filtered
            if str(record.get("fields", {}).get("Google Maps Place Id") or "").strip() == place_id
        ]
        if not matched_by_place:
            raise ValueError(f"place_id not found for selected records: {place_id}")
        filtered = matched_by_place

    if recovery_record_ids and not record_id and not place_id:
        filtered = [record for record in filtered if record.get("id") in recovery_record_ids]
        found_ids = {record.get("id") for record in filtered}
        missing_ids = sorted(recovery_record_ids - found_ids)
        if missing_ids:
            raise ValueError(f"Recovery manifest record_id values not found: {', '.join(missing_ids)}")

    if not record_id and not place_id and not recovery_record_ids:
        if args.filter == "google-photos":
            filtered = [record for record in filtered if google_photo_occurrences(record) > 0]
        elif args.filter == "place-photos":
            filtered = [record for record in filtered if legacy_place_photo_occurrences(record) > 0]
        elif args.filter == "all-photo-ready":
            filtered = [record for record in filtered if is_photo_ready_place(record.get("fields", {}))]
        elif args.filter != "all":
            raise ValueError(f"Unsupported filter: {args.filter}")

    if args.max_places > 0:
        filtered = filtered[:args.max_places]
    return filtered


def fallback_place_data(record: Dict[str, Any]) -> Dict[str, Any]:
    fields = record.get("fields", {})
    return {
        "place_id": str(fields.get("Google Maps Place Id") or "").strip(),
        "place_name": fields.get("Place", "Unknown"),
        "photos": {},
    }


def record_without_google_photos(record: Dict[str, Any]) -> Dict[str, Any]:
    copied_record = copy.deepcopy(record)
    fields = copied_record.setdefault("fields", {})
    retained_urls = [
        url for url in parse_url_list(fields.get(PHOTOS_FIELD))
        if not is_google_hosted_photo_url(url)
    ]
    fields[PHOTOS_FIELD] = json.dumps(retained_urls)
    return copied_record


def record_without_legacy_place_photos(record: Dict[str, Any]) -> Dict[str, Any]:
    copied_record = copy.deepcopy(record)
    fields = copied_record.setdefault("fields", {})
    retained_urls = [
        url for url in parse_url_list(fields.get(PHOTOS_FIELD))
        if not is_legacy_place_photo_url(url)
    ]
    fields[PHOTOS_FIELD] = json.dumps(retained_urls)
    return copied_record


def dedupe_urls(urls: Iterable[str]) -> List[str]:
    deduped: List[str] = []
    seen_urls: Set[str] = set()
    for url in urls:
        if not isinstance(url, str) or not url.startswith("http") or url in seen_urls:
            continue
        seen_urls.add(url)
        deduped.append(url)
    return deduped


def recovery_urls_from_place_data(place_data: Dict[str, Any]) -> List[str]:
    photos_section = place_data.get("photos", {}) if isinstance(place_data, dict) else {}
    urls: List[str] = []
    if isinstance(photos_section, dict):
        urls.extend([
            url for url in parse_url_list(photos_section.get("photo_urls"))
            if is_google_hosted_photo_url(url)
        ])
        raw_data = photos_section.get("raw_data")
        raw_records = []
        if isinstance(raw_data, dict) and isinstance(raw_data.get("photos_data"), list):
            raw_records = raw_data.get("photos_data", [])
        elif isinstance(raw_data, list):
            raw_records = raw_data
        for photo_record in raw_records:
            if not isinstance(photo_record, dict):
                continue
            url = photo_record.get("photo_url_big")
            if isinstance(url, str) and is_google_hosted_photo_url(url):
                urls.append(url)
    return dedupe_urls(urls)


def apply_recovery_manifest_entry(
    place_data: Dict[str, Any],
    recovery_entry: Dict[str, Any],
    max_source_urls: int,
) -> Tuple[Dict[str, Any], List[str], Dict[str, Any]]:
    entry_urls = [
        url for url in parse_url_list(recovery_entry.get("photo_urls"))
        if is_google_hosted_photo_url(url)
    ]
    source_urls = dedupe_urls(entry_urls) or recovery_urls_from_place_data(place_data)
    effective_max = int(recovery_entry.get("max_source_urls") or max_source_urls or 0)
    if effective_max > 0:
        source_urls = source_urls[:effective_max]

    recovered_place_data = copy.deepcopy(place_data) if isinstance(place_data, dict) else {}
    photos_section = recovered_place_data.setdefault("photos", {})
    photos_section["photo_urls"] = source_urls
    photos_section["raw_data"] = {
        "photos_data": [{"photo_url_big": url, "photo_tags": [], "photo_date": ""} for url in source_urls]
    }
    summary = {
        "recovery_manifest_used": True,
        "recovery_manifest_source": recovery_entry.get("source", ""),
        "recovery_manifest_photo_url_count": len(source_urls),
        "recovery_manifest_record_id": recovery_entry.get("record_id", ""),
        "recovery_manifest_max_source_urls": effective_max,
    }
    warnings = [] if source_urls else ["recovery_manifest_source_urls_empty"]
    return recovered_place_data, warnings, summary


def fetch_fresh_google_place_photos(place_id: str, max_photos: int = 10) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    if not os.environ.get("GOOGLE_MAPS_API_KEY"):
        return None, ["fresh_google_places_photos_skipped_missing_google_maps_api_key"]
    try:
        from services.place_data_service import GoogleMapsProvider
    except Exception as exc:
        return None, [f"fresh_google_places_photos_skipped_import_failed: {exc}"]

    try:
        photos_payload = GoogleMapsProvider().get_place_photos(place_id)
    except Exception as exc:
        return None, [f"fresh_google_places_photos_failed: {exc}"]

    photo_urls = parse_url_list(photos_payload.get("photo_urls"))[:max_photos]
    if not photo_urls:
        message = photos_payload.get("message") or "no photo URLs returned"
        return None, [f"fresh_google_places_photos_empty: {message}"]

    narrowed_payload = copy.deepcopy(photos_payload)
    narrowed_payload["photo_urls"] = photo_urls
    raw_data = narrowed_payload.get("raw_data")
    if isinstance(raw_data, dict) and isinstance(raw_data.get("photos_data"), list):
        raw_data["photos_data"] = raw_data["photos_data"][:max_photos]
    return {
        "place_id": place_id,
        "photos": narrowed_payload,
    }, [f"fresh_google_places_photos_loaded: {len(photo_urls)}"]


def load_place_data(record: Dict[str, Any], data_root: Path, city: str) -> Tuple[Dict[str, Any], List[str]]:
    fields = record.get("fields", {})
    place_id = str(fields.get("Google Maps Place Id") or "").strip()
    place_file = data_root / city / f"{place_id}.json"
    if not place_file.exists():
        return fallback_place_data(record), [f"missing_local_json: {place_file}"]
    try:
        with place_file.open("r", encoding="utf-8") as place_handle:
            return json.load(place_handle), []
    except Exception as exc:
        return fallback_place_data(record), [f"invalid_local_json: {place_file}: {exc}"]


def compact_place_result(
    status: str,
    message: str,
    place_name: str,
    record_id: str,
    summary: Dict[str, Any],
    selected_airtable_urls: Optional[List[str]] = None,
    assets: Optional[List[Dict[str, Any]]] = None,
    failures: Optional[List[Dict[str, Any]]] = None,
    place_id: str = "",
    warnings: Optional[List[str]] = None,
    skip_reason: Optional[str] = None,
    error_reason: Optional[str] = None,
) -> Dict[str, Any]:
    selected_urls = [url for url in (selected_airtable_urls or []) if isinstance(url, str)]
    asset_items = [asset for asset in (assets or []) if isinstance(asset, dict)]
    failure_items = [failure for failure in (failures or []) if isinstance(failure, dict)]
    compact_summary = dict(summary or {})
    compact_summary.setdefault("selected_airtable_count", len(selected_urls))
    compact_summary.setdefault("azure_assets_count", len(asset_items))
    compact_summary.setdefault("failed_upload_count", len(failure_items))
    compact_summary.setdefault("blob_bytes", sum(int(asset.get("bytes", 0) or 0) for asset in asset_items))

    result: Dict[str, Any] = {
        "status": status,
        "message": message,
        "place_name": place_name,
        "record_id": record_id,
        "summary": compact_summary,
        "selected_airtable_count": len(selected_urls),
        "asset_count": len(asset_items),
        "failure_count": len(failure_items),
    }
    if place_id:
        result["place_id"] = place_id
    if warnings:
        result["warnings"] = warnings
    if skip_reason:
        result["skip_reason"] = skip_reason
    if error_reason:
        result["error_reason"] = error_reason
    if selected_urls:
        result["selected_airtable_url_samples"] = selected_urls[:3]
    if asset_items:
        result["asset_status_counts"] = count_by_key(asset_items, "status")
    if failure_items:
        result["failure_reason_counts"] = count_by_key(failure_items, "reason")
        error_samples = [failure.get("error") for failure in failure_items if failure.get("error")]
        if error_samples:
            result["failure_error_samples"] = error_samples[:3]
    return result


def count_by_key(items: Iterable[Dict[str, Any]], key: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in items:
        value = str(item.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return counts


def all_candidate_downloads_failed(summary: Dict[str, Any], failures: List[Dict[str, Any]]) -> bool:
    candidate_count = int(summary.get("candidate_count", 0) or 0)
    failed_upload_count = int(summary.get("failed_upload_count", 0) or 0)
    if candidate_count <= 0 or failed_upload_count < candidate_count or not failures:
        return False
    return all(failure.get("reason") == "download_failed" for failure in failures if isinstance(failure, dict))


def finalize_local_result(
    record: Dict[str, Any],
    run_config: MigrationRunConfig,
    asset_result: Dict[str, Any],
    warnings: List[str],
    airtable_client: AirtablePhotoClient,
) -> Dict[str, Any]:
    fields = record.get("fields", {})
    place_name = fields.get("Place", "Unknown")
    place_id = str(fields.get("Google Maps Place Id") or "").strip()
    record_id = record.get("id", "")
    selected_urls = asset_result.get("selected_airtable_urls", [])
    summary = asset_result.get("summary", {})
    candidate_count = int(summary.get("candidate_count", 0) or 0)
    summary.update({
        "local_data_file_used": not any(str(warning).startswith(("missing_local_json", "invalid_local_json")) for warning in warnings),
        "airtable_write_requested": run_config.write_airtable,
        "airtable_write_attempted": False,
        "airtable_update_applied": False,
        "airtable_update_skipped_no_change": False,
        "airtable_update_failed": False,
    })

    if candidate_count == 0 and not selected_urls:
        return compact_place_result(
            status="skipped",
            skip_reason="no_migratable_photo_urls",
            message=(
                "Skipped: no migratable photo URLs found after checking Airtable Photos, "
                "local data file photos.photo_urls, and raw provider photo_url_big sources."
            ),
            warnings=warnings,
            place_name=place_name,
            place_id=place_id,
            record_id=record_id,
            summary=summary,
            selected_airtable_urls=selected_urls,
            assets=asset_result.get("assets", []),
            failures=asset_result.get("failures", []),
        )

    if not run_config.dry_run and not selected_urls:
        failures = [failure for failure in asset_result.get("failures", []) if isinstance(failure, dict)]
        if all_candidate_downloads_failed(summary, failures):
            return compact_place_result(
                status="skipped",
                skip_reason="all_photo_downloads_failed",
                message="Skipped: all photo candidate downloads failed, so Airtable Photos was left unchanged.",
                warnings=warnings,
                place_name=place_name,
                place_id=place_id,
                record_id=record_id,
                summary=summary,
                selected_airtable_urls=selected_urls,
                assets=asset_result.get("assets", []),
                failures=asset_result.get("failures", []),
            )
        return compact_place_result(
            status="error",
            error_reason="no_selected_azure_urls",
            message=(
                "Migration found photo candidates but selected zero Azure Photos URLs; "
                "refusing to overwrite Airtable Photos with an empty list."
            ),
            warnings=warnings,
            place_name=place_name,
            place_id=place_id,
            record_id=record_id,
            summary=summary,
            selected_airtable_urls=selected_urls,
            assets=asset_result.get("assets", []),
            failures=asset_result.get("failures", []),
        )

    status = "would_update" if run_config.dry_run else "updated"
    message = f"Processed {candidate_count} candidates"
    if not run_config.dry_run and run_config.write_airtable:
        summary["airtable_write_attempted"] = True
        try:
            update_result = airtable_client.update_photos(record, selected_urls)
        except Exception as exc:
            summary["airtable_update_failed"] = True
            return compact_place_result(
                status="error",
                error_reason="airtable_update_failed",
                message=f"Airtable update failed: {exc}",
                warnings=warnings,
                place_name=place_name,
                place_id=place_id,
                record_id=record_id,
                summary=summary,
                selected_airtable_urls=selected_urls,
                assets=asset_result.get("assets", []),
                failures=asset_result.get("failures", []),
            )
        summary["airtable_update_applied"] = bool(update_result.get("updated", False))
        summary["airtable_update_skipped_no_change"] = not bool(update_result.get("updated", False))

    if selected_urls:
        record.setdefault("fields", {})[PHOTOS_FIELD] = json.dumps(selected_urls)

    return compact_place_result(
        status=status,
        message=message,
        warnings=warnings,
        place_name=place_name,
        place_id=place_id,
        record_id=record_id,
        summary=summary,
        selected_airtable_urls=selected_urls,
        assets=asset_result.get("assets", []),
        failures=asset_result.get("failures", []),
    )


def process_place(
    record: Dict[str, Any],
    data_root: Path,
    run_config: MigrationRunConfig,
    airtable_client: AirtablePhotoClient,
    service: Optional[PhotoAssetService] = None,
) -> Dict[str, Any]:
    fields = record.get("fields", {}) if isinstance(record, dict) else {}
    place_name = fields.get("Place", "Unknown")
    record_id = record.get("id", "") if isinstance(record, dict) else ""
    place_id = str(fields.get("Google Maps Place Id") or "").strip()
    if not record or "fields" not in record:
        return {"status": "error", "message": "Invalid place record", "summary": {}}
    if not is_photo_ready_place(fields):
        return compact_place_result(
            status="skipped",
            skip_reason="ignored_missing_place_id",
            message="Skipped: no Google Maps Place Id; photo paths ignore non-photo-ready records.",
            place_name=place_name,
            place_id=place_id,
            record_id=record_id,
            summary={"ignored_missing_place_id": True},
        )

    recovery_entry = (run_config.recovery_entries_by_record_id or {}).get(record_id)
    record_for_processing = record_without_legacy_place_photos(record) if recovery_entry else record
    place_data, warnings = load_place_data(record_for_processing, data_root, run_config.city)
    recovery_summary: Dict[str, Any] = {}
    if recovery_entry:
        place_data, recovery_warnings, recovery_summary = apply_recovery_manifest_entry(
            place_data,
            recovery_entry,
            run_config.recovery_max_source_urls,
        )
        warnings = [*warnings, *recovery_warnings]
    photo_asset_service = service or PhotoAssetService()
    photo_asset_config = run_config.to_photo_asset_config()
    place_context = photo_asset_service.prepare_place_context(record_for_processing, place_data, photo_asset_config)
    if place_context.get("status") == "skipped" and "result" in place_context:
        return place_context["result"]
    place_context["warnings"] = [*warnings, *place_context.get("warnings", [])]
    candidates = place_context.get("inventory", [])
    batch_result = photo_asset_service.process_candidate_batch(place_context, candidates, photo_asset_config)
    asset_result = photo_asset_service.finalize_place_assets(
        place_context,
        batch_result.get("assets", []),
        batch_result.get("failures", []),
    )
    if recovery_summary:
        asset_result.setdefault("summary", {}).update(recovery_summary)
    if (
        run_config.refresh_google_photos_on_download_failure
        and not run_config.dry_run
        and run_config.upload
        and all_candidate_downloads_failed(asset_result.get("summary", {}), asset_result.get("failures", []))
    ):
        stale_summary = asset_result.get("summary", {})
        fresh_place_data, refresh_warnings = fetch_fresh_google_place_photos(place_id, max_photos=10)
        place_context["warnings"] = [*place_context.get("warnings", []), *refresh_warnings]
        if fresh_place_data:
            retry_source_record = record_for_processing if recovery_entry else record
            retry_record = record_without_google_photos(retry_source_record)
            retry_context = photo_asset_service.prepare_place_context(retry_record, fresh_place_data, photo_asset_config)
            retry_context["warnings"] = [*place_context.get("warnings", []), *retry_context.get("warnings", [])]
            retry_candidates = retry_context.get("inventory", [])
            retry_batch_result = photo_asset_service.process_candidate_batch(retry_context, retry_candidates, photo_asset_config)
            asset_result = photo_asset_service.finalize_place_assets(
                retry_context,
                retry_batch_result.get("assets", []),
                retry_batch_result.get("failures", []),
            )
            asset_result.setdefault("summary", {}).update({
                "retried_with_fresh_google_places_photos": True,
                "fresh_google_places_photo_limit": 10,
                "stale_candidate_count": stale_summary.get("candidate_count", 0),
                "stale_failed_upload_count": stale_summary.get("failed_upload_count", 0),
            })
            place_context = retry_context
    return finalize_local_result(record, run_config, asset_result, place_context.get("warnings", []), airtable_client)


def aggregate_migration_results(results: List[Dict[str, Any]], dry_run: bool) -> Dict[str, Any]:
    errors = len([result for result in results if result.get("status") == "error"])
    return {
        "total_places": len(results),
        "dry_run": dry_run,
        "updated": len([result for result in results if result.get("status") == "updated"]),
        "would_update": len([result for result in results if result.get("status") == "would_update"]),
        "skipped": len([result for result in results if result.get("status") == "skipped"]),
        "errors": errors,
        "candidate_count": sum(int(result.get("summary", {}).get("candidate_count", 0) or 0) for result in results),
        "azure_assets_count": sum(int(result.get("summary", {}).get("azure_assets_count", 0) or 0) for result in results),
        "selected_airtable_count": sum(int(result.get("summary", {}).get("selected_airtable_count", 0) or 0) for result in results),
        "failed_upload_count": sum(int(result.get("summary", {}).get("failed_upload_count", 0) or 0) for result in results),
        "blob_bytes": sum(int(result.get("summary", {}).get("blob_bytes", 0) or 0) for result in results),
        "airtable_write_requested": sum(1 for result in results if result.get("summary", {}).get("airtable_write_requested")),
        "airtable_write_attempted": sum(1 for result in results if result.get("summary", {}).get("airtable_write_attempted")),
        "airtable_updates_applied": sum(1 for result in results if result.get("summary", {}).get("airtable_update_applied")),
        "airtable_updates_skipped_no_change": sum(1 for result in results if result.get("summary", {}).get("airtable_update_skipped_no_change")),
        "airtable_update_failures": sum(1 for result in results if result.get("summary", {}).get("airtable_update_failed")),
        "success": errors == 0,
    }


def run_audit(records: List[Dict[str, Any]], city: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    results = [audit_single_place_photo_assets({"place": record, "config": {"city": city}}) for record in records]
    return results, _aggregate_audit_results(results)


def write_report(report_path: Path, report: Dict[str, Any]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as report_handle:
        json.dump(report, report_handle, indent=2)
        report_handle.write("\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate Airtable photo URLs to canonical Azure Blob URLs locally.")
    parser.add_argument("--city", default="charlotte")
    parser.add_argument("--view", default="All")
    parser.add_argument("--settings-file", type=Path, default=AZURE_FUNCTION_DIR / "local.settings.json")
    parser.add_argument("--data-root", type=Path, default=REPO_ROOT / "data" / "places")
    parser.add_argument("--filter", choices=["google-photos", "place-photos", "all-photo-ready", "all"], default="google-photos")
    parser.add_argument("--record-id", default="")
    parser.add_argument("--place-id", default="")
    parser.add_argument("--max-places", "--limit", dest="max_places", type=int, default=0)
    parser.add_argument(
        "--recovery-manifest",
        type=Path,
        help="Targeted recovery manifest with Airtable record IDs and optional replacement source photo URLs.",
    )
    parser.add_argument(
        "--recovery-max-photos",
        type=int,
        default=10,
        help="Maximum recovered Google source URLs to use per manifest record unless overridden by the manifest entry.",
    )
    parser.add_argument("--write", action="store_true", help="Apply Azure uploads and Airtable writes. Default is dry-run.")
    parser.add_argument(
        "--confirm-write",
        "--confirm-production",
        dest="confirm_write",
        action="store_true",
        help="Required for broad writes.",
    )
    parser.add_argument("--try-url-variants", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--download-timeout-seconds", type=int, default=20)
    parser.add_argument(
        "--refresh-google-photos",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="On write, fetch up to 10 fresh Google Places photo media URLs if all stored photo downloads fail.",
    )
    parser.add_argument(
        "--include-legacy-blobs",
        action="store_true",
        help="Also scan legacy Azure place-photos and curator-photos containers for candidates.",
    )
    parser.add_argument("--audit", action="store_true")
    parser.add_argument("--no-audit", action="store_true")
    parser.add_argument("--report-json", type=Path)
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def require_broad_write_confirmation(args: argparse.Namespace) -> None:
    record_id = str(getattr(args, "record_id", "") or "").strip()
    place_id = str(getattr(args, "place_id", "") or "").strip()
    targeted = bool(record_id or place_id or getattr(args, "recovery_manifest", None))
    limited = int(args.max_places or 0) > 0
    if args.write and not targeted and not limited and not args.confirm_write:
        raise RuntimeError("Broad writes require --confirm-write.")


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(levelname)s %(message)s")
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    try:
        require_broad_write_confirmation(args)
        load_dotenv_if_present()
        loaded_settings = load_local_settings(args.settings_file.resolve())
        validate_required_env()

        recovery_manifest = load_recovery_manifest(args.recovery_manifest.resolve() if args.recovery_manifest else None)
        recovery_entries = recovery_entries_by_record_id(recovery_manifest) if recovery_manifest else {}
        args.recovery_record_ids = set(recovery_entries.keys())

        dry_run = not args.write
        run_config = MigrationRunConfig(
            city=args.city,
            dry_run=dry_run,
            upload=args.write,
            write_airtable=args.write,
            try_url_variants=args.try_url_variants,
            download_timeout_seconds=args.download_timeout_seconds,
            include_legacy_blob_candidates=args.include_legacy_blobs,
            refresh_google_photos_on_download_failure=args.refresh_google_photos,
            recovery_entries_by_record_id=recovery_entries,
            recovery_max_source_urls=args.recovery_max_photos,
        )
        airtable_client = AirtablePhotoClient()
        all_records = airtable_client.fetch_records(args.view)
        baseline_google_counts = count_google_photo_rows(all_records)
        baseline_legacy_place_counts = count_legacy_place_photo_rows(all_records)
        target_records = select_target_records(all_records, args)

        print(json.dumps({
            "settings_loaded": sorted([key for key in loaded_settings if key in REQUIRED_ENV_VARS]),
            "view": args.view,
            "dry_run": dry_run,
            "filter": args.filter,
            "records_fetched": len(all_records),
            "records_targeted": len(target_records),
            "recovery_manifest": str(args.recovery_manifest.resolve()) if args.recovery_manifest else "",
            "recovery_manifest_records": len(recovery_entries),
            "recovery_max_photos": args.recovery_max_photos,
            "include_legacy_blob_candidates": run_config.include_legacy_blob_candidates,
            "refresh_google_photos_on_download_failure": run_config.refresh_google_photos_on_download_failure,
            "baseline_google_photos": baseline_google_counts,
            "baseline_legacy_place_photos": baseline_legacy_place_counts,
        }, indent=2))

        photo_asset_service = PhotoAssetService()
        migration_results: List[Dict[str, Any]] = []
        for record in target_records:
            result = process_place(record, args.data_root.resolve(), run_config, airtable_client, photo_asset_service)
            migration_results.append(result)
            print(f"{result.get('status')}: {result.get('place_name')} ({result.get('record_id')}) - {result.get('message')}")

        should_audit = args.audit or (args.write and not args.no_audit)
        audit_results: List[Dict[str, Any]] = []
        audit_totals: Dict[str, Any] = {}
        if should_audit:
            audit_results, audit_totals = run_audit(target_records, args.city)

        post_google_counts = None
        post_legacy_place_counts = None
        if args.write:
            post_write_records = airtable_client.fetch_records(args.view)
            post_google_counts = count_google_photo_rows(post_write_records)
            post_legacy_place_counts = count_legacy_place_photo_rows(post_write_records)

        report = {
            "migration_totals": aggregate_migration_results(migration_results, dry_run),
            "status_counts": count_by_key(migration_results, "status"),
            "baseline_google_photos": baseline_google_counts,
            "baseline_legacy_place_photos": baseline_legacy_place_counts,
            "post_write_google_photos": post_google_counts,
            "post_write_legacy_place_photos": post_legacy_place_counts,
            "recovery_manifest": recovery_manifest,
            "curator_status_counts": {},
            "audit_totals": audit_totals,
            "migration_results": migration_results,
            "curator_results": [],
            "audit_results": audit_results,
        }
        if args.report_json:
            write_report(args.report_json.resolve(), report)
            print(f"Wrote report: {args.report_json.resolve()}")
        print(json.dumps({
            "migration_totals": report["migration_totals"],
            "status_counts": report["status_counts"],
            "post_write_google_photos": post_google_counts,
            "post_write_legacy_place_photos": post_legacy_place_counts,
            "audit_totals": audit_totals,
        }, indent=2))

        if report["migration_totals"].get("errors"):
            return 2
        if audit_totals and not audit_totals.get("success", False):
            return 3
        return 0
    except KeyboardInterrupt:
        logging.warning("Local photo migration interrupted by user.")
        return 130
    except Exception as exc:
        logging.error("Local photo migration failed: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())