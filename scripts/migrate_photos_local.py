from __future__ import annotations

import argparse
import ast
import copy
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Lock, local
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
AZURE_FUNCTION_DIR = REPO_ROOT / "azure-function"
if str(AZURE_FUNCTION_DIR) not in sys.path:
    sys.path.insert(0, str(AZURE_FUNCTION_DIR))

from pyairtable import Api, Table  # noqa: E402

from blueprints.photo_assets import _aggregate_audit_results, audit_single_place_photo_assets  # noqa: E402
from services.photo_asset_service import (  # noqa: E402
    AZURE_ACCOUNT_HOST,
    PhotoAssetConfig,
    PhotoAssetService,
    classify_azure_photo_url,
    is_photo_ready_place,
    parse_url_list,
)
from services.photo_publisher_service import (  # noqa: E402
    PHOTOS_CONTAINER,
    PhotoPublisherService,
    canonical_photo_url,
)
from services.utils import delete_blob_from_container_with_status, list_blobs_in_container  # noqa: E402


TABLE_NAME = "Charlotte Third Places"
PHOTOS_FIELD = "Photos"
PHOTOS_BACKUP_FIELD = "Photos Backup"
AIRTABLE_FIELDS = [
    "Place",
    "Google Maps Place Id",
    PHOTOS_FIELD,
    "Curator Photos",
]
PHOTO_COUNT_AUDIT_FIELDS = [
    "Place",
    "Google Maps Place Id",
    PHOTOS_FIELD,
    PHOTOS_BACKUP_FIELD,
]
PHOTO_REPAIR_FIELDS = PHOTO_COUNT_AUDIT_FIELDS
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
DEFAULT_MAX_WORKERS = 5
AIRTABLE_WRITE_INTERVAL_SECONDS = 0.25
DISPLAY_VARIANT_CATEGORIES = {"new_display_variant_standard", "new_display_variant_curator"}
THUMBNAIL_VARIANT_CATEGORIES = {"new_thumbnail_variant_standard", "new_thumbnail_variant_curator"}
ROOT_AZURE_CATEGORIES = {"new_standard", "new_curator"}


def photo_manifest_from_url(url: str) -> Dict[str, str]:
    return {"display": url, "thumbnail": url}


def parse_photo_manifest_list(value: Any) -> List[Dict[str, str]]:
    # Migration-only compatibility for old Photos/Photos Backup values.
    if isinstance(value, list):
        parsed_value = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed_value = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            try:
                parsed_value = ast.literal_eval(value)
            except (ValueError, SyntaxError):
                return []
    else:
        return []

    if not isinstance(parsed_value, list):
        return []

    manifests: List[Dict[str, str]] = []
    for item in parsed_value:
        if isinstance(item, str) and item.startswith("http"):
            manifests.append(photo_manifest_from_url(item))
        elif isinstance(item, dict):
            display_url = str(item.get("display") or "").strip()
            thumbnail_url = str(item.get("thumbnail") or "").strip()
            if display_url.startswith("http") and thumbnail_url.startswith("http"):
                manifests.append({"display": display_url, "thumbnail": thumbnail_url})
    return manifests


@dataclass
class MigrationRunConfig:
    city: str = "charlotte"
    dry_run: bool = True
    upload: bool = False
    write_airtable: bool = False
    try_url_variants: bool = True
    download_timeout_seconds: int = 20
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
        )


class AirtableWriteLimiter:
    def __init__(self, min_interval_seconds: float = AIRTABLE_WRITE_INTERVAL_SECONDS):
        self.min_interval_seconds = min_interval_seconds
        self._lock = Lock()
        self._next_write_at = 0.0

    def wait(self) -> None:
        if self.min_interval_seconds <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait_seconds = max(0.0, self._next_write_at - now)
            if wait_seconds > 0:
                time.sleep(wait_seconds)
                now = time.monotonic()
            self._next_write_at = now + self.min_interval_seconds


class AirtablePhotoClient:
    def __init__(self, table: Optional[Table] = None, write_limiter: Optional[AirtableWriteLimiter] = None):
        self.table = table or Api(os.environ["AIRTABLE_PERSONAL_ACCESS_TOKEN"]).table(
            os.environ["AIRTABLE_BASE_ID"],
            TABLE_NAME,
        )
        self.write_limiter = write_limiter

    def fetch_records(self, view: str, fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        return self.table.all(view=view, fields=fields or AIRTABLE_FIELDS, sort=["-Created Time"])

    def fetch_photo_count_audit_records(self, view: str) -> List[Dict[str, Any]]:
        return self.fetch_records(view, fields=PHOTO_COUNT_AUDIT_FIELDS)

    def fetch_photo_repair_records(self, view: str) -> List[Dict[str, Any]]:
        return self.fetch_records(view, fields=PHOTO_REPAIR_FIELDS)

    def update_photos(self, record: Dict[str, Any], selected_photos: List[Dict[str, str]]) -> Dict[str, Any]:
        record_id = record.get("id", "")
        current_fields = record.get("fields", {}) if isinstance(record, dict) else {}
        current_value = current_fields.get(PHOTOS_FIELD)
        update_value = json.dumps(selected_photos)
        result = {
            "updated": False,
            "field_name": PHOTOS_FIELD,
            "record_id": record_id,
            "old_value": current_value,
            "new_value": update_value,
        }

        if parse_photo_manifest_list(current_value) == selected_photos:
            return result

        # This migration writes only the live Photos field. Any archival process
        # for previous values is intentionally outside this script.
        if self.write_limiter:
            self.write_limiter.wait()
        self.table.update(record_id, {PHOTOS_FIELD: update_value})
        if not self.write_limiter:
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


def google_photo_occurrences(record: Dict[str, Any]) -> int:
    photos_value = record.get("fields", {}).get(PHOTOS_FIELD)
    urls = parse_url_list(photos_value)
    if urls:
        return len([url for url in urls if is_google_hosted_photo_url(url)])
    if photos_value is None:
        return 0
    return len(GOOGLE_HOST_TEXT_PATTERN.findall(json.dumps(photos_value, ensure_ascii=False)))


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


def count_photo_entries(value: Any) -> int:
    return len(parse_photo_manifest_list(value))


def audit_photos_backup_counts(records: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    records_checked = 0
    mismatches: List[Dict[str, Any]] = []
    for record in records:
        records_checked += 1
        fields = record.get("fields", {}) if isinstance(record, dict) else {}
        photos_count = count_photo_entries(fields.get(PHOTOS_FIELD))
        photos_backup_count = count_photo_entries(fields.get(PHOTOS_BACKUP_FIELD))
        if photos_count == photos_backup_count:
            continue
        mismatches.append({
            "place_name": fields.get("Place", "Unknown"),
            "place_id": str(fields.get("Google Maps Place Id") or "").strip(),
            "record_id": record.get("id", "") if isinstance(record, dict) else "",
            "photos_count": photos_count,
            "photos_backup_count": photos_backup_count,
            "diff": photos_count - photos_backup_count,
        })

    mismatch_count = len(mismatches)
    return {
        "success": mismatch_count == 0,
        "records_checked": records_checked,
        "matched_count": records_checked - mismatch_count,
        "mismatch_count": mismatch_count,
        "mismatches": mismatches,
    }


def print_photos_backup_count_audit(report: Dict[str, Any]) -> None:
    print(json.dumps({
        "success": report.get("success", False),
        "records_checked": report.get("records_checked", 0),
        "matched_count": report.get("matched_count", 0),
        "mismatch_count": report.get("mismatch_count", 0),
    }, indent=2))
    for mismatch in report.get("mismatches", []):
        print(
            f"{mismatch.get('place_name')} "
            f"({mismatch.get('place_id') or 'missing place id'}, {mismatch.get('record_id')}): "
            f"Photos={mismatch.get('photos_count')} "
            f"Photos Backup={mismatch.get('photos_backup_count')} "
            f"diff={mismatch.get('diff')}"
        )


def parse_jsonish_list(value: Any, field_name: str) -> Tuple[List[Any], List[str]]:
    if isinstance(value, list):
        return value, []
    if value is None or (isinstance(value, str) and not value.strip()):
        return [], []
    if not isinstance(value, str):
        return [], [f"{field_name}_not_list"]
    try:
        parsed_value = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        try:
            parsed_value = ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return [], [f"{field_name}_invalid_json"]
    if not isinstance(parsed_value, list):
        return [], [f"{field_name}_not_list"]
    return parsed_value, []


def parse_photos_backup_entries(value: Any) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
    raw_items, parse_errors = parse_jsonish_list(value, PHOTOS_BACKUP_FIELD)
    entries: List[Dict[str, Any]] = []
    warnings: List[str] = []
    errors = list(parse_errors)
    seen_keys: Set[str] = set()
    duplicate_count = 0

    for index, item in enumerate(raw_items):
        source_path = f"fields.{PHOTOS_BACKUP_FIELD}[{index}]"
        entry: Optional[Dict[str, Any]] = None
        if isinstance(item, str):
            source_url = item.strip()
            if not source_url.startswith("http"):
                errors.append(f"{source_path}: non_http_url")
                continue
            entry = {
                "kind": "source_url",
                "source_url": source_url,
                "source_path": source_path,
                "index": index,
            }
        elif isinstance(item, dict):
            display_url = str(item.get("display") or "").strip()
            thumbnail_url = str(item.get("thumbnail") or "").strip()
            if not display_url.startswith("http") or not thumbnail_url.startswith("http"):
                errors.append(f"{source_path}: missing_display_or_thumbnail_url")
                continue
            entry = {
                "kind": "manifest",
                "photo_manifest": {"display": display_url, "thumbnail": thumbnail_url},
                "source_path": source_path,
                "index": index,
            }
        else:
            errors.append(f"{source_path}: unsupported_entry_type")
            continue

        duplicate_key = entry.get("source_url") or entry.get("photo_manifest", {}).get("display", "")
        if duplicate_key in seen_keys:
            duplicate_count += 1
        seen_keys.add(duplicate_key)
        entries.append(entry)

    if duplicate_count:
        warnings.append(f"photos_backup_duplicate_entries: {duplicate_count}")
    return entries, warnings, errors


def served_manifest_is_valid_for_place(photo_manifest: Dict[str, str], city: str, place_id: str) -> bool:
    display_classification = classify_azure_photo_url(photo_manifest.get("display", ""), city, place_id)
    thumbnail_classification = classify_azure_photo_url(photo_manifest.get("thumbnail", ""), city, place_id)
    return (
        display_classification.get("category") in DISPLAY_VARIANT_CATEGORIES
        and display_classification.get("reason") == "valid"
        and thumbnail_classification.get("category") in THUMBNAIL_VARIANT_CATEGORIES
        and thumbnail_classification.get("reason") == "valid"
    )


def source_hash_from_azure_photo_url(url: str, city: str, place_id: str) -> Optional[str]:
    classification = classify_azure_photo_url(url, city, place_id)
    if classification.get("category") not in ROOT_AZURE_CATEGORIES or classification.get("reason") != "valid":
        return None
    filename = classification.get("blob_path", "").rsplit("/", 1)[-1]
    if not filename:
        return None
    return filename.rsplit(".", 1)[0]


def manifest_from_served_variant_url(url: str, city: str, place_id: str) -> Optional[Dict[str, str]]:
    classification = classify_azure_photo_url(url, city, place_id)
    if classification.get("category") not in DISPLAY_VARIANT_CATEGORIES | THUMBNAIL_VARIANT_CATEGORIES:
        return None
    if classification.get("reason") != "valid":
        return None
    blob_path = classification.get("blob_path", "")
    parts = blob_path.split("/", 2)
    if len(parts) != 3:
        return None
    filename = parts[2]
    return {
        "display": canonical_photo_url(f"{place_id}/display/{filename}"),
        "thumbnail": canonical_photo_url(f"{place_id}/thumbnail/{filename}"),
    }


def publish_backup_source_entry(
    entry: Dict[str, Any],
    record: Dict[str, Any],
    run_config: MigrationRunConfig,
    publisher: PhotoPublisherService,
) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    fields = record.get("fields", {}) if isinstance(record, dict) else {}
    place_id = str(fields.get("Google Maps Place Id") or "").strip()
    place_name = fields.get("Place", "Unknown")
    record_id = record.get("id", "") if isinstance(record, dict) else ""
    source_url = entry.get("source_url", "")

    served_manifest = manifest_from_served_variant_url(source_url, run_config.city, place_id)
    if served_manifest:
        return served_manifest, {
            "status": "existing_manifest",
            "photo_manifest": served_manifest,
            "source_url": source_url,
            "source_path": entry.get("source_path", ""),
        }, None

    publish_result = publisher.publish_standard_url(
        source_url,
        place_id,
        record_id,
        place_name,
        source_hash=source_hash_from_azure_photo_url(source_url, run_config.city, place_id),
        source_field=PHOTOS_BACKUP_FIELD,
        source_path=entry.get("source_path", ""),
        dry_run=run_config.dry_run,
        upload=run_config.upload,
        try_url_variants=run_config.try_url_variants,
        download_timeout_seconds=run_config.download_timeout_seconds,
    )
    if not publish_result.get("success"):
        return None, None, {
            "reason": publish_result.get("status", "publish_failed"),
            "error": publish_result.get("error", "publish failed"),
            "source_url": source_url,
            "source_path": entry.get("source_path", ""),
            "attempts": publish_result.get("attempts", []),
        }
    return publish_result.get("photo_manifest"), publish_result, None


def repair_photos_from_backup_record(
    record: Dict[str, Any],
    run_config: MigrationRunConfig,
    airtable_client: AirtablePhotoClient,
    publisher: Optional[PhotoPublisherService] = None,
) -> Dict[str, Any]:
    fields = record.get("fields", {}) if isinstance(record, dict) else {}
    place_name = fields.get("Place", "Unknown")
    place_id = str(fields.get("Google Maps Place Id") or "").strip()
    record_id = record.get("id", "") if isinstance(record, dict) else ""
    if not record or "fields" not in record:
        return {"status": "error", "message": "Invalid place record", "summary": {}}
    if not is_photo_ready_place(fields):
        return compact_place_result(
            status="skipped",
            skip_reason="ignored_missing_place_id",
            message="Skipped: no Google Maps Place Id; backup repair requires photo-ready records.",
            place_name=place_name,
            place_id=place_id,
            record_id=record_id,
            summary={"ignored_missing_place_id": True},
        )

    entries, warnings, parse_errors = parse_photos_backup_entries(fields.get(PHOTOS_BACKUP_FIELD))
    current_photos = parse_photo_manifest_list(fields.get(PHOTOS_FIELD))
    summary: Dict[str, Any] = {
        "place_name": place_name,
        "place_id": place_id,
        "record_id": record_id,
        "current_photos_count": len(current_photos),
        "photos_backup_count": len(entries),
        "desired_photos_count": 0,
        "local_data_file_used": False,
        "provider_raw_sources_used": False,
        "photos_backup_read_only": True,
        "airtable_write_requested": run_config.write_airtable,
        "airtable_write_attempted": False,
        "airtable_update_applied": False,
        "airtable_update_skipped_no_change": False,
        "airtable_update_failed": False,
        "warnings": warnings,
    }
    if parse_errors:
        summary["failed_upload_count"] = len(parse_errors)
        return compact_place_result(
            status="error",
            error_reason="invalid_photos_backup",
            message="Photos Backup could not be parsed into repair entries.",
            warnings=[*warnings, *parse_errors],
            place_name=place_name,
            place_id=place_id,
            record_id=record_id,
            summary=summary,
            failures=[{"reason": "invalid_photos_backup", "error": error} for error in parse_errors],
        )
    if not entries:
        return compact_place_result(
            status="skipped",
            skip_reason="skipped_empty_backup",
            message="Skipped: Photos Backup is empty, so live Photos was left unchanged.",
            warnings=warnings,
            place_name=place_name,
            place_id=place_id,
            record_id=record_id,
            summary=summary,
        )

    photo_publisher = publisher or PhotoPublisherService()
    desired_photos: List[Dict[str, str]] = []
    assets: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for entry in entries:
        if entry.get("kind") == "manifest":
            photo_manifest = entry.get("photo_manifest", {})
            if not served_manifest_is_valid_for_place(photo_manifest, run_config.city, place_id):
                failures.append({
                    "reason": "invalid_backup_manifest",
                    "error": "Backup manifest is not a valid display/thumbnail Azure pair for this place.",
                    "source_path": entry.get("source_path", ""),
                })
                continue
            desired_photos.append(photo_manifest)
            assets.append({"status": "existing_manifest", "photo_manifest": photo_manifest, "source_path": entry.get("source_path", "")})
            continue

        photo_manifest, asset, failure = publish_backup_source_entry(entry, record, run_config, photo_publisher)
        if failure:
            failures.append(failure)
            continue
        if photo_manifest:
            desired_photos.append(photo_manifest)
        if asset:
            assets.append(asset)

    summary.update({
        "desired_photos_count": len(desired_photos),
        "selected_airtable_count": len(desired_photos),
        "azure_assets_count": len(assets),
        "failed_upload_count": len(failures),
        "blob_bytes": sum(int(asset.get("bytes", 0) or 0) for asset in assets),
    })
    if failures:
        return compact_place_result(
            status="error",
            error_reason="photos_backup_repair_failed",
            message="Photos Backup repair failed; live Photos was left unchanged.",
            warnings=warnings,
            place_name=place_name,
            place_id=place_id,
            record_id=record_id,
            summary=summary,
            selected_airtable_photos=desired_photos,
            assets=assets,
            failures=failures,
        )
    if len(desired_photos) != len(entries):
        return compact_place_result(
            status="error",
            error_reason="photos_backup_count_mismatch",
            message="Repair did not produce exactly one Photos manifest per Photos Backup entry.",
            warnings=warnings,
            place_name=place_name,
            place_id=place_id,
            record_id=record_id,
            summary=summary,
            selected_airtable_photos=desired_photos,
            assets=assets,
        )
    if current_photos == desired_photos:
        summary["airtable_update_skipped_no_change"] = True
        return compact_place_result(
            status="skipped",
            skip_reason="skipped_no_change",
            message="Skipped: live Photos already matches Photos Backup repair output.",
            warnings=warnings,
            place_name=place_name,
            place_id=place_id,
            record_id=record_id,
            summary=summary,
            selected_airtable_photos=desired_photos,
            assets=assets,
        )

    status = "would_update" if run_config.dry_run else "updated"
    if not run_config.dry_run and run_config.write_airtable:
        summary["airtable_write_attempted"] = True
        try:
            update_result = airtable_client.update_photos(record, desired_photos)
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
                selected_airtable_photos=desired_photos,
                assets=assets,
            )
        summary["airtable_update_applied"] = bool(update_result.get("updated", False))
        summary["airtable_update_skipped_no_change"] = not bool(update_result.get("updated", False))
        record.setdefault("fields", {})[PHOTOS_FIELD] = json.dumps(desired_photos)

    return compact_place_result(
        status=status,
        message="Repaired Photos from live Photos Backup." if status == "updated" else "Would repair Photos from live Photos Backup.",
        warnings=warnings,
        place_name=place_name,
        place_id=place_id,
        record_id=record_id,
        summary=summary,
        selected_airtable_photos=desired_photos,
        assets=assets,
    )


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
        elif args.filter == "all-photo-ready":
            filtered = [record for record in filtered if is_photo_ready_place(record.get("fields", {}))]
        elif args.filter != "all":
            raise ValueError(f"Unsupported filter: {args.filter}")

    if args.max_places > 0:
        filtered = filtered[:args.max_places]
    return filtered


def select_live_airtable_records(records: List[Dict[str, Any]], args: argparse.Namespace) -> List[Dict[str, Any]]:
    record_id = args.record_id.strip()
    place_id = args.place_id.strip()
    filtered = records

    if record_id:
        filtered = [record for record in filtered if record.get("id") == record_id]
        if not filtered:
            raise ValueError(f"record_id not found: {record_id}")

    if place_id:
        filtered = [
            record for record in filtered
            if str(record.get("fields", {}).get("Google Maps Place Id") or "").strip() == place_id
        ]
        if not filtered:
            raise ValueError(f"place_id not found for selected records: {place_id}")

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
    selected_airtable_photos: Optional[List[Dict[str, str]]] = None,
    assets: Optional[List[Dict[str, Any]]] = None,
    failures: Optional[List[Dict[str, Any]]] = None,
    place_id: str = "",
    warnings: Optional[List[str]] = None,
    skip_reason: Optional[str] = None,
    error_reason: Optional[str] = None,
) -> Dict[str, Any]:
    selected_urls = [url for url in (selected_airtable_urls or []) if isinstance(url, str)]
    selected_photos = [photo for photo in (selected_airtable_photos or []) if isinstance(photo, dict)]
    selected_count = len(selected_photos) if selected_photos else len(selected_urls)
    asset_items = [asset for asset in (assets or []) if isinstance(asset, dict)]
    failure_items = [failure for failure in (failures or []) if isinstance(failure, dict)]
    compact_summary = dict(summary or {})
    compact_summary.setdefault("selected_airtable_count", selected_count)
    compact_summary.setdefault("azure_assets_count", len(asset_items))
    compact_summary.setdefault("failed_upload_count", len(failure_items))
    compact_summary.setdefault("blob_bytes", sum(int(asset.get("bytes", 0) or 0) for asset in asset_items))

    result: Dict[str, Any] = {
        "status": status,
        "message": message,
        "place_name": place_name,
        "record_id": record_id,
        "summary": compact_summary,
        "selected_airtable_count": selected_count,
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
    if selected_photos:
        result["selected_airtable_photo_samples"] = selected_photos[:3]
        if any(str(asset.get("status") or "").startswith("would_upload") for asset in asset_items):
            result["selected_airtable_photo_samples_status"] = "planned_not_uploaded"
            result["selected_airtable_photo_samples_note"] = (
                "Dry-run planned URLs; display and thumbnail blobs may return 404 until --write uploads them."
            )
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
    selected_photos = asset_result.get("selected_airtable_photos") or [photo_manifest_from_url(url) for url in selected_urls if isinstance(url, str)]
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
            selected_airtable_photos=selected_photos,
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
                selected_airtable_photos=selected_photos,
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
            selected_airtable_photos=selected_photos,
            assets=asset_result.get("assets", []),
            failures=asset_result.get("failures", []),
        )

    status = "would_update" if run_config.dry_run else "updated"
    message = f"Processed {candidate_count} candidates"
    if not run_config.dry_run and run_config.write_airtable:
        summary["airtable_write_attempted"] = True
        try:
            update_result = airtable_client.update_photos(record, selected_photos)
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
                selected_airtable_photos=selected_photos,
                assets=asset_result.get("assets", []),
                failures=asset_result.get("failures", []),
            )
        summary["airtable_update_applied"] = bool(update_result.get("updated", False))
        summary["airtable_update_skipped_no_change"] = not bool(update_result.get("updated", False))

    if selected_urls:
        record.setdefault("fields", {})[PHOTOS_FIELD] = json.dumps(selected_photos)

    return compact_place_result(
        status=status,
        message=message,
        warnings=warnings,
        place_name=place_name,
        place_id=place_id,
        record_id=record_id,
        summary=summary,
        selected_airtable_urls=selected_urls,
        selected_airtable_photos=selected_photos,
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
    place_data, warnings = load_place_data(record, data_root, run_config.city)
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
    place_context = photo_asset_service.prepare_place_context(record, place_data, photo_asset_config)
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
            retry_record = record_without_google_photos(record)
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


def process_target_records(
    target_records: List[Dict[str, Any]],
    data_root: Path,
    run_config: MigrationRunConfig,
    max_workers: int = DEFAULT_MAX_WORKERS,
    airtable_client_factory: Optional[Callable[[Optional[AirtableWriteLimiter]], Any]] = None,
    photo_asset_service_factory: Optional[Callable[[], Any]] = None,
    progress_callback: Optional[Callable[[int, int, Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")

    def make_airtable_client(write_limiter: Optional[AirtableWriteLimiter] = None):
        if airtable_client_factory:
            return airtable_client_factory(write_limiter)
        return AirtablePhotoClient(write_limiter=write_limiter)

    def make_photo_asset_service():
        if photo_asset_service_factory:
            return photo_asset_service_factory()
        return PhotoAssetService()

    total_records = len(target_records)
    if max_workers == 1 or total_records <= 1:
        airtable_client = make_airtable_client(None)
        photo_asset_service = make_photo_asset_service()
        migration_results: List[Dict[str, Any]] = []
        for record in target_records:
            result = process_place(record, data_root, run_config, airtable_client, photo_asset_service)
            migration_results.append(result)
            if progress_callback:
                progress_callback(len(migration_results), total_records, result)
        return migration_results

    worker_state = local()
    write_limiter = AirtableWriteLimiter()
    migration_results: List[Optional[Dict[str, Any]]] = [None] * total_records

    def worker(record: Dict[str, Any]) -> Dict[str, Any]:
        if not hasattr(worker_state, "airtable_client"):
            worker_state.airtable_client = make_airtable_client(write_limiter)
        if not hasattr(worker_state, "photo_asset_service"):
            worker_state.photo_asset_service = make_photo_asset_service()
        return process_place(
            record,
            data_root,
            run_config,
            worker_state.airtable_client,
            worker_state.photo_asset_service,
        )

    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, record): index for index, record in enumerate(target_records)}
        for future in as_completed(futures):
            index = futures[future]
            result = future.result()
            migration_results[index] = result
            completed += 1
            if progress_callback:
                progress_callback(completed, total_records, result)

    return [result for result in migration_results if result is not None]


def process_backup_repair_records(
    target_records: List[Dict[str, Any]],
    run_config: MigrationRunConfig,
    max_workers: int = DEFAULT_MAX_WORKERS,
    airtable_client_factory: Optional[Callable[[Optional[AirtableWriteLimiter]], Any]] = None,
    publisher_factory: Optional[Callable[[], Any]] = None,
    progress_callback: Optional[Callable[[int, int, Dict[str, Any]], None]] = None,
) -> List[Dict[str, Any]]:
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")

    def make_airtable_client(write_limiter: Optional[AirtableWriteLimiter] = None):
        if airtable_client_factory:
            return airtable_client_factory(write_limiter)
        return AirtablePhotoClient(write_limiter=write_limiter)

    def make_publisher():
        if publisher_factory:
            return publisher_factory()
        return PhotoPublisherService()

    total_records = len(target_records)
    if max_workers == 1 or total_records <= 1:
        airtable_client = make_airtable_client(None)
        publisher = make_publisher()
        repair_results: List[Dict[str, Any]] = []
        for record in target_records:
            result = repair_photos_from_backup_record(record, run_config, airtable_client, publisher)
            repair_results.append(result)
            if progress_callback:
                progress_callback(len(repair_results), total_records, result)
        return repair_results

    worker_state = local()
    write_limiter = AirtableWriteLimiter()
    repair_results: List[Optional[Dict[str, Any]]] = [None] * total_records

    def worker(record: Dict[str, Any]) -> Dict[str, Any]:
        if not hasattr(worker_state, "airtable_client"):
            worker_state.airtable_client = make_airtable_client(write_limiter)
        if not hasattr(worker_state, "publisher"):
            worker_state.publisher = make_publisher()
        return repair_photos_from_backup_record(record, run_config, worker_state.airtable_client, worker_state.publisher)

    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(worker, record): index for index, record in enumerate(target_records)}
        for future in as_completed(futures):
            index = futures[future]
            result = future.result()
            repair_results[index] = result
            completed += 1
            if progress_callback:
                progress_callback(completed, total_records, result)

    return [result for result in repair_results if result is not None]


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


def aggregate_backup_repair_results(results: List[Dict[str, Any]], dry_run: bool) -> Dict[str, Any]:
    errors = len([result for result in results if result.get("status") == "error"])
    return {
        "total_places": len(results),
        "dry_run": dry_run,
        "updated": len([result for result in results if result.get("status") == "updated"]),
        "would_update": len([result for result in results if result.get("status") == "would_update"]),
        "skipped": len([result for result in results if result.get("status") == "skipped"]),
        "errors": errors,
        "current_photos_count": sum(int(result.get("summary", {}).get("current_photos_count", 0) or 0) for result in results),
        "photos_backup_count": sum(int(result.get("summary", {}).get("photos_backup_count", 0) or 0) for result in results),
        "desired_photos_count": sum(int(result.get("summary", {}).get("desired_photos_count", 0) or 0) for result in results),
        "failed_upload_count": sum(int(result.get("summary", {}).get("failed_upload_count", 0) or 0) for result in results),
        "airtable_write_requested": sum(1 for result in results if result.get("summary", {}).get("airtable_write_requested")),
        "airtable_write_attempted": sum(1 for result in results if result.get("summary", {}).get("airtable_write_attempted")),
        "airtable_updates_applied": sum(1 for result in results if result.get("summary", {}).get("airtable_update_applied")),
        "airtable_updates_skipped_no_change": sum(1 for result in results if result.get("summary", {}).get("airtable_update_skipped_no_change")),
        "airtable_update_failures": sum(1 for result in results if result.get("summary", {}).get("airtable_update_failed")),
        "success": errors == 0,
    }


def azure_url_for_blob_path(blob_path: str) -> str:
    return f"https://{AZURE_ACCOUNT_HOST}/{PHOTOS_CONTAINER}/{blob_path}"


def photo_manifest_urls(value: Any) -> List[str]:
    return [
        url
        for photo in parse_photo_manifest_list(value)
        for url in (photo.get("display", ""), photo.get("thumbnail", ""))
        if isinstance(url, str) and url.startswith("http")
    ]


def photos_backup_azure_urls(value: Any, city: str, place_id: str) -> Tuple[List[str], List[str]]:
    entries, warnings, errors = parse_photos_backup_entries(value)
    urls: List[str] = []
    for entry in entries:
        if entry.get("kind") == "manifest":
            for url in (entry.get("photo_manifest", {}).get("display", ""), entry.get("photo_manifest", {}).get("thumbnail", "")):
                if urlparse(url).netloc.lower() == AZURE_ACCOUNT_HOST:
                    urls.append(url)
            continue
        source_url = entry.get("source_url", "")
        if urlparse(source_url).netloc.lower() == AZURE_ACCOUNT_HOST:
            classification = classify_azure_photo_url(source_url, city, place_id)
            if classification.get("reason") == "valid":
                urls.append(source_url)
    return list(dict.fromkeys(urls)), [*warnings, *errors]


def protected_azure_urls_for_record(record: Dict[str, Any], city: str) -> Tuple[Set[str], List[str]]:
    fields = record.get("fields", {}) if isinstance(record, dict) else {}
    place_id = str(fields.get("Google Maps Place Id") or "").strip()
    live_photo_urls = [url for url in photo_manifest_urls(fields.get(PHOTOS_FIELD)) if urlparse(url).netloc.lower() == AZURE_ACCOUNT_HOST]
    backup_urls, warnings = photos_backup_azure_urls(fields.get(PHOTOS_BACKUP_FIELD), city, place_id)
    return set(live_photo_urls + backup_urls), warnings


def is_photo_folder_marker_blob_path(blob_path: str, place_id: str) -> bool:
    normalized_path = (blob_path or "").strip().rstrip("/")
    if not normalized_path or not place_id:
        return False
    return normalized_path in {
        place_id,
        f"{place_id}/display",
        f"{place_id}/thumbnail",
    }


def blob_cleanup_reason(blob_path: str) -> str:
    parts = blob_path.split("/", 2)
    if len(parts) == 3 and parts[1] == "display":
        return "unreferenced_display_variant"
    if len(parts) == 3 and parts[1] == "thumbnail":
        return "unreferenced_thumbnail_variant"
    if len(parts) == 2:
        return "unreferenced_root_blob"
    return "unreferenced_blob"


def audit_single_place_storage_cleanup(record: Dict[str, Any], city: str) -> Dict[str, Any]:
    fields = record.get("fields", {}) if isinstance(record, dict) else {}
    place_name = fields.get("Place", "Unknown")
    place_id = str(fields.get("Google Maps Place Id") or "").strip()
    record_id = record.get("id", "") if isinstance(record, dict) else ""
    if not is_photo_ready_place(fields):
        return {
            "status": "skipped",
            "skip_reason": "ignored_missing_place_id",
            "place_name": place_name,
            "record_id": record_id,
        }

    blob_paths = list_blobs_in_container(PHOTOS_CONTAINER, prefix=f"{place_id}/")
    photo_blob_paths = [blob_path for blob_path in blob_paths if not is_photo_folder_marker_blob_path(blob_path, place_id)]
    protected_urls, warnings = protected_azure_urls_for_record(record, city)
    candidates = []
    for blob_path in sorted(photo_blob_paths):
        url = azure_url_for_blob_path(blob_path)
        if url in protected_urls:
            continue
        candidates.append({
            "container": PHOTOS_CONTAINER,
            "blob_path": blob_path,
            "url": url,
            "reason": blob_cleanup_reason(blob_path),
        })

    root_blob_count = len([blob for blob in photo_blob_paths if len(blob.split("/", 2)) == 2])
    display_blob_count = len([blob for blob in photo_blob_paths if len(blob.split("/", 2)) == 3 and blob.split("/", 2)[1] == "display"])
    thumbnail_blob_count = len([blob for blob in photo_blob_paths if len(blob.split("/", 2)) == 3 and blob.split("/", 2)[1] == "thumbnail"])
    return {
        "status": "ok",
        "place_name": place_name,
        "place_id": place_id,
        "record_id": record_id,
        "blob_count": len(photo_blob_paths),
        "folder_marker_blob_count": len(blob_paths) - len(photo_blob_paths),
        "root_blob_count": root_blob_count,
        "display_blob_count": display_blob_count,
        "thumbnail_blob_count": thumbnail_blob_count,
        "protected_url_count": len(protected_urls),
        "cleanup_candidate_count": len(candidates),
        "cleanup_candidate_samples": candidates[:3],
        "cleanup_candidates": candidates,
        "warnings": warnings,
    }


def build_azure_storage_cleanup_manifest(
    records: List[Dict[str, Any]],
    city: str,
    progress_callback: Optional[Callable[[int, int, Dict[str, Any]], None]] = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> Dict[str, Any]:
    total_records = len(records)
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")

    results: List[Dict[str, Any]] = []

    def audit_record(record: Dict[str, Any]) -> Dict[str, Any]:
        return audit_single_place_storage_cleanup(record, city)

    if max_workers == 1 or total_records <= 1:
        for completed, record in enumerate(records, start=1):
            result = audit_record(record)
            results.append(result)
            if progress_callback:
                progress_callback(completed, total_records, result)
    else:
        indexed_results: List[Optional[Dict[str, Any]]] = [None] * total_records
        completed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_index = {executor.submit(audit_record, record): index for index, record in enumerate(records)}
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                result = future.result()
                indexed_results[index] = result
                completed += 1
                if progress_callback:
                    progress_callback(completed, total_records, result)
        results = [result for result in indexed_results if result is not None]
    records_with_candidates = [result for result in results if result.get("cleanup_candidate_count", 0) > 0]
    return {
        "success": True,
        "city": city,
        "dry_run": True,
        "total_places": len(results),
        "records_with_cleanup_candidates": len(records_with_candidates),
        "cleanup_candidate_count": sum(int(result.get("cleanup_candidate_count", 0) or 0) for result in results),
        "status_counts": count_by_key(results, "status"),
        "results": results,
    }


def delete_azure_storage_cleanup_manifest(
    cleanup_manifest: Dict[str, Any],
    current_records: List[Dict[str, Any]],
    city: str,
    dry_run: bool,
    progress_callback: Optional[Callable[[int, int, Dict[str, Any]], None]] = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> Dict[str, Any]:
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")

    protected_urls_by_place_id: Dict[str, Set[str]] = {}
    for record in current_records:
        fields = record.get("fields", {}) if isinstance(record, dict) else {}
        place_id = str(fields.get("Google Maps Place Id") or "").strip()
        if not place_id:
            continue
        protected_urls_by_place_id[place_id] = protected_azure_urls_for_record(record, city)[0]

    attempted = 0
    would_delete = 0
    deleted = 0
    skipped_protected = 0
    skipped_not_targeted = 0
    skipped_folder_markers = 0
    missing_already = 0
    failed = 0
    samples: List[Dict[str, Any]] = []
    delete_jobs: List[Tuple[str, Dict[str, Any]]] = []
    total_candidates = sum(
        len(result.get("cleanup_candidates", []))
        for result in cleanup_manifest.get("results", [])
        if isinstance(result, dict)
    )

    progress_completed = 0

    def emit_progress(status: str, candidate: Dict[str, Any], place_id: str) -> None:
        nonlocal progress_completed
        progress_completed += 1
        if progress_callback:
            progress_callback(progress_completed, total_candidates, {
                "status": status,
                "place_id": place_id,
                "candidate": candidate,
            })

    def record_delete_result(status: str, candidate: Dict[str, Any], place_id: str) -> None:
        nonlocal deleted, missing_already, failed
        if status == "deleted":
            deleted += 1
            if len(samples) < 3:
                samples.append(candidate)
        elif status == "missing_already":
            missing_already += 1
            if len(samples) < 3:
                samples.append(candidate)
        else:
            failed += 1
        emit_progress(status, candidate, place_id)

    def delete_candidate(job: Tuple[str, Dict[str, Any]]) -> Tuple[str, Dict[str, Any], str]:
        place_id, candidate = job
        blob_path = candidate.get("blob_path", "")
        status = delete_blob_from_container_with_status(candidate.get("container") or PHOTOS_CONTAINER, blob_path)
        return place_id, candidate, status

    for result in cleanup_manifest.get("results", []):
        place_id = str(result.get("place_id") or "").strip()
        protected_urls = protected_urls_by_place_id.get(place_id, set())
        for candidate in result.get("cleanup_candidates", []):
            attempted += 1
            url = candidate.get("url", "")
            blob_path = candidate.get("blob_path", "")
            if place_id not in protected_urls_by_place_id:
                skipped_not_targeted += 1
                emit_progress("skipped_not_targeted", candidate, place_id)
                continue
            if is_photo_folder_marker_blob_path(blob_path, place_id):
                skipped_folder_markers += 1
                emit_progress("skipped_folder_marker", candidate, place_id)
                continue
            if url in protected_urls:
                skipped_protected += 1
                emit_progress("skipped_protected", candidate, place_id)
                continue
            if dry_run:
                would_delete += 1
                if len(samples) < 3:
                    samples.append(candidate)
                emit_progress("would_delete", candidate, place_id)
                continue
            delete_jobs.append((place_id, candidate))

    if not dry_run and delete_jobs:
        if max_workers == 1 or len(delete_jobs) == 1:
            for job in delete_jobs:
                place_id, candidate, status = delete_candidate(job)
                record_delete_result(status, candidate, place_id)
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_job = {executor.submit(delete_candidate, job): job for job in delete_jobs}
                for future in as_completed(future_to_job):
                    place_id, candidate, status = future.result()
                    record_delete_result(status, candidate, place_id)

    return {
        "success": failed == 0,
        "dry_run": dry_run,
        "attempted": attempted,
        "would_delete": would_delete,
        "deleted": deleted,
        "missing_already": missing_already,
        "skipped_protected": skipped_protected,
        "skipped_not_targeted": skipped_not_targeted,
        "skipped_folder_markers": skipped_folder_markers,
        "failed": failed,
        "samples": samples,
    }


def run_audit(
    records: List[Dict[str, Any]],
    city: str,
    progress_callback: Optional[Callable[[int, int, Dict[str, Any]], None]] = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    total_records = len(records)
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")

    def audit_record(record: Dict[str, Any]) -> Dict[str, Any]:
        return audit_single_place_photo_assets({"place": record, "config": {"city": city}})

    if max_workers == 1 or total_records <= 1:
        results = []
        for completed, record in enumerate(records, start=1):
            result = audit_record(record)
            results.append(result)
            if progress_callback:
                progress_callback(completed, total_records, result)
    else:
        indexed_results: List[Optional[Dict[str, Any]]] = [None] * total_records
        completed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_index = {executor.submit(audit_record, record): index for index, record in enumerate(records)}
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                result = future.result()
                indexed_results[index] = result
                completed += 1
                if progress_callback:
                    progress_callback(completed, total_records, result)
        results = [result for result in indexed_results if result is not None]
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
    parser.add_argument("--filter", choices=["google-photos", "all-photo-ready", "all"], default="google-photos")
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
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Maximum parallel place workers. Use 1 for single-threaded processing.",
    )
    parser.add_argument(
        "--audit-photos-backup-counts",
        action="store_true",
        help="Read-only audit comparing Photos count to Photos Backup count for each Airtable record.",
    )
    parser.add_argument(
        "--repair-photos-from-backup",
        action="store_true",
        help="Repair live Airtable Photos from live read-only Photos Backup. Does not read local CSV or JSON data.",
    )
    parser.add_argument(
        "--audit-azure-storage",
        action="store_true",
        help="Read-only audit comparing fixed live Airtable Photos to Azure blob storage.",
    )
    parser.add_argument(
        "--plan-azure-storage-cleanup",
        action="store_true",
        help="Read-only cleanup planning that emits unreferenced Azure blob candidates.",
    )
    parser.add_argument(
        "--cleanup-azure-storage",
        action="store_true",
        help="Delete Azure blobs from a reviewed cleanup manifest only when combined with --write.",
    )
    parser.add_argument("--cleanup-manifest", type=Path)
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
        if args.audit_photos_backup_counts and args.write:
            raise RuntimeError("--audit-photos-backup-counts is read-only and cannot be combined with --write.")
        require_broad_write_confirmation(args)
        if args.max_workers < 1:
            raise RuntimeError("--max-workers must be at least 1.")
        load_dotenv_if_present()
        loaded_settings = load_local_settings(args.settings_file.resolve())
        validate_required_env()

        airtable_client = AirtablePhotoClient()
        if args.audit_photos_backup_counts:
            audit_records = airtable_client.fetch_photo_count_audit_records(args.view)
            report = audit_photos_backup_counts(select_live_airtable_records(audit_records, args))
            if args.report_json:
                write_report(args.report_json.resolve(), report)
                print(f"Wrote report: {args.report_json.resolve()}")
            print_photos_backup_count_audit(report)
            return 0 if report.get("success", False) else 3

        if args.repair_photos_from_backup:
            dry_run = not args.write
            run_config = MigrationRunConfig(
                city=args.city,
                dry_run=dry_run,
                upload=args.write,
                write_airtable=args.write,
                try_url_variants=args.try_url_variants,
                download_timeout_seconds=args.download_timeout_seconds,
            )
            repair_records = airtable_client.fetch_photo_repair_records(args.view)
            target_records = select_live_airtable_records(repair_records, args)
            print(json.dumps({
                "settings_loaded": sorted([key for key in loaded_settings if key in REQUIRED_ENV_VARS]),
                "view": args.view,
                "dry_run": dry_run,
                "records_fetched": len(repair_records),
                "records_targeted": len(target_records),
                "max_workers": args.max_workers,
                "source": "live_airtable_photos_backup",
                "local_csv_used": False,
                "local_json_used": False,
                "photos_backup_read_only": True,
            }, indent=2))

            def print_repair_progress(completed: int, total: int, result: Dict[str, Any]) -> None:
                print(
                    f"[{completed}/{total}] {result.get('status')}: "
                    f"{result.get('place_name')} ({result.get('record_id')}) - {result.get('message')}"
                )

            repair_results = process_backup_repair_records(
                target_records,
                run_config,
                max_workers=args.max_workers,
                progress_callback=print_repair_progress,
            )
            post_repair_audit: Optional[Dict[str, Any]] = None
            if args.write:
                post_write_records = airtable_client.fetch_photo_count_audit_records(args.view)
                post_repair_audit = audit_photos_backup_counts(select_live_airtable_records(post_write_records, args))

            report = {
                "repair_totals": aggregate_backup_repair_results(repair_results, dry_run),
                "status_counts": count_by_key(repair_results, "status"),
                "repair_results": repair_results,
                "post_repair_photos_backup_count_audit": post_repair_audit,
            }
            if args.report_json:
                write_report(args.report_json.resolve(), report)
                print(f"Wrote report: {args.report_json.resolve()}")
            print(json.dumps({
                "repair_totals": report["repair_totals"],
                "status_counts": report["status_counts"],
                "post_repair_photos_backup_count_audit": post_repair_audit,
            }, indent=2))
            if report["repair_totals"].get("errors"):
                return 2
            if post_repair_audit and not post_repair_audit.get("success", False):
                return 3
            return 0

        if args.audit_azure_storage:
            audit_records = airtable_client.fetch_photo_repair_records(args.view)
            target_records = select_live_airtable_records(audit_records, args)
            print(f"Auditing Azure storage for {len(target_records)} records with {args.max_workers} worker(s)...", flush=True)

            def print_audit_progress(completed: int, total: int, result: Dict[str, Any]) -> None:
                status = result.get("status")
                place_name = result.get("place_name", "Unknown")
                record_id = result.get("record_id", "")
                if status == "ok":
                    message = (
                        f"{int(result.get('missing_blob_reference_count', 0) or 0)} missing, "
                        f"{int(result.get('unserved_blob_count', 0) or 0)} unserved"
                    )
                else:
                    message = str(result.get("skip_reason") or "")
                print(f"[{completed}/{total}] azure-audit: {status}: {place_name} ({record_id}) - {message}", flush=True)

            audit_results, audit_totals = run_audit(
                target_records,
                args.city,
                progress_callback=print_audit_progress,
                max_workers=args.max_workers,
            )
            cleanup_manifest = build_azure_storage_cleanup_manifest(target_records, args.city, max_workers=args.max_workers)
            audit_totals["cleanup_candidate_count"] = cleanup_manifest.get("cleanup_candidate_count", 0)
            audit_totals["records_with_cleanup_candidates"] = cleanup_manifest.get("records_with_cleanup_candidates", 0)
            report = {
                "audit_totals": audit_totals,
                "audit_results": audit_results,
                "cleanup_manifest_preview": cleanup_manifest,
            }
            if args.report_json:
                write_report(args.report_json.resolve(), report)
                print(f"Wrote report: {args.report_json.resolve()}")
            print(json.dumps({"audit_totals": audit_totals}, indent=2))
            return 0 if audit_totals.get("success", False) and audit_totals.get("cleanup_candidate_count", 0) == 0 else 3

        if args.plan_azure_storage_cleanup or (args.cleanup_azure_storage and not args.write):
            cleanup_records = airtable_client.fetch_photo_repair_records(args.view)
            target_records = select_live_airtable_records(cleanup_records, args)
            print(f"Planning Azure storage cleanup for {len(target_records)} records with {args.max_workers} worker(s)...", flush=True)

            def print_cleanup_plan_progress(completed: int, total: int, result: Dict[str, Any]) -> None:
                status = result.get("status")
                place_name = result.get("place_name", "Unknown")
                record_id = result.get("record_id", "")
                if status == "ok":
                    message = f"{int(result.get('cleanup_candidate_count', 0) or 0)} candidate(s)"
                else:
                    message = str(result.get("skip_reason") or "")
                print(f"[{completed}/{total}] cleanup-plan: {status}: {place_name} ({record_id}) - {message}", flush=True)

            cleanup_manifest = build_azure_storage_cleanup_manifest(
                target_records,
                args.city,
                progress_callback=print_cleanup_plan_progress,
                max_workers=args.max_workers,
            )
            if args.report_json:
                write_report(args.report_json.resolve(), cleanup_manifest)
                print(f"Wrote report: {args.report_json.resolve()}")
            print(json.dumps({
                "total_places": cleanup_manifest.get("total_places", 0),
                "records_with_cleanup_candidates": cleanup_manifest.get("records_with_cleanup_candidates", 0),
                "cleanup_candidate_count": cleanup_manifest.get("cleanup_candidate_count", 0),
            }, indent=2))
            return 0 if cleanup_manifest.get("cleanup_candidate_count", 0) == 0 else 3

        if args.cleanup_azure_storage and args.write:
            if not args.cleanup_manifest:
                raise RuntimeError("--cleanup-azure-storage --write requires --cleanup-manifest.")
            with args.cleanup_manifest.resolve().open("r", encoding="utf-8") as manifest_handle:
                cleanup_manifest = json.load(manifest_handle)
            current_records = airtable_client.fetch_photo_repair_records(args.view)
            target_records = select_live_airtable_records(current_records, args)
            total_candidates = sum(
                len(result.get("cleanup_candidates", []))
                for result in cleanup_manifest.get("results", [])
                if isinstance(result, dict)
            )
            print(
                f"Deleting Azure storage cleanup manifest candidates for {len(target_records)} records "
                f"({total_candidates} candidate(s)) with {args.max_workers} worker(s)...",
                flush=True,
            )

            def print_cleanup_delete_progress(completed: int, total: int, result: Dict[str, Any]) -> None:
                status = result.get("status")
                candidate = result.get("candidate", {})
                blob_path = candidate.get("blob_path", "") if isinstance(candidate, dict) else ""
                if completed == 1 or completed == total or completed % 250 == 0 or status not in {"deleted", "would_delete", "missing_already"}:
                    print(f"[{completed}/{total}] cleanup-delete: {status}: {blob_path}", flush=True)

            cleanup_report = delete_azure_storage_cleanup_manifest(
                cleanup_manifest,
                target_records,
                args.city,
                dry_run=False,
                progress_callback=print_cleanup_delete_progress,
                max_workers=args.max_workers,
            )
            if args.report_json:
                write_report(args.report_json.resolve(), cleanup_report)
                print(f"Wrote report: {args.report_json.resolve()}")
            print(json.dumps(cleanup_report, indent=2))
            return 0 if cleanup_report.get("success", False) else 3

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
            refresh_google_photos_on_download_failure=args.refresh_google_photos,
            recovery_entries_by_record_id=recovery_entries,
            recovery_max_source_urls=args.recovery_max_photos,
        )
        all_records = airtable_client.fetch_records(args.view)
        baseline_google_counts = count_google_photo_rows(all_records)
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
            "refresh_google_photos_on_download_failure": run_config.refresh_google_photos_on_download_failure,
            "max_workers": args.max_workers,
            "baseline_google_photos": baseline_google_counts,
        }, indent=2))

        def print_progress(completed: int, total: int, result: Dict[str, Any]) -> None:
            print(
                f"[{completed}/{total}] {result.get('status')}: "
                f"{result.get('place_name')} ({result.get('record_id')}) - {result.get('message')}"
            )

        migration_results = process_target_records(
            target_records,
            args.data_root.resolve(),
            run_config,
            max_workers=args.max_workers,
            progress_callback=print_progress,
        )

        should_audit = args.audit or (args.write and not args.no_audit)
        audit_results: List[Dict[str, Any]] = []
        audit_totals: Dict[str, Any] = {}
        if should_audit:
            audit_results, audit_totals = run_audit(target_records, args.city)

        post_google_counts = None
        if args.write:
            post_write_records = airtable_client.fetch_records(args.view)
            post_google_counts = count_google_photo_rows(post_write_records)

        report = {
            "migration_totals": aggregate_migration_results(migration_results, dry_run),
            "status_counts": count_by_key(migration_results, "status"),
            "baseline_google_photos": baseline_google_counts,
            "post_write_google_photos": post_google_counts,
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