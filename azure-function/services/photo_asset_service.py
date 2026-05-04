from __future__ import annotations

import ast
import copy
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from services.photo_publisher_service import (
    AZURE_ACCOUNT_HOST,
    LEGACY_CURATOR_PHOTOS_CONTAINER,
    LEGACY_PLACE_PHOTOS_CONTAINER,
    PHOTOS_CONTAINER,
    PhotoPublisherService,
    azure_blob_url,
)
from services.utils import list_blobs_in_container


CURATOR_PHOTO_URLS_FIELD = "Curator Photo URLs"
VALID_BLOB_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}


@dataclass
class PhotoAssetConfig:
    city: str = "charlotte"
    dry_run: bool = True
    upload: bool = False
    try_url_variants: bool = True


def parse_url_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str) and item.startswith("http")]
    if not isinstance(value, str) or not value.strip():
        return []
    try:
        parsed_value = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        try:
            parsed_value = ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return []
    if isinstance(parsed_value, list):
        return [item for item in parsed_value if isinstance(item, str) and item.startswith("http")]
    return []


def canonicalize_url(url: str) -> str:
    return (url or "").strip()


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_photo_ready_place(fields: Dict[str, Any]) -> bool:
    return bool(str((fields or {}).get("Google Maps Place Id") or "").strip())


def parse_photo_date(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        return datetime.min
    for date_format in ("%m/%d/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.replace("Z", ""), date_format)
        except ValueError:
            continue
    return datetime.min


def classify_azure_photo_url(url: str, city: str = "charlotte", place_id: str = "") -> Dict[str, str]:
    parsed = urlparse(url)
    if parsed.scheme != "https":
        return {"category": "invalid", "blob_path": "", "reason": "invalid_scheme"}
    if parsed.netloc.lower() != AZURE_ACCOUNT_HOST:
        return {"category": "non_azure", "blob_path": "", "reason": "invalid_host"}
    path_parts = parsed.path.lstrip("/").split("/", 1)
    if len(path_parts) != 2:
        return {"category": "invalid", "blob_path": "", "reason": "invalid_path"}

    container, blob_path = path_parts
    if container == PHOTOS_CONTAINER:
        parts = blob_path.split("/", 1)
        if len(parts) != 2 or not parts[0] or (place_id and parts[0] != place_id):
            return {"category": "invalid", "blob_path": blob_path, "reason": "invalid_blob_prefix"}
        filename = parts[1].rsplit("/", 1)[-1]
        extension = f".{filename.rsplit('.', 1)[-1]}" if "." in filename else ""
        if extension.lower() not in VALID_BLOB_EXTENSIONS:
            return {"category": "invalid", "blob_path": blob_path, "reason": "invalid_blob_extension"}
        if filename.startswith("curator-"):
            return {"category": "new_curator", "blob_path": blob_path, "reason": "valid"}
        if re.fullmatch(r"[A-Fa-f0-9]{64}(\.[A-Za-z0-9]+)", filename):
            return {"category": "new_standard", "blob_path": blob_path, "reason": "valid"}
        return {"category": "invalid", "blob_path": blob_path, "reason": "invalid_blob_name"}

    if container == LEGACY_PLACE_PHOTOS_CONTAINER:
        expected_prefix = f"{city}/{place_id}/" if place_id else f"{city}/"
        if not blob_path.startswith(expected_prefix):
            return {"category": "legacy_place_photo", "blob_path": blob_path, "reason": "invalid_blob_prefix"}
        filename = blob_path.rsplit("/", 1)[-1]
        match = re.fullmatch(r"[A-Fa-f0-9]{64}(\.[A-Za-z0-9]+)", filename)
        if not match:
            return {"category": "legacy_place_photo", "blob_path": blob_path, "reason": "invalid_blob_name"}
        if match.group(1).lower() not in VALID_BLOB_EXTENSIONS:
            return {"category": "legacy_place_photo", "blob_path": blob_path, "reason": "invalid_blob_extension"}
        return {"category": "legacy_place_photo", "blob_path": blob_path, "reason": "valid"}

    if container == LEGACY_CURATOR_PHOTOS_CONTAINER:
        return {"category": "legacy_curator", "blob_path": blob_path, "reason": "valid" if blob_path else "invalid_blob_path"}
    return {"category": "other_azure", "blob_path": blob_path, "reason": "invalid_container"}


def is_canonical_curator_photo_azure_url(url: str, place_id: str = "") -> bool:
    classification = classify_azure_photo_url(url, place_id=place_id)
    return classification["category"] == "new_curator" and classification["reason"] == "valid"


def is_legacy_curator_photo_azure_url(url: str) -> bool:
    classification = classify_azure_photo_url(url)
    return classification["category"] == "legacy_curator" and classification["reason"] == "valid"


def is_curator_photo_azure_url(url: str) -> bool:
    return is_canonical_curator_photo_azure_url(url) or is_legacy_curator_photo_azure_url(url)


def curator_photo_urls_field_from_airtable(fields: Dict[str, Any]) -> List[str]:
    urls: List[str] = []
    seen_urls: set[str] = set()
    for url in parse_url_list(fields.get(CURATOR_PHOTO_URLS_FIELD)):
        canonical_url = canonicalize_url(url)
        if not canonical_url or canonical_url in seen_urls:
            continue
        seen_urls.add(canonical_url)
        urls.append(canonical_url)
    return urls


def preserved_curator_photo_urls_from_airtable(fields: Dict[str, Any]) -> List[str]:
    place_id = str((fields or {}).get("Google Maps Place Id") or "").strip()
    urls: List[str] = []
    seen_urls: set[str] = set()
    for source_value in (fields.get("Photos"), fields.get(CURATOR_PHOTO_URLS_FIELD)):
        for url in parse_url_list(source_value):
            canonical_url = canonicalize_url(url)
            if canonical_url in seen_urls or not is_canonical_curator_photo_azure_url(canonical_url, place_id):
                continue
            seen_urls.add(canonical_url)
            urls.append(canonical_url)
    return urls


def build_display_photo_urls(curator_urls: List[str], provider_urls: List[str], max_photos: int = 30) -> List[str]:
    merged_urls: List[str] = []
    seen_urls: set[str] = set()
    for url in [*curator_urls, *provider_urls]:
        canonical_url = canonicalize_url(url)
        if not canonical_url or canonical_url in seen_urls:
            continue
        seen_urls.add(canonical_url)
        merged_urls.append(canonical_url)
        if len(merged_urls) >= max_photos:
            break
    return merged_urls


def merge_preserved_photo_urls(preserved_urls: List[str], selected_asset_urls: List[str], max_photos: int = 30) -> List[str]:
    return build_display_photo_urls(preserved_urls, selected_asset_urls, max_photos=max_photos)


def remove_photo_manifest_fields(place_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    cleaned = copy.deepcopy(place_data) if isinstance(place_data, dict) else {}
    photos_section = cleaned.get("photos")
    if isinstance(photos_section, dict):
        photos_section.pop("azure_assets", None)
        photos_section.pop("azure_asset_failures", None)
        photos_section.pop("azure_asset_summary", None)
    return cleaned


def raw_photos_data(photos_section: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw_data = photos_section.get("raw_data", []) if isinstance(photos_section, dict) else []
    if isinstance(raw_data, list):
        return [item for item in raw_data if isinstance(item, dict)]
    if isinstance(raw_data, dict):
        photos_data = raw_data.get("photos_data", [])
        if isinstance(photos_data, list):
            return [item for item in photos_data if isinstance(item, dict)]
    return []


def build_candidate(source_url: str, source_field: str, source_path: str, photo_record: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    canonical_url = canonicalize_url(source_url)
    record = copy.deepcopy(photo_record) if isinstance(photo_record, dict) else {}
    record["photo_url_big"] = canonical_url
    photo_id = str(record.get("photo_id") or "").strip()
    dedupe_key = photo_id or canonical_url
    return {
        "source_url": source_url,
        "canonical_source_url": canonical_url,
        "source_url_sha256": sha256_hex(canonical_url),
        "source_host": urlparse(canonical_url).netloc.lower(),
        "source_field": source_field,
        "source_path": source_path,
        "photo_record": record,
        "dedupe_key": dedupe_key,
        "provenance": [{"field": source_field, "path": source_path}],
        "duplicate_provenance": [],
    }


def _candidate_is_newer(candidate: Dict[str, Any], existing: Dict[str, Any]) -> bool:
    return parse_photo_date(candidate.get("photo_record", {}).get("photo_date")) > parse_photo_date(existing.get("photo_record", {}).get("photo_date"))


def build_place_photo_inventory(
    airtable_record: Dict[str, Any],
    place_data: Optional[Dict[str, Any]],
    city: str = "charlotte",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    fields = airtable_record.get("fields", {}) if isinstance(airtable_record, dict) else {}
    photos_section = place_data.get("photos", {}) if isinstance(place_data, dict) else {}
    raw_records = raw_photos_data(photos_section)
    candidate_map: Dict[str, Dict[str, Any]] = {}
    duplicate_count = 0

    def add_candidate(candidate: Dict[str, Any]) -> None:
        nonlocal duplicate_count
        key = candidate["dedupe_key"]
        existing = candidate_map.get(key)
        if existing is None:
            candidate_map[key] = candidate
            return
        duplicate_count += 1
        existing["provenance"].extend(candidate["provenance"])
        existing["duplicate_provenance"].extend(candidate["provenance"])
        if _candidate_is_newer(candidate, existing):
            candidate["provenance"] = existing["provenance"]
            candidate["duplicate_provenance"] = existing["duplicate_provenance"]
            candidate_map[key] = candidate

    for index, url in enumerate(parse_url_list(fields.get("Photos"))):
        if is_curator_photo_azure_url(canonicalize_url(url)):
            continue
        add_candidate(build_candidate(url, "Airtable Photos", f"fields.Photos[{index}]", None))

    photo_urls = photos_section.get("photo_urls", []) if isinstance(photos_section, dict) else []
    for index, url in enumerate(parse_url_list(photo_urls)):
        add_candidate(build_candidate(url, "photos.photo_urls", f"photos.photo_urls[{index}]", None))

    for index, photo_record in enumerate(raw_records):
        url = photo_record.get("photo_url_big")
        if isinstance(url, str) and url.startswith("http"):
            add_candidate(build_candidate(url, "photos.raw_data.photos_data.photo_url_big", f"photos.raw_data.photos_data[{index}].photo_url_big", photo_record))

    inventory = list(candidate_map.values())
    summary = {
        "city": city,
        "candidate_count": len(inventory),
        "duplicate_count": duplicate_count,
        "airtable_photos_count": len(parse_url_list(fields.get("Photos"))),
        "airtable_photos_google_count": len(parse_url_list(fields.get("Photos Google"))),
        "data_file_photo_urls_count": len(parse_url_list(photo_urls)),
        "provider_raw_photo_url_big_count": len([
            record for record in raw_records
            if isinstance(record.get("photo_url_big"), str) and record.get("photo_url_big", "").startswith("http")
        ]),
    }
    return inventory, summary


def select_prioritized_photo_records(photo_records: List[Dict[str, Any]], max_photos: int = 30) -> List[Dict[str, Any]]:
    unique: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    for record in [record for record in photo_records if isinstance(record, dict) and record.get("photo_url_big")]:
        url = record["photo_url_big"]
        if url in seen_urls:
            continue
        unique.append(record)
        seen_urls.add(url)
        if len(unique) >= max_photos:
            break
    return unique


class PhotoAssetService:
    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()
        self.publisher = PhotoPublisherService(self.session)

    def process_place(self, airtable_record: Dict[str, Any], place_data: Optional[Dict[str, Any]], config: PhotoAssetConfig) -> Dict[str, Any]:
        fields = airtable_record.get("fields", {}) if isinstance(airtable_record, dict) else {}
        record_id = airtable_record.get("id", "") if isinstance(airtable_record, dict) else ""
        place_id = str(fields.get("Google Maps Place Id", "") or "").strip()
        place_name = fields.get("Place", "Unknown")
        if not is_photo_ready_place(fields):
            return self._skipped_missing_id(place_name, record_id, place_id)

        working_place_data = place_data if isinstance(place_data, dict) else {"photos": {}}
        inventory, inventory_summary = build_place_photo_inventory(airtable_record, working_place_data, config.city)
        warnings: List[str] = []
        seen_source_urls = {candidate.get("canonical_source_url") for candidate in inventory}
        for legacy_candidate in self._legacy_blob_candidates(config.city, place_id, record_id, place_name, warnings):
            if legacy_candidate["canonical_source_url"] in seen_source_urls:
                continue
            seen_source_urls.add(legacy_candidate["canonical_source_url"])
            inventory.append(legacy_candidate)

        inventory_summary["candidate_count"] = len(inventory)
        inventory_summary["legacy_blob_candidate_count"] = len([candidate for candidate in inventory if str(candidate.get("source_field", "")).startswith("legacy.")])
        curator_photo_urls_field_urls = curator_photo_urls_field_from_airtable(fields)
        preserved_curator_urls = preserved_curator_photo_urls_from_airtable(fields)

        success_assets: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []
        for candidate in inventory:
            source_url = candidate["canonical_source_url"]
            source_hash = candidate["source_url_sha256"]
            classification = classify_azure_photo_url(source_url, config.city, place_id)
            if classification["category"] == "new_curator":
                continue
            if classification["category"] == "new_standard" and classification["reason"] == "valid":
                success_assets.append(self._asset_record(candidate, place_name, place_id, record_id, source_url, classification["blob_path"], "", "", "existing_azure"))
                continue
            if (
                urlparse(source_url).netloc.lower() == AZURE_ACCOUNT_HOST
                and not (
                    classification["category"] in {"legacy_curator", "legacy_place_photo"}
                    and classification["reason"] == "valid"
                )
            ):
                failures.append(self._failure_record(candidate, place_name, place_id, record_id, "invalid_existing_azure_url", [], None, classification.get("reason", "invalid")))
                continue

            if classification["category"] == "legacy_curator" or candidate.get("source_kind") == "legacy_curator":
                publish_result = self.publisher.publish_legacy_curator_url(
                    source_url,
                    place_id,
                    record_id,
                    place_name,
                    source_hash=source_hash,
                    source_field=candidate.get("source_field", "legacy.curator-photos"),
                    source_path=candidate.get("source_path", ""),
                    dry_run=config.dry_run,
                    upload=config.upload,
                    try_url_variants=config.try_url_variants,
                )
            else:
                publish_result = self.publisher.publish_standard_url(
                    source_url,
                    place_id,
                    record_id,
                    place_name,
                    source_hash=source_hash,
                    source_field=candidate.get("source_field", ""),
                    source_path=candidate.get("source_path", ""),
                    dry_run=config.dry_run,
                    upload=config.upload,
                    try_url_variants=config.try_url_variants,
                )

            if not publish_result.get("success"):
                failures.append(self._failure_record(candidate, place_name, place_id, record_id, publish_result.get("status", "publish_failed"), publish_result.get("attempts", []), publish_result.get("http_status"), publish_result.get("error", "publish failed")))
                continue

            asset = self._asset_record(
                candidate,
                place_name,
                place_id,
                record_id,
                publish_result["azure_url"],
                publish_result["blob_path"],
                publish_result["content_sha256"],
                publish_result["content_type"],
                publish_result["status"],
                bytes_count=publish_result.get("bytes", 0),
                attempts=publish_result.get("attempts", []),
            )
            for key in ("conversion_status", "converted_to_webp", "fallback_original", "fallback_reason", "conversion_warning", "conversion_error", "webp_encoder_available"):
                if key in publish_result:
                    asset[key] = publish_result[key]
            success_assets.append(asset)

        success_assets = self._dedupe_assets_by_azure_url(success_assets)
        selected_asset_urls = self._selected_asset_urls(inventory, success_assets)
        selected_urls = merge_preserved_photo_urls(preserved_curator_urls, selected_asset_urls, max_photos=30)
        selected_url_set = set(selected_urls)
        unselected_curator_photo_urls_field_urls = [
            url for url in curator_photo_urls_field_urls
            if url not in selected_url_set
        ]
        unsupported_curator_photo_urls_field_urls = [
            url for url in curator_photo_urls_field_urls
            if not is_curator_photo_azure_url(url)
        ]
        for asset in success_assets:
            asset["selected_for_airtable"] = asset.get("azure_url") in selected_url_set

        known_success_hashes = {asset.get("source_url_sha256") for asset in success_assets}
        kept_failures = [failure for failure in failures if failure.get("source_url_sha256") not in known_success_hashes]
        summary = {
            **inventory_summary,
            "place_name": place_name,
            "place_id": place_id,
            "record_id": record_id,
            "generated_at": utc_now_iso(),
            "azure_assets_count": len(success_assets),
            "failed_upload_count": len(kept_failures),
            "pending_upload_count": len([asset for asset in success_assets if asset.get("status") == "would_upload"]),
            "selected_airtable_count": len(selected_urls),
            "curator_photo_urls_field_count": len(curator_photo_urls_field_urls),
            "selected_curator_photo_urls_field_count": len([url for url in curator_photo_urls_field_urls if url in selected_url_set]),
            "unselected_curator_photo_urls_field_count": len(unselected_curator_photo_urls_field_urls),
            "unselected_curator_photo_urls_field_urls": unselected_curator_photo_urls_field_urls,
            "unsupported_curator_photo_urls_field_count": len(unsupported_curator_photo_urls_field_urls),
            "unsupported_curator_photo_urls_field_urls": unsupported_curator_photo_urls_field_urls,
            "preserved_curator_airtable_photos_count": len(preserved_curator_urls),
            "selected_curator_airtable_photos_count": len([url for url in preserved_curator_urls if url in selected_url_set]),
            "successful_but_unserved_count": len([asset for asset in success_assets if not asset.get("selected_for_airtable")]),
            "blob_bytes": sum(int(asset.get("bytes", 0) or 0) for asset in success_assets),
            "webp_converted_count": len([asset for asset in success_assets if asset.get("converted_to_webp")]),
            "webp_fallback_original_count": len([asset for asset in success_assets if asset.get("fallback_original")]),
            "webp_conversion_failed_count": len([asset for asset in success_assets if asset.get("fallback_reason") == "conversion_failed"]),
            "legacy_curator_copied_unserved_count": len([asset for asset in success_assets if asset.get("source_field") == "legacy.curator-photos" and not asset.get("selected_for_airtable")]),
            "non_azure_airtable_photos_count": len([url for url in parse_url_list(fields.get("Photos")) if urlparse(url).netloc.lower() != AZURE_ACCOUNT_HOST]),
            "warnings": warnings,
        }
        return {
            "status": "processed",
            "place_name": place_name,
            "place_id": place_id,
            "record_id": record_id,
            "inventory": inventory,
            "assets": success_assets,
            "failures": kept_failures,
            "pending": [],
            "selected_airtable_urls": selected_urls,
            "selected_source_urls": self._source_selection_urls(inventory),
            "summary": summary,
        }

    def _skipped_missing_id(self, place_name: str, record_id: str, place_id: str) -> Dict[str, Any]:
        return {
            "status": "skipped",
            "skip_reason": "ignored_missing_place_id",
            "message": "Skipped photo work because the record has no Google Maps Place Id.",
            "place_name": place_name,
            "place_id": place_id,
            "record_id": record_id,
            "inventory": [],
            "assets": [],
            "failures": [],
            "pending": [],
            "selected_airtable_urls": [],
            "selected_source_urls": [],
            "summary": {
                "place_name": place_name,
                "place_id": place_id,
                "record_id": record_id,
                "ignored_missing_place_id": True,
                "candidate_count": 0,
                "azure_assets_count": 0,
                "failed_upload_count": 0,
                "selected_airtable_count": 0,
            },
        }

    def _legacy_blob_candidates(self, city: str, place_id: str, record_id: str, place_name: str, warnings: List[str]) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []
        try:
            for blob_path in list_blobs_in_container(LEGACY_PLACE_PHOTOS_CONTAINER, prefix=f"{city}/{place_id}/"):
                candidates.append({
                    **build_candidate(azure_blob_url(LEGACY_PLACE_PHOTOS_CONTAINER, blob_path), "legacy.place-photos", f"{LEGACY_PLACE_PHOTOS_CONTAINER}/{blob_path}", None),
                    "source_kind": "legacy_place_photo",
                })
        except Exception as exc:
            logging.warning("Failed to list legacy place photo blobs for %s (%s): %s", place_name, place_id, exc)
            warnings.append(f"legacy_place_blob_list_failed: {exc}")
        if record_id:
            try:
                for blob_path in list_blobs_in_container(LEGACY_CURATOR_PHOTOS_CONTAINER, prefix=f"{record_id}/"):
                    candidates.append({
                        **build_candidate(azure_blob_url(LEGACY_CURATOR_PHOTOS_CONTAINER, blob_path), "legacy.curator-photos", f"{LEGACY_CURATOR_PHOTOS_CONTAINER}/{blob_path}", None),
                        "source_kind": "legacy_curator",
                    })
            except Exception as exc:
                logging.warning("Failed to list legacy curator blobs for %s (%s): %s", place_name, record_id, exc)
                warnings.append(f"legacy_curator_blob_list_failed: {exc}")
        return candidates

    def _selected_asset_urls(self, inventory: List[Dict[str, Any]], assets: List[Dict[str, Any]]) -> List[str]:
        assets_by_hash = {asset.get("source_url_sha256"): asset for asset in assets if asset.get("source_url_sha256")}
        selected_urls: List[str] = []
        seen_urls: set[str] = set()
        for candidate in inventory:
            if candidate.get("source_kind") == "legacy_curator":
                continue
            asset = assets_by_hash.get(candidate.get("source_url_sha256"))
            azure_url = asset.get("azure_url") if asset else ""
            if not azure_url or azure_url in seen_urls:
                continue
            selected_urls.append(azure_url)
            seen_urls.add(azure_url)
            if len(selected_urls) >= 30:
                break
        return selected_urls

    def _asset_record(
        self,
        candidate: Dict[str, Any],
        place_name: str,
        place_id: str,
        record_id: str,
        azure_url: str,
        blob_path: str,
        content_sha256: str,
        content_type: str,
        status: str,
        bytes_count: int = 0,
        attempts: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        return {
            "place_name": place_name,
            "place_id": place_id,
            "record_id": record_id,
            "source_url": candidate["source_url"],
            "canonical_source_url": candidate["canonical_source_url"],
            "source_url_sha256": candidate["source_url_sha256"],
            "source_host": candidate["source_host"],
            "source_field": candidate["source_field"],
            "source_path": candidate["source_path"],
            "provenance": candidate.get("provenance", []),
            "duplicate_provenance": candidate.get("duplicate_provenance", []),
            "azure_url": azure_url,
            "blob_container": PHOTOS_CONTAINER,
            "blob_path": blob_path,
            "content_sha256": content_sha256,
            "content_type": content_type,
            "bytes": bytes_count,
            "status": status,
            "selected_for_airtable": False,
            "attempts": attempts or [],
            "uploaded_at": utc_now_iso(),
        }

    def _failure_record(
        self,
        candidate: Dict[str, Any],
        place_name: str,
        place_id: str,
        record_id: str,
        reason: str,
        attempts: List[Dict[str, Any]],
        http_status: Optional[int],
        error: str,
    ) -> Dict[str, Any]:
        return {
            "place_name": place_name,
            "place_id": place_id,
            "record_id": record_id,
            "source_url": candidate["source_url"],
            "canonical_source_url": candidate["canonical_source_url"],
            "source_url_sha256": candidate["source_url_sha256"],
            "source_host": candidate["source_host"],
            "source_field": candidate["source_field"],
            "source_path": candidate["source_path"],
            "provenance": candidate.get("provenance", []),
            "http_status": http_status,
            "attempted_urls": [attempt.get("url") for attempt in attempts],
            "attempts": attempts,
            "error": error,
            "reason": reason,
            "failed_at": utc_now_iso(),
        }

    def _dedupe_assets_by_azure_url(self, assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        deduped: List[Dict[str, Any]] = []
        indexes_by_url: Dict[str, int] = {}
        for asset in assets:
            azure_url = asset.get("azure_url")
            if not azure_url:
                deduped.append(asset)
                continue
            existing_index = indexes_by_url.get(azure_url)
            if existing_index is None:
                indexes_by_url[azure_url] = len(deduped)
                deduped.append(asset)
                continue
            existing = deduped[existing_index]
            existing["duplicate_provenance"] = existing.get("duplicate_provenance", []) + asset.get("provenance", [])
        return deduped

    def _source_selection_urls(self, inventory: List[Dict[str, Any]]) -> List[str]:
        records: List[Dict[str, Any]] = []
        for candidate in inventory:
            photo_record = copy.deepcopy(candidate.get("photo_record", {}))
            photo_record["photo_url_big"] = candidate["canonical_source_url"]
            photo_record["source_url_sha256"] = candidate["source_url_sha256"]
            records.append(photo_record)
        return [record["photo_url_big"] for record in select_prioritized_photo_records(records, max_photos=30)]
