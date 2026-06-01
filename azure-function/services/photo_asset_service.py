from __future__ import annotations

import ast
import copy
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from services.photo_publisher_service import (
    AZURE_ACCOUNT_HOST,
    PHOTOS_CONTAINER,
    PhotoPublisherService,
)


VALID_BLOB_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}


@dataclass
class PhotoAssetConfig:
    city: str = "charlotte"
    dry_run: bool = True
    upload: bool = False
    try_url_variants: bool = True
    download_timeout_seconds: int = 20


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


def _parse_manifest_list_value(value: Any, field_name: str = "Photos") -> List[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed_value = json.loads(value)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError(f"{field_name} must be a JSON array of display/thumbnail manifests") from exc
        if not isinstance(parsed_value, list):
            raise ValueError(f"{field_name} must be a JSON array of display/thumbnail manifests")
        return parsed_value
    if value in (None, ""):
        return []
    raise ValueError(f"{field_name} must be a JSON array of display/thumbnail manifests")


def parse_photo_manifest_list(value: Any, field_name: str = "Photos") -> List[Dict[str, str]]:
    parsed_value = _parse_manifest_list_value(value, field_name)
    manifests: List[Dict[str, str]] = []
    for index, item in enumerate(parsed_value):
        if not isinstance(item, dict):
            raise ValueError(f"{field_name}[{index}] must be an object with display and thumbnail URLs")
        display_url = canonicalize_url(str(item.get("display") or ""))
        thumbnail_url = canonicalize_url(str(item.get("thumbnail") or ""))
        if not display_url.startswith("http") or not thumbnail_url.startswith("http"):
            raise ValueError(f"{field_name}[{index}] must include HTTP display and thumbnail URLs")
        manifests.append({"display": display_url, "thumbnail": thumbnail_url})
    return manifests


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
        parts = blob_path.split("/", 2)
        if len(parts) < 2 or not parts[0] or (place_id and parts[0] != place_id):
            return {"category": "invalid", "blob_path": blob_path, "reason": "invalid_blob_prefix"}
        if len(parts) == 3 and parts[1] in {"display", "thumbnail"}:
            filename = parts[2].rsplit("/", 1)[-1]
            variant_prefix = f"new_{parts[1]}_variant"
        elif len(parts) == 2:
            filename = parts[1].rsplit("/", 1)[-1]
            variant_prefix = "new"
        else:
            return {"category": "invalid", "blob_path": blob_path, "reason": "invalid_blob_prefix"}
        extension = f".{filename.rsplit('.', 1)[-1]}" if "." in filename else ""
        if extension.lower() not in VALID_BLOB_EXTENSIONS:
            return {"category": "invalid", "blob_path": blob_path, "reason": "invalid_blob_extension"}
        if filename.startswith("curator-"):
            return {"category": f"{variant_prefix}_curator", "blob_path": blob_path, "reason": "valid"}
        if re.fullmatch(r"[A-Fa-f0-9]{64}(\.[A-Za-z0-9]+)", filename):
            return {"category": f"{variant_prefix}_standard", "blob_path": blob_path, "reason": "valid"}
        return {"category": "invalid", "blob_path": blob_path, "reason": "invalid_blob_name"}

    return {"category": "other_azure", "blob_path": blob_path, "reason": "invalid_container"}


def is_canonical_curator_photo_azure_url(url: str, place_id: str = "") -> bool:
    classification = classify_azure_photo_url(url, place_id=place_id)
    return classification["category"] in {"new_curator", "new_display_variant_curator", "new_thumbnail_variant_curator"} and classification["reason"] == "valid"


def is_curator_photo_azure_url(url: str) -> bool:
    return is_canonical_curator_photo_azure_url(url)


def preserved_curator_photo_urls_from_airtable(fields: Dict[str, Any]) -> List[str]:
    place_id = str((fields or {}).get("Google Maps Place Id") or "").strip()
    urls: List[str] = []
    seen_urls: set[str] = set()
    for photo in parse_photo_manifest_list(fields.get("Photos")):
        canonical_url = canonicalize_url(photo.get("display", ""))
        if canonical_url in seen_urls or not is_canonical_curator_photo_azure_url(canonical_url, place_id):
            continue
        seen_urls.add(canonical_url)
        urls.append(canonical_url)
    return urls


def build_display_photo_manifests(curator_manifests: List[Dict[str, str]], provider_manifests: List[Dict[str, str]]) -> List[Dict[str, str]]:
    # Curator photos keep their first-place ordering, then provider manifests
    # fill in behind them. Dedupe by display URL so thumbnails stay paired.
    merged_photos: List[Dict[str, str]] = []
    seen_display_urls: set[str] = set()
    for photo in [*curator_manifests, *provider_manifests]:
        display_url = canonicalize_url(photo.get("display", ""))
        thumbnail_url = canonicalize_url(photo.get("thumbnail", ""))
        if not display_url or not thumbnail_url or display_url in seen_display_urls:
            continue
        seen_display_urls.add(display_url)
        merged_photos.append({"display": display_url, "thumbnail": thumbnail_url})
    return merged_photos


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


def raw_photo_fallback_urls(photos_section: Dict[str, Any]) -> Dict[str, str]:
    raw_data = photos_section.get("raw_data", {}) if isinstance(photos_section, dict) else {}
    if not isinstance(raw_data, dict):
        return {}
    return {
        field_name: raw_data[field_name]
        for field_name in ("photo", "street_view")
        if isinstance(raw_data.get(field_name), str) and raw_data[field_name].startswith("http")
    }


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
    raw_fallback_urls = raw_photo_fallback_urls(photos_section)
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

    airtable_photo_manifests = parse_photo_manifest_list(fields.get("Photos"))
    for index, photo_manifest in enumerate(airtable_photo_manifests):
        candidate = build_candidate(photo_manifest["display"], "Airtable Photos", f"fields.Photos[{index}].display", None)
        candidate["photo_manifest"] = photo_manifest
        add_candidate(candidate)

    photo_urls = photos_section.get("photo_urls", []) if isinstance(photos_section, dict) else []
    for index, url in enumerate(parse_url_list(photo_urls)):
        add_candidate(build_candidate(url, "photos.photo_urls", f"photos.photo_urls[{index}]", None))

    for index, photo_record in enumerate(raw_records):
        url = photo_record.get("photo_url_big")
        if isinstance(url, str) and url.startswith("http"):
            add_candidate(build_candidate(url, "photos.raw_data.photos_data.photo_url_big", f"photos.raw_data.photos_data[{index}].photo_url_big", photo_record))

    for field_name, url in raw_fallback_urls.items():
        add_candidate(build_candidate(url, f"photos.raw_data.{field_name}", f"photos.raw_data.{field_name}", {"photo_source": field_name}))

    inventory = list(candidate_map.values())
    summary = {
        "city": city,
        "candidate_count": len(inventory),
        "duplicate_count": duplicate_count,
        "airtable_photos_count": len(airtable_photo_manifests),
        "airtable_photos_google_count": len(parse_url_list(fields.get("Photos Google"))),
        "data_file_photo_urls_count": len(parse_url_list(photo_urls)),
        "provider_raw_photo_url_big_count": len([
            record for record in raw_records
            if isinstance(record.get("photo_url_big"), str) and record.get("photo_url_big", "").startswith("http")
        ]),
        "provider_raw_photo_count": 1 if raw_fallback_urls.get("photo") else 0,
        "provider_raw_street_view_count": 1 if raw_fallback_urls.get("street_view") else 0,
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
        place_context = self.prepare_place_context(airtable_record, place_data, config)
        if place_context.get("status") == "skipped" and "result" in place_context:
            return place_context["result"]
        batch_result = self.process_candidate_batch(place_context, place_context.get("inventory", []), config)
        return self.finalize_place_assets(place_context, batch_result["assets"], batch_result["failures"])

    def prepare_place_context(self, airtable_record: Dict[str, Any], place_data: Optional[Dict[str, Any]], config: PhotoAssetConfig) -> Dict[str, Any]:
        fields = airtable_record.get("fields", {}) if isinstance(airtable_record, dict) else {}
        record_id = airtable_record.get("id", "") if isinstance(airtable_record, dict) else ""
        place_id = str(fields.get("Google Maps Place Id", "") or "").strip()
        place_name = fields.get("Place", "Unknown")
        if not is_photo_ready_place(fields):
            return {
                "status": "skipped",
                "result": self._skipped_missing_id(place_name, record_id, place_id),
            }

        working_place_data = place_data if isinstance(place_data, dict) else {"photos": {}}
        inventory, inventory_summary = build_place_photo_inventory(airtable_record, working_place_data, config.city)
        warnings: List[str] = []

        inventory_summary["candidate_count"] = len(inventory)
        return {
            "status": "prepared",
            "place_name": place_name,
            "place_id": place_id,
            "record_id": record_id,
            "inventory": inventory,
            "inventory_summary": inventory_summary,
            "warnings": warnings,
            "preserved_curator_urls": [],
            "non_azure_airtable_photos_count": len([photo for photo in parse_photo_manifest_list(fields.get("Photos")) if urlparse(photo.get("display", "")).netloc.lower() != AZURE_ACCOUNT_HOST]),
        }

    def process_candidate_batch(self, place_context: Dict[str, Any], candidates: List[Dict[str, Any]], config: PhotoAssetConfig) -> Dict[str, Any]:
        place_name = place_context.get("place_name", "Unknown")
        place_id = place_context.get("place_id", "")
        record_id = place_context.get("record_id", "")
        success_assets: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []

        for candidate in candidates:
            source_url = candidate["canonical_source_url"]
            source_hash = candidate["source_url_sha256"]
            classification = classify_azure_photo_url(source_url, config.city, place_id)
            if classification["category"] in {
                "new_display_variant_standard",
                "new_thumbnail_variant_standard",
                "new_display_variant_curator",
                "new_thumbnail_variant_curator",
            } and classification["reason"] == "valid":
                # Already-migrated manifest URLs are valid served assets, so keep
                # the existing display/thumbnail pair instead of regenerating it.
                success_assets.append(self._asset_record(candidate, place_name, place_id, record_id, source_url, classification["blob_path"], "", "", "existing_azure"))
                continue
            if (
                urlparse(source_url).netloc.lower() == AZURE_ACCOUNT_HOST
                and not (
                    classification["category"] in {"new_standard", "new_curator"}
                    and classification["reason"] == "valid"
                )
            ):
                failures.append(self._failure_record(candidate, place_name, place_id, record_id, "invalid_existing_azure_url", [], None, classification.get("reason", "invalid")))
                continue

            if classification["category"] == "new_curator":
                publish_result = self.publisher.publish_curator_source_url(
                    source_url,
                    place_id,
                    record_id,
                    place_name,
                    source_hash=source_hash,
                    source_field=candidate.get("source_field", "Airtable Photos"),
                    source_path=candidate.get("source_path", ""),
                    dry_run=config.dry_run,
                    upload=config.upload,
                    try_url_variants=config.try_url_variants,
                    download_timeout_seconds=config.download_timeout_seconds,
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
                    download_timeout_seconds=config.download_timeout_seconds,
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
            for key in ("photo_manifest", "thumbnail_url", "display_blob_path", "thumbnail_blob_path", "variant_bytes", "thumbnail_bytes"):
                if key in publish_result:
                    asset[key] = publish_result[key]
            success_assets.append(asset)

        return {"assets": success_assets, "failures": failures}

    def finalize_place_assets(self, place_context: Dict[str, Any], success_assets: List[Dict[str, Any]], failures: List[Dict[str, Any]]) -> Dict[str, Any]:
        if place_context.get("status") == "skipped" and "result" in place_context:
            return place_context["result"]

        place_name = place_context.get("place_name", "Unknown")
        place_id = place_context.get("place_id", "")
        record_id = place_context.get("record_id", "")
        inventory = place_context.get("inventory", [])
        inventory_summary = place_context.get("inventory_summary", {})
        preserved_curator_urls = place_context.get("preserved_curator_urls", [])
        warnings = list(place_context.get("warnings", []))

        success_assets = self._dedupe_assets_by_azure_url(success_assets)
        selected_asset_manifests = self._selected_asset_manifests(inventory, success_assets)
        selected_photos = build_display_photo_manifests(preserved_curator_urls, selected_asset_manifests)
        selected_urls = [photo["display"] for photo in selected_photos]
        selected_url_set = set(selected_urls)
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
            "preserved_curator_airtable_photos_count": len(preserved_curator_urls),
            "selected_curator_airtable_photos_count": len([url for url in selected_urls if is_curator_photo_azure_url(url)]),
            "successful_but_unserved_count": len([asset for asset in success_assets if not asset.get("selected_for_airtable")]),
            "blob_bytes": sum(int(asset.get("bytes", 0) or 0) for asset in success_assets),
            "webp_converted_count": len([asset for asset in success_assets if asset.get("converted_to_webp")]),
            "webp_fallback_original_count": len([asset for asset in success_assets if asset.get("fallback_original")]),
            "webp_conversion_failed_count": len([asset for asset in success_assets if asset.get("fallback_reason") == "conversion_failed"]),
            "non_azure_airtable_photos_count": int(place_context.get("non_azure_airtable_photos_count", 0) or 0),
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
            "selected_airtable_photos": selected_photos,
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
            "selected_airtable_photos": [],
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

    def _selected_asset_manifests(self, inventory: List[Dict[str, Any]], assets: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        assets_by_hash = {asset.get("source_url_sha256"): asset for asset in assets if asset.get("source_url_sha256")}
        selected_photos: List[Dict[str, str]] = []
        seen_urls: set[str] = set()
        for candidate in inventory:
            asset = assets_by_hash.get(candidate.get("source_url_sha256"))
            photo_manifest = asset.get("photo_manifest") if asset else None
            if not isinstance(photo_manifest, dict):
                photo_manifest = {}
            display_url = canonicalize_url(str(photo_manifest.get("display", "")))
            thumbnail_url = canonicalize_url(str(photo_manifest.get("thumbnail", "")))
            if not display_url or not thumbnail_url or display_url in seen_urls:
                continue
            selected_photos.append({"display": display_url, "thumbnail": thumbnail_url})
            seen_urls.add(display_url)
        return selected_photos

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
            "source_kind": candidate.get("source_kind", ""),
            "provenance": candidate.get("provenance", []),
            "duplicate_provenance": candidate.get("duplicate_provenance", []),
            "azure_url": azure_url,
            "photo_manifest": candidate.get("photo_manifest", {}),
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
            "source_kind": candidate.get("source_kind", ""),
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
