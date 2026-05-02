from __future__ import annotations

import ast
import copy
import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests

from services.utils import upload_blob_to_container


PLACE_PHOTOS_CONTAINER = "place-photos"
AZURE_ACCOUNT_HOST = "thirdplacesdata.blob.core.windows.net"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}
IMAGE_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
}
UNSUPPORTED_IMAGE_CONTENT_TYPES = {"image/gif", "image/svg+xml"}
VALID_BLOB_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}


@dataclass
class PhotoAssetConfig:
    city: str = "charlotte"
    dry_run: bool = True
    upload: bool = False
    write_airtable: bool = False
    overwrite: bool = False
    retry_failures: bool = False
    failure_ttl_hours: int = 168
    try_url_variants: bool = True


def parse_url_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, str) and item.startswith("http")]
    if not isinstance(value, str) or not value.strip():
        return []

    parsed_value = None
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


def parse_photo_date(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        return datetime.min
    for date_format in ("%m/%d/%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value.replace("Z", ""), date_format)
        except ValueError:
            continue
    return datetime.min


def is_google_hosted_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return host in {
        "lh3.googleusercontent.com",
        "lh5.googleusercontent.com",
        "streetviewpixels-pa.googleapis.com",
    }


def google_size_variants(url: str) -> List[str]:
    parsed = urlparse(url)
    if parsed.netloc.lower() not in {"lh3.googleusercontent.com", "lh5.googleusercontent.com"}:
        return [url]

    base_url = re.sub(r"=(w|h|s|r)[A-Za-z0-9_.=-]+$", "", url)
    variants = [
        url,
        base_url,
        f"{base_url}=s0",
        f"{base_url}=s1600",
        f"{base_url}=w800-h500-k-no",
        f"{base_url}=w1600-h1000-k-no",
        f"{base_url}=w2048-h2048-k-no",
    ]
    unique_variants: List[str] = []
    seen: set[str] = set()
    for variant in variants:
        if variant in seen:
            continue
        seen.add(variant)
        unique_variants.append(variant)
    return unique_variants


def sniff_image_content(data: bytes) -> Tuple[Optional[str], Optional[str]]:
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg", ".jpg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png", ".png"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif", ".gif"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp", ".webp"
    return None, None


def extension_for_content_type(content_type: str) -> str:
    clean_type = (content_type or "").split(";", 1)[0].strip().lower()
    return IMAGE_EXTENSIONS.get(clean_type, ".jpg")


def validated_content_type(header_content_type: str, data: bytes) -> Tuple[Optional[str], str]:
    sniffed_type, sniffed_extension = sniff_image_content(data[:64])
    header_type = (header_content_type or "").split(";", 1)[0].strip().lower()
    if sniffed_type:
        if sniffed_type in UNSUPPORTED_IMAGE_CONTENT_TYPES:
            return None, sniffed_extension or extension_for_content_type(sniffed_type)
        return sniffed_type, sniffed_extension or extension_for_content_type(sniffed_type)
    if header_type.startswith("image/"):
        if header_type in UNSUPPORTED_IMAGE_CONTENT_TYPES:
            return None, extension_for_content_type(header_type)
        return header_type, extension_for_content_type(header_type)
    return None, ".jpg"


def is_valid_existing_place_photo_azure_url(url: str, city: str, place_id: str) -> Tuple[bool, str, str]:
    parsed = urlparse(url)
    if parsed.scheme != "https":
        return False, "", "invalid_scheme"
    if parsed.netloc.lower() != AZURE_ACCOUNT_HOST:
        return False, "", "invalid_host"

    path_parts = parsed.path.lstrip("/").split("/", 1)
    if len(path_parts) != 2 or path_parts[0] != PLACE_PHOTOS_CONTAINER:
        return False, "", "invalid_container"

    blob_path = path_parts[1]
    expected_prefix = f"{city}/{place_id}/"
    if not blob_path.startswith(expected_prefix):
        return False, blob_path, "invalid_blob_prefix"

    filename = blob_path.rsplit("/", 1)[-1]
    match = re.fullmatch(r"[A-Fa-f0-9]{64}(\.[A-Za-z0-9]+)", filename)
    if not match:
        return False, blob_path, "invalid_blob_name"
    if match.group(1).lower() not in VALID_BLOB_EXTENSIONS:
        return False, blob_path, "invalid_blob_extension"
    return True, blob_path, "valid"


def raw_photos_data(photos_section: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw_data = photos_section.get("raw_data", []) if isinstance(photos_section, dict) else []
    if isinstance(raw_data, list):
        return [item for item in raw_data if isinstance(item, dict)]
    if isinstance(raw_data, dict):
        photos_data = raw_data.get("photos_data", [])
        if isinstance(photos_data, list):
            return [item for item in photos_data if isinstance(item, dict)]
    return []


def build_candidate(
    source_url: str,
    source_field: str,
    source_path: str,
    photo_record: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
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
    candidate_date = parse_photo_date(candidate.get("photo_record", {}).get("photo_date"))
    existing_date = parse_photo_date(existing.get("photo_record", {}).get("photo_date"))
    return candidate_date > existing_date


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
        add_candidate(build_candidate(url, "Airtable Photos", f"fields.Photos[{index}]", None))

    photo_urls = photos_section.get("photo_urls", []) if isinstance(photos_section, dict) else []
    for index, url in enumerate(parse_url_list(photo_urls)):
        add_candidate(build_candidate(url, "photos.photo_urls", f"photos.photo_urls[{index}]", None))

    for index, photo_record in enumerate(raw_records):
        url = photo_record.get("photo_url_big")
        if isinstance(url, str) and url.startswith("http"):
            add_candidate(
                build_candidate(
                    url,
                    "photos.raw_data.photos_data.photo_url_big",
                    f"photos.raw_data.photos_data[{index}].photo_url_big",
                    photo_record,
                )
            )

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
    if not photo_records:
        return []

    sorted_records = sorted(
        [record for record in photo_records if isinstance(record, dict) and record.get("photo_url_big")],
        key=lambda record: parse_photo_date(record.get("photo_date", "")),
        reverse=True,
    )
    front: List[Dict[str, Any]] = []
    vibe: List[Dict[str, Any]] = []
    all_tag: List[Dict[str, Any]] = []
    other: List[Dict[str, Any]] = []
    tagless: List[Dict[str, Any]] = []

    for record in sorted_records:
        tags = record.get("photo_tags", [])
        if not isinstance(tags, list) or not tags:
            tagless.append(record)
            continue
        if "front" in tags:
            front.append(record)
        elif "vibe" in tags:
            vibe.append(record)
        elif "all" in tags:
            all_tag.append(record)
        elif "other" in tags:
            other.append(record)
        else:
            tagless.append(record)

    selected: List[Dict[str, Any]] = []
    selected.extend(vibe[:max_photos])
    remaining = max_photos - len(selected)
    selected.extend(front[:min(5, len(front), remaining)])
    remaining = max_photos - len(selected)
    selected.extend(all_tag[:remaining])
    remaining = max_photos - len(selected)
    selected.extend(other[:remaining])
    remaining = max_photos - len(selected)
    selected.extend(tagless[:remaining])

    unique: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    for record in selected:
        url = record["photo_url_big"]
        if url in seen_urls:
            continue
        unique.append(record)
        seen_urls.add(url)
    return unique[:max_photos]


class PhotoAssetService:
    def __init__(self, session: Optional[requests.Session] = None):
        self.session = session or requests.Session()

    def download_image_asset(
        self,
        source_url: str,
        try_url_variants: bool = True,
        timeout: int = 60,
        max_bytes: int = 12_000_000,
    ) -> Dict[str, Any]:
        urls_to_try = [source_url]
        if try_url_variants and is_google_hosted_url(source_url):
            urls_to_try = google_size_variants(source_url)

        attempts: List[Dict[str, Any]] = []
        for attempted_url in urls_to_try:
            try:
                with self.session.get(
                    attempted_url,
                    headers=REQUEST_HEADERS,
                    timeout=timeout,
                    stream=True,
                    allow_redirects=True,
                ) as response:
                    attempt = {
                        "url": attempted_url,
                        "status_code": response.status_code,
                        "content_type": response.headers.get("Content-Type", ""),
                        "effective_url": response.url,
                        "bytes": 0,
                        "error": None,
                    }
                    if response.status_code != 200:
                        attempt["error"] = f"HTTP {response.status_code}"
                        attempts.append(attempt)
                        continue

                    chunks: List[bytes] = []
                    total_bytes = 0
                    for chunk in response.iter_content(chunk_size=64 * 1024):
                        if not chunk:
                            continue
                        total_bytes += len(chunk)
                        if total_bytes > max_bytes:
                            attempt["bytes"] = total_bytes
                            attempt["error"] = f"response exceeded {max_bytes} byte limit"
                            attempts.append(attempt)
                            break
                        chunks.append(chunk)
                    else:
                        data = b"".join(chunks)
                        attempt["bytes"] = len(data)
                        content_type, extension = validated_content_type(attempt["content_type"], data)
                        if not data:
                            attempt["error"] = "empty response body"
                            attempts.append(attempt)
                            continue
                        if content_type is None:
                            attempt["error"] = "response body did not look like an image"
                            attempts.append(attempt)
                            continue
                        attempts.append(attempt)
                        return {
                            "success": True,
                            "data": data,
                            "content_type": content_type,
                            "extension": extension,
                            "content_sha256": hashlib.sha256(data).hexdigest(),
                            "attempts": attempts,
                            "successful_url": attempted_url,
                            "effective_url": response.url,
                            "bytes": len(data),
                        }
            except Exception as exc:
                attempts.append({"url": attempted_url, "status_code": None, "content_type": "", "bytes": 0, "error": str(exc)})

        last_attempt = attempts[-1] if attempts else {}
        return {
            "success": False,
            "attempts": attempts,
            "error": last_attempt.get("error") or "all variants failed",
            "status_code": last_attempt.get("status_code"),
        }

    def process_place(
        self,
        airtable_record: Dict[str, Any],
        place_data: Optional[Dict[str, Any]],
        config: PhotoAssetConfig,
    ) -> Dict[str, Any]:
        fields = airtable_record.get("fields", {}) if isinstance(airtable_record, dict) else {}
        record_id = airtable_record.get("id", "") if isinstance(airtable_record, dict) else ""
        place_id = fields.get("Google Maps Place Id", "")
        place_name = fields.get("Place", "Unknown")
        working_place_data = place_data if isinstance(place_data, dict) else {"photos": {}}
        photos_section = working_place_data.setdefault("photos", {})
        inventory, inventory_summary = build_place_photo_inventory(airtable_record, working_place_data, config.city)
        existing_assets = {
            asset.get("source_url_sha256"): asset
            for asset in photos_section.get("azure_assets", [])
            if isinstance(asset, dict) and asset.get("source_url_sha256")
        }
        existing_failures = {
            failure.get("source_url_sha256"): failure
            for failure in photos_section.get("azure_asset_failures", [])
            if isinstance(failure, dict) and failure.get("source_url_sha256")
        }

        success_assets: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []
        pending: List[Dict[str, Any]] = []

        for candidate in inventory:
            source_hash = candidate["source_url_sha256"]
            source_url = candidate["canonical_source_url"]
            if urlparse(source_url).netloc.lower() == AZURE_ACCOUNT_HOST:
                is_valid, blob_path, reason = is_valid_existing_place_photo_azure_url(source_url, config.city, place_id)
                if is_valid:
                    success_assets.append(self._asset_record(candidate, place_name, place_id, record_id, source_url, blob_path, "", "", "existing_azure"))
                    continue
                failures.append(self._failure_record(candidate, place_name, place_id, record_id, "invalid_existing_azure_url", [], None, reason))
                continue

            existing_asset = existing_assets.get(source_hash)
            if existing_asset and not config.overwrite:
                asset = copy.deepcopy(existing_asset)
                asset["provenance"] = candidate.get("provenance", [])
                success_assets.append(asset)
                continue

            existing_failure = existing_failures.get(source_hash)
            if existing_failure and not self._should_retry_failure(existing_failure, config):
                failures.append(copy.deepcopy(existing_failure))
                continue

            if config.dry_run or not config.upload:
                pending.append({**candidate, "reason": "upload_skipped"})
                continue

            download_result = self.download_image_asset(source_url, try_url_variants=config.try_url_variants)
            if not download_result.get("success"):
                failures.append(
                    self._failure_record(
                        candidate,
                        place_name,
                        place_id,
                        record_id,
                        "download_failed",
                        download_result.get("attempts", []),
                        download_result.get("status_code"),
                        download_result.get("error", "download failed"),
                    )
                )
                continue

            extension = download_result["extension"]
            blob_path = f"{config.city}/{place_id}/{source_hash}{extension}"
            metadata = {
                "airtable_record_id": record_id,
                "place_id": place_id,
                "source_url_sha256": source_hash,
                "source_host": candidate["source_host"][:128],
                "source_field": candidate["source_field"][:128],
                "content_sha256": download_result["content_sha256"],
                "content_type": download_result["content_type"],
                "uploaded_at": utc_now_iso(),
            }
            azure_url = upload_blob_to_container(
                PLACE_PHOTOS_CONTAINER,
                blob_path,
                download_result["data"],
                content_type=download_result["content_type"],
                metadata=metadata,
                cache_control="public, max-age=31536000, immutable",
                public_access="blob",
                overwrite=True,
            )
            success_assets.append(
                self._asset_record(
                    candidate,
                    place_name,
                    place_id,
                    record_id,
                    azure_url,
                    blob_path,
                    download_result["content_sha256"],
                    download_result["content_type"],
                    "uploaded",
                    bytes_count=download_result.get("bytes", 0),
                    attempts=download_result.get("attempts", []),
                )
            )

        success_assets = self._dedupe_assets_by_azure_url(success_assets)
        selected_records = self._selection_records(success_assets, inventory)
        selected_urls = [record["photo_url_big"] for record in select_prioritized_photo_records(selected_records, max_photos=30)]
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
            "pending_upload_count": len(pending),
            "selected_airtable_count": len(selected_urls),
            "successful_but_unserved_count": len([asset for asset in success_assets if not asset.get("selected_for_airtable")]),
            "non_azure_airtable_photos_count": len([
                url for url in parse_url_list(fields.get("Photos"))
                if urlparse(url).netloc.lower() != AZURE_ACCOUNT_HOST
            ]),
        }

        if not config.dry_run:
            photos_section["azure_assets"] = success_assets
            photos_section["azure_asset_failures"] = kept_failures
            photos_section["azure_asset_summary"] = summary

        return {
            "place_name": place_name,
            "place_id": place_id,
            "record_id": record_id,
            "inventory": inventory,
            "assets": success_assets,
            "failures": kept_failures,
            "pending": pending,
            "selected_airtable_urls": selected_urls,
            "selected_source_urls": self._source_selection_urls(inventory),
            "summary": summary,
            "place_data": working_place_data,
        }

    def build_health_report(self, airtable_record: Dict[str, Any], place_data: Optional[Dict[str, Any]], city: str = "charlotte") -> Dict[str, Any]:
        fields = airtable_record.get("fields", {}) if isinstance(airtable_record, dict) else {}
        place_id = fields.get("Google Maps Place Id", "")
        photos_section = place_data.get("photos", {}) if isinstance(place_data, dict) else {}
        inventory, inventory_summary = build_place_photo_inventory(airtable_record, place_data, city)
        airtable_photos = parse_url_list(fields.get("Photos"))
        non_azure_airtable = [url for url in airtable_photos if urlparse(url).netloc.lower() != AZURE_ACCOUNT_HOST]
        azure_assets = photos_section.get("azure_assets", []) if isinstance(photos_section, dict) else []
        failures = photos_section.get("azure_asset_failures", []) if isinstance(photos_section, dict) else []
        selected_azure = [url for url in airtable_photos if is_valid_existing_place_photo_azure_url(url, city, place_id)[0]]
        return {
            **inventory_summary,
            "place_name": fields.get("Place", "Unknown"),
            "place_id": place_id,
            "record_id": airtable_record.get("id", ""),
            "data_file_source_total": len(inventory),
            "airtable_photos_count": len(airtable_photos),
            "airtable_photos_google_count": len(parse_url_list(fields.get("Photos Google"))),
            "provider_raw_photo_url_big_count": inventory_summary["provider_raw_photo_url_big_count"],
            "azure_assets_count": len([asset for asset in azure_assets if isinstance(asset, dict)]),
            "selected_azure_display_urls_count": len(selected_azure),
            "failed_upload_count": len([failure for failure in failures if isinstance(failure, dict)]),
            "successful_but_unserved_count": len([
                asset for asset in azure_assets
                if isinstance(asset, dict) and not asset.get("selected_for_airtable")
            ]),
            "duplicate_count": inventory_summary["duplicate_count"],
            "remaining_non_azure_airtable_photos_count": len(non_azure_airtable),
            "remaining_non_azure_airtable_photos": non_azure_airtable,
        }

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
            "blob_container": PLACE_PHOTOS_CONTAINER,
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

    def _should_retry_failure(self, failure: Dict[str, Any], config: PhotoAssetConfig) -> bool:
        if not config.retry_failures:
            return False
        failed_at = failure.get("failed_at")
        if not isinstance(failed_at, str):
            return True
        try:
            failed_datetime = datetime.fromisoformat(failed_at.replace("Z", "+00:00"))
        except ValueError:
            return True
        if failed_datetime.tzinfo is None:
            failed_datetime = failed_datetime.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - failed_datetime > timedelta(hours=config.failure_ttl_hours)

    def _selection_records(self, assets: List[Dict[str, Any]], inventory: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        candidates_by_hash = {candidate["source_url_sha256"]: candidate for candidate in inventory}
        records: List[Dict[str, Any]] = []
        for asset in assets:
            candidate = candidates_by_hash.get(asset.get("source_url_sha256"), {})
            photo_record = copy.deepcopy(candidate.get("photo_record", {}))
            photo_record["photo_url_big"] = asset.get("azure_url")
            photo_record["source_url_sha256"] = asset.get("source_url_sha256")
            records.append(photo_record)
        return records

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
            if existing.get("status") == "existing_azure" and asset.get("status") != "existing_azure":
                asset["duplicate_provenance"] = asset.get("duplicate_provenance", []) + existing.get("provenance", [])
                deduped[existing_index] = asset
            else:
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