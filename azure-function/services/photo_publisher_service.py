from __future__ import annotations

import hashlib
import logging
import re
import unicodedata
from pathlib import PurePosixPath
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests

from services.image_conversion_service import convert_image_for_upload, validated_content_type
from services.utils import upload_blob_to_container


PHOTOS_CONTAINER = "photos"
LEGACY_PLACE_PHOTOS_CONTAINER = "place-photos"
LEGACY_CURATOR_PHOTOS_CONTAINER = "curator-photos"
AZURE_ACCOUNT_HOST = "thirdplacesdata.blob.core.windows.net"
CACHE_CONTROL_IMMUTABLE = "public, max-age=31536000, immutable"
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


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def canonical_photo_url(blob_path: str) -> str:
    return f"https://{AZURE_ACCOUNT_HOST}/{PHOTOS_CONTAINER}/{blob_path}"


def azure_blob_url(container_name: str, blob_path: str) -> str:
    return f"https://{AZURE_ACCOUNT_HOST}/{container_name}/{blob_path}"


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


def safe_filename_stem(filename: str) -> str:
    stem = PurePosixPath(filename or "photo").stem or "photo"
    normalized = unicodedata.normalize("NFKD", stem).encode("ascii", "ignore").decode("ascii")
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", normalized).strip(".-_")
    return safe[:80] or "photo"


class PhotoPublisherService:
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
                            attempt["error"] = "response body did not look like a supported image"
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

    def publish_standard_url(
        self,
        source_url: str,
        place_id: str,
        record_id: str,
        place_name: str,
        source_hash: Optional[str] = None,
        source_field: str = "",
        source_path: str = "",
        dry_run: bool = True,
        upload: bool = False,
        try_url_variants: bool = True,
        download_timeout_seconds: int = 60,
    ) -> Dict[str, Any]:
        return self._publish_url(
            source_url=source_url,
            place_id=place_id,
            record_id=record_id,
            place_name=place_name,
            source_hash=source_hash,
            source_field=source_field,
            source_path=source_path,
            dry_run=dry_run,
            upload=upload,
            try_url_variants=try_url_variants,
            download_timeout_seconds=download_timeout_seconds,
            blob_kind="standard",
        )

    def publish_legacy_curator_url(
        self,
        source_url: str,
        place_id: str,
        record_id: str,
        place_name: str,
        source_hash: Optional[str] = None,
        source_field: str = "legacy.curator-photos",
        source_path: str = "",
        dry_run: bool = True,
        upload: bool = False,
        try_url_variants: bool = True,
        download_timeout_seconds: int = 60,
    ) -> Dict[str, Any]:
        return self._publish_url(
            source_url=source_url,
            place_id=place_id,
            record_id=record_id,
            place_name=place_name,
            source_hash=source_hash,
            source_field=source_field,
            source_path=source_path,
            dry_run=dry_run,
            upload=upload,
            try_url_variants=try_url_variants,
            download_timeout_seconds=download_timeout_seconds,
            blob_kind="legacy_curator",
        )

    def publish_curator_attachment(
        self,
        attachment: Dict[str, Any],
        place_id: str,
        record_id: str,
        place_name: str,
        dry_run: bool = True,
        upload: bool = False,
        try_url_variants: bool = True,
        download_timeout_seconds: int = 60,
    ) -> Dict[str, Any]:
        attachment_id = str(attachment.get("id") or "").strip()
        filename = str(attachment.get("filename") or "photo.jpg").strip()
        source_url = str(attachment.get("url") or "").strip()
        if not attachment_id or not source_url:
            return {
                "success": False,
                "status": "missing_attachment_data",
                "error": "attachment id or url missing",
                "attachment_id": attachment_id,
                "filename": filename,
                "source_url": source_url,
            }

        return self._publish_url(
            source_url=source_url,
            place_id=place_id,
            record_id=record_id,
            place_name=place_name,
            source_hash=sha256_hex(source_url),
            source_field="Curator Photos",
            source_path=f"fields.Curator Photos[{attachment_id}]",
            dry_run=dry_run,
            upload=upload,
            try_url_variants=try_url_variants,
            download_timeout_seconds=download_timeout_seconds,
            blob_kind="curator",
            attachment_id=attachment_id,
            filename=filename,
        )

    def _publish_url(
        self,
        source_url: str,
        place_id: str,
        record_id: str,
        place_name: str,
        source_hash: Optional[str],
        source_field: str,
        source_path: str,
        dry_run: bool,
        upload: bool,
        try_url_variants: bool,
        download_timeout_seconds: int,
        blob_kind: str,
        attachment_id: str = "",
        filename: str = "",
    ) -> Dict[str, Any]:
        canonical_source_url = (source_url or "").strip()
        effective_source_hash = source_hash or sha256_hex(canonical_source_url)
        if dry_run or not upload:
            blob_path = self._blob_path(blob_kind, place_id, effective_source_hash, ".webp", attachment_id, filename)
            return {
                "success": True,
                "status": "would_upload",
                "azure_url": canonical_photo_url(blob_path),
                "blob_container": PHOTOS_CONTAINER,
                "blob_path": blob_path,
                "content_sha256": "",
                "content_type": "image/webp",
                "bytes": 0,
                "source_url": source_url,
                "canonical_source_url": canonical_source_url,
                "source_url_sha256": effective_source_hash,
                "source_field": source_field,
                "source_path": source_path,
                "attempts": [],
                "conversion_status": "dry_run",
                "converted_to_webp": False,
                "fallback_original": False,
                "fallback_reason": "",
                "conversion_warning": "",
                "conversion_error": "",
                "webp_encoder_available": False,
                "dry_run": dry_run,
            }

        download_result = self.download_image_asset(
            canonical_source_url,
            try_url_variants=try_url_variants,
            timeout=download_timeout_seconds,
        )
        if not download_result.get("success"):
            return {
                "success": False,
                "status": "download_failed",
                "error": download_result.get("error", "download failed"),
                "http_status": download_result.get("status_code"),
                "attempts": download_result.get("attempts", []),
                "source_url": source_url,
                "canonical_source_url": canonical_source_url,
                "source_url_sha256": effective_source_hash,
                "source_field": source_field,
                "source_path": source_path,
            }

        conversion = convert_image_for_upload(
            download_result["data"],
            download_result["content_type"],
            download_result.get("extension", ""),
        )
        if not conversion.success:
            return {
                "success": False,
                "status": conversion.status,
                "error": conversion.error,
                "attempts": download_result.get("attempts", []),
                "source_url": source_url,
                "canonical_source_url": canonical_source_url,
                "source_url_sha256": effective_source_hash,
                "source_field": source_field,
                "source_path": source_path,
            }

        blob_path = self._blob_path(blob_kind, place_id, effective_source_hash, conversion.extension, attachment_id, filename)
        metadata = {
            "airtable_record_id": record_id[:128],
            "place_id": place_id[:128],
            "place_name": place_name[:128],
            "source_url_sha256": effective_source_hash,
            "source_host": urlparse(canonical_source_url).netloc.lower()[:128],
            "source_field": source_field[:128],
            "content_sha256": conversion.content_sha256,
            "content_type": conversion.content_type,
            "conversion_status": conversion.status[:128],
        }
        azure_url = upload_blob_to_container(
            PHOTOS_CONTAINER,
            blob_path,
            conversion.data,
            content_type=conversion.content_type,
            metadata=metadata,
            cache_control=CACHE_CONTROL_IMMUTABLE,
            public_access="blob",
            overwrite=True,
        )

        return {
            "success": True,
            "status": "uploaded",
            "azure_url": azure_url,
            "blob_container": PHOTOS_CONTAINER,
            "blob_path": blob_path,
            "content_sha256": conversion.content_sha256,
            "content_type": conversion.content_type,
            "bytes": len(conversion.data),
            "source_url": source_url,
            "canonical_source_url": canonical_source_url,
            "source_url_sha256": effective_source_hash,
            "source_field": source_field,
            "source_path": source_path,
            "attempts": download_result.get("attempts", []),
            "conversion_status": conversion.status,
            "converted_to_webp": conversion.converted_to_webp,
            "fallback_original": conversion.fallback_original,
            "fallback_reason": conversion.fallback_reason,
            "conversion_warning": conversion.warning,
            "conversion_error": conversion.error,
            "webp_encoder_available": conversion.webp_encoder_available,
            "dry_run": dry_run,
        }

    def _blob_path(
        self,
        blob_kind: str,
        place_id: str,
        source_hash: str,
        extension: str,
        attachment_id: str = "",
        filename: str = "",
    ) -> str:
        if blob_kind == "curator":
            return f"{place_id}/curator-{attachment_id}-{safe_filename_stem(filename)}{extension}"
        if blob_kind == "legacy_curator":
            return f"{place_id}/curator-legacy-{source_hash}{extension}"
        return f"{place_id}/{source_hash}{extension}"