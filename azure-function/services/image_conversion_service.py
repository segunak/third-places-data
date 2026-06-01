from __future__ import annotations

import hashlib
import io
import logging
from dataclasses import dataclass
from typing import Optional, Tuple


IMAGE_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
}

# Served photo targets: display images stay sharp in the carousel without forcing
# WebViews to decode originals, while thumbnails are only for rails and filmstrips.
# Python underscores are digit separators, so 800_000 is the same value as 800000.
DISPLAY_MAX_LONG_EDGE = 1600
DISPLAY_MAX_BYTES = 800_000
THUMBNAIL_SIZE = 256
THUMBNAIL_MAX_BYTES = 50_000


@dataclass
class ImageVariantResult:
    success: bool
    variant: str
    data: bytes
    content_type: str
    extension: str
    content_sha256: str
    width: int
    height: int
    bytes: int
    status: str
    warning: str = ""
    error: str = ""


@dataclass
class ImageConversionResult:
    success: bool
    data: bytes
    content_type: str
    extension: str
    content_sha256: str
    original_content_type: str
    original_extension: str
    status: str
    converted_to_webp: bool = False
    fallback_original: bool = False
    fallback_reason: str = ""
    warning: str = ""
    error: str = ""
    webp_encoder_available: bool = False


def sniff_image_content(data: bytes) -> Tuple[Optional[str], Optional[str]]:
    sample = data[:64]
    if sample.startswith(b"\xff\xd8\xff"):
        return "image/jpeg", ".jpg"
    if sample.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png", ".png"
    if sample.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif", ".gif"
    if len(sample) >= 12 and sample[:4] == b"RIFF" and sample[8:12] == b"WEBP":
        return "image/webp", ".webp"
    return None, None


def extension_for_content_type(content_type: str) -> str:
    clean_type = (content_type or "").split(";", 1)[0].strip().lower()
    return IMAGE_EXTENSIONS.get(clean_type, ".jpg")


def validated_content_type(header_content_type: str, data: bytes) -> Tuple[Optional[str], str]:
    sniffed_type, sniffed_extension = sniff_image_content(data)
    header_type = (header_content_type or "").split(";", 1)[0].strip().lower()
    if sniffed_type:
        if sniffed_type not in IMAGE_EXTENSIONS:
            return None, sniffed_extension or extension_for_content_type(sniffed_type)
        return sniffed_type, IMAGE_EXTENSIONS[sniffed_type]
    if header_type in IMAGE_EXTENSIONS:
        return header_type, IMAGE_EXTENSIONS[header_type]
    return None, extension_for_content_type(header_type)


def webp_encoder_available() -> bool:
    try:
        from PIL import features

        return bool(features.check("webp"))
    except Exception:
        return False


def _fallback_result(
    data: bytes,
    content_type: str,
    extension: str,
    reason: str,
    warning: str = "",
    error: str = "",
) -> ImageConversionResult:
    return ImageConversionResult(
        success=True,
        data=data,
        content_type=content_type,
        extension=extension,
        content_sha256=hashlib.sha256(data).hexdigest(),
        original_content_type=content_type,
        original_extension=extension,
        status=reason,
        fallback_original=True,
        fallback_reason=reason,
        warning=warning or reason,
        error=error,
        webp_encoder_available=webp_encoder_available(),
    )


def convert_image_for_upload(
    data: bytes,
    source_content_type: str,
    source_extension: str = "",
    webp_quality: int = 82,
) -> ImageConversionResult:
    content_type, extension = validated_content_type(source_content_type, data)
    if not data:
        return ImageConversionResult(
            success=False,
            data=b"",
            content_type="",
            extension=source_extension or ".jpg",
            content_sha256="",
            original_content_type=source_content_type or "",
            original_extension=source_extension or "",
            status="empty_image",
            error="empty image bytes",
            webp_encoder_available=webp_encoder_available(),
        )

    if content_type is None:
        return ImageConversionResult(
            success=False,
            data=data,
            content_type=source_content_type or "",
            extension=extension or source_extension or ".jpg",
            content_sha256=hashlib.sha256(data).hexdigest(),
            original_content_type=source_content_type or "",
            original_extension=extension or source_extension or "",
            status="unsupported_format",
            error="unsupported image content type",
            webp_encoder_available=webp_encoder_available(),
        )

    if content_type == "image/webp":
        return ImageConversionResult(
            success=True,
            data=data,
            content_type="image/webp",
            extension=".webp",
            content_sha256=hashlib.sha256(data).hexdigest(),
            original_content_type=content_type,
            original_extension=extension or ".webp",
            status="already_webp",
            webp_encoder_available=webp_encoder_available(),
        )

    try:
        from PIL import Image, ImageOps, features
    except Exception as exc:
        logging.warning("Pillow is unavailable; uploading original image bytes: %s", exc)
        return _fallback_result(data, content_type, extension, "pillow_unavailable", error=str(exc))

    if not features.check("webp"):
        logging.warning("Pillow WebP encoder is unavailable; uploading original image bytes.")
        return _fallback_result(data, content_type, extension, "webp_encoder_unavailable")

    try:
        with Image.open(io.BytesIO(data)) as image:
            image = ImageOps.exif_transpose(image)
            icc_profile = image.info.get("icc_profile")
            if image.mode not in ("RGB", "RGBA"):
                if image.mode == "P" and "transparency" in image.info:
                    image = image.convert("RGBA")
                else:
                    image = image.convert("RGB")

            output = io.BytesIO()
            save_kwargs = {
                "format": "WEBP",
                "quality": webp_quality,
                "method": 4,
            }
            if icc_profile:
                save_kwargs["icc_profile"] = icc_profile
            image.save(output, **save_kwargs)
            converted_data = output.getvalue()
            return ImageConversionResult(
                success=True,
                data=converted_data,
                content_type="image/webp",
                extension=".webp",
                content_sha256=hashlib.sha256(converted_data).hexdigest(),
                original_content_type=content_type,
                original_extension=extension,
                status="converted_webp",
                converted_to_webp=True,
                webp_encoder_available=True,
            )
    except Exception as exc:
        logging.warning("Image conversion to WebP failed; uploading original image bytes: %s", exc)
        return _fallback_result(data, content_type, extension, "conversion_failed", error=str(exc))


def _failed_variant(variant: str, status: str, error: str, extension: str = ".jpg") -> ImageVariantResult:
    return ImageVariantResult(
        success=False,
        variant=variant,
        data=b"",
        content_type="",
        extension=extension,
        content_sha256="",
        width=0,
        height=0,
        bytes=0,
        status=status,
        error=error,
    )


def _prepare_pillow_image(data: bytes, source_content_type: str, source_extension: str):
    content_type, extension = validated_content_type(source_content_type, data)
    if not data:
        return None, None, extension or source_extension or ".jpg", "empty_image", "empty image bytes"
    if content_type is None:
        return None, None, extension or source_extension or ".jpg", "unsupported_format", "unsupported image content type"

    try:
        from PIL import Image, ImageOps, features
    except Exception as exc:
        return None, None, extension or source_extension or ".jpg", "pillow_unavailable", str(exc)

    if not features.check("webp"):
        return None, None, extension or source_extension or ".jpg", "webp_encoder_unavailable", "Pillow WebP encoder is unavailable"

    try:
        image = Image.open(io.BytesIO(data))
        image = ImageOps.exif_transpose(image)
        if image.mode not in ("RGB", "RGBA"):
            if image.mode == "P" and "transparency" in image.info:
                image = image.convert("RGBA")
            else:
                image = image.convert("RGB")
        return image, content_type, extension or source_extension or ".jpg", "ready", ""
    except Exception as exc:
        return None, content_type, extension or source_extension or ".jpg", "image_open_failed", str(exc)


def _save_webp_variant(image, variant: str, quality: int, method: int = 6) -> ImageVariantResult:
    output = io.BytesIO()
    image.save(output, format="WEBP", quality=quality, method=method)
    variant_data = output.getvalue()
    return ImageVariantResult(
        success=True,
        variant=variant,
        data=variant_data,
        content_type="image/webp",
        extension=".webp",
        content_sha256=hashlib.sha256(variant_data).hexdigest(),
        width=image.width,
        height=image.height,
        bytes=len(variant_data),
        status="generated_webp",
    )


def _resize_to_max_long_edge(image, max_long_edge: int):
    if max(image.width, image.height) <= max_long_edge:
        return image.copy()
    resized = image.copy()
    resized.thumbnail((max_long_edge, max_long_edge))
    return resized


def _center_crop_square(image):
    side = min(image.width, image.height)
    left = max(0, (image.width - side) // 2)
    top = max(0, (image.height - side) // 2)
    return image.crop((left, top, left + side, top + side))


def generate_served_image_variants(
    data: bytes,
    source_content_type: str,
    source_extension: str = "",
) -> dict[str, ImageVariantResult]:
    image, _content_type, extension, status, error = _prepare_pillow_image(data, source_content_type, source_extension)
    if image is None:
        return {
            "display": _failed_variant("display", status, error, extension),
            "thumbnail": _failed_variant("thumbnail", status, error, extension),
        }

    try:
        with image:
            # Preserve the original aspect ratio for the main image and retry with
            # smaller/lower-quality output if it is still too large to serve cheaply.
            display_image = _resize_to_max_long_edge(image, DISPLAY_MAX_LONG_EDGE)
            display = _save_webp_variant(display_image, "display", quality=82)
            if display.bytes > DISPLAY_MAX_BYTES:
                display = _save_webp_variant(display_image, "display", quality=78)
                display.warning = "display_retry_quality_78"
            if display.bytes > DISPLAY_MAX_BYTES:
                display_image = _resize_to_max_long_edge(image, 1400)
                display = _save_webp_variant(display_image, "display", quality=78)
                display.warning = "display_retry_1400_quality_78"
            if display.bytes > DISPLAY_MAX_BYTES:
                display.warning = "display_exceeds_800kb_after_retries"

            # Thumbnail rails render fixed square boxes, so generate a true small
            # square asset instead of asking the browser to decode the display image.
            thumbnail_image = _center_crop_square(image)
            if thumbnail_image.width > THUMBNAIL_SIZE:
                thumbnail_image = thumbnail_image.resize((THUMBNAIL_SIZE, THUMBNAIL_SIZE))
            thumbnail = _save_webp_variant(thumbnail_image, "thumbnail", quality=70)
            if thumbnail.bytes > THUMBNAIL_MAX_BYTES:
                thumbnail.warning = "thumbnail_exceeds_50kb"

            return {
                "display": display,
                "thumbnail": thumbnail,
            }
    except Exception as exc:
        logging.warning("Image variant generation failed: %s", exc)
        return {
            "display": _failed_variant("display", "variant_generation_failed", str(exc), extension),
            "thumbnail": _failed_variant("thumbnail", "variant_generation_failed", str(exc), extension),
        }