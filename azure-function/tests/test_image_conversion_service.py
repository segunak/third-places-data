import io

import pytest

from services.image_conversion_service import IMAGE_EXTENSIONS, generate_served_image_variants, convert_image_for_upload, validated_content_type


def _webp_available():
    try:
        from PIL import features

        return bool(features.check("webp"))
    except Exception:
        return False


def _jpeg_bytes(width=2400, height=1200, color=(32, 96, 160)):
    from PIL import Image

    image = Image.new("RGB", (width, height), color)
    output = io.BytesIO()
    image.save(output, format="JPEG", quality=95)
    return output.getvalue()


def test_validated_content_type_rejects_sniffed_type_not_in_image_extensions():
    assert "image/gif" not in IMAGE_EXTENSIONS

    content_type, extension = validated_content_type("image/gif", b"GIF89aimage")

    assert content_type is None
    assert extension == ".gif"


def test_validated_content_type_rejects_header_type_not_in_image_extensions():
    assert "image/avif" not in IMAGE_EXTENSIONS

    content_type, extension = validated_content_type("image/avif", b"not enough to sniff")

    assert content_type is None
    assert extension == ".jpg"


def test_convert_image_for_upload_rejects_empty_image():
    result = convert_image_for_upload(b"", "image/jpeg", ".jpg")

    assert result.success is False
    assert result.status == "empty_image"
    assert result.error == "empty image bytes"


def test_convert_image_for_upload_keeps_existing_webp():
    data = b"RIFF\x1a\x00\x00\x00WEBPVP8 minimal"

    result = convert_image_for_upload(data, "image/webp", ".webp")

    assert result.success is True
    assert result.status == "already_webp"
    assert result.content_type == "image/webp"
    assert result.extension == ".webp"
    assert result.data == data


@pytest.mark.skipif(not _webp_available(), reason="Pillow WebP encoder is unavailable")
def test_generate_served_image_variants_creates_bounded_display_and_thumbnail():
    variants = generate_served_image_variants(_jpeg_bytes(), "image/jpeg", ".jpg")

    display = variants["display"]
    thumbnail = variants["thumbnail"]

    assert display.success is True
    assert display.variant == "display"
    assert display.content_type == "image/webp"
    assert display.extension == ".webp"
    assert max(display.width, display.height) == 1600
    assert display.bytes == len(display.data)

    assert thumbnail.success is True
    assert thumbnail.variant == "thumbnail"
    assert thumbnail.content_type == "image/webp"
    assert thumbnail.width == 256
    assert thumbnail.height == 256
    assert thumbnail.bytes == len(thumbnail.data)


def test_generate_served_image_variants_rejects_empty_image():
    variants = generate_served_image_variants(b"", "image/jpeg", ".jpg")

    assert variants["display"].success is False
    assert variants["display"].status == "empty_image"
    assert variants["thumbnail"].success is False
    assert variants["thumbnail"].status == "empty_image"
