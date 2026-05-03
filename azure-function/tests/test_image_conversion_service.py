from services.image_conversion_service import IMAGE_EXTENSIONS, convert_image_for_upload, validated_content_type


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
