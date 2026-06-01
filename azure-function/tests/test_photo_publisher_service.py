from services.image_conversion_service import ImageVariantResult
from services.photo_publisher_service import PhotoPublisherService


def _variant(name: str, data: bytes) -> ImageVariantResult:
    return ImageVariantResult(
        success=True,
        variant=name,
        data=data,
        content_type="image/webp",
        extension=".webp",
        content_sha256=f"{name}-sha",
        width=1600 if name == "display" else 256,
        height=900 if name == "display" else 256,
        bytes=len(data),
        status="generated_webp",
    )


def test_publish_standard_url_dry_run_returns_display_thumbnail_manifest(monkeypatch):
    service = PhotoPublisherService()
    checked_blob_paths = []

    def fake_blob_exists(container, blob_path):
        checked_blob_paths.append(blob_path)
        return False

    monkeypatch.setattr("services.photo_publisher_service.blob_exists_in_container", fake_blob_exists)

    result = service.publish_standard_url(
        "https://example.com/photo.jpg",
        place_id="ChIJ123",
        record_id="rec123",
        place_name="Test Place",
        source_hash="abc123",
        dry_run=True,
        upload=False,
    )

    assert result["success"] is True
    assert result["status"] == "would_upload_missing_both"
    assert result["photo_manifest"] == {
        "display": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/abc123.webp",
        "thumbnail": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/abc123.webp",
    }
    assert result["azure_url"] == result["photo_manifest"]["display"]
    assert result["thumbnail_url"] == result["photo_manifest"]["thumbnail"]
    assert result["display_blob_path"] == "ChIJ123/display/abc123.webp"
    assert result["thumbnail_blob_path"] == "ChIJ123/thumbnail/abc123.webp"
    assert result["display_blob_exists"] is False
    assert result["thumbnail_blob_exists"] is False
    assert result["missing_variants"] == ["display", "thumbnail"]
    assert checked_blob_paths == [
        "ChIJ123/display/abc123.webp",
        "ChIJ123/thumbnail/abc123.webp",
    ]


def test_publish_standard_url_dry_run_reports_existing_variants(monkeypatch):
    service = PhotoPublisherService()
    monkeypatch.setattr("services.photo_publisher_service.blob_exists_in_container", lambda container, blob_path: True)

    result = service.publish_standard_url(
        "https://example.com/photo.jpg",
        place_id="ChIJ123",
        record_id="rec123",
        place_name="Test Place",
        source_hash="abc123",
        dry_run=True,
        upload=False,
    )

    assert result["success"] is True
    assert result["status"] == "already_exists"
    assert result["display_blob_exists"] is True
    assert result["thumbnail_blob_exists"] is True
    assert result["missing_variants"] == []


def test_publish_standard_url_dry_run_reports_missing_thumbnail(monkeypatch):
    service = PhotoPublisherService()

    def fake_blob_exists(container, blob_path):
        return "/display/" in blob_path

    monkeypatch.setattr("services.photo_publisher_service.blob_exists_in_container", fake_blob_exists)

    result = service.publish_standard_url(
        "https://example.com/photo.jpg",
        place_id="ChIJ123",
        record_id="rec123",
        place_name="Test Place",
        source_hash="abc123",
        dry_run=True,
        upload=False,
    )

    assert result["status"] == "would_upload_missing_thumbnail"
    assert result["display_blob_exists"] is True
    assert result["thumbnail_blob_exists"] is False
    assert result["missing_variants"] == ["thumbnail"]


def test_publish_standard_url_uploads_display_and_thumbnail_variants(monkeypatch):
    service = PhotoPublisherService()
    uploads = []
    monkeypatch.setattr("services.photo_publisher_service.blob_exists_in_container", lambda container, blob_path: False)

    monkeypatch.setattr(
        service,
        "download_image_asset",
        lambda source_url, try_url_variants=True, timeout=60: {
            "success": True,
            "data": b"source-image",
            "content_type": "image/jpeg",
            "extension": ".jpg",
            "attempts": [],
        },
    )
    monkeypatch.setattr(
        "services.photo_publisher_service.generate_served_image_variants",
        lambda data, source_content_type, source_extension="": {
            "display": _variant("display", b"display-bytes"),
            "thumbnail": _variant("thumbnail", b"thumbnail-bytes"),
        },
    )

    def fake_upload(container, blob_path, data, content_type, metadata, cache_control, public_access, overwrite):
        uploads.append({
            "container": container,
            "blob_path": blob_path,
            "data": data,
            "content_type": content_type,
            "metadata": metadata,
            "cache_control": cache_control,
            "public_access": public_access,
            "overwrite": overwrite,
        })
        return f"https://thirdplacesdata.blob.core.windows.net/{container}/{blob_path}"

    monkeypatch.setattr("services.photo_publisher_service.upload_blob_to_container", fake_upload)

    result = service.publish_standard_url(
        "https://example.com/photo.jpg",
        place_id="ChIJ123",
        record_id="rec123",
        place_name="Test Place",
        source_hash="abc123",
        dry_run=False,
        upload=True,
    )

    assert result["success"] is True
    assert result["photo_manifest"] == {
        "display": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/abc123.webp",
        "thumbnail": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/abc123.webp",
    }
    assert [upload["blob_path"] for upload in uploads] == [
        "ChIJ123/display/abc123.webp",
        "ChIJ123/thumbnail/abc123.webp",
    ]
    assert [upload["metadata"]["variant"] for upload in uploads] == ["display", "thumbnail"]
    assert all(upload["content_type"] == "image/webp" for upload in uploads)
    assert all(upload["cache_control"] == "public, max-age=31536000, immutable" for upload in uploads)


def test_publish_standard_url_reuses_existing_display_and_thumbnail_variants(monkeypatch):
    service = PhotoPublisherService()
    checked_blob_paths = []

    def fake_blob_exists(container, blob_path):
        checked_blob_paths.append(blob_path)
        return True

    def fail_call(*args, **kwargs):
        raise AssertionError("existing standard variants should skip download, conversion, and upload")

    monkeypatch.setattr("services.photo_publisher_service.blob_exists_in_container", fake_blob_exists)
    monkeypatch.setattr(service, "download_image_asset", fail_call)
    monkeypatch.setattr("services.photo_publisher_service.generate_served_image_variants", fail_call)
    monkeypatch.setattr("services.photo_publisher_service.upload_blob_to_container", fail_call)

    result = service.publish_standard_url(
        "https://example.com/photo.jpg",
        place_id="ChIJ123",
        record_id="rec123",
        place_name="Test Place",
        source_hash="abc123",
        dry_run=False,
        upload=True,
    )

    assert result["success"] is True
    assert result["status"] == "already_exists"
    assert result["photo_manifest"] == {
        "display": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/abc123.webp",
        "thumbnail": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/abc123.webp",
    }
    assert checked_blob_paths == [
        "ChIJ123/display/abc123.webp",
        "ChIJ123/thumbnail/abc123.webp",
    ]


def test_publish_curator_attachment_preserves_curator_filename_prefix_in_variant_paths(monkeypatch):
    service = PhotoPublisherService()
    monkeypatch.setattr("services.photo_publisher_service.blob_exists_in_container", lambda container, blob_path: False)

    result = service.publish_curator_attachment(
        {"id": "attABC123", "filename": "Cafe Interior.JPG", "url": "https://airtable.example/photo"},
        place_id="ChIJ123",
        record_id="rec123",
        place_name="Test Place",
        dry_run=True,
        upload=False,
    )

    assert result["photo_manifest"] == {
        "display": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/curator-attABC123-Cafe-Interior.webp",
        "thumbnail": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/curator-attABC123-Cafe-Interior.webp",
    }


def test_publish_curator_attachment_reuses_existing_display_and_thumbnail_variants(monkeypatch):
    service = PhotoPublisherService()
    checked_blob_paths = []

    def fake_blob_exists(container, blob_path):
        checked_blob_paths.append(blob_path)
        return True

    def fail_call(*args, **kwargs):
        raise AssertionError("existing variants should skip download, conversion, and upload")

    monkeypatch.setattr("services.photo_publisher_service.blob_exists_in_container", fake_blob_exists)
    monkeypatch.setattr(service, "download_image_asset", fail_call)
    monkeypatch.setattr("services.photo_publisher_service.generate_served_image_variants", fail_call)
    monkeypatch.setattr("services.photo_publisher_service.upload_blob_to_container", fail_call)

    result = service.publish_curator_attachment(
        {"id": "attABC123", "filename": "Cafe Interior.JPG", "url": "https://airtable.example/photo"},
        place_id="ChIJ123",
        record_id="rec123",
        place_name="Test Place",
        dry_run=False,
        upload=True,
    )

    assert result["success"] is True
    assert result["status"] == "already_exists"
    assert result["photo_manifest"] == {
        "display": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/curator-attABC123-Cafe-Interior.webp",
        "thumbnail": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/curator-attABC123-Cafe-Interior.webp",
    }
    assert checked_blob_paths == [
        "ChIJ123/display/curator-attABC123-Cafe-Interior.webp",
        "ChIJ123/thumbnail/curator-attABC123-Cafe-Interior.webp",
    ]


def test_publish_curator_attachment_uploads_when_a_variant_is_missing(monkeypatch):
    service = PhotoPublisherService()
    checked_blob_paths = []
    uploads = []

    def fake_blob_exists(container, blob_path):
        checked_blob_paths.append(blob_path)
        return "/thumbnail/" not in blob_path

    monkeypatch.setattr("services.photo_publisher_service.blob_exists_in_container", fake_blob_exists)
    monkeypatch.setattr(
        service,
        "download_image_asset",
        lambda source_url, try_url_variants=True, timeout=60: {
            "success": True,
            "data": b"source-image",
            "content_type": "image/jpeg",
            "extension": ".jpg",
            "attempts": [],
        },
    )
    monkeypatch.setattr(
        "services.photo_publisher_service.generate_served_image_variants",
        lambda data, source_content_type, source_extension="": {
            "display": _variant("display", b"display-bytes"),
            "thumbnail": _variant("thumbnail", b"thumbnail-bytes"),
        },
    )

    def fake_upload(container, blob_path, data, content_type, metadata, cache_control, public_access, overwrite):
        uploads.append(blob_path)
        return f"https://thirdplacesdata.blob.core.windows.net/{container}/{blob_path}"

    monkeypatch.setattr("services.photo_publisher_service.upload_blob_to_container", fake_upload)

    result = service.publish_curator_attachment(
        {"id": "attABC123", "filename": "Cafe Interior.JPG", "url": "https://airtable.example/photo"},
        place_id="ChIJ123",
        record_id="rec123",
        place_name="Test Place",
        dry_run=False,
        upload=True,
    )

    assert result["success"] is True
    assert result["status"] == "uploaded"
    assert checked_blob_paths == [
        "ChIJ123/display/curator-attABC123-Cafe-Interior.webp",
        "ChIJ123/thumbnail/curator-attABC123-Cafe-Interior.webp",
    ]
    assert uploads == [
        "ChIJ123/display/curator-attABC123-Cafe-Interior.webp",
        "ChIJ123/thumbnail/curator-attABC123-Cafe-Interior.webp",
    ]


def test_publish_curator_source_url_uses_curator_variant_prefix(monkeypatch):
    service = PhotoPublisherService()
    monkeypatch.setattr("services.photo_publisher_service.blob_exists_in_container", lambda container, blob_path: False)

    result = service.publish_curator_source_url(
        "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/curator-att123-front.webp",
        place_id="ChIJ123",
        record_id="rec123",
        place_name="Test Place",
        source_hash="abc123",
        dry_run=True,
        upload=False,
    )

    assert result["photo_manifest"] == {
        "display": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/curator-abc123.webp",
        "thumbnail": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/curator-abc123.webp",
    }
