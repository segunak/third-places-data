import json
from unittest import mock

from azure.core.exceptions import ResourceNotFoundError

from services import utils
from services.photo_asset_service import (
    PhotoAssetConfig,
    PhotoAssetService,
    build_place_photo_inventory,
    is_valid_existing_place_photo_azure_url,
    sha256_hex,
    validated_content_type,
)


def _place_record(photos="", photos_google=""):
    return {
        "id": "rec123",
        "fields": {
            "Place": "Test Place",
            "Google Maps Place Id": "ChIJ123",
            "Photos": photos,
            "Photos Google": photos_google,
        },
    }


def test_inventory_includes_only_in_scope_sources_and_dedupes():
    place_data = {
        "photos": {
            "photo_urls": [
                "https://example.com/photo-a.jpg",
                "https://example.com/photo-b.jpg",
            ],
            "raw_data": {
                "photos_data": [
                    {
                        "photo_id": "photo-a",
                        "photo_url_big": "https://example.com/photo-a.jpg",
                        "photo_tags": ["vibe"],
                        "photo_date": "12/01/2025 10:00:00",
                        "photo_url": "https://example.com/excluded-small.jpg",
                        "photo_source_video": "https://example.com/excluded-video.mp4",
                    },
                    {
                        "photo_id": "photo-c",
                        "photo_url_big": "https://example.com/photo-c.jpg",
                        "photo_tags": ["front"],
                        "photo_date": "11/01/2025 10:00:00",
                    },
                ]
            },
        }
    }

    inventory, summary = build_place_photo_inventory(
        _place_record(photos='["https://example.com/photo-a.jpg"]'),
        place_data,
    )

    source_urls = {candidate["canonical_source_url"] for candidate in inventory}
    assert source_urls == {
        "https://example.com/photo-a.jpg",
        "https://example.com/photo-b.jpg",
        "https://example.com/photo-c.jpg",
    }
    assert "https://example.com/excluded-small.jpg" not in source_urls
    assert "https://example.com/excluded-video.mp4" not in source_urls
    assert summary["duplicate_count"] == 1


def test_existing_place_photo_azure_url_validation():
    valid_url = "https://thirdplacesdata.blob.core.windows.net/place-photos/charlotte/ChIJ123/" + ("a" * 64) + ".jpg"
    invalid_url = "https://thirdplacesdata.blob.core.windows.net/curator-photos/rec123/photo.jpg"

    assert is_valid_existing_place_photo_azure_url(valid_url, "charlotte", "ChIJ123")[0] is True
    is_valid, _, reason = is_valid_existing_place_photo_azure_url(invalid_url, "charlotte", "ChIJ123")
    assert is_valid is False
    assert reason == "invalid_container"

    is_valid, _, reason = is_valid_existing_place_photo_azure_url(
        "https://thirdplacesdata.blob.core.windows.net/place-photos/charlotte/ChIJ123/not-a-hash.jpg",
        "charlotte",
        "ChIJ123",
    )
    assert is_valid is False
    assert reason == "invalid_blob_name"

    is_valid, _, reason = is_valid_existing_place_photo_azure_url(
        "https://thirdplacesdata.blob.core.windows.net/place-photos/charlotte/ChIJ123/" + ("a" * 64) + ".gif",
        "charlotte",
        "ChIJ123",
    )
    assert is_valid is False
    assert reason == "invalid_blob_extension"


def test_gif_content_is_not_supported_for_place_photo_assets():
    content_type, extension = validated_content_type("image/gif", b"GIF89aimage")

    assert content_type is None
    assert extension == ".gif"


def test_process_place_uploads_successful_assets_and_selects_azure_urls(monkeypatch):
    place_data = {
        "photos": {
            "raw_data": {
                "photos_data": [
                    {
                        "photo_url_big": "https://example.com/vibe.jpg",
                        "photo_tags": ["vibe"],
                        "photo_date": "12/01/2025 10:00:00",
                    },
                    {
                        "photo_url_big": "https://example.com/front.jpg",
                        "photo_tags": ["front"],
                        "photo_date": "11/01/2025 10:00:00",
                    },
                ]
            }
        }
    }

    service = PhotoAssetService()
    monkeypatch.setattr(
        service,
        "download_image_asset",
        lambda source_url, try_url_variants=True: {
            "success": True,
            "data": b"\xff\xd8\xffimage",
            "content_type": "image/jpeg",
            "extension": ".jpg",
            "content_sha256": "contenthash",
            "attempts": [],
            "bytes": 8,
        },
    )
    monkeypatch.setattr(
        "services.photo_asset_service.upload_blob_to_container",
        lambda container, blob_path, data, content_type, metadata, cache_control, public_access, overwrite: (
            f"https://thirdplacesdata.blob.core.windows.net/{container}/{blob_path}"
        ),
    )

    result = service.process_place(
        _place_record(),
        place_data,
        PhotoAssetConfig(dry_run=False, upload=True, write_airtable=True),
    )

    assert result["summary"]["azure_assets_count"] == 2
    assert len(result["selected_airtable_urls"]) == 2
    assert all("thirdplacesdata.blob.core.windows.net/place-photos/charlotte/ChIJ123" in url for url in result["selected_airtable_urls"])
    assert result["place_data"]["photos"]["azure_assets"]


def test_process_place_records_invalid_existing_azure_url_failure():
    service = PhotoAssetService()
    result = service.process_place(
        _place_record(photos='["https://thirdplacesdata.blob.core.windows.net/curator-photos/rec123/photo.jpg"]'),
        {"photos": {}},
        PhotoAssetConfig(dry_run=False, upload=True),
    )

    assert result["summary"]["failed_upload_count"] == 1
    assert result["failures"][0]["reason"] == "invalid_existing_azure_url"
    assert result["failures"][0]["error"] == "invalid_container"


def test_process_place_dry_run_selected_sources_use_photo_priority():
    place_data = {
        "photos": {
            "raw_data": {
                "photos_data": [
                    {
                        "photo_url_big": "https://example.com/front-new.jpg",
                        "photo_tags": ["front"],
                        "photo_date": "12/01/2025 10:00:00",
                    },
                    {
                        "photo_url_big": "https://example.com/vibe-old.jpg",
                        "photo_tags": ["vibe"],
                        "photo_date": "01/01/2024 10:00:00",
                    },
                ]
            }
        }
    }

    result = PhotoAssetService().process_place(
        _place_record(),
        place_data,
        PhotoAssetConfig(dry_run=True, upload=False),
    )

    assert result["selected_source_urls"] == [
        "https://example.com/vibe-old.jpg",
        "https://example.com/front-new.jpg",
    ]


def test_process_place_dedupes_existing_azure_display_url_against_manifest_asset():
    source_url = "https://example.com/source.jpg"
    source_hash = sha256_hex(source_url)
    azure_url = f"https://thirdplacesdata.blob.core.windows.net/place-photos/charlotte/ChIJ123/{source_hash}.jpg"
    place_data = {
        "photos": {
            "raw_data": {"photos_data": [{"photo_url_big": source_url, "photo_tags": ["vibe"]}]},
            "azure_assets": [
                {
                    "source_url_sha256": source_hash,
                    "source_url": source_url,
                    "canonical_source_url": source_url,
                    "source_host": "example.com",
                    "source_field": "photos.raw_data.photos_data.photo_url_big",
                    "source_path": "photos.raw_data.photos_data[0].photo_url_big",
                    "azure_url": azure_url,
                    "blob_path": f"charlotte/ChIJ123/{source_hash}.jpg",
                    "status": "uploaded",
                    "selected_for_airtable": True,
                }
            ],
        }
    }

    result = PhotoAssetService().process_place(
        _place_record(photos=json.dumps([azure_url])),
        place_data,
        PhotoAssetConfig(dry_run=False, upload=True),
    )

    assert len(result["assets"]) == 1
    assert result["assets"][0]["azure_url"] == azure_url
    assert result["summary"]["azure_assets_count"] == 1


def test_ensure_container_exists_creates_missing_container(monkeypatch):
    container_client = mock.MagicMock()
    container_client.get_container_properties.side_effect = ResourceNotFoundError(message="missing")
    blob_service_client = mock.MagicMock()
    blob_service_client.get_container_client.return_value = container_client
    monkeypatch.setattr(utils, "_get_blob_service_client", lambda: blob_service_client)

    utils.ensure_container_exists("place-photos", public_access="blob")

    container_client.create_container.assert_called_once()


def test_ensure_container_exists_uses_existing_container(monkeypatch):
    container_client = mock.MagicMock()
    container_client.get_container_properties.return_value = {"name": "place-photos"}
    blob_service_client = mock.MagicMock()
    blob_service_client.get_container_client.return_value = container_client
    monkeypatch.setattr(utils, "_get_blob_service_client", lambda: blob_service_client)

    utils.ensure_container_exists("place-photos", public_access="blob")

    container_client.create_container.assert_not_called()