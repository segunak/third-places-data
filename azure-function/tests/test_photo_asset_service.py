import json
from unittest import mock

import pytest
from azure.core.exceptions import ResourceNotFoundError

from services import utils
from services.image_conversion_service import validated_content_type
from services.photo_asset_service import (
    PhotoAssetConfig,
    PhotoAssetService,
    build_display_photo_urls,
    build_place_photo_inventory,
    classify_azure_photo_url,
    is_curator_photo_azure_url,
    sha256_hex,
)


@pytest.fixture(autouse=True)
def no_legacy_blob_listing(monkeypatch):
    monkeypatch.setattr("services.photo_asset_service.list_blobs_in_container", lambda *args, **kwargs: [])


def _standard_photo_url(place_id="ChIJ123", digest=None, extension=".jpg"):
    return f"https://thirdplacesdata.blob.core.windows.net/photos/{place_id}/{digest or ('a' * 64)}{extension}"


def _curator_photo_url(place_id="ChIJ123", name="curator-attA-front.webp"):
    return f"https://thirdplacesdata.blob.core.windows.net/photos/{place_id}/{name}"


def _place_record(photos="", photos_google="", curator_photo_urls=""):
    return {
        "id": "rec123",
        "fields": {
            "Place": "Test Place",
            "Google Maps Place Id": "ChIJ123",
            "Photos": photos,
            "Photos Google": photos_google,
            "Curator Photo URLs": curator_photo_urls,
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
    valid_url = _standard_photo_url()
    invalid_url = "https://thirdplacesdata.blob.core.windows.net/other-container/rec123/photo.jpg"

    assert classify_azure_photo_url(valid_url, "charlotte", "ChIJ123") == {
        "category": "new_standard",
        "blob_path": "ChIJ123/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.jpg",
        "reason": "valid",
    }
    assert classify_azure_photo_url(invalid_url, "charlotte", "ChIJ123")["reason"] == "invalid_container"

    classification = classify_azure_photo_url(
        "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/not-a-hash.jpg",
        "charlotte",
        "ChIJ123",
    )
    assert classification["category"] == "invalid"
    assert classification["reason"] == "invalid_blob_name"

    classification = classify_azure_photo_url(
        "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/" + ("a" * 64) + ".gif",
        "charlotte",
        "ChIJ123",
    )
    assert classification["category"] == "invalid"
    assert classification["reason"] == "invalid_blob_extension"


def test_curator_photo_azure_url_validation():
    assert is_curator_photo_azure_url("https://thirdplacesdata.blob.core.windows.net/curator-photos/rec123/photo.jpg") is True
    assert is_curator_photo_azure_url("https://thirdplacesdata.blob.core.windows.net/place-photos/charlotte/ChIJ123/photo.jpg") is False
    assert is_curator_photo_azure_url(_curator_photo_url()) is True
    assert is_curator_photo_azure_url(_standard_photo_url()) is False


def test_build_display_photo_urls_puts_curator_urls_first_and_dedupes():
    curator_urls = [
        _curator_photo_url(name="curator-attA-front.webp"),
        _curator_photo_url(name="curator-attB-interior.webp"),
    ]
    provider_urls = [
        curator_urls[0],
        _standard_photo_url(),
    ]

    assert build_display_photo_urls(curator_urls, provider_urls) == [
        curator_urls[0],
        curator_urls[1],
        provider_urls[1],
    ]


def test_inventory_skips_curator_photo_urls_from_airtable_photos():
    curator_url = _curator_photo_url()
    provider_url = "https://example.com/provider.jpg"

    inventory, summary = build_place_photo_inventory(
        _place_record(photos=json.dumps([curator_url, provider_url])),
        {"photos": {}},
    )

    assert {candidate["canonical_source_url"] for candidate in inventory} == {provider_url}
    assert summary["airtable_photos_count"] == 2
    assert summary["candidate_count"] == 1


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
        service.publisher,
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
        "services.photo_publisher_service.upload_blob_to_container",
        lambda container, blob_path, data, content_type, metadata, cache_control, public_access, overwrite: (
            f"https://thirdplacesdata.blob.core.windows.net/{container}/{blob_path}"
        ),
    )

    result = service.process_place(
        _place_record(),
        place_data,
        PhotoAssetConfig(dry_run=False, upload=True),
    )

    assert result["summary"]["azure_assets_count"] == 2
    assert len(result["selected_airtable_urls"]) == 2
    assert all("thirdplacesdata.blob.core.windows.net/photos/ChIJ123/" in url for url in result["selected_airtable_urls"])
    assert "place_data" not in result


def test_process_place_records_invalid_existing_azure_url_failure():
    service = PhotoAssetService()
    result = service.process_place(
        _place_record(photos='["https://thirdplacesdata.blob.core.windows.net/other-container/rec123/photo.jpg"]'),
        {"photos": {}},
        PhotoAssetConfig(dry_run=False, upload=True),
    )

    assert result["summary"]["failed_upload_count"] == 1
    assert result["failures"][0]["reason"] == "invalid_existing_azure_url"
    assert result["failures"][0]["error"] == "invalid_container"


def test_process_place_preserves_curator_photo_urls_at_front(monkeypatch):
    curator_urls = [
        _curator_photo_url(name="curator-attA-front.webp"),
        _curator_photo_url(name="curator-attB-interior.webp"),
    ]
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
    downloaded_urls = []

    def mock_download(source_url, try_url_variants=True):
        downloaded_urls.append(source_url)
        return {
            "success": True,
            "data": b"\xff\xd8\xffimage",
            "content_type": "image/jpeg",
            "extension": ".jpg",
            "content_sha256": "contenthash",
            "attempts": [],
            "bytes": 8,
        }

    monkeypatch.setattr(service.publisher, "download_image_asset", mock_download)
    monkeypatch.setattr(
        "services.photo_publisher_service.upload_blob_to_container",
        lambda container, blob_path, data, content_type, metadata, cache_control, public_access, overwrite: (
            f"https://thirdplacesdata.blob.core.windows.net/{container}/{blob_path}"
        ),
    )

    result = service.process_place(
        _place_record(curator_photo_urls=json.dumps(curator_urls)),
        place_data,
        PhotoAssetConfig(dry_run=False, upload=True),
    )

    assert result["selected_airtable_urls"][:2] == curator_urls
    assert all(url not in downloaded_urls for url in curator_urls)
    assert result["summary"]["preserved_curator_airtable_photos_count"] == 2
    assert result["summary"]["selected_curator_airtable_photos_count"] == 2
    assert result["summary"]["curator_photo_urls_field_count"] == 2
    assert result["summary"]["unselected_curator_photo_urls_field_count"] == 0
    assert result["summary"]["failed_upload_count"] == 0
    assert len(result["selected_airtable_urls"]) == 4


def test_process_place_preserves_curator_photo_urls_from_photos_field():
    curator_url = _curator_photo_url()

    result = PhotoAssetService().process_place(
        _place_record(photos=json.dumps([curator_url])),
        {"photos": {}},
        PhotoAssetConfig(dry_run=False, upload=True),
    )

    assert result["selected_airtable_urls"] == [curator_url]
    assert result["summary"]["candidate_count"] == 0
    assert result["summary"]["preserved_curator_airtable_photos_count"] == 1
    assert result["summary"]["curator_photo_urls_field_count"] == 0
    assert result["summary"]["failed_upload_count"] == 0
    assert result["failures"] == []


def test_process_place_reports_uncopied_curator_photo_urls_field_values():
    legacy_field_url = "https://legacy.example.com/curator-photo.jpg"

    result = PhotoAssetService().process_place(
        _place_record(curator_photo_urls=json.dumps([legacy_field_url])),
        {"photos": {}},
        PhotoAssetConfig(dry_run=False, upload=True),
    )

    assert result["summary"]["curator_photo_urls_field_count"] == 1
    assert result["summary"]["unsupported_curator_photo_urls_field_count"] == 1
    assert result["summary"]["unselected_curator_photo_urls_field_count"] == 1
    assert result["summary"]["unselected_curator_photo_urls_field_urls"] == [legacy_field_url]


def test_process_place_dry_run_selected_sources_use_source_order():
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
        "https://example.com/front-new.jpg",
        "https://example.com/vibe-old.jpg",
    ]


def test_process_place_accepts_existing_canonical_url_and_ignores_retired_manifest():
    azure_url = _standard_photo_url(digest=sha256_hex("canonical-source"))
    place_data = {
        "photos": {
            "azure_assets": [
                {
                    "source_url_sha256": "retired-manifest-entry",
                    "azure_url": azure_url,
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
    assert "place_data" not in result


def test_ensure_container_exists_creates_missing_container(monkeypatch):
    container_client = mock.MagicMock()
    container_client.get_container_properties.side_effect = ResourceNotFoundError(message="missing")
    blob_service_client = mock.MagicMock()
    blob_service_client.get_container_client.return_value = container_client
    monkeypatch.setattr(utils, "_get_blob_service_client", lambda: blob_service_client)

    utils.ensure_container_exists("photos", public_access="blob")

    container_client.create_container.assert_called_once()


def test_ensure_container_exists_uses_existing_container(monkeypatch):
    container_client = mock.MagicMock()
    container_client.get_container_properties.return_value = {"name": "photos"}
    blob_service_client = mock.MagicMock()
    blob_service_client.get_container_client.return_value = container_client
    monkeypatch.setattr(utils, "_get_blob_service_client", lambda: blob_service_client)

    utils.ensure_container_exists("photos", public_access="blob")

    container_client.create_container.assert_not_called()