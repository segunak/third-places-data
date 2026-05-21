import json
from unittest import mock

import pytest
from azure.core.exceptions import ResourceNotFoundError

from services import utils
from services.image_conversion_service import ImageVariantResult
from services.image_conversion_service import validated_content_type
from services.photo_asset_service import (
    PhotoAssetConfig,
    PhotoAssetService,
    build_place_photo_inventory,
    classify_azure_photo_url,
    is_curator_photo_azure_url,
    sha256_hex,
)


@pytest.fixture(autouse=True)
def fake_variant_generation(monkeypatch):
    def variant(name, data):
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

    monkeypatch.setattr(
        "services.photo_publisher_service.generate_served_image_variants",
        lambda data, source_content_type, source_extension="": {
            "display": variant("display", b"display-bytes"),
            "thumbnail": variant("thumbnail", b"thumbnail-bytes"),
        },
    )


def _standard_photo_url(place_id="ChIJ123", digest=None, extension=".jpg"):
    return f"https://thirdplacesdata.blob.core.windows.net/photos/{place_id}/{digest or ('a' * 64)}{extension}"


def _curator_photo_url(place_id="ChIJ123", name="curator-attA-front.webp"):
    return f"https://thirdplacesdata.blob.core.windows.net/photos/{place_id}/{name}"


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


def test_inventory_includes_raw_photo_and_street_view_fallbacks():
    hero_url = "https://lh3.googleusercontent.com/p/hero=w800-h500-k-no"
    street_view_url = "https://streetviewpixels-pa.googleapis.com/v1/thumbnail?panoid=abc&w=1600&h=1000"
    place_data = {
        "photos": {
            "photo_urls": [],
            "raw_data": {
                "photos_data": [],
                "photo": hero_url,
                "street_view": street_view_url,
            },
        }
    }

    inventory, summary = build_place_photo_inventory(_place_record(), place_data)

    assert [candidate["canonical_source_url"] for candidate in inventory] == [hero_url, street_view_url]
    assert [candidate["source_field"] for candidate in inventory] == [
        "photos.raw_data.photo",
        "photos.raw_data.street_view",
    ]
    assert summary["candidate_count"] == 2
    assert summary["provider_raw_photo_url_big_count"] == 0
    assert summary["provider_raw_photo_count"] == 1
    assert summary["provider_raw_street_view_count"] == 1


def test_inventory_dedupes_raw_photo_fallback_against_photo_urls():
    hero_url = "https://lh3.googleusercontent.com/p/hero=w800-h500-k-no"
    place_data = {
        "photos": {
            "photo_urls": [hero_url],
            "raw_data": {
                "photos_data": [],
                "photo": hero_url,
            },
        }
    }

    inventory, summary = build_place_photo_inventory(_place_record(), place_data)

    assert [candidate["canonical_source_url"] for candidate in inventory] == [hero_url]
    assert inventory[0]["source_field"] == "photos.photo_urls"
    assert inventory[0]["duplicate_provenance"] == [{
        "field": "photos.raw_data.photo",
        "path": "photos.raw_data.photo",
    }]
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


def test_variant_photo_azure_url_validation():
    standard_variant = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/" + ("a" * 64) + ".webp"
    curator_variant = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/curator-att123-front.webp"

    assert classify_azure_photo_url(standard_variant, "charlotte", "ChIJ123") == {
        "category": "new_display_variant_standard",
        "blob_path": "ChIJ123/display/" + ("a" * 64) + ".webp",
        "reason": "valid",
    }
    assert classify_azure_photo_url(curator_variant, "charlotte", "ChIJ123") == {
        "category": "new_thumbnail_variant_curator",
        "blob_path": "ChIJ123/thumbnail/curator-att123-front.webp",
        "reason": "valid",
    }
    assert is_curator_photo_azure_url(curator_variant) is True


def test_curator_photo_azure_url_validation():
    assert is_curator_photo_azure_url(_curator_photo_url()) is True
    assert is_curator_photo_azure_url(_standard_photo_url()) is False


def test_inventory_includes_curator_azure_urls_from_airtable_photos_as_variant_sources():
    curator_url = _curator_photo_url()
    provider_url = "https://example.com/provider.jpg"

    inventory, summary = build_place_photo_inventory(
        _place_record(photos=json.dumps([curator_url, provider_url])),
        {"photos": {}},
    )

    assert {candidate["canonical_source_url"] for candidate in inventory} == {curator_url, provider_url}
    assert summary["airtable_photos_count"] == 2
    assert summary["candidate_count"] == 2


def test_inventory_preserves_airtable_photo_manifests_as_variant_sources():
    display_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/" + ("a" * 64) + ".webp"
    thumbnail_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/" + ("a" * 64) + ".webp"
    photo_manifest = {"display": display_url, "thumbnail": thumbnail_url}

    inventory, summary = build_place_photo_inventory(
        _place_record(photos=json.dumps([photo_manifest])),
        {"photos": {}},
    )

    assert len(inventory) == 1
    assert inventory[0]["canonical_source_url"] == display_url
    assert inventory[0]["photo_manifest"] == photo_manifest
    assert summary["airtable_photos_count"] == 1


def test_process_place_keeps_existing_airtable_photo_manifest_variants():
    display_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/" + ("a" * 64) + ".webp"
    thumbnail_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/" + ("a" * 64) + ".webp"
    photo_manifest = {"display": display_url, "thumbnail": thumbnail_url}

    result = PhotoAssetService().process_place(
        _place_record(photos=json.dumps([photo_manifest])),
        {"photos": {}},
        PhotoAssetConfig(dry_run=True, upload=False),
    )

    assert result["selected_airtable_photos"] == [photo_manifest]
    assert result["selected_airtable_urls"] == [display_url]
    assert result["assets"][0]["status"] == "existing_azure"
    assert result["summary"]["candidate_count"] == 1


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
        lambda source_url, try_url_variants=True, timeout=60: {
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
    assert result["selected_airtable_photos"] == [
        {"display": result["selected_airtable_urls"][0], "thumbnail": result["assets"][0]["photo_manifest"]["thumbnail"]},
        {"display": result["selected_airtable_urls"][1], "thumbnail": result["assets"][1]["photo_manifest"]["thumbnail"]},
    ]
    assert "place_data" not in result


def test_process_place_passes_configured_download_timeout(monkeypatch):
    service = PhotoAssetService()
    observed_timeouts = []

    def mock_download(source_url, try_url_variants=True, timeout=60):
        observed_timeouts.append(timeout)
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
        _place_record(),
        {"photos": {"photo_urls": ["https://example.com/vibe.jpg"]}},
        PhotoAssetConfig(dry_run=False, upload=True, download_timeout_seconds=7),
    )

    assert result["summary"]["azure_assets_count"] == 1
    assert observed_timeouts == [7]


def test_chunk_helpers_preserve_selection_order(monkeypatch):
    place_data = {"photos": {"photo_urls": [
        "https://example.com/one.jpg",
        "https://example.com/two.jpg",
        "https://example.com/three.jpg",
    ]}}
    service = PhotoAssetService()

    monkeypatch.setattr(
        service.publisher,
        "download_image_asset",
        lambda source_url, try_url_variants=True, timeout=60: {
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

    config = PhotoAssetConfig(dry_run=False, upload=True)
    place_context = service.prepare_place_context(_place_record(), place_data, config)
    first_batch = service.process_candidate_batch(place_context, place_context["inventory"][:2], config)
    second_batch = service.process_candidate_batch(place_context, place_context["inventory"][2:], config)
    result = service.finalize_place_assets(
        place_context,
        [*first_batch["assets"], *second_batch["assets"]],
        [*first_batch["failures"], *second_batch["failures"]],
    )

    assert result["summary"]["candidate_count"] == 3
    assert result["summary"]["azure_assets_count"] == 3
    assert len(result["selected_airtable_urls"]) == 3
    assert [asset["azure_url"] for asset in result["assets"]] == result["selected_airtable_urls"]
    assert [photo["display"] for photo in result["selected_airtable_photos"]] == result["selected_airtable_urls"]


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


def test_process_place_generates_curator_variants_at_front(monkeypatch):
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

    def mock_download(source_url, try_url_variants=True, timeout=60):
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
        _place_record(photos=json.dumps(curator_urls)),
        place_data,
        PhotoAssetConfig(dry_run=False, upload=True),
    )

    assert "/display/curator-" in result["selected_airtable_urls"][0]
    assert "/display/curator-" in result["selected_airtable_urls"][1]
    assert result["selected_airtable_photos"][0]["thumbnail"].replace("/thumbnail/", "/display/") == result["selected_airtable_urls"][0]
    assert result["selected_airtable_photos"][1]["thumbnail"].replace("/thumbnail/", "/display/") == result["selected_airtable_urls"][1]
    assert downloaded_urls[:2] == curator_urls
    assert result["summary"]["preserved_curator_airtable_photos_count"] == 0
    assert result["summary"]["selected_curator_airtable_photos_count"] == 2
    assert result["summary"]["failed_upload_count"] == 0
    assert len(result["selected_airtable_urls"]) == 4


def test_process_place_plans_curator_variants_from_photos_field():
    curator_url = _curator_photo_url()

    result = PhotoAssetService().process_place(
        _place_record(photos=json.dumps([curator_url])),
        {"photos": {}},
        PhotoAssetConfig(dry_run=True, upload=False),
    )

    assert len(result["selected_airtable_photos"]) == 1
    assert result["selected_airtable_photos"][0]["display"].startswith("https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/curator-")
    assert result["selected_airtable_photos"][0]["thumbnail"].startswith("https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/curator-")
    assert result["summary"]["candidate_count"] == 1
    assert result["summary"]["preserved_curator_airtable_photos_count"] == 0
    assert result["summary"]["failed_upload_count"] == 0
    assert result["failures"] == []


def test_process_place_does_not_cap_selected_airtable_photo_manifests():
    place_data = {
        "photos": {
            "photo_urls": [f"https://example.com/photo-{index}.jpg" for index in range(31)]
        }
    }

    result = PhotoAssetService().process_place(
        _place_record(),
        place_data,
        PhotoAssetConfig(dry_run=True, upload=False),
    )

    assert len(result["selected_airtable_photos"]) == 31
    assert len(result["selected_airtable_urls"]) == 31
    assert result["summary"]["selected_airtable_count"] == 31


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


def test_process_place_plans_variants_for_existing_canonical_url_and_ignores_retired_manifest():
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
        PhotoAssetConfig(dry_run=True, upload=False),
    )

    assert len(result["assets"]) == 1
    assert result["assets"][0]["azure_url"].startswith("https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/")
    assert result["assets"][0]["photo_manifest"]["thumbnail"].startswith("https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/")
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