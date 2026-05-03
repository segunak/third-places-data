import logging
import json

from blueprints import photo_assets


class DummyRequest:
    def __init__(self, params):
        self.params = params


class DummyAirtableService:
    def __init__(self, provider_type):
        self.provider_type = provider_type
        self.all_third_places = [
            {
                "id": "rec123",
                "fields": {
                    "Place": "Daily Ritual Coffee",
                    "Google Maps Place Id": "ChIJ5YHD3oOhVogRAV83qWzHmgg",
                    "Photos": json.dumps([
                        "https://thirdplacesdata.blob.core.windows.net/place-photos/charlotte/ChIJ5YHD3oOhVogRAV83qWzHmgg/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.jpg"
                    ]),
                    "Photos Google": json.dumps(["https://example.com/google.jpg"]),
                },
            }
        ]


def test_base_config_defaults():
    config = photo_assets._base_config_from_params({})

    assert config["city"] == "charlotte"
    assert config["dry_run"] is True
    assert config["upload"] is False
    assert config["write_airtable"] is False
    assert config["failure_ttl_hours"] == 168


def test_base_config_rejects_invalid_int():
    try:
        photo_assets._base_config_from_params({"max_places": "bad"})
    except ValueError as exc:
        assert "Invalid integer value" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_compact_place_result_summarizes_large_asset_details():
    compact = photo_assets._compact_place_result({
        "status": "error",
        "error_reason": "no_selected_azure_urls",
        "message": "failed",
        "place_name": "Large Result Place",
        "place_id": "ChIJ123",
        "record_id": "rec123",
        "summary": {"candidate_count": 2},
        "selected_airtable_urls": ["https://example.com/one.jpg"],
        "assets": [{"status": "uploaded", "large": "not returned"}],
        "failures": [
            {"reason": "download_failed", "error": "HTTP 403", "attempts": [{"url": "large"}]},
            {"reason": "download_failed", "error": "HTTP 403", "attempts": [{"url": "large"}]},
        ],
    })

    assert compact["selected_airtable_count"] == 1
    assert compact["asset_count"] == 1
    assert compact["asset_status_counts"] == {"uploaded": 1}
    assert compact["failure_count"] == 2
    assert compact["failure_reason_counts"] == {"download_failed": 2}
    assert compact["failure_error_samples"] == ["HTTP 403", "HTTP 403"]
    assert "assets" not in compact
    assert "failures" not in compact


def test_filter_places_rejects_mismatched_record_and_place_id():
    places = [
        {"id": "rec123", "fields": {"Google Maps Place Id": "ChIJ123"}}
    ]

    try:
        photo_assets._filter_places(places, {"record_id": "rec123", "place_id": "ChIJ456", "max_places": 0})
    except ValueError as exc:
        assert "different Airtable records" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_migrate_single_place_uses_data_file_sources_when_airtable_photos_empty(monkeypatch):
    monkeypatch.setattr(
        photo_assets,
        "fetch_data_github",
        lambda path: (
            True,
            {
                "photos": {
                    "photo_urls": ["https://example.com/cached.jpg"],
                    "raw_data": {"photos_data": [{"photo_url_big": "https://example.com/raw.jpg"}]},
                }
            },
            "ok",
        ),
    )

    result = photo_assets.migrate_single_place_photo_assets({
        "place": {
            "id": "rec-data-file-sources",
            "fields": {
                "Place": "Data File Sources Place",
                "Google Maps Place Id": "ChIJ123",
                "Photos": "",
            },
        },
        "config": {"city": "charlotte", "dry_run": True},
    })

    assert result["status"] == "would_update"
    assert result["message"] == "Processed 2 candidates"
    assert result["summary"]["airtable_photos_count"] == 0
    assert result["summary"]["data_file_photo_urls_count"] == 1
    assert result["summary"]["provider_raw_photo_url_big_count"] == 1


def test_migrate_single_place_skip_message_explains_missing_place_id(caplog):
    caplog.set_level(logging.INFO)

    result = photo_assets.migrate_single_place_photo_assets({
        "place": {
            "id": "rec-no-place-id",
            "fields": {
                "Place": "No Place Id Place",
                "Photos": json.dumps(["https://example.com/photo.jpg"]),
            },
        },
        "config": {"city": "charlotte", "dry_run": True},
    })

    assert result["status"] == "skipped"
    assert result["skip_reason"] == "missing_google_maps_place_id"
    assert result["message"] == "Skipped: no Google Maps Place Id; Azure photo blobs require a place_id in their path."
    assert "missing Google Maps Place Id" in caplog.text


def test_migrate_single_place_errors_when_missing_place_id_has_uncopied_curator_photo_urls(caplog):
    caplog.set_level(logging.ERROR)
    curator_url = "https://thirdplacesdata.blob.core.windows.net/curator-photos/rec-no-place-id/photo.jpg"

    result = photo_assets.migrate_single_place_photo_assets({
        "place": {
            "id": "rec-no-place-id",
            "fields": {
                "Place": "No Place Id Place",
                "Curator Photo URLs": json.dumps([curator_url]),
                "Photos": json.dumps([]),
            },
        },
        "config": {"city": "charlotte", "dry_run": True},
    })

    assert result["status"] == "error"
    assert result["error_reason"] == "curator_photo_urls_not_copied_missing_place_id"
    assert result["summary"]["unselected_curator_photo_urls_field_count"] == 1
    assert curator_url in caplog.text


def test_migrate_single_place_skips_missing_place_id_when_curator_photo_urls_already_in_photos(caplog):
    caplog.set_level(logging.INFO)
    curator_url = "https://thirdplacesdata.blob.core.windows.net/curator-photos/rec-no-place-id/photo.jpg"

    result = photo_assets.migrate_single_place_photo_assets({
        "place": {
            "id": "rec-no-place-id",
            "fields": {
                "Place": "No Place Id Place",
                "Curator Photo URLs": json.dumps([curator_url]),
                "Photos": json.dumps([curator_url]),
            },
        },
        "config": {"city": "charlotte", "dry_run": True},
    })

    assert result["status"] == "skipped"
    assert result["skip_reason"] == "missing_google_maps_place_id"
    assert "missing Google Maps Place Id" in caplog.text


def test_migrate_single_place_skips_when_all_photo_sources_empty(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    monkeypatch.setattr(photo_assets, "fetch_data_github", lambda path: (True, {"photos": {"photo_urls": []}}, "ok"))

    result = photo_assets.migrate_single_place_photo_assets({
        "place": {
            "id": "rec-empty",
            "fields": {
                "Place": "Empty Sources Place",
                "Google Maps Place Id": "ChIJ123",
                "Photos": json.dumps(["", "not-a-url"]),
            },
        },
        "config": {"city": "charlotte", "dry_run": True},
    })

    assert result["status"] == "skipped"
    assert result["skip_reason"] == "no_migratable_photo_urls"
    assert result["message"] == (
        "Skipped: no migratable photo URLs found after checking Airtable Photos, "
        "data file photos.photo_urls, and raw provider photo_url_big sources."
    )
    assert result["summary"]["candidate_count"] == 0
    assert "candidate_count=0" in caplog.text
    assert "airtable_photos_count=0" in caplog.text
    assert "data_file_photo_urls_count=0" in caplog.text
    assert "provider_raw_photo_url_big_count=0" in caplog.text


def test_migrate_single_place_skips_zero_candidate_result(monkeypatch):
    class DummyPhotoAssetService:
        def process_place(self, place, place_data, config):
            return {
                "selected_airtable_urls": [],
                "summary": {"candidate_count": 0, "airtable_photos_count": 1},
                "assets": [],
                "failures": [],
            }

    monkeypatch.setattr(photo_assets, "PhotoAssetService", DummyPhotoAssetService)
    monkeypatch.setattr(photo_assets, "fetch_data_github", lambda path: (True, {"photos": {}}, "ok"))

    result = photo_assets.migrate_single_place_photo_assets({
        "place": {
            "id": "rec-zero",
            "fields": {
                "Place": "Zero Candidate Place",
                "Google Maps Place Id": "ChIJ123",
                "Photos": json.dumps(["https://example.com/photo.jpg"]),
            },
        },
        "config": {"city": "charlotte", "dry_run": True},
    })

    assert result["status"] == "skipped"
    assert result["skip_reason"] == "no_migratable_photo_urls"
    assert result["message"] == (
        "Skipped: no migratable photo URLs found after checking Airtable Photos, "
        "data file photos.photo_urls, and raw provider photo_url_big sources."
    )


def test_migrate_single_place_processes_preserved_urls_without_candidates(monkeypatch):
    curator_url = "https://thirdplacesdata.blob.core.windows.net/curator-photos/rec123/photo.jpg"

    class DummyPhotoAssetService:
        def process_place(self, place, place_data, config):
            return {
                "selected_airtable_urls": [curator_url],
                "summary": {"candidate_count": 0, "selected_airtable_count": 1},
                "assets": [],
                "failures": [],
            }

    monkeypatch.setattr(photo_assets, "PhotoAssetService", DummyPhotoAssetService)
    monkeypatch.setattr(photo_assets, "fetch_data_github", lambda path: (True, {"photos": {}}, "ok"))

    result = photo_assets.migrate_single_place_photo_assets({
        "place": {
            "id": "rec-curator-only",
            "fields": {
                "Place": "Curator Only Place",
                "Google Maps Place Id": "ChIJ123",
                "Curator Photo URLs": json.dumps([curator_url]),
            },
        },
        "config": {"city": "charlotte", "dry_run": True},
    })

    assert result["status"] == "would_update"
    assert result["selected_airtable_urls"] == [curator_url]
    assert result["summary"]["candidate_count"] == 0


def test_migrate_single_place_errors_when_curator_photo_urls_field_would_not_be_copied(monkeypatch):
    uncopied_url = "https://legacy.example.com/curator-photo.jpg"

    class DummyPhotoAssetService:
        def process_place(self, place, place_data, config):
            return {
                "selected_airtable_urls": [],
                "summary": {
                    "candidate_count": 0,
                    "curator_photo_urls_field_count": 1,
                    "unselected_curator_photo_urls_field_count": 1,
                    "unselected_curator_photo_urls_field_urls": [uncopied_url],
                },
                "assets": [],
                "failures": [],
            }

    monkeypatch.setattr(photo_assets, "PhotoAssetService", DummyPhotoAssetService)
    monkeypatch.setattr(photo_assets, "fetch_data_github", lambda path: (True, {"photos": {}}, "ok"))

    result = photo_assets.migrate_single_place_photo_assets({
        "place": {
            "id": "rec-uncopied-curator-url",
            "fields": {
                "Place": "Uncopied Curator URL Place",
                "Google Maps Place Id": "ChIJ123",
                "Curator Photo URLs": json.dumps([uncopied_url]),
            },
        },
        "config": {"city": "charlotte", "dry_run": True},
    })

    assert result["status"] == "error"
    assert result["error_reason"] == "curator_photo_urls_not_copied"
    assert result["summary"]["unselected_curator_photo_urls_field_urls"] == [uncopied_url]


def test_migrate_single_place_refuses_empty_airtable_photos_update(monkeypatch):
    class DummyPhotoAssetService:
        def process_place(self, place, place_data, config):
            return {
                "selected_airtable_urls": [],
                "summary": {
                    "candidate_count": 2,
                    "azure_assets_count": 0,
                    "failed_upload_count": 2,
                },
                "assets": [],
                "failures": [{"reason": "download_failed"}, {"reason": "download_failed"}],
            }

    class FailingAirtableService:
        def __init__(self, provider_type):
            self.provider_type = provider_type

        def update_place_record(self, *args, **kwargs):
            raise AssertionError("Airtable should not be updated with an empty Photos list")

    def fail_save(*args, **kwargs):
        raise AssertionError("Data file should not be saved when no Azure URLs were selected")

    monkeypatch.setattr(photo_assets, "PhotoAssetService", DummyPhotoAssetService)
    monkeypatch.setattr(photo_assets, "AirtableService", FailingAirtableService)
    monkeypatch.setattr(photo_assets, "fetch_data_github", lambda path: (True, {"photos": {}}, "ok"))
    monkeypatch.setattr(photo_assets, "save_data_github", fail_save)

    result = photo_assets.migrate_single_place_photo_assets({
        "place": {
            "id": "rec-empty-selected",
            "fields": {
                "Place": "Empty Selected Place",
                "Google Maps Place Id": "ChIJ123",
                "Photos": json.dumps(["https://example.com/photo.jpg"]),
            },
        },
        "config": {
            "city": "charlotte",
            "provider_type": "outscraper",
            "dry_run": False,
            "upload": True,
            "write_airtable": True,
        },
    })

    assert result["status"] == "error"
    assert result["error_reason"] == "no_selected_azure_urls"
    assert "refusing to overwrite Airtable Photos with an empty list" in result["message"]
    assert result["selected_airtable_urls"] == []
    assert result["summary"]["candidate_count"] == 2


def test_photo_health_check_reports_counts(monkeypatch):
    monkeypatch.setattr(photo_assets, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(
        photo_assets,
        "fetch_data_github",
        lambda path: (
            True,
            {
                "photos": {
                    "photo_urls": ["https://example.com/source.jpg"],
                    "raw_data": {"photos_data": [{"photo_url_big": "https://example.com/raw.jpg"}]},
                    "azure_assets": [{"azure_url": "https://example.com/asset.jpg", "selected_for_airtable": False}],
                    "azure_asset_failures": [{"reason": "download_failed"}],
                }
            },
            "ok",
        ),
    )

    response = photo_assets.photo_health_check(
        DummyRequest({"city": "charlotte", "place_id": "ChIJ5YHD3oOhVogRAV83qWzHmgg"})
    )
    body = json.loads(response.get_body().decode("utf-8"))

    assert response.status_code == 200
    assert body["success"] is True
    assert body["data"]["airtable_photos_count"] == 1
    assert body["data"]["airtable_photos_google_count"] == 1
    assert body["data"]["provider_raw_photo_url_big_count"] == 1
    assert body["data"]["azure_assets_count"] == 1
    assert body["data"]["failed_upload_count"] == 1