import json
import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import migrate_photos_local as migrate


class FakeAirtableClient:
    def __init__(self):
        self.updated = []

    def update_photos(self, record, selected_urls):
        self.updated.append((record["id"], selected_urls))
        return {"updated": True, "old_value": "[]", "new_value": json.dumps(selected_urls)}


class FailingAirtableClient:
    def update_photos(self, record, selected_urls):
        raise AssertionError("Airtable should not be updated")


def make_record(record_id="rec123", place_id="ChIJ123", photos=None):
    fields = {
        "Place": "Test Cafe",
        "Google Maps Place Id": place_id,
    }
    if photos is not None:
        fields["Photos"] = photos
    return {"id": record_id, "fields": fields}


def test_load_local_settings_preserves_existing_env(monkeypatch, tmp_path):
    settings_file = tmp_path / "local.settings.json"
    settings_file.write_text(
        json.dumps({
            "Values": {
                "AzureWebJobsStorage": "from-file",
                "LOCAL_ONLY_SETTING": "loaded",
            }
        }),
        encoding="utf-8",
    )
    monkeypatch.setenv("AzureWebJobsStorage", "from-env")
    monkeypatch.delenv("LOCAL_ONLY_SETTING", raising=False)

    loaded = migrate.load_local_settings(settings_file)

    assert os.environ["AzureWebJobsStorage"] == "from-env"
    assert os.environ["LOCAL_ONLY_SETTING"] == "loaded"
    assert loaded == ["LOCAL_ONLY_SETTING"]


def test_select_target_records_defaults_to_google_photos_filter():
    records = [
        make_record("rec-google", photos=json.dumps(["https://lh3.googleusercontent.com/photo=s0"])),
        make_record("rec-azure", photos=json.dumps(["https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/photo.webp"])),
        make_record("rec-empty", photos=json.dumps([])),
    ]
    args = type("Args", (), {
        "record_id": "",
        "place_id": "",
        "filter": "google-photos",
        "max_places": 0,
    })()

    selected = migrate.select_target_records(records, args)

    assert [record["id"] for record in selected] == ["rec-google"]


def test_select_target_records_supports_place_photos_filter():
    records = [
        make_record("rec-place-photos", photos=json.dumps(["https://thirdplacesdata.blob.core.windows.net/place-photos/charlotte/ChIJ123/photo.jpg"])),
        make_record("rec-azure", photos=json.dumps(["https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/photo.webp"])),
    ]
    args = type("Args", (), {
        "record_id": "",
        "place_id": "",
        "filter": "place-photos",
        "max_places": 0,
        "recovery_record_ids": set(),
    })()

    selected = migrate.select_target_records(records, args)

    assert [record["id"] for record in selected] == ["rec-place-photos"]


def test_select_target_records_uses_recovery_manifest_ids():
    records = [
        make_record("rec-target", photos=json.dumps([])),
        make_record("rec-other", photos=json.dumps(["https://lh3.googleusercontent.com/photo=s0"])),
    ]
    args = type("Args", (), {
        "record_id": "",
        "place_id": "",
        "filter": "google-photos",
        "max_places": 0,
        "recovery_record_ids": {"rec-target"},
    })()

    selected = migrate.select_target_records(records, args)

    assert [record["id"] for record in selected] == ["rec-target"]


def test_targeted_record_bypasses_default_google_filter():
    records = [make_record("rec-azure", photos=json.dumps(["https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/photo.webp"]))]
    args = type("Args", (), {
        "record_id": "rec-azure",
        "place_id": "",
        "filter": "google-photos",
        "max_places": 0,
    })()

    selected = migrate.select_target_records(records, args)

    assert selected == records


def test_process_place_skips_missing_place_id(tmp_path):
    record = make_record(place_id="", photos=json.dumps(["https://lh3.googleusercontent.com/photo=s0"]))
    result = migrate.process_place(
        record,
        tmp_path,
        migrate.MigrationRunConfig(dry_run=True),
        FailingAirtableClient(),
    )

    assert result["status"] == "skipped"
    assert result["skip_reason"] == "ignored_missing_place_id"


def test_process_place_dry_run_does_not_update_airtable(tmp_path):
    selected_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp"

    class DummyPhotoAssetService:
        def prepare_place_context(self, record, place_data, config):
            assert config.dry_run is True
            assert config.include_legacy_blob_candidates is False
            return {
                "status": "prepared",
                "place_name": "Test Cafe",
                "place_id": "ChIJ123",
                "record_id": "rec123",
                "inventory": [{"canonical_source_url": "https://lh3.googleusercontent.com/photo=s0"}],
                "warnings": [],
            }

        def process_candidate_batch(self, place_context, candidates, config):
            return {
                "assets": [{"azure_url": selected_url, "status": "would_upload", "bytes": 0}],
                "failures": [],
            }

        def finalize_place_assets(self, place_context, assets, failures):
            return {
                "selected_airtable_urls": [selected_url],
                "summary": {"candidate_count": 1, "selected_airtable_count": 1, "azure_assets_count": 1},
                "assets": assets,
                "failures": failures,
            }

    record = make_record(photos=json.dumps(["https://lh3.googleusercontent.com/photo=s0"]))
    result = migrate.process_place(
        record,
        tmp_path,
        migrate.MigrationRunConfig(dry_run=True, upload=False, write_airtable=False),
        FailingAirtableClient(),
        DummyPhotoAssetService(),
    )

    assert result["status"] == "would_update"
    assert result["selected_airtable_count"] == 1
    assert result["asset_status_counts"] == {"would_upload": 1}


def test_process_place_recovery_manifest_removes_legacy_place_photo_candidates(tmp_path):
    recovery_url = "https://lh3.googleusercontent.com/recovered=s0"
    selected_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp"
    legacy_url = "https://thirdplacesdata.blob.core.windows.net/place-photos/charlotte/ChIJ123/photo.jpg"
    curator_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/curator-att123-photo.webp"

    class RecoveryPhotoAssetService:
        def prepare_place_context(self, record, place_data, config):
            retained_photos = json.loads(record["fields"]["Photos"])
            assert legacy_url not in retained_photos
            assert retained_photos == [curator_url]
            assert place_data["photos"]["photo_urls"] == [recovery_url]
            return {
                "status": "prepared",
                "place_name": "Test Cafe",
                "place_id": "ChIJ123",
                "record_id": "rec123",
                "inventory": [{"canonical_source_url": recovery_url}],
                "warnings": [],
            }

        def process_candidate_batch(self, place_context, candidates, config):
            return {
                "assets": [{"azure_url": selected_url, "status": "would_upload", "bytes": 0}],
                "failures": [],
            }

        def finalize_place_assets(self, place_context, assets, failures):
            return {
                "selected_airtable_urls": [curator_url, selected_url],
                "summary": {"candidate_count": 1, "selected_airtable_count": 2, "azure_assets_count": 1},
                "assets": assets,
                "failures": failures,
            }

    record = make_record(photos=json.dumps([curator_url, legacy_url]))
    result = migrate.process_place(
        record,
        tmp_path,
        migrate.MigrationRunConfig(
            dry_run=True,
            upload=False,
            write_airtable=False,
            recovery_entries_by_record_id={
                "rec123": {
                    "record_id": "rec123",
                    "source": "test_recovery",
                    "photo_urls": [recovery_url],
                }
            },
        ),
        FailingAirtableClient(),
        RecoveryPhotoAssetService(),
    )

    assert result["status"] == "would_update"
    assert result["selected_airtable_count"] == 2
    assert result["summary"]["recovery_manifest_used"] is True


def test_record_without_google_photos_preserves_non_google_urls():
    azure_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/photo.webp"
    other_url = "https://example.com/photo.jpg"
    record = make_record(photos=json.dumps([
        "https://lh3.googleusercontent.com/photo=s0",
        azure_url,
        "https://maps.googleapis.com/maps/api/place/photo?photo_reference=abc",
        other_url,
    ]))

    cleaned = migrate.record_without_google_photos(record)

    assert json.loads(cleaned["fields"]["Photos"]) == [azure_url, other_url]
    assert len(json.loads(record["fields"]["Photos"])) == 4


def test_record_without_legacy_place_photos_preserves_canonical_photos():
    curator_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/curator-att123-photo.webp"
    standard_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp"
    legacy_url = "https://thirdplacesdata.blob.core.windows.net/place-photos/charlotte/ChIJ123/photo.jpg"
    record = make_record(photos=json.dumps([curator_url, legacy_url, standard_url]))

    cleaned = migrate.record_without_legacy_place_photos(record)

    assert json.loads(cleaned["fields"]["Photos"]) == [curator_url, standard_url]
    assert json.loads(record["fields"]["Photos"])[1] == legacy_url


def test_apply_recovery_manifest_entry_prefers_manifest_urls_and_limits():
    place_data = {
        "photos": {
            "photo_urls": ["https://lh3.googleusercontent.com/local=s0"],
            "raw_data": {
                "photos_data": [{"photo_url_big": "https://lh3.googleusercontent.com/raw=s0"}]
            },
        }
    }
    entry = {
        "record_id": "rec123",
        "source": "historical_csv_photos_google",
        "max_source_urls": 1,
        "photo_urls": [
            "https://lh3.googleusercontent.com/historical-1=s0",
            "https://lh3.googleusercontent.com/historical-2=s0",
        ],
    }

    recovered_data, warnings, summary = migrate.apply_recovery_manifest_entry(place_data, entry, max_source_urls=10)

    assert warnings == []
    assert recovered_data["photos"]["photo_urls"] == ["https://lh3.googleusercontent.com/historical-1=s0"]
    assert recovered_data["photos"]["raw_data"]["photos_data"] == [
        {"photo_url_big": "https://lh3.googleusercontent.com/historical-1=s0", "photo_tags": [], "photo_date": ""}
    ]
    assert summary["recovery_manifest_used"] is True
    assert summary["recovery_manifest_photo_url_count"] == 1


def test_apply_recovery_manifest_entry_uses_local_place_data_when_manifest_urls_absent():
    place_data = {
        "photos": {
            "photo_urls": [
                "https://thirdplacesdata.blob.core.windows.net/place-photos/charlotte/ChIJ123/photo.jpg",
            ],
            "raw_data": {
                "photos_data": [
                    {"photo_url_big": "https://lh3.googleusercontent.com/raw-1=s0"},
                    {"photo_url_big": "https://lh3.googleusercontent.com/raw-2=s0"},
                ]
            },
        }
    }
    entry = {"record_id": "rec123", "source": "local_json_google"}

    recovered_data, warnings, summary = migrate.apply_recovery_manifest_entry(place_data, entry, max_source_urls=1)

    assert warnings == []
    assert recovered_data["photos"]["photo_urls"] == ["https://lh3.googleusercontent.com/raw-1=s0"]
    assert summary["recovery_manifest_photo_url_count"] == 1


def test_fetch_fresh_google_place_photos_limits_to_ten(monkeypatch):
    monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "test-key")
    from services import place_data_service

    urls = [f"https://lh3.googleusercontent.com/fresh-{index}=s0" for index in range(12)]

    class DummyGoogleMapsProvider:
        def get_place_photos(self, place_id):
            return {
                "place_id": place_id,
                "photo_urls": urls,
                "raw_data": {
                    "photos_data": [
                        {"photo_url_big": url}
                        for url in urls
                    ]
                },
            }

    monkeypatch.setattr(place_data_service, "GoogleMapsProvider", DummyGoogleMapsProvider)

    place_data, warnings = migrate.fetch_fresh_google_place_photos("ChIJ123", max_photos=10)

    assert warnings == ["fresh_google_places_photos_loaded: 10"]
    assert place_data is not None
    assert place_data["photos"]["photo_urls"] == urls[:10]
    assert len(place_data["photos"]["raw_data"]["photos_data"]) == 10


def test_process_place_retries_with_ten_fresh_google_places_photos_after_all_downloads_fail(monkeypatch, tmp_path):
    selected_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.webp"

    class RetryingPhotoAssetService:
        def __init__(self):
            self.prepare_calls = []

        def prepare_place_context(self, record, place_data, config):
            self.prepare_calls.append((record, place_data))
            if len(self.prepare_calls) == 1:
                return {
                    "status": "prepared",
                    "place_name": "Test Cafe",
                    "place_id": "ChIJ123",
                    "record_id": "rec123",
                    "inventory": [{"canonical_source_url": "https://lh3.googleusercontent.com/stale=s0"}],
                    "warnings": [],
                }
            assert json.loads(record["fields"]["Photos"]) == []
            assert place_data["photos"]["photo_urls"] == ["https://lh3.googleusercontent.com/fresh=s0"]
            return {
                "status": "prepared",
                "place_name": "Test Cafe",
                "place_id": "ChIJ123",
                "record_id": "rec123",
                "inventory": [{"canonical_source_url": "https://lh3.googleusercontent.com/fresh=s0"}],
                "warnings": [],
            }

        def process_candidate_batch(self, place_context, candidates, config):
            source_url = candidates[0]["canonical_source_url"]
            if "stale" in source_url:
                return {
                    "assets": [],
                    "failures": [{"reason": "download_failed", "error": "HTTP 403"}],
                }
            return {
                "assets": [{"azure_url": selected_url, "status": "uploaded", "bytes": 12}],
                "failures": [],
            }

        def finalize_place_assets(self, place_context, assets, failures):
            if failures:
                return {
                    "selected_airtable_urls": [],
                    "summary": {"candidate_count": 1, "failed_upload_count": 1, "azure_assets_count": 0},
                    "assets": [],
                    "failures": failures,
                }
            return {
                "selected_airtable_urls": [selected_url],
                "summary": {"candidate_count": 1, "selected_airtable_count": 1, "azure_assets_count": 1},
                "assets": assets,
                "failures": [],
            }

    monkeypatch.setattr(
        migrate,
        "fetch_fresh_google_place_photos",
        lambda place_id, max_photos=10: ({"place_id": place_id, "photos": {"photo_urls": ["https://lh3.googleusercontent.com/fresh=s0"]}}, ["fresh_google_places_photos_loaded: 1"]),
    )
    client = FakeAirtableClient()
    service = RetryingPhotoAssetService()
    record = make_record(photos=json.dumps(["https://lh3.googleusercontent.com/stale=s0"]))

    result = migrate.process_place(
        record,
        tmp_path,
        migrate.MigrationRunConfig(dry_run=False, upload=True, write_airtable=True),
        client,
        service,
    )

    assert result["status"] == "updated"
    assert result["summary"]["retried_with_fresh_google_places_photos"] is True
    assert result["summary"]["fresh_google_places_photo_limit"] == 10
    assert result["summary"]["stale_candidate_count"] == 1
    assert "fresh_google_places_photos_loaded: 1" in result["warnings"]
    assert client.updated == [("rec123", [selected_url])]
    assert len(service.prepare_calls) == 2


def test_finalize_local_result_skips_when_all_downloads_fail():
    asset_result = {
        "selected_airtable_urls": [],
        "summary": {"candidate_count": 2, "failed_upload_count": 2, "azure_assets_count": 0},
        "assets": [],
        "failures": [{"reason": "download_failed"}, {"reason": "download_failed"}],
    }

    result = migrate.finalize_local_result(
        make_record(),
        migrate.MigrationRunConfig(dry_run=False, upload=True, write_airtable=True),
        asset_result,
        [],
        FailingAirtableClient(),
    )

    assert result["status"] == "skipped"
    assert result["skip_reason"] == "all_photo_downloads_failed"
    assert result["failure_reason_counts"] == {"download_failed": 2}


def test_finalize_local_result_updates_airtable_once():
    selected_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp"
    asset_result = {
        "selected_airtable_urls": [selected_url],
        "summary": {"candidate_count": 1, "selected_airtable_count": 1, "azure_assets_count": 1},
        "assets": [],
        "failures": [],
    }
    client = FakeAirtableClient()
    record = make_record(photos=json.dumps(["https://lh3.googleusercontent.com/photo=s0"]))

    result = migrate.finalize_local_result(
        record,
        migrate.MigrationRunConfig(dry_run=False, upload=True, write_airtable=True),
        asset_result,
        [],
        client,
    )

    assert result["status"] == "updated"
    assert client.updated == [("rec123", [selected_url])]
    assert json.loads(record["fields"]["Photos"]) == [selected_url]
    assert result["summary"]["airtable_write_attempted"] is True
    assert result["summary"]["airtable_update_applied"] is True