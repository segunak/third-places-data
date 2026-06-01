import json
import os
import sys
from pathlib import Path
from threading import Event, Lock
from unittest import mock


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


def make_record(record_id="rec123", place_id="ChIJ123", photos=None, photos_backup=None):
    fields = {
        "Place": "Test Cafe",
        "Google Maps Place Id": place_id,
    }
    if photos is not None:
        fields["Photos"] = photos
    if photos_backup is not None:
        fields["Photos Backup"] = photos_backup
    return {"id": record_id, "fields": fields}


def test_default_max_workers_is_five():
    assert migrate.DEFAULT_MAX_WORKERS == 5


def test_process_target_records_parallel_preserves_input_order_and_uses_worker_resources(monkeypatch, tmp_path):
    records = [make_record(record_id=f"rec-{index}") for index in range(4)]
    started = []
    started_lock = Lock()
    release = Event()
    client_instances = []
    service_instances = []

    class DummyAirtableClient:
        def __init__(self, write_limiter):
            self.write_limiter = write_limiter

    class DummyPhotoAssetService:
        pass

    def client_factory(write_limiter):
        client = DummyAirtableClient(write_limiter)
        client_instances.append(client)
        return client

    def service_factory():
        service = DummyPhotoAssetService()
        service_instances.append(service)
        return service

    def fake_process_place(record, data_root, run_config, airtable_client, service):
        with started_lock:
            started.append(record["id"])
            if len(started) >= 2:
                release.set()
        release.wait(timeout=1)
        return {
            "status": "updated",
            "place_name": record["fields"]["Place"],
            "record_id": record["id"],
            "message": "done",
            "summary": {},
        }

    monkeypatch.setattr(migrate, "process_place", fake_process_place)

    progress = []
    results = migrate.process_target_records(
        records,
        tmp_path,
        migrate.MigrationRunConfig(dry_run=False, upload=True, write_airtable=True),
        max_workers=2,
        airtable_client_factory=client_factory,
        photo_asset_service_factory=service_factory,
        progress_callback=lambda completed, total, result: progress.append((completed, total, result["record_id"])),
    )

    assert [result["record_id"] for result in results] == [record["id"] for record in records]
    assert progress[-1][:2] == (4, 4)
    assert len(client_instances) >= 2
    assert len(service_instances) >= 2
    assert all(client.write_limiter is not None for client in client_instances)


def test_process_target_records_single_worker_reuses_one_client_and_service(monkeypatch, tmp_path):
    records = [make_record(record_id="rec-one"), make_record(record_id="rec-two")]
    client_instances = []
    service_instances = []

    def client_factory(write_limiter):
        client = object()
        client_instances.append((client, write_limiter))
        return client

    def service_factory():
        service = object()
        service_instances.append(service)
        return service

    def fake_process_place(record, data_root, run_config, airtable_client, service):
        return {
            "status": "updated",
            "place_name": record["fields"]["Place"],
            "record_id": record["id"],
            "message": "done",
            "summary": {},
        }

    monkeypatch.setattr(migrate, "process_place", fake_process_place)

    results = migrate.process_target_records(
        records,
        tmp_path,
        migrate.MigrationRunConfig(dry_run=False, upload=True, write_airtable=True),
        max_workers=1,
        airtable_client_factory=client_factory,
        photo_asset_service_factory=service_factory,
        progress_callback=None,
    )

    assert [result["record_id"] for result in results] == ["rec-one", "rec-two"]
    assert len(client_instances) == 1
    assert client_instances[0][1] is None
    assert len(service_instances) == 1


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


def test_process_place_recovery_manifest_uses_recovered_google_sources(tmp_path):
    recovery_url = "https://lh3.googleusercontent.com/recovered=s0"
    selected_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp"
    curator_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/curator-att123-photo.webp"

    class RecoveryPhotoAssetService:
        def prepare_place_context(self, record, place_data, config):
            retained_photos = json.loads(record["fields"]["Photos"])
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

    record = make_record(photos=json.dumps([curator_url]))
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
                "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp",
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
    assert client.updated == [("rec123", [{"display": selected_url, "thumbnail": selected_url}])]
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
    assert client.updated == [("rec123", [{"display": selected_url, "thumbnail": selected_url}])]
    assert json.loads(record["fields"]["Photos"]) == [{"display": selected_url, "thumbnail": selected_url}]
    assert result["summary"]["airtable_write_attempted"] is True
    assert result["summary"]["airtable_update_applied"] is True


def test_airtable_photo_client_updates_only_photos_field(monkeypatch):
    current_photos = json.dumps(["https://lh3.googleusercontent.com/photo=s0"])
    selected_photo = {
        "display": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/photo.webp",
        "thumbnail": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/photo.webp",
    }
    table = mock.MagicMock()
    monkeypatch.setattr(migrate, "time", mock.MagicMock())

    client = migrate.AirtablePhotoClient(table)
    result = client.update_photos(make_record(photos=current_photos), [selected_photo])

    table.get.assert_not_called()
    table.update.assert_called_once_with("rec123", {"Photos": json.dumps([selected_photo])})
    assert result["updated"] is True


def test_airtable_photo_client_uses_write_limiter_for_real_update():
    current_photos = json.dumps([])
    selected_photo = {
        "display": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/photo.webp",
        "thumbnail": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/photo.webp",
    }
    table = mock.MagicMock()
    write_limiter = mock.MagicMock()

    client = migrate.AirtablePhotoClient(table, write_limiter=write_limiter)
    result = client.update_photos(make_record(photos=current_photos), [selected_photo])

    write_limiter.wait.assert_called_once_with()
    table.update.assert_called_once_with("rec123", {"Photos": json.dumps([selected_photo])})
    assert result["updated"] is True


def test_airtable_photo_client_skips_write_limiter_when_photos_match():
    selected_photo = {
        "display": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/photo.webp",
        "thumbnail": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/photo.webp",
    }
    table = mock.MagicMock()
    write_limiter = mock.MagicMock()

    client = migrate.AirtablePhotoClient(table, write_limiter=write_limiter)
    result = client.update_photos(make_record(photos=json.dumps([selected_photo])), [selected_photo])

    write_limiter.wait.assert_not_called()
    table.update.assert_not_called()
    assert result["updated"] is False


def test_parse_photos_backup_entries_preserves_order_and_warns_on_duplicates():
    first_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/root-a.webp"
    second_manifest = {
        "display": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/root-b.webp",
        "thumbnail": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/root-b.webp",
    }

    entries, warnings, errors = migrate.parse_photos_backup_entries(json.dumps([first_url, second_manifest, first_url]))

    assert errors == []
    assert warnings == ["photos_backup_duplicate_entries: 1"]
    assert [entry["kind"] for entry in entries] == ["source_url", "manifest", "source_url"]
    assert entries[0]["source_url"] == first_url
    assert entries[1]["photo_manifest"] == second_manifest


def test_repair_photos_from_backup_dry_run_uses_live_backup_only(monkeypatch):
    root_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp"
    current_photos = json.dumps([
        {
            "display": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/inflated.webp",
            "thumbnail": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/inflated.webp",
        }
    ])

    class DummyPublisher:
        def publish_standard_url(self, source_url, place_id, record_id, place_name, **kwargs):
            assert source_url == root_url
            assert kwargs["source_field"] == "Photos Backup"
            assert kwargs["source_hash"] == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            return {
                "success": True,
                "status": "would_upload",
                "photo_manifest": {
                    "display": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp",
                    "thumbnail": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp",
                },
                "bytes": 0,
            }

    monkeypatch.setattr(migrate, "load_place_data", mock.MagicMock(side_effect=AssertionError("repair must not read local JSON")))
    monkeypatch.setattr(migrate, "process_place", mock.MagicMock(side_effect=AssertionError("repair must not use legacy migration flow")))

    result = migrate.repair_photos_from_backup_record(
        make_record(photos=current_photos, photos_backup=json.dumps([root_url])),
        migrate.MigrationRunConfig(dry_run=True, upload=False, write_airtable=False),
        FailingAirtableClient(),
        DummyPublisher(),
    )

    assert result["status"] == "would_update"
    assert result["summary"]["current_photos_count"] == 1
    assert result["summary"]["photos_backup_count"] == 1
    assert result["summary"]["desired_photos_count"] == 1
    assert result["selected_airtable_count"] == 1
    assert result["selected_airtable_photo_samples_status"] == "planned_not_uploaded"
    assert result["summary"]["local_data_file_used"] is False
    assert result["summary"]["provider_raw_sources_used"] is False
    assert result["summary"]["photos_backup_read_only"] is True


def test_repair_photos_from_backup_write_updates_only_photos_and_preserves_backup():
    filename = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp"
    backup_manifest = {
        "display": f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/{filename}",
        "thumbnail": f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/{filename}",
    }
    backup_value = json.dumps([backup_manifest])
    table = mock.MagicMock()
    record = make_record(photos=json.dumps([]), photos_backup=backup_value)
    client = migrate.AirtablePhotoClient(table)

    result = migrate.repair_photos_from_backup_record(
        record,
        migrate.MigrationRunConfig(dry_run=False, upload=True, write_airtable=True),
        client,
    )

    assert result["status"] == "updated"
    table.update.assert_called_once_with("rec123", {"Photos": json.dumps([backup_manifest])})
    assert record["fields"]["Photos Backup"] == backup_value
    assert json.loads(record["fields"]["Photos"]) == [backup_manifest]


def test_repair_photos_from_backup_fails_malformed_backup_without_update():
    client = FailingAirtableClient()

    result = migrate.repair_photos_from_backup_record(
        make_record(photos=json.dumps([]), photos_backup="not json"),
        migrate.MigrationRunConfig(dry_run=False, upload=True, write_airtable=True),
        client,
    )

    assert result["status"] == "error"
    assert result["error_reason"] == "invalid_photos_backup"
    assert result["summary"]["airtable_write_attempted"] is False


def test_airtable_photo_client_fetches_live_repair_fields():
    table = mock.MagicMock()
    table.all.return_value = []

    client = migrate.AirtablePhotoClient(table)
    client.fetch_photo_repair_records("All")

    table.all.assert_called_once_with(view="All", fields=["Place", "Google Maps Place Id", "Photos", "Photos Backup"], sort=["-Created Time"])


def test_select_live_airtable_records_targets_without_google_photo_filter():
    records = [
        make_record("rec-target", place_id="ChIJ-target", photos=json.dumps([]), photos_backup=json.dumps([])),
        make_record("rec-other", place_id="ChIJ-other", photos=json.dumps(["https://lh3.googleusercontent.com/photo=s0"]), photos_backup=json.dumps([])),
    ]
    args = type("Args", (), {
        "record_id": "",
        "place_id": "ChIJ-target",
        "max_places": 0,
    })()

    selected = migrate.select_live_airtable_records(records, args)

    assert [record["id"] for record in selected] == ["rec-target"]


def test_build_azure_storage_cleanup_manifest_protects_fixed_photos_and_backup_urls(monkeypatch):
    live_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    backup_hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    extra_hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    live_manifest = {
        "display": f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/{live_hash}.webp",
        "thumbnail": f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/{live_hash}.webp",
    }
    backup_root_url = f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/{backup_hash}.webp"
    record = make_record(photos=json.dumps([live_manifest]), photos_backup=json.dumps([backup_root_url]))

    monkeypatch.setattr(
        migrate,
        "list_blobs_in_container",
        lambda container, prefix="": [
            "ChIJ123/display",
            "ChIJ123/thumbnail/",
            f"ChIJ123/display/{live_hash}.webp",
            f"ChIJ123/thumbnail/{live_hash}.webp",
            f"ChIJ123/{backup_hash}.webp",
            f"ChIJ123/{extra_hash}.webp",
            f"ChIJ123/display/{extra_hash}.webp",
        ],
    )

    manifest = migrate.build_azure_storage_cleanup_manifest([record], "charlotte")

    candidates = manifest["results"][0]["cleanup_candidates"]
    assert manifest["cleanup_candidate_count"] == 2
    assert manifest["results"][0]["folder_marker_blob_count"] == 2
    assert manifest["results"][0]["blob_count"] == 5
    assert [candidate["blob_path"] for candidate in candidates] == [
        f"ChIJ123/{extra_hash}.webp",
        f"ChIJ123/display/{extra_hash}.webp",
    ]


def test_build_azure_storage_cleanup_manifest_reports_progress(monkeypatch):
    records = [
        make_record(record_id="rec-ready", place_id="ChIJ123", photos=json.dumps([]), photos_backup=json.dumps([])),
        make_record(record_id="rec-missing-place", place_id="", photos=json.dumps([]), photos_backup=json.dumps([])),
    ]
    monkeypatch.setattr(migrate, "list_blobs_in_container", lambda container, prefix="": [])
    progress = []

    migrate.build_azure_storage_cleanup_manifest(
        records,
        "charlotte",
        progress_callback=lambda completed, total, result: progress.append(
            (completed, total, result["status"], result["record_id"])
        ),
        max_workers=1,
    )

    assert progress == [
        (1, 2, "ok", "rec-ready"),
        (2, 2, "skipped", "rec-missing-place"),
    ]


def test_build_azure_storage_cleanup_manifest_uses_max_workers(monkeypatch):
    records = [
        make_record(record_id="rec-one", place_id="ChIJ-one", photos=json.dumps([]), photos_backup=json.dumps([])),
        make_record(record_id="rec-two", place_id="ChIJ-two", photos=json.dumps([]), photos_backup=json.dumps([])),
    ]
    created_workers = []

    class FakeFuture:
        def __init__(self, value):
            self.value = value

        def result(self):
            return self.value

    class FakeExecutor:
        def __init__(self, max_workers):
            created_workers.append(max_workers)
            self.futures = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def submit(self, func, record):
            future = FakeFuture(func(record))
            self.futures.append(future)
            return future

    monkeypatch.setattr(migrate, "list_blobs_in_container", lambda container, prefix="": [])
    monkeypatch.setattr(migrate, "ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr(migrate, "as_completed", lambda futures: list(futures))

    manifest = migrate.build_azure_storage_cleanup_manifest(records, "charlotte", max_workers=8)

    assert created_workers == [8]
    assert [result["record_id"] for result in manifest["results"]] == ["rec-one", "rec-two"]


def test_run_audit_uses_max_workers_and_preserves_order(monkeypatch):
    records = [
        make_record(record_id="rec-one", place_id="ChIJ-one"),
        make_record(record_id="rec-two", place_id="ChIJ-two"),
    ]
    created_workers = []

    class FakeFuture:
        def __init__(self, value):
            self.value = value

        def result(self):
            return self.value

    class FakeExecutor:
        def __init__(self, max_workers):
            created_workers.append(max_workers)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def submit(self, func, record):
            return FakeFuture(func(record))

    def fake_audit(payload):
        record = payload["place"]
        return {
            "status": "ok",
            "record_id": record["id"],
            "missing_blob_reference_count": 0,
            "unserved_blob_count": 0,
            "airtable_photo_url_count": 0,
            "canonical_photo_url_count": 0,
            "canonical_curator_url_count": 0,
            "canonical_standard_url_count": 0,
            "invalid_azure_airtable_url_count": 0,
            "non_azure_airtable_url_count": 0,
            "new_container_blob_count": 0,
        }

    monkeypatch.setattr(migrate, "audit_single_place_photo_assets", fake_audit)
    monkeypatch.setattr(migrate, "ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr(migrate, "as_completed", lambda futures: list(futures))

    results, totals = migrate.run_audit(records, "charlotte", max_workers=6)

    assert created_workers == [6]
    assert [result["record_id"] for result in results] == ["rec-one", "rec-two"]
    assert totals["total_places"] == 2


def test_delete_azure_storage_cleanup_manifest_rechecks_live_protection(monkeypatch):
    live_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    extra_hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    live_manifest = {
        "display": f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/{live_hash}.webp",
        "thumbnail": f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/{live_hash}.webp",
    }
    record = make_record(photos=json.dumps([live_manifest]), photos_backup=json.dumps([]))
    deleted = []
    monkeypatch.setattr(migrate, "delete_blob_from_container_with_status", lambda container, blob_path: deleted.append(blob_path) or "deleted")
    cleanup_manifest = {
        "results": [
            {
                "place_id": "ChIJ123",
                "cleanup_candidates": [
                    {
                        "container": "photos",
                        "blob_path": f"ChIJ123/display/{live_hash}.webp",
                        "url": live_manifest["display"],
                        "reason": "unreferenced_display_variant",
                    },
                    {
                        "container": "photos",
                        "blob_path": f"ChIJ123/{extra_hash}.webp",
                        "url": f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/{extra_hash}.webp",
                        "reason": "unreferenced_root_blob",
                    },
                ],
            }
        ]
    }

    report = migrate.delete_azure_storage_cleanup_manifest(cleanup_manifest, [record], "charlotte", dry_run=False)

    assert report["attempted"] == 2
    assert report["skipped_protected"] == 1
    assert report["deleted"] == 1
    assert deleted == [f"ChIJ123/{extra_hash}.webp"]


def test_delete_azure_storage_cleanup_manifest_reports_progress(monkeypatch):
    live_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    extra_hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    live_manifest = {
        "display": f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/{live_hash}.webp",
        "thumbnail": f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/{live_hash}.webp",
    }
    record = make_record(photos=json.dumps([live_manifest]), photos_backup=json.dumps([]))
    monkeypatch.setattr(migrate, "delete_blob_from_container_with_status", lambda container, blob_path: "deleted")
    cleanup_manifest = {
        "results": [
            {
                "place_id": "ChIJ123",
                "cleanup_candidates": [
                    {
                        "container": "photos",
                        "blob_path": f"ChIJ123/display/{live_hash}.webp",
                        "url": live_manifest["display"],
                        "reason": "unreferenced_display_variant",
                    },
                    {
                        "container": "photos",
                        "blob_path": f"ChIJ123/{extra_hash}.webp",
                        "url": f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/{extra_hash}.webp",
                        "reason": "unreferenced_root_blob",
                    },
                ],
            }
        ]
    }
    progress = []

    migrate.delete_azure_storage_cleanup_manifest(
        cleanup_manifest,
        [record],
        "charlotte",
        dry_run=False,
        progress_callback=lambda completed, total, result: progress.append((completed, total, result["status"])),
    )

    assert progress == [
        (1, 2, "skipped_protected"),
        (2, 2, "deleted"),
    ]


def test_delete_azure_storage_cleanup_manifest_treats_missing_blob_as_success(monkeypatch):
    missing_hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    record = make_record(photos=json.dumps([]), photos_backup=json.dumps([]))
    monkeypatch.setattr(migrate, "delete_blob_from_container_with_status", lambda container, blob_path: "missing_already")
    cleanup_manifest = {
        "results": [
            {
                "place_id": "ChIJ123",
                "cleanup_candidates": [
                    {
                        "container": "photos",
                        "blob_path": f"ChIJ123/{missing_hash}.webp",
                        "url": f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/{missing_hash}.webp",
                        "reason": "unreferenced_root_blob",
                    }
                ],
            }
        ]
    }

    report = migrate.delete_azure_storage_cleanup_manifest(cleanup_manifest, [record], "charlotte", dry_run=False)

    assert report["success"] is True
    assert report["attempted"] == 1
    assert report["deleted"] == 0
    assert report["missing_already"] == 1
    assert report["failed"] == 0


def test_delete_azure_storage_cleanup_manifest_uses_max_workers(monkeypatch):
    hashes = [
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ]
    record = make_record(photos=json.dumps([]), photos_backup=json.dumps([]))
    submitted_blob_paths = []
    created_workers = []

    class FakeFuture:
        def __init__(self, value):
            self.value = value

        def result(self):
            return self.value

    class FakeExecutor:
        def __init__(self, max_workers):
            created_workers.append(max_workers)
            self.futures = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def submit(self, func, job):
            submitted_blob_paths.append(job[1]["blob_path"])
            future = FakeFuture(func(job))
            self.futures.append(future)
            return future

    cleanup_manifest = {
        "results": [
            {
                "place_id": "ChIJ123",
                "cleanup_candidates": [
                    {
                        "container": "photos",
                        "blob_path": f"ChIJ123/{photo_hash}.webp",
                        "url": f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/{photo_hash}.webp",
                        "reason": "unreferenced_root_blob",
                    }
                    for photo_hash in hashes
                ],
            }
        ]
    }
    monkeypatch.setattr(migrate, "delete_blob_from_container_with_status", lambda container, blob_path: "deleted")
    monkeypatch.setattr(migrate, "ThreadPoolExecutor", FakeExecutor)
    monkeypatch.setattr(migrate, "as_completed", lambda futures: list(futures))

    report = migrate.delete_azure_storage_cleanup_manifest(
        cleanup_manifest,
        [record],
        "charlotte",
        dry_run=False,
        max_workers=7,
    )

    assert created_workers == [7]
    assert submitted_blob_paths == [f"ChIJ123/{photo_hash}.webp" for photo_hash in hashes]
    assert report["deleted"] == 2


def test_delete_azure_storage_cleanup_manifest_skips_manifest_entries_outside_live_target(monkeypatch):
    target_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    other_hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    record = make_record(photos=json.dumps([]), photos_backup=json.dumps([]))
    deleted = []
    monkeypatch.setattr(migrate, "delete_blob_from_container_with_status", lambda container, blob_path: deleted.append(blob_path) or "deleted")
    cleanup_manifest = {
        "results": [
            {
                "place_id": "ChIJ123",
                "cleanup_candidates": [
                    {
                        "container": "photos",
                        "blob_path": f"ChIJ123/{target_hash}.webp",
                        "url": f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/{target_hash}.webp",
                        "reason": "unreferenced_root_blob",
                    }
                ],
            },
            {
                "place_id": "ChIJ-other",
                "cleanup_candidates": [
                    {
                        "container": "photos",
                        "blob_path": f"ChIJ-other/{other_hash}.webp",
                        "url": f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ-other/{other_hash}.webp",
                        "reason": "unreferenced_root_blob",
                    }
                ],
            },
        ]
    }

    report = migrate.delete_azure_storage_cleanup_manifest(cleanup_manifest, [record], "charlotte", dry_run=False)

    assert report["attempted"] == 2
    assert report["skipped_not_targeted"] == 1
    assert report["deleted"] == 1
    assert deleted == [f"ChIJ123/{target_hash}.webp"]


def test_delete_azure_storage_cleanup_manifest_skips_folder_marker_entries(monkeypatch):
    record = make_record(photos=json.dumps([]), photos_backup=json.dumps([]))
    deleted = []
    monkeypatch.setattr(migrate, "delete_blob_from_container_with_status", lambda container, blob_path: deleted.append(blob_path) or "deleted")
    cleanup_manifest = {
        "results": [
            {
                "place_id": "ChIJ123",
                "cleanup_candidates": [
                    {
                        "container": "photos",
                        "blob_path": "ChIJ123/display",
                        "url": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display",
                        "reason": "unreferenced_root_blob",
                    },
                    {
                        "container": "photos",
                        "blob_path": "ChIJ123/thumbnail/",
                        "url": "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/thumbnail/",
                        "reason": "unreferenced_root_blob",
                    },
                ],
            }
        ]
    }

    report = migrate.delete_azure_storage_cleanup_manifest(cleanup_manifest, [record], "charlotte", dry_run=False)

    assert report["attempted"] == 2
    assert report["skipped_folder_markers"] == 2
    assert report["deleted"] == 0
    assert report["failed"] == 0
    assert deleted == []
