import logging
import json

from blueprints import photo_assets


class DummyRequest:
    def __init__(self, params):
        self.params = params


class FakePhotoAssetsOrchestrationContext:
    def __init__(self, input_data):
        self._input_data = input_data
        self.activity_calls = []
        self.sub_orchestrator_calls = []
        self.custom_statuses = []
        self.is_replaying = False

    def get_input(self):
        return self._input_data

    def call_activity(self, name, input_data):
        activity_call = {"name": name, "input": input_data}
        self.activity_calls.append(activity_call)
        return activity_call

    def call_sub_orchestrator(self, name, input_data):
        sub_orchestrator_call = {"name": name, "input": input_data, "kind": "sub_orchestrator"}
        self.sub_orchestrator_calls.append(sub_orchestrator_call)
        return sub_orchestrator_call

    def task_all(self, tasks):
        return {"name": "task_all", "tasks": tasks}

    def set_custom_status(self, status):
        self.custom_statuses.append(status)


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
                        "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ5YHD3oOhVogRAV83qWzHmgg/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.jpg"
                    ]),
                },
            }
        ]


def run_photo_assets_migration_orchestrator(context):
    return photo_assets.photo_assets_migration_orchestrator._function._func.orchestrator_function(context)


def run_migrate_place_photo_assets_orchestrator(context):
    return photo_assets.migrate_place_photo_assets_orchestrator._function._func.orchestrator_function(context)


def test_base_config_defaults():
    config = photo_assets._base_config_from_params({})

    assert config["city"] == "charlotte"
    assert config["dry_run"] is True
    assert config["upload"] is False
    assert config["write_airtable"] is False
    assert config["try_url_variants"] is True
    assert config["migration_concurrency"] == 20
    assert config["candidate_chunk_size"] == 2
    assert config["download_timeout_seconds"] == 20


def test_base_config_uses_upload_safe_defaults_for_live_runs():
    config = photo_assets._base_config_from_params({"dry_run": "false"})

    assert config["dry_run"] is False
    assert config["upload"] is True
    assert config["write_airtable"] is True
    assert config["migration_concurrency"] == 5
    assert config["candidate_chunk_size"] == 2
    assert config["download_timeout_seconds"] == 20


def test_base_config_rejects_invalid_int():
    try:
        photo_assets._base_config_from_params({"max_places": "bad"})
    except ValueError as exc:
        assert "Invalid integer value" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_base_config_rejects_non_positive_migration_controls():
    try:
        photo_assets._base_config_from_params({"migration_concurrency": "0"})
    except ValueError as exc:
        assert "migration_concurrency must be greater than zero" in str(exc)
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


def test_compact_place_result_preserves_compact_activity_fields():
    compact = photo_assets._compact_place_result({
        "status": "would_update",
        "place_name": "Compact Cafe",
        "place_id": "ChIJcompact",
        "record_id": "recCompact",
        "summary": {"candidate_count": 12, "blob_bytes": 1234},
        "selected_airtable_count": 30,
        "selected_airtable_url_samples": ["https://example.com/one.jpg"],
        "asset_count": 42,
        "asset_status_counts": {"would_upload": 42},
        "failure_count": 2,
        "failure_reason_counts": {"download_failed": 2},
        "failure_error_samples": ["HTTP 403"],
    })

    assert compact["selected_airtable_count"] == 30
    assert compact["selected_airtable_url_samples"] == ["https://example.com/one.jpg"]
    assert compact["asset_count"] == 42
    assert compact["asset_status_counts"] == {"would_upload": 42}
    assert compact["failure_count"] == 2
    assert compact["failure_reason_counts"] == {"download_failed": 2}
    assert compact["failure_error_samples"] == ["HTTP 403"]


def test_migration_progress_status_includes_recent_results_and_failure_details():
    status = photo_assets._migration_progress_status(
        {"city": "charlotte", "provider_type": "outscraper", "dry_run": True},
        "failed",
        1,
        2,
        1,
        2,
        [{
            "status": "error",
            "error_reason": "no_selected_azure_urls",
            "message": "No selected URLs",
            "place_name": "Broken Coffee",
            "place_id": "ChIJbroken",
            "record_id": "recBroken",
            "summary": {"candidate_count": 3, "failed_upload_count": 3},
            "failures": [{"reason": "download_failed", "error": "HTTP 403"}],
        }],
        error=RuntimeError("status details available"),
    )

    assert status["phase"] == "failed"
    assert status["processed_places"] == 1
    assert status["error_count"] == 1
    assert status["recent_place_results"][0]["place_name"] == "Broken Coffee"
    assert status["recent_error_results"][0]["failure_reason_counts"] == {"download_failed": 1}
    assert status["failure"] == {"type": "RuntimeError", "message": "status details available"}


def test_migration_progress_status_omits_verbose_results_by_default():
    status = photo_assets._migration_progress_status(
        {"city": "charlotte", "provider_type": "outscraper", "dry_run": True},
        "migration_batch_completed",
        2,
        2,
        1,
        1,
        [
            {"status": "would_update", "place_name": "Ready Coffee", "summary": {"candidate_count": 12}},
            {"status": "skipped", "place_name": "No Place Id", "summary": {}},
        ],
    )

    assert status["phase"] == "migration_batch_completed"
    assert status["processed_places"] == 2
    assert status["place_status_counts_so_far"] == {"would_update": 1, "skipped": 1}
    assert "recent_place_results" not in status
    assert "recent_problem_results" not in status
    assert "recent_error_results" not in status


def test_migration_output_data_returns_all_compact_failures_and_expected_skip_samples():
    data = photo_assets._migration_output_data(
        {"success": False, "errors": 1},
        {"success": True, "errors": 0},
        {"success": False, "errors": 1},
        [
            {"status": "would_update", "place_name": "Ready Coffee", "summary": {"candidate_count": 12}},
            {"status": "error", "place_name": "Broken Coffee", "summary": {"candidate_count": 2}},
            {"status": "skipped", "skip_reason": "ignored_missing_place_id", "place_name": "No Place Id"},
            {"status": "skipped", "skip_reason": "all_photo_downloads_failed", "place_name": "Download Failed Cafe"},
        ],
        [
            {"status": "no_change", "place_name": "Ready Coffee"},
            {"status": "failed", "place_name": "Curator Failure", "photos_failed": 1},
        ],
        [
            {"status": "error", "place_name": "Broken Coffee", "missing_blob_reference_count": 1},
            {"status": "skipped", "skip_reason": "ignored_missing_place_id", "place_name": "Audit No Place Id"},
        ],
    )

    assert data["result_counts"] == {"place_results": 4, "curator_results": 2, "audit_results": 2}
    assert data["place_status_counts"] == {"would_update": 1, "error": 1, "skipped": 2}
    assert data["place_skip_reason_counts"] == {"ignored_missing_place_id": 1, "all_photo_downloads_failed": 1}
    assert data["place_error_results"][0]["place_name"] == "Broken Coffee"
    assert data["place_unexpected_skip_results"][0]["place_name"] == "Download Failed Cafe"
    assert data["expected_place_skip_samples"]["ignored_missing_place_id"][0]["place_name"] == "No Place Id"
    assert data["curator_error_results"][0]["place_name"] == "Curator Failure"
    assert data["audit_error_results"][0]["missing_blob_reference_count"] == 1
    assert data["expected_audit_skip_samples"]["ignored_missing_place_id"][0]["place_name"] == "Audit No Place Id"
    assert "place_results" not in data
    assert "curator_results" not in data
    assert "audit_results" not in data


def test_photo_assets_migration_orchestrator_updates_custom_status_by_phase():
    context = FakePhotoAssetsOrchestrationContext({
        "provider_type": "outscraper",
        "city": "charlotte",
        "dry_run": True,
        "upload": False,
        "write_airtable": False,
    })
    places = [
        {"id": "rec123", "fields": {"Place": "Daily Ritual", "Google Maps Place Id": "ChIJ123"}},
        {"id": "rec456", "fields": {"Place": "Quiet Library", "Google Maps Place Id": "ChIJ456"}},
    ]

    orchestrator = run_photo_assets_migration_orchestrator(context)
    get_all_call = next(orchestrator)

    assert get_all_call["name"] == "get_all_third_places"
    assert context.custom_statuses == []

    migration_call = orchestrator.send(places)
    assert migration_call["name"] == "task_all"
    assert [task["name"] for task in migration_call["tasks"]] == [
        "migrate_single_place_photo_assets",
        "migrate_single_place_photo_assets",
    ]

    migration_results = [
        {"status": "would_update", "place_name": "Daily Ritual", "place_id": "ChIJ123", "summary": {"candidate_count": 2}},
        {"status": "skipped", "skip_reason": "ignored_missing_place_id", "place_name": "Quiet Library", "place_id": "ChIJ456", "summary": {}},
    ]
    curator_call = orchestrator.send(migration_results)
    assert [task["name"] for task in curator_call["tasks"]] == [
        "sync_single_place_curator_photos",
        "sync_single_place_curator_photos",
    ]

    curator_results = [
        {"status": "no_change", "place_name": "Daily Ritual", "place_id": "ChIJ123"},
        {"status": "no_change", "place_name": "Quiet Library", "place_id": "ChIJ456"},
    ]
    audit_call = orchestrator.send(curator_results)
    assert [task["name"] for task in audit_call["tasks"]] == [
        "audit_single_place_photo_assets",
        "audit_single_place_photo_assets",
    ]

    audit_results = [
        {"status": "ok", "place_name": "Daily Ritual", "place_id": "ChIJ123"},
        {"status": "ok", "place_name": "Quiet Library", "place_id": "ChIJ456"},
    ]
    try:
        orchestrator.send(audit_results)
    except StopIteration as exc:
        result = exc.value
    else:
        raise AssertionError("Expected orchestrator to complete")

    phases = [status["phase"] for status in context.custom_statuses]
    assert phases == [
        "places_loaded",
        "migration_batch_running",
        "migration_batch_completed",
        "curator_batch_running",
        "curator_batch_completed",
        "audit_batch_running",
        "audit_batch_completed",
        "completed",
    ]
    assert result["success"] is True
    assert context.custom_statuses[-1]["curator_totals_so_far"]["total_places"] == 2
    assert context.custom_statuses[-1]["audit_totals_so_far"]["total_places"] == 2
    assert "recent_place_results" not in context.custom_statuses[-1]
    assert result["data"]["result_counts"] == {"place_results": 2, "curator_results": 2, "audit_results": 2}
    assert result["data"]["place_status_counts"] == {"would_update": 1, "skipped": 1}
    assert result["data"]["place_error_results"] == []
    assert result["data"]["place_unexpected_skip_results"] == []
    assert result["data"]["expected_place_skip_samples"]["ignored_missing_place_id"][0]["place_name"] == "Quiet Library"
    assert "place_results" not in result["data"]
    assert "curator_results" not in result["data"]
    assert "audit_results" not in result["data"]


def test_photo_assets_migration_orchestrator_uses_chunked_sub_orchestrator_for_uploads():
    context = FakePhotoAssetsOrchestrationContext({
        "provider_type": "outscraper",
        "city": "charlotte",
        "dry_run": False,
        "upload": True,
        "write_airtable": True,
        "migration_concurrency": 1,
        "candidate_chunk_size": 2,
    })
    places = [
        {"id": "rec123", "fields": {"Place": "Daily Ritual", "Google Maps Place Id": "ChIJ123"}},
        {"id": "rec456", "fields": {"Place": "Quiet Library", "Google Maps Place Id": "ChIJ456"}},
    ]

    orchestrator = run_photo_assets_migration_orchestrator(context)
    get_all_call = next(orchestrator)
    assert get_all_call["name"] == "get_all_third_places"

    migration_call = orchestrator.send(places)
    assert migration_call["name"] == "task_all"
    assert [task["name"] for task in migration_call["tasks"]] == ["migrate_place_photo_assets_orchestrator"]
    assert migration_call["tasks"][0]["kind"] == "sub_orchestrator"
    assert migration_call["tasks"][0]["input"]["config"]["candidate_chunk_size"] == 2


def test_migrate_place_photo_assets_orchestrator_processes_all_candidate_chunks():
    context = FakePhotoAssetsOrchestrationContext({
        "place": {"id": "rec123", "fields": {"Place": "Daily Ritual", "Google Maps Place Id": "ChIJ123"}},
        "config": {"candidate_chunk_size": 2},
    })
    orchestrator = run_migrate_place_photo_assets_orchestrator(context)

    prepare_call = next(orchestrator)
    assert prepare_call["name"] == "prepare_place_photo_asset_migration"

    prepared = {
        "status": "prepared",
        "candidate_count": 5,
        "place_context": {"place_name": "Daily Ritual", "place_id": "ChIJ123", "record_id": "rec123"},
    }
    first_chunk_call = orchestrator.send(prepared)
    assert first_chunk_call["name"] == "migrate_place_photo_asset_chunk"
    assert first_chunk_call["input"]["candidate_start"] == 0
    assert first_chunk_call["input"]["candidate_count"] == 2

    second_chunk_call = orchestrator.send({"status": "processed", "chunk_index": 1})
    assert second_chunk_call["input"]["candidate_start"] == 2
    assert second_chunk_call["input"]["candidate_count"] == 2

    third_chunk_call = orchestrator.send({"status": "processed", "chunk_index": 2})
    assert third_chunk_call["input"]["candidate_start"] == 4
    assert third_chunk_call["input"]["candidate_count"] == 1

    finalize_call = orchestrator.send({"status": "processed", "chunk_index": 3})
    assert finalize_call["name"] == "finalize_place_photo_asset_migration"
    assert len(finalize_call["input"]["chunk_results"]) == 3

    try:
        orchestrator.send({"status": "updated", "summary": {}})
    except StopIteration as exc:
        assert exc.value["status"] == "updated"
    else:
        raise AssertionError("Expected place orchestrator to complete")


def test_photo_assets_migration_orchestrator_reraises_infrastructure_failure_after_status_update():
    context = FakePhotoAssetsOrchestrationContext({
        "provider_type": "outscraper",
        "city": "charlotte",
        "dry_run": True,
    })

    orchestrator = run_photo_assets_migration_orchestrator(context)
    get_all_call = next(orchestrator)
    assert get_all_call["name"] == "get_all_third_places"

    try:
        orchestrator.throw(RuntimeError("Airtable unavailable"))
    except RuntimeError as exc:
        assert str(exc) == "Airtable unavailable"
    else:
        raise AssertionError("Expected orchestrator to re-raise infrastructure failure")

    assert context.custom_statuses[-1]["phase"] == "failed"
    assert context.custom_statuses[-1]["failure"] == {
        "type": "RuntimeError",
        "message": "Airtable unavailable",
    }


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
    assert result["selected_airtable_count"] == result["summary"]["selected_airtable_count"]
    assert result["asset_count"] == result["summary"]["azure_assets_count"]
    assert "assets" not in result
    assert "failures" not in result
    assert "inventory" not in result
    assert "selected_source_urls" not in result


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
    assert result["skip_reason"] == "ignored_missing_place_id"
    assert result["message"] == "Skipped: no Google Maps Place Id; photo paths ignore non-photo-ready records."
    assert "missing Google Maps Place Id" in caplog.text


def test_migrate_single_place_ignores_missing_place_id_with_legacy_curator_photo_urls(caplog):
    caplog.set_level(logging.INFO)
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

    assert result["status"] == "skipped"
    assert result["skip_reason"] == "ignored_missing_place_id"
    assert result["summary"]["ignored_missing_place_id"] is True
    assert "missing Google Maps Place Id" in caplog.text


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
    assert result["skip_reason"] == "ignored_missing_place_id"
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
    assert result["selected_airtable_count"] == 1
    assert result["selected_airtable_url_samples"] == [curator_url]
    assert result["summary"]["candidate_count"] == 0


def test_migrate_single_place_skips_when_only_unsupported_legacy_curator_url_exists(monkeypatch):
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

    assert result["status"] == "skipped"
    assert result["skip_reason"] == "no_migratable_photo_urls"
    assert result["summary"]["unselected_curator_photo_urls_field_urls"] == [uncopied_url]


def test_migrate_single_place_skips_when_all_photo_downloads_fail(monkeypatch):
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

    monkeypatch.setattr(photo_assets, "PhotoAssetService", DummyPhotoAssetService)
    monkeypatch.setattr(photo_assets, "AirtableService", FailingAirtableService)
    monkeypatch.setattr(photo_assets, "fetch_data_github", lambda path: (True, {"photos": {}}, "ok"))

    result = photo_assets.migrate_single_place_photo_assets({
        "place": {
            "id": "rec-downloads-failed",
            "fields": {
                "Place": "Downloads Failed Place",
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

    assert result["status"] == "skipped"
    assert result["skip_reason"] == "all_photo_downloads_failed"
    assert "all photo candidate downloads failed" in result["message"]
    assert result["selected_airtable_count"] == 0
    assert result["failure_count"] == 2
    assert result["failure_reason_counts"] == {"download_failed": 2}
    assert result["summary"]["candidate_count"] == 2


def test_migrate_single_place_refuses_unexplained_empty_airtable_photos_update(monkeypatch):
    class DummyPhotoAssetService:
        def process_place(self, place, place_data, config):
            return {
                "selected_airtable_urls": [],
                "summary": {
                    "candidate_count": 2,
                    "azure_assets_count": 0,
                    "failed_upload_count": 0,
                },
                "assets": [],
                "failures": [],
            }

    class FailingAirtableService:
        def __init__(self, provider_type):
            self.provider_type = provider_type

        def update_place_record(self, *args, **kwargs):
            raise AssertionError("Airtable should not be updated with an empty Photos list")

    monkeypatch.setattr(photo_assets, "PhotoAssetService", DummyPhotoAssetService)
    monkeypatch.setattr(photo_assets, "AirtableService", FailingAirtableService)
    monkeypatch.setattr(photo_assets, "fetch_data_github", lambda path: (True, {"photos": {}}, "ok"))

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
    assert result["selected_airtable_count"] == 0
    assert result["summary"]["candidate_count"] == 2


def test_migrate_single_place_reports_airtable_update_applied(monkeypatch):
    selected_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/" + ("a" * 64) + ".webp"

    class DummyPhotoAssetService:
        def process_place(self, place, place_data, config):
            return {
                "selected_airtable_urls": [selected_url],
                "summary": {"candidate_count": 1, "selected_airtable_count": 1},
                "assets": [],
                "failures": [],
            }

    class UpdatingAirtableService:
        def __init__(self, provider_type):
            self.provider_type = provider_type

        def update_place_record(self, **kwargs):
            return {
                "updated": True,
                "old_value": json.dumps(["https://example.com/old.jpg"]),
                "new_value": kwargs["update_value"],
            }

    monkeypatch.setattr(photo_assets, "PhotoAssetService", DummyPhotoAssetService)
    monkeypatch.setattr(photo_assets, "AirtableService", UpdatingAirtableService)
    monkeypatch.setattr(photo_assets, "fetch_data_github", lambda path: (True, {"photos": {}}, "ok"))

    result = photo_assets.migrate_single_place_photo_assets({
        "place": {
            "id": "rec-update-applied",
            "fields": {
                "Place": "Update Applied Place",
                "Google Maps Place Id": "ChIJ123",
                "Photos": json.dumps(["https://example.com/old.jpg"]),
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

    assert result["status"] == "updated"
    assert result["summary"]["github_data_file_save_attempted"] is False
    assert result["summary"]["github_data_file_saved"] is False
    assert result["summary"]["airtable_write_attempted"] is True
    assert result["summary"]["airtable_update_applied"] is True
    assert result["summary"]["airtable_update_skipped_no_change"] is False
    assert result["summary"]["airtable_update_failed"] is False


def test_migrate_single_place_reports_airtable_no_change(monkeypatch):
    selected_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/" + ("a" * 64) + ".webp"
    selected_json = json.dumps([selected_url])

    class DummyPhotoAssetService:
        def process_place(self, place, place_data, config):
            return {
                "selected_airtable_urls": [selected_url],
                "summary": {"candidate_count": 1, "selected_airtable_count": 1},
                "assets": [],
                "failures": [],
            }

    class NoChangeAirtableService:
        def __init__(self, provider_type):
            self.provider_type = provider_type

        def update_place_record(self, **kwargs):
            return {
                "updated": False,
                "old_value": selected_json,
                "new_value": kwargs["update_value"],
            }

    monkeypatch.setattr(photo_assets, "PhotoAssetService", DummyPhotoAssetService)
    monkeypatch.setattr(photo_assets, "AirtableService", NoChangeAirtableService)
    monkeypatch.setattr(photo_assets, "fetch_data_github", lambda path: (True, {"photos": {}}, "ok"))

    result = photo_assets.migrate_single_place_photo_assets({
        "place": {
            "id": "rec-no-change",
            "fields": {
                "Place": "No Change Place",
                "Google Maps Place Id": "ChIJ123",
                "Photos": selected_json,
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

    assert result["status"] == "updated"
    assert result["summary"]["github_data_file_save_attempted"] is False
    assert result["summary"]["github_data_file_saved"] is False
    assert result["summary"]["airtable_write_attempted"] is True
    assert result["summary"]["airtable_update_applied"] is False
    assert result["summary"]["airtable_update_skipped_no_change"] is True
    assert result["summary"]["airtable_update_failed"] is False


def test_aggregate_results_counts_write_diagnostics():
    totals = photo_assets._aggregate_results([
        {"status": "updated", "summary": {
            "github_data_file_save_attempted": True,
            "github_data_file_saved": True,
            "airtable_write_requested": True,
            "airtable_write_attempted": True,
            "airtable_update_applied": True,
            "blob_bytes": 100,
        }},
        {"status": "updated", "summary": {
            "github_data_file_save_attempted": True,
            "github_data_file_saved": True,
            "airtable_write_requested": True,
            "airtable_write_attempted": True,
            "airtable_update_skipped_no_change": True,
            "blob_bytes": 250,
        }},
        {"status": "error", "summary": {
            "github_data_file_save_attempted": True,
            "github_data_file_save_failed": True,
            "airtable_write_requested": True,
        }},
    ], dry_run=False)

    assert totals["github_data_file_save_attempts"] == 3
    assert totals["github_data_files_saved"] == 2
    assert totals["github_data_file_save_failures"] == 1
    assert totals["airtable_write_requested"] == 3
    assert totals["airtable_write_attempts"] == 2
    assert totals["airtable_updates_applied"] == 1
    assert totals["airtable_updates_skipped_no_change"] == 1
    assert totals["airtable_update_failures"] == 0
    assert totals["blob_bytes"] == 350


def test_photo_health_check_reports_counts(monkeypatch):
    monkeypatch.setattr(photo_assets, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(
        photo_assets,
        "list_blobs_in_container",
        lambda container, prefix="": [
            f"{prefix}aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.jpg"
        ] if container == photo_assets.PHOTOS_CONTAINER else [],
    )

    response = photo_assets.photo_health_check(
        DummyRequest({"city": "charlotte", "place_id": "ChIJ5YHD3oOhVogRAV83qWzHmgg"})
    )
    body = json.loads(response.get_body().decode("utf-8"))

    assert response.status_code == 200
    assert body["success"] is True
    assert body["data"]["airtable_photo_url_count"] == 1
    assert body["data"]["canonical_standard_url_count"] == 1
    assert body["data"]["legacy_airtable_url_count"] == 0
    assert body["data"]["non_azure_airtable_url_count"] == 0
    assert body["data"]["new_container_blob_count"] == 1