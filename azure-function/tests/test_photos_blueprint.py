import json

from blueprints import photos


class DummyAirtableService:
    def __init__(self, provider_type):
        self.provider_type = provider_type


class DummyProvider:
    def __init__(self, provider_photos=None):
        self._provider_photos = provider_photos or {"photo_urls": [], "raw_data": {}}

    def _is_valid_photo_url(self, url):
        return isinstance(url, str) and url.startswith("http")

    def _select_prioritized_photos(self, photos_data, max_photos=30):
        selected = []
        seen = set()
        for photo in photos_data:
            photo_url = photo.get("photo_url_big")
            if not photo_url or photo_url in seen:
                continue
            selected.append(photo_url)
            seen.add(photo_url)
            if len(selected) >= max_photos:
                break
        return selected

    def get_place_photos(self, place_id):
        return self._provider_photos


class DummyRequest:
    def __init__(self, params):
        self.params = params


class FakeOrchestrationContext:
    def __init__(self, input_data):
        self._input_data = input_data
        self.activity_calls = []

    def get_input(self):
        return self._input_data

    def call_activity(self, name, input_data):
        activity_call = {"name": name, "input": input_data}
        self.activity_calls.append(activity_call)
        return activity_call

    def task_all(self, tasks):
        return {"name": "task_all", "tasks": tasks}


def run_refresh_all_photos_orchestrator(context):
    return photos.refresh_all_photos_orchestrator._function._func.orchestrator_function(context)


def test_validate_refresh_all_photos_request_success_defaults():
    parsed, error_response = photos.validate_refresh_all_photos_request(
        DummyRequest({"provider_type": "outscraper"})
    )

    assert error_response is None
    assert parsed["provider_type"] == "outscraper"
    assert parsed["city"] == "charlotte"
    assert parsed["dry_run"] is True
    assert parsed["place_id"] == ""
    assert parsed["sequential_mode"] is False
    assert parsed["max_places"] is None
    assert parsed["photo_source_mode"] == "refresh_from_data_file_raw_data"


def test_validate_refresh_all_photos_request_accepts_place_id_filter():
    parsed, error_response = photos.validate_refresh_all_photos_request(
        DummyRequest({
            "provider_type": "outscraper",
            "place_id": " ChIJ123 ",
        })
    )

    assert error_response is None
    assert parsed["place_id"] == "ChIJ123"


def test_filter_places_for_photo_refresh_filters_by_place_id():
    places = [
        {"id": "rec123", "fields": {"Google Maps Place Id": "ChIJ123"}},
        {"id": "rec456", "fields": {"Google Maps Place Id": "ChIJ456"}},
    ]

    filtered = photos.filter_places_for_photo_refresh(places, {"place_id": "ChIJ456"})

    assert filtered == [places[1]]


def test_filter_places_for_photo_refresh_rejects_duplicate_place_id():
    places = [
        {"id": "rec123", "fields": {"Google Maps Place Id": "ChIJ123"}},
        {"id": "rec456", "fields": {"Google Maps Place Id": "ChIJ123"}},
    ]

    try:
        photos.filter_places_for_photo_refresh(places, {"place_id": "ChIJ123"})
    except ValueError as exc:
        assert "duplicate place_id found" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_filter_places_for_photo_refresh_rejects_missing_place_id():
    places = [
        {"id": "rec123", "fields": {"Google Maps Place Id": "ChIJ123"}},
        {"id": "rec456", "fields": {"Google Maps Place Id": "ChIJ456"}},
    ]

    try:
        photos.filter_places_for_photo_refresh(places, {"place_id": "ChIJ789"})
    except ValueError as exc:
        assert "place_id not found" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_refresh_all_photos_orchestrator_applies_place_id_before_parallel_fanout():
    places = [
        {"id": "rec123", "fields": {"Google Maps Place Id": "ChIJ123", "Place": "Wrong One"}},
        {"id": "rec456", "fields": {"Google Maps Place Id": "ChIJ456", "Place": "Target"}},
        {"id": "rec789", "fields": {"Google Maps Place Id": "ChIJ789", "Place": "Wrong Two"}},
    ]
    context = FakeOrchestrationContext({
        "provider_type": "outscraper",
        "city": "charlotte",
        "place_id": "ChIJ456",
        "dry_run": True,
        "sequential_mode": False,
        "photo_source_mode": "refresh_from_data_provider",
    })

    orchestrator = run_refresh_all_photos_orchestrator(context)
    get_all_call = next(orchestrator)

    assert get_all_call["name"] == "get_all_third_places"

    fanout_call = orchestrator.send(places)
    assert fanout_call["name"] == "task_all"
    assert len(fanout_call["tasks"]) == 1

    refresh_call = fanout_call["tasks"][0]
    assert refresh_call["name"] == "refresh_single_place_photos"
    assert refresh_call["input"]["place"] == places[1]
    assert refresh_call["input"]["config"]["place_id"] == "ChIJ456"

    try:
        orchestrator.send([{"status": "would_update", "message": "ok"}])
    except StopIteration as exc:
        result = exc.value
    else:
        raise AssertionError("Expected orchestrator to complete")

    refresh_calls = [
        call for call in context.activity_calls
        if call["name"] == "refresh_single_place_photos"
    ]
    assert len(refresh_calls) == 1
    assert refresh_calls[0]["input"]["place"]["id"] == "rec456"
    assert result["success"] is True
    assert result["data"]["total_places"] == 1
    assert result["data"]["updated"] == 1
    assert len(result["data"]["place_results"]) == 1


def test_refresh_all_photos_orchestrator_missing_place_id_does_not_fan_out():
    places = [
        {"id": "rec123", "fields": {"Google Maps Place Id": "ChIJ123", "Place": "Wrong One"}},
        {"id": "rec456", "fields": {"Google Maps Place Id": "ChIJ456", "Place": "Wrong Two"}},
    ]
    context = FakeOrchestrationContext({
        "provider_type": "outscraper",
        "city": "charlotte",
        "place_id": "ChIJ789",
        "dry_run": True,
        "sequential_mode": False,
        "photo_source_mode": "refresh_from_data_provider",
    })

    orchestrator = run_refresh_all_photos_orchestrator(context)
    get_all_call = next(orchestrator)

    assert get_all_call["name"] == "get_all_third_places"

    try:
        orchestrator.send(places)
    except StopIteration as exc:
        result = exc.value
    else:
        raise AssertionError("Expected orchestrator to stop after missing place_id")

    refresh_calls = [
        call for call in context.activity_calls
        if call["name"] == "refresh_single_place_photos"
    ]
    assert refresh_calls == []
    assert result["success"] is False
    assert result["error"] == "place_id not found: ChIJ789"


def test_validate_refresh_all_photos_request_invalid_photo_source_mode():
    parsed, error_response = photos.validate_refresh_all_photos_request(
        DummyRequest({
            "provider_type": "outscraper",
            "photo_source_mode": "bad_mode"
        })
    )

    assert parsed is None
    assert error_response.status_code == 400
    body = json.loads(error_response.get_body().decode("utf-8"))
    assert body["message"] == "Invalid photo_source_mode"


def test_validate_refresh_all_photos_request_invalid_max_places():
    parsed, error_response = photos.validate_refresh_all_photos_request(
        DummyRequest({
            "provider_type": "outscraper",
            "max_places": "not_an_int"
        })
    )

    assert parsed is None
    assert error_response.status_code == 400
    body = json.loads(error_response.get_body().decode("utf-8"))
    assert body["message"] == "Invalid max_places value"


def test_refresh_single_place_photos_invalid_mode_falls_back_to_raw_data_branch(monkeypatch):
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: DummyProvider()),
    )
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (
            True,
            {
                "photos": {
                    "photo_urls": [],
                    "raw_data": {
                        "photos_data": [
                            {
                                "photo_url_big": "https://example.com/raw-photo-1",
                                "photo_tags": ["vibe"],
                                "photo_date": "12/01/2024 10:00:00",
                            }
                        ]
                    },
                }
            },
            "ok",
        ),
    )

    activity_input = {
        "place": {
            "id": "rec-invalid-mode",
            "fields": {
                "Place": "Invalid Mode Place",
                "Google Maps Place Id": "ChIJ-invalid-mode",
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": True,
            "photo_source_mode": "bad_mode",
        },
    }

    result = photos.refresh_single_place_photos(activity_input)
    assert result["status"] == "would_update"
    assert result["photos_after"] == 1


def test_refresh_single_place_photos_from_data_provider_dry_run(monkeypatch):
    provider_result = {
        "photo_urls": [
            "https://lh5.googleusercontent.com/gps-cs-s/provider-photo-1",
            "https://lh5.googleusercontent.com/p/provider-photo-2"
        ],
        "raw_data": {"photos_data": []}
    }

    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: DummyProvider(provider_photos=provider_result)),
    )
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (True, {"photos": {"photo_urls": []}}, "ok"),
    )

    activity_input = {
        "place": {
            "id": "rec123",
            "fields": {
                "Place": "Test Place",
                "Google Maps Place Id": "ChIJ123",
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": True,
            "photo_source_mode": "refresh_from_data_provider",
        },
    }

    result = photos.refresh_single_place_photos(activity_input)

    assert result["status"] == "would_update"
    assert result["photos_before"] == 0
    assert result["photos_after"] == 2


def test_refresh_single_place_photos_from_raw_data_dry_run(monkeypatch):
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: DummyProvider()),
    )
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (
            True,
            {
                "photos": {
                    "photo_urls": [],
                    "raw_data": {
                        "photos_data": [
                            {
                                "photo_url_big": "https://lh5.googleusercontent.com/gps-cs-s/raw-photo-1",
                                "photo_tags": ["vibe"],
                                "photo_date": "12/01/2024 10:00:00",
                            },
                            {
                                "photo_url_big": "https://lh5.googleusercontent.com/p/raw-photo-2",
                                "photo_tags": ["front"],
                                "photo_date": "11/01/2024 10:00:00",
                            },
                        ]
                    },
                }
            },
            "ok",
        ),
    )

    activity_input = {
        "place": {
            "id": "rec456",
            "fields": {
                "Place": "Raw Data Place",
                "Google Maps Place Id": "ChIJ456",
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": True,
            "photo_source_mode": "refresh_from_data_file_raw_data",
        },
    }

    result = photos.refresh_single_place_photos(activity_input)

    assert result["status"] == "would_update"
    assert result["photos_before"] == 0
    assert result["photos_after"] == 2


def test_refresh_single_place_photos_falls_back_to_airtable_photos_without_raw_data(monkeypatch):
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: DummyProvider()),
    )
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (True, {"photos": {"photo_urls": []}}, "ok"),
    )

    activity_input = {
        "place": {
            "id": "rec-airtable-photos",
            "fields": {
                "Place": "Airtable Photos Place",
                "Google Maps Place Id": "ChIJ-airtable-photos",
                "Photos": json.dumps(["https://example.com/airtable-photo.jpg"]),
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": True,
            "photo_source_mode": "refresh_from_data_file_raw_data",
        },
    }

    result = photos.refresh_single_place_photos(activity_input)

    assert result["status"] == "would_update"
    assert result["photos_before"] == 1
    assert result["cached_photo_urls_before"] == 0
    assert result["photos_after"] == 1


def test_refresh_single_place_photos_counts_existing_airtable_azure_photos_before(monkeypatch):
    existing_azure_urls = [
        "https://thirdplacesdata.blob.core.windows.net/place-photos/charlotte/ChIJ-azure/"
        + ("a" * 64)
        + ".jpg",
        "https://thirdplacesdata.blob.core.windows.net/place-photos/charlotte/ChIJ-azure/"
        + ("b" * 64)
        + ".webp",
    ]
    provider_result = {
        "photo_urls": ["https://lh5.googleusercontent.com/gps-cs-s/provider-photo-1"],
        "raw_data": {"photos_data": []},
    }

    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: DummyProvider(provider_photos=provider_result)),
    )
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (True, {"photos": {"photo_urls": []}}, "ok"),
    )

    activity_input = {
        "place": {
            "id": "rec-airtable-azure",
            "fields": {
                "Place": "Already Migrated Place",
                "Google Maps Place Id": "ChIJ-azure",
                "Photos": json.dumps(existing_azure_urls),
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": True,
            "photo_source_mode": "refresh_from_data_provider",
        },
    }

    result = photos.refresh_single_place_photos(activity_input)

    assert result["status"] == "would_update"
    assert result["photos_before"] == 2
    assert result["cached_photo_urls_before"] == 0
    assert result["photos_after"] == 3


def test_refresh_single_place_photos_from_cached_photo_urls_dry_run(monkeypatch):
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: DummyProvider()),
    )
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (
            True,
            {
                "photos": {
                    "photo_urls": [
                        "https://lh5.googleusercontent.com/gps-cs-s/cached-1",
                        "https://lh5.googleusercontent.com/p/cached-2",
                    ]
                }
            },
            "ok",
        ),
    )

    activity_input = {
        "place": {
            "id": "rec789",
            "fields": {
                "Place": "Cached Photos Place",
                "Google Maps Place Id": "ChIJ789",
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": True,
            "photo_source_mode": "refresh_from_data_file_photo_urls",
        },
    }

    result = photos.refresh_single_place_photos(activity_input)

    assert result["status"] == "would_update"
    assert result["photos_before"] == 0
    assert result["cached_photo_urls_before"] == 2
    assert result["photos_after"] == 2


def test_refresh_single_place_photos_cached_photo_urls_missing_is_skipped(monkeypatch):
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: DummyProvider()),
    )
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (True, {"photos": {"photo_urls": []}}, "ok"),
    )

    activity_input = {
        "place": {
            "id": "rec000",
            "fields": {
                "Place": "No Cached Photos",
                "Google Maps Place Id": "ChIJ000",
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": True,
            "photo_source_mode": "refresh_from_data_file_photo_urls",
        },
    }

    result = photos.refresh_single_place_photos(activity_input)

    assert result["status"] == "skipped"
    assert result["message"] == "No cached photo_urls found"


def test_refresh_single_place_photos_non_dry_run_preserves_curator_display_and_source_cache(monkeypatch):
    provider_urls = [
        "https://lh5.googleusercontent.com/gps-cs-s/provider-photo-1",
        "https://lh5.googleusercontent.com/p/provider-photo-2",
    ]
    curator_url = "https://thirdplacesdata.blob.core.windows.net/curator-photos/rec123/att1_photo.jpg"
    provider_azure_url = (
        "https://thirdplacesdata.blob.core.windows.net/place-photos/charlotte/ChIJ123/"
        + ("a" * 64)
        + ".jpg"
    )
    saved_payload = {}
    airtable_updates = []

    class CaptureAirtableService:
        def __init__(self, provider_type):
            self.provider_type = provider_type

        def update_place_record(self, record_id, field_to_update, update_value, overwrite):
            airtable_updates.append({
                "record_id": record_id,
                "field_to_update": field_to_update,
                "update_value": update_value,
                "overwrite": overwrite,
            })
            return {"updated": True}

    class DummyPhotoAssetService:
        def process_place(self, place, place_data, config):
            assert place_data["photos"]["photo_urls"] == provider_urls
            return {
                "summary": {"selected_airtable_count": 2},
                "failures": [],
                "assets": [],
                "selected_source_urls": provider_urls,
                "selected_airtable_urls": [curator_url, provider_azure_url],
                "place_data": place_data,
            }

    def save_data(updated_json, path):
        saved_payload["path"] = path
        saved_payload["json"] = json.loads(updated_json)
        return True, "ok"

    monkeypatch.setattr(photos, "AirtableService", CaptureAirtableService)
    monkeypatch.setattr(photos, "PhotoAssetService", DummyPhotoAssetService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: DummyProvider(provider_photos={"photo_urls": provider_urls, "raw_data": {"photos_data": []}})),
    )
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (True, {"photos": {"photo_urls": ["https://old.example/cache.jpg"]}}, "ok"),
    )
    monkeypatch.setattr(photos, "save_data_github", save_data)

    activity_input = {
        "place": {
            "id": "rec123",
            "fields": {
                "Place": "Test Place",
                "Google Maps Place Id": "ChIJ123",
                "Photos": json.dumps([curator_url]),
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": False,
            "upload": True,
            "write_airtable": True,
            "photo_source_mode": "refresh_from_data_provider",
        },
    }

    result = photos.refresh_single_place_photos(activity_input)

    assert result["status"] == "updated"
    assert result["photos_after"] == 2
    assert saved_payload["path"] == "data/places/charlotte/ChIJ123.json"
    assert saved_payload["json"]["photos"]["photo_urls"] == provider_urls
    assert airtable_updates == [{
        "record_id": "rec123",
        "field_to_update": "Photos",
        "update_value": json.dumps([curator_url, provider_azure_url]),
        "overwrite": True,
    }]
