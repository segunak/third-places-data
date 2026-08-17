import asyncio
import json

import pytest

from blueprints import photos
from constants import MAX_SELECTED_PHOTOS, PHOTO_REFRESH_BATCH_SIZE


class DummyAirtableService:
    def __init__(self, provider_type, initialize_provider=True):
        self.provider_type = provider_type


class DummyProvider:
    def __init__(self, provider_photos=None):
        self._provider_photos = provider_photos or {"photo_urls": [], "raw_data": {}}
        self.get_place_photos_calls = []

    def _is_valid_photo_url(self, url):
        return isinstance(url, str) and url.startswith("http")

    def _select_prioritized_photos(self, photos_data, max_photos=MAX_SELECTED_PHOTOS):
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

    def select_photos_from_raw_data(self, raw_data, max_photos=MAX_SELECTED_PHOTOS):
        records = raw_data.get("photos_data", []) if isinstance(raw_data, dict) else raw_data
        records = [record for record in records if isinstance(record, dict)] if isinstance(records, list) else []
        selected = self._select_prioritized_photos(records, max_photos=max_photos)
        return {
            "raw_photo_count": len(records),
            "valid_photo_count": len({record.get("photo_url_big") for record in records if self._is_valid_photo_url(record.get("photo_url_big"))}),
            "photo_urls": selected,
        }

    def get_place_photos(self, place_id):
        self.get_place_photos_calls.append(place_id)
        return self._provider_photos


class DummyRequest:
    def __init__(self, params):
        self.params = params


class DummyDurableClient:
    def __init__(self):
        self.started = []

    async def start_new(self, orchestrator_name, client_input=None):
        self.started.append({"name": orchestrator_name, "input": client_input})
        return "instance-123"

    def create_check_status_response(self, req, instance_id):
        return {"instance_id": instance_id}


def _photo_manifest(display_url, thumbnail_url=None):
    return {"display": display_url, "thumbnail": thumbnail_url or display_url.replace("/display/", "/thumbnail/")}


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


def test_validate_refresh_all_photos_request_success_required_controls():
    parsed, error_response = photos.validate_refresh_all_photos_request(
        DummyRequest({
            "provider_type": "outscraper",
            "view": "Production",
            "refresh_below": "30",
        })
    )

    assert error_response is None
    assert parsed["provider_type"] == "outscraper"
    assert parsed["city"] == "charlotte"
    assert parsed["dry_run"] is True
    assert parsed["place_id"] == ""
    assert parsed["max_places"] is None
    assert parsed["view"] == "Production"
    assert parsed["refresh_below"] == 30
    assert "batch_size" not in parsed
    assert parsed["force_provider"] is False


def test_validate_refresh_all_photos_request_requires_refresh_below():
    parsed, error_response = photos.validate_refresh_all_photos_request(
        DummyRequest({"provider_type": "outscraper", "view": "Production"})
    )

    assert parsed is None
    assert error_response.status_code == 400
    assert json.loads(error_response.get_body().decode("utf-8"))["message"] == "Missing refresh_below value"


def test_validate_refresh_all_photos_request_does_not_expose_batch_size():
    parsed, error_response = photos.validate_refresh_all_photos_request(
        DummyRequest({
            "provider_type": "outscraper",
            "view": "Production",
            "refresh_below": "30",
            "batch_size": "99",
        })
    )

    assert error_response is None
    assert "batch_size" not in parsed


def test_validate_refresh_all_photos_request_requires_view():
    parsed, error_response = photos.validate_refresh_all_photos_request(
        DummyRequest({
            "provider_type": "outscraper",
            "refresh_below": "30",
        })
    )

    assert parsed is None
    assert error_response.status_code == 400
    assert json.loads(error_response.get_body().decode("utf-8"))["message"] == "Missing view value"


def test_validate_refresh_all_photos_request_accepts_bulk_controls():
    parsed, error_response = photos.validate_refresh_all_photos_request(
        DummyRequest({
            "provider_type": "outscraper",
            "view": "Insufficient Photos",
            "refresh_below": "25",
            "max_places": "12",
        })
    )

    assert error_response is None
    assert parsed["view"] == "Insufficient Photos"
    assert parsed["refresh_below"] == 25
    assert "batch_size" not in parsed
    assert parsed["max_places"] == 12


def test_validate_refresh_all_photos_request_accepts_maximum_refresh_below():
    parsed, error_response = photos.validate_refresh_all_photos_request(
        DummyRequest({
            "provider_type": "outscraper",
            "view": "Production",
            "refresh_below": str(MAX_SELECTED_PHOTOS),
        })
    )

    assert error_response is None
    assert parsed["refresh_below"] == MAX_SELECTED_PHOTOS


def test_validate_refresh_all_photos_request_rejects_refresh_below_above_maximum():
    unsafe_value = MAX_SELECTED_PHOTOS + 1
    parsed, error_response = photos.validate_refresh_all_photos_request(
        DummyRequest({
            "provider_type": "outscraper",
            "view": "Production",
            "refresh_below": str(unsafe_value),
        })
    )

    assert parsed is None
    assert error_response.status_code == 400
    body = json.loads(error_response.get_body().decode("utf-8"))
    assert body["message"] == "refresh_below exceeds the safe maximum"
    assert f"MAX_SELECTED_PHOTOS ({MAX_SELECTED_PHOTOS})" in body["error"]
    assert f"received {unsafe_value}" in body["error"]
    assert "could refresh every eligible place" in body["error"]


def test_plan_places_for_photo_refresh_reports_every_record():
    curator = _photo_manifest(
        "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ-low/display/curator-att1.webp"
    )
    places = [
        {
            "id": "rec-sufficient",
            "fields": {
                "Place": "Sufficient",
                "Google Maps Place Id": "ChIJ-sufficient",
                "Photos": json.dumps([_photo_manifest(f"https://example.com/display/{index}.webp", f"https://example.com/thumbnail/{index}.webp") for index in range(30)]),
            },
        },
        {
            "id": "rec-low",
            "fields": {
                "Place": "Low",
                "Google Maps Place Id": "ChIJ-low",
                "Photos": json.dumps([curator]),
            },
        },
        {"id": "rec-missing", "fields": {"Place": "Missing ID", "Photos": "[]"}},
        {
            "id": "rec-limited",
            "fields": {
                "Place": "Limited",
                "Google Maps Place Id": "ChIJ-limited",
                "Photos": "[]",
            },
        },
    ]

    selected, skipped = photos.plan_places_for_photo_refresh(
        places,
        {"refresh_below": 30, "max_places": 1},
    )

    assert [item["place"]["id"] for item in selected] == ["rec-limited"]
    assert {result["record_id"]: result["status"] for result in skipped} == {
        "rec-sufficient": "skipped_sufficient",
        "rec-low": "skipped_max_places",
        "rec-missing": "skipped_missing_place_id",
    }
    assert len(selected) + len(skipped) == len(places)


def test_validate_refresh_all_photos_request_accepts_place_id_filter():
    parsed, error_response = photos.validate_refresh_all_photos_request(
        DummyRequest({
            "provider_type": "outscraper",
            "place_id": " ChIJ123 ",
        })
    )

    assert error_response is None
    assert parsed["place_id"] == "ChIJ123"
    assert parsed["force_provider"] is True
    assert "view" not in parsed
    assert "refresh_below" not in parsed
    assert "batch_size" not in parsed


def test_refresh_all_photos_starter_passes_required_bulk_controls():
    client = DummyDurableClient()

    response = asyncio.run(photos.refresh_all_photos._function._func.__wrapped__(
        DummyRequest({
            "provider_type": "outscraper",
            "view": "Production",
            "refresh_below": "30",
        }),
        client,
    ))

    assert response == {"instance_id": "instance-123"}
    orchestration_input = client.started[0]["input"]
    assert orchestration_input["view"] == "Production"
    assert orchestration_input["refresh_below"] == 30
    assert "batch_size" not in orchestration_input
    assert orchestration_input["place_id"] == ""


def test_refresh_all_photos_starter_omits_bulk_controls_for_single_place():
    client = DummyDurableClient()

    response = asyncio.run(photos.refresh_all_photos._function._func.__wrapped__(
        DummyRequest({
            "provider_type": "outscraper",
            "place_id": "ChIJ123",
            "dry_run": "false",
        }),
        client,
    ))

    assert response == {"instance_id": "instance-123"}
    orchestration_input = client.started[0]["input"]
    assert orchestration_input["place_id"] == "ChIJ123"
    assert orchestration_input["force_provider"] is True
    assert "view" not in orchestration_input
    assert "refresh_below" not in orchestration_input
    assert "batch_size" not in orchestration_input


def test_refresh_all_photos_orchestrator_looks_up_place_id_and_forces_provider():
    place = {
        "id": "rec456",
        "fields": {"Google Maps Place Id": "ChIJ456", "Place": "Target"},
    }
    context = FakeOrchestrationContext({
        "provider_type": "outscraper",
        "city": "charlotte",
        "place_id": "ChIJ456",
        "dry_run": True,
    })

    orchestrator = run_refresh_all_photos_orchestrator(context)
    find_call = next(orchestrator)

    assert find_call == {
        "name": "find_place_by_id",
        "input": {"place_id": "ChIJ456", "provider_type": "outscraper"},
    }

    refresh_call = orchestrator.send(place)
    assert refresh_call["name"] == "refresh_single_place_photos"
    assert refresh_call["input"]["place"] == place
    assert refresh_call["input"]["config"]["place_id"] == "ChIJ456"
    assert refresh_call["input"]["config"]["force_provider"] is True
    assert "view" not in refresh_call["input"]["config"]
    assert "refresh_below" not in refresh_call["input"]["config"]
    assert "batch_size" not in refresh_call["input"]["config"]

    try:
        orchestrator.send({"status": "would_fetch_provider", "message": "ok", "provider_called": False})
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
    assert result["data"]["updated"] == 0
    assert result["data"]["would_fetch_provider"] == 1
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
    })

    orchestrator = run_refresh_all_photos_orchestrator(context)
    find_call = next(orchestrator)

    assert find_call["name"] == "find_place_by_id"

    try:
        orchestrator.send(None)
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


def test_refresh_all_photos_orchestrator_batches_and_reports_every_record():
    sufficient_photos = [
        _photo_manifest(f"https://example.com/display/{index}.webp", f"https://example.com/thumbnail/{index}.webp")
        for index in range(30)
    ]
    places = [
        {
            "id": f"rec-{letter}",
            "fields": {
                "Place": letter.upper(),
                "Google Maps Place Id": f"ChIJ-{letter}",
                "Photos": "[]",
            },
        }
        for letter in ("a", "b", "c", "d", "e", "f")
    ] + [
        {
            "id": "rec-sufficient",
            "fields": {
                "Place": "Sufficient",
                "Google Maps Place Id": "ChIJ-sufficient",
                "Photos": json.dumps(sufficient_photos),
            },
        },
        {"id": "rec-missing", "fields": {"Place": "Missing", "Photos": "[]"}},
    ]
    context = FakeOrchestrationContext({
        "provider_type": "outscraper",
        "city": "charlotte",
        "view": "Production",
        "dry_run": True,
        "refresh_below": 30,
    })

    orchestrator = run_refresh_all_photos_orchestrator(context)
    assert next(orchestrator)["name"] == "get_all_third_places"

    first_batch = orchestrator.send(places)
    assert first_batch["name"] == "task_all"
    assert len(first_batch["tasks"]) == PHOTO_REFRESH_BATCH_SIZE

    second_batch = orchestrator.send([
        {"record_id": f"rec-{letter}", "status": "would_use_cache", "provider_called": False}
        for letter in ("a", "b", "c", "d", "e")
    ])
    assert second_batch["name"] == "task_all"
    assert len(second_batch["tasks"]) == 1

    try:
        orchestrator.send([
            {"record_id": "rec-f", "status": "would_fetch_provider", "provider_called": False},
        ])
    except StopIteration as exc:
        result = exc.value
    else:
        raise AssertionError("Expected orchestrator to complete")

    assert result["success"] is True
    assert result["data"]["total_places"] == 8
    assert result["data"]["selected_places"] == 6
    assert result["data"]["skipped"] == 2
    assert len(result["data"]["place_results"]) == 8
    assert {item["record_id"] for item in result["data"]["place_results"]} == {
        "rec-a", "rec-b", "rec-c", "rec-d", "rec-e", "rec-f", "rec-sufficient", "rec-missing"
    }


def test_validate_refresh_all_photos_request_invalid_photo_source_mode():
    parsed, error_response = photos.validate_refresh_all_photos_request(
        DummyRequest({
            "provider_type": "outscraper",
            "view": "Production",
            "refresh_below": "30",
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
            "view": "Production",
            "refresh_below": "30",
            "max_places": "not_an_int"
        })
    )

    assert parsed is None
    assert error_response.status_code == 400
    body = json.loads(error_response.get_body().decode("utf-8"))
    assert body["message"] == "Invalid max_places value"


def test_refresh_single_place_photos_invalid_internal_mode_requires_provider(monkeypatch):
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
    assert result["status"] == "would_fetch_provider"


def test_refresh_single_place_photos_from_data_provider_dry_run(monkeypatch):
    provider_result = {
        "photo_urls": [
            "https://lh5.googleusercontent.com/gps-cs-s/provider-photo-1",
            "https://lh5.googleusercontent.com/p/provider-photo-2"
        ],
        "raw_data": {"photos_data": []}
    }

    provider = DummyProvider(provider_photos=provider_result)
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: provider),
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

    assert result["status"] == "would_fetch_provider"
    assert result["photos_before"] == 0
    assert result["provider_called"] is False
    assert provider.get_place_photos_calls == []


def test_refresh_single_place_photos_dry_run_does_not_initialize_clients(monkeypatch):
    def fail_if_called(*args, **kwargs):
        raise AssertionError("Dry run must not initialize live clients")

    monkeypatch.setattr(photos, "AirtableService", fail_if_called)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(fail_if_called),
    )
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (True, {"photos": {"photo_urls": [], "raw_data": {}}}, "ok"),
    )

    result = photos.refresh_single_place_photos({
        "place": {
            "id": "rec-dry-run",
            "fields": {
                "Place": "Dry Run Place",
                "Google Maps Place Id": "ChIJ-dry-run",
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": True,
        },
    })

    assert result["status"] == "would_fetch_provider"
    assert result["provider_called"] is False


def test_refresh_single_place_photos_cache_first_dry_run_uses_sufficient_raw_cache(monkeypatch):
    provider = DummyProvider()
    raw_records = [
        {
            "photo_url_big": f"https://example.com/photo-{index}.jpg",
            "photo_tags": ["vibe"],
            "photo_date": "12/01/2025 10:00:00",
        }
        for index in range(MAX_SELECTED_PHOTOS)
    ]
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(photos.PlaceDataProviderFactory, "get_provider", staticmethod(lambda provider_type: provider))
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (True, {"photos": {"raw_data": {"photos_data": raw_records}}}, "ok"),
    )

    result = photos.refresh_single_place_photos({
        "place": {
            "id": "rec-cache",
            "fields": {"Place": "Cache Place", "Google Maps Place Id": "ChIJ-cache", "Photos": "[]"},
        },
        "config": {"provider_type": "outscraper", "city": "charlotte", "dry_run": True},
    })

    assert result["status"] == "would_use_cache"
    assert result["selection_source"] == "cache"
    assert result["cached_raw_photo_count"] == MAX_SELECTED_PHOTOS
    assert result["cached_selected_photo_count"] == MAX_SELECTED_PHOTOS
    assert result["provider_called"] is False
    assert provider.get_place_photos_calls == []


def test_refresh_single_place_photos_cache_first_uses_available_google_cache(monkeypatch):
    provider = DummyProvider()
    raw_records = [
        {"photo_url_big": f"https://example.com/google-photo-{index}.jpg"}
        for index in range(10)
    ]
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(photos.PlaceDataProviderFactory, "get_provider", staticmethod(lambda provider_type: provider))
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (True, {
            "photos_provider_type": "google",
            "photos": {"raw_data": {"photos_data": raw_records}},
        }, "ok"),
    )

    result = photos.refresh_single_place_photos({
        "place": {
            "id": "rec-google-cache",
            "fields": {"Place": "Google Cache", "Google Maps Place Id": "ChIJ-google-cache"},
        },
        "config": {"provider_type": "google", "city": "charlotte", "dry_run": True},
    })

    assert result["status"] == "would_use_cache"
    assert result["cached_selected_photo_count"] == 10
    assert provider.get_place_photos_calls == []


def test_refresh_single_place_photos_cache_first_dry_run_reports_provider_fetch(monkeypatch):
    provider = DummyProvider()
    raw_records = [
        {"photo_url_big": f"https://example.com/photo-{index}.jpg", "photo_tags": ["vibe"]}
        for index in range(MAX_SELECTED_PHOTOS - 1)
    ]
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(photos.PlaceDataProviderFactory, "get_provider", staticmethod(lambda provider_type: provider))
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (True, {"photos": {"raw_data": {"photos_data": raw_records}}}, "ok"),
    )

    result = photos.refresh_single_place_photos({
        "place": {
            "id": "rec-provider",
            "fields": {"Place": "Provider Place", "Google Maps Place Id": "ChIJ-provider", "Photos": "[]"},
        },
        "config": {"provider_type": "outscraper", "city": "charlotte", "dry_run": True},
    })

    assert result["status"] == "would_fetch_provider"
    assert result["selection_source"] == "provider"
    assert result["cached_selected_photo_count"] == MAX_SELECTED_PHOTOS - 1
    assert result["provider_called"] is False
    assert provider.get_place_photos_calls == []


def test_refresh_single_place_photos_missing_data_file_is_cache_miss(monkeypatch):
    provider = DummyProvider()
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: provider),
    )
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (False, None, f"File {path} not found in repository"),
    )

    result = photos.refresh_single_place_photos({
        "place": {
            "id": "rec-missing-file",
            "fields": {
                "Place": "Missing Data File",
                "Google Maps Place Id": "ChIJ-missing-file",
                "Photos": "[]",
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": True,
        },
    })

    assert result["status"] == "would_fetch_provider"
    assert result["selection_source"] == "provider"
    assert result["data_file_status"] == "missing"
    assert provider.get_place_photos_calls == []


def test_refresh_single_place_photos_live_run_creates_missing_data_file(monkeypatch):
    provider_url = "https://example.com/provider.jpg"
    provider = DummyProvider({
        "photo_urls": [provider_url],
        "raw_data": {
            "photos_data": [{"photo_url_big": provider_url}],
        },
    })
    saved_payload = {}

    class SuccessfulPhotoAssetService:
        def process_place(self, place, place_data, config):
            manifest = _photo_manifest(
                "https://thirdplacesdata.blob.core.windows.net/photos/"
                "ChIJ-missing-file/display/" + ("a" * 64) + ".webp"
            )
            return {
                "summary": {"selected_airtable_count": 1},
                "failures": [],
                "assets": [],
                "selected_airtable_photos": [manifest],
                "selected_airtable_urls": [manifest["display"]],
            }

    def save_data(content, path):
        saved_payload["path"] = path
        saved_payload["content"] = json.loads(content)
        return True, "created"

    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(photos, "PhotoAssetService", SuccessfulPhotoAssetService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: provider),
    )
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (False, None, f"File {path} not found in repository"),
    )
    monkeypatch.setattr(photos, "save_data_github", save_data)

    result = photos.refresh_single_place_photos({
        "place": {
            "id": "rec-missing-file",
            "fields": {
                "Place": "Missing Data File",
                "Google Maps Place Id": "ChIJ-missing-file",
                "Photos": "[]",
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": False,
            "upload": False,
            "write_airtable": False,
        },
    })

    assert result["status"] == "updated_data_file"
    assert result["data_file_status"] == "updated"
    assert result["provider_called"] is True
    assert provider.get_place_photos_calls == ["ChIJ-missing-file"]
    assert saved_payload["path"] == "data/places/charlotte/ChIJ-missing-file.json"
    assert saved_payload["content"]["photos"]["photo_urls"] == [provider_url]


def test_refresh_single_place_photos_data_file_fetch_error_still_fails(monkeypatch):
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: DummyProvider()),
    )
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (False, None, "Network error while fetching from GitHub: timeout"),
    )

    result = photos.refresh_single_place_photos({
        "place": {
            "id": "rec-fetch-error",
            "fields": {
                "Place": "Fetch Error",
                "Google Maps Place Id": "ChIJ-fetch-error",
                "Photos": "[]",
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": True,
        },
    })

    assert result["status"] == "error"
    assert result["message"] == (
        "Failed to read data file: Network error while fetching from GitHub: timeout"
    )


def test_refresh_single_place_photos_reports_provider_fetch_failure(monkeypatch):
    provider = DummyProvider({
        "photo_urls": [],
        "raw_data": {},
        "error": "provider unavailable",
    })
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(photos.PlaceDataProviderFactory, "get_provider", staticmethod(lambda provider_type: provider))
    monkeypatch.setattr(photos, "fetch_data_github", lambda path: (True, {"photos": {}}, "ok"))
    monkeypatch.setattr(
        photos,
        "save_data_github",
        lambda content, path: (_ for _ in ()).throw(AssertionError("Provider failures must not be saved")),
    )

    result = photos.refresh_single_place_photos({
        "place": {
            "id": "rec-provider-error",
            "fields": {"Place": "Provider Error", "Google Maps Place Id": "ChIJ-provider-error"},
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": False,
            "force_provider": True,
        },
    })

    assert result["status"] == "failed_provider_fetch"
    assert result["provider_called"] is True
    assert result["message"] == "outscraper photo fetch failed: provider unavailable"


def test_refresh_single_place_photos_cache_uses_selectable_count_not_raw_count(monkeypatch):
    provider = DummyProvider()
    raw_records = [
        {"photo_url_big": f"https://example.com/photo-{index % 20}.jpg", "photo_tags": ["vibe"]}
        for index in range(MAX_SELECTED_PHOTOS + 10)
    ]
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(photos.PlaceDataProviderFactory, "get_provider", staticmethod(lambda provider_type: provider))
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (True, {"photos": {"raw_data": {"photos_data": raw_records}}}, "ok"),
    )

    result = photos.refresh_single_place_photos({
        "place": {
            "id": "rec-duplicates",
            "fields": {"Place": "Duplicates", "Google Maps Place Id": "ChIJ-duplicates", "Photos": "[]"},
        },
        "config": {"provider_type": "outscraper", "city": "charlotte", "dry_run": True},
    })

    assert result["cached_raw_photo_count"] == MAX_SELECTED_PHOTOS + 10
    assert result["cached_selected_photo_count"] == 20
    assert result["status"] == "would_fetch_provider"


def test_refresh_single_place_photos_selected_urls_without_raw_data_are_cache_miss(monkeypatch):
    provider = DummyProvider()
    selected_urls = [f"https://example.com/photo-{index}.jpg" for index in range(MAX_SELECTED_PHOTOS)]
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(photos.PlaceDataProviderFactory, "get_provider", staticmethod(lambda provider_type: provider))
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (True, {"photos": {"photo_urls": selected_urls}}, "ok"),
    )

    result = photos.refresh_single_place_photos({
        "place": {
            "id": "rec-selected-only",
            "fields": {"Place": "Selected Only", "Google Maps Place Id": "ChIJ-selected-only", "Photos": "[]"},
        },
        "config": {"provider_type": "outscraper", "city": "charlotte", "dry_run": True},
    })

    assert result["cached_photo_urls_before"] == MAX_SELECTED_PHOTOS
    assert result["cached_raw_photo_count"] == 0
    assert result["cached_selected_photo_count"] == 0
    assert result["status"] == "would_fetch_provider"


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

    assert result["status"] == "would_use_cache"
    assert result["photos_before"] == 0
    assert result["photos_after"] == 2


def test_refresh_single_place_photos_force_overrides_cached_source_mode(monkeypatch):
    provider = DummyProvider()
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: provider),
    )
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (True, {
            "photos": {
                "raw_data": {
                    "photos_data": [
                        {"photo_url_big": "https://example.com/cached.jpg"},
                    ],
                },
            },
        }, "ok"),
    )

    result = photos.refresh_single_place_photos({
        "place": {
            "id": "rec-force",
            "fields": {
                "Place": "Forced Place",
                "Google Maps Place Id": "ChIJ-force",
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": True,
            "force_provider": True,
            "photo_source_mode": "refresh_from_data_file_raw_data",
        },
    })

    assert result["status"] == "would_fetch_provider"
    assert result["selection_source"] == "provider"
    assert provider.get_place_photos_calls == []


def test_refresh_single_place_photos_raw_data_mode_does_not_use_airtable_as_provider_source(monkeypatch):
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
                "Photos": json.dumps([_photo_manifest("https://example.com/airtable-photo.jpg", "https://example.com/airtable-photo-thumb.jpg")]),
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

    assert result["status"] == "skipped_no_photos"
    assert result["photos_before"] == 1
    assert result["cached_photo_urls_before"] == 0
    assert result["photos_after"] == 0


def test_refresh_single_place_photos_provider_dry_run_leaves_existing_manifests_unchanged(monkeypatch):
    existing_azure_photos = [
        _photo_manifest("https://thirdplacesdata.blob.core.windows.net/photos/ChIJ-azure/display/"
        + ("a" * 64)
        + ".webp"),
        _photo_manifest("https://thirdplacesdata.blob.core.windows.net/photos/ChIJ-azure/display/"
        + ("b" * 64)
        + ".webp"),
    ]
    provider_result = {
        "photo_urls": ["https://lh5.googleusercontent.com/gps-cs-s/provider-photo-1"],
        "raw_data": {"photos_data": []},
    }

    provider = DummyProvider(provider_photos=provider_result)
    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: provider),
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
                "Photos": json.dumps(existing_azure_photos),
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

    assert result["status"] == "would_fetch_provider"
    assert result["photos_before"] == 2
    assert result["cached_photo_urls_before"] == 0
    assert result["photos_after"] == 0
    assert provider.get_place_photos_calls == []


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

    assert result["status"] == "would_use_cache"
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

    assert result["status"] == "skipped_no_photos"
    assert result["message"] == "No selectable cached photos."


def test_refresh_single_place_photos_fails_when_asset_result_has_urls_without_manifests(monkeypatch):
    provider_urls = ["https://lh5.googleusercontent.com/gps-cs-s/provider-photo-1"]
    provider_azure_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/" + ("a" * 64) + ".webp"

    class DummyPhotoAssetService:
        def process_place(self, place, place_data, config):
            return {
                "summary": {"selected_airtable_count": 1},
                "failures": [],
                "assets": [],
                "selected_source_urls": provider_urls,
                "selected_airtable_urls": [provider_azure_url],
            }

    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(photos, "PhotoAssetService", DummyPhotoAssetService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: DummyProvider(provider_photos={"photo_urls": provider_urls, "raw_data": {"photos_data": []}})),
    )
    monkeypatch.setattr(photos, "fetch_data_github", lambda path: (True, {"photos": {"photo_urls": []}}, "ok"))
    monkeypatch.setattr(photos, "save_data_github", lambda content, path: (True, "saved"))

    result = photos.refresh_single_place_photos({
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
            "dry_run": False,
            "upload": True,
            "write_airtable": True,
            "photo_source_mode": "refresh_from_data_provider",
        },
    })

    assert result["status"] == "error"
    assert "without thumbnail manifests" in result["message"]


def test_refresh_single_place_photos_reports_complete_publish_failure(monkeypatch):
    provider_url = "https://example.com/provider.jpg"

    class FailedPhotoAssetService:
        def process_place(self, place, place_data, config):
            return {
                "summary": {"selected_airtable_count": 0, "failed_upload_count": 1},
                "failures": [{"source_url": provider_url, "error": "download failed"}],
                "assets": [],
                "selected_airtable_photos": [],
                "selected_airtable_urls": [],
            }

    monkeypatch.setattr(photos, "AirtableService", DummyAirtableService)
    monkeypatch.setattr(photos, "PhotoAssetService", FailedPhotoAssetService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: DummyProvider({
            "photo_urls": [provider_url],
            "raw_data": {"photos_data": [{"photo_url_big": provider_url}]},
        })),
    )
    monkeypatch.setattr(photos, "fetch_data_github", lambda path: (True, {"photos": {}}, "ok"))
    monkeypatch.setattr(photos, "save_data_github", lambda content, path: (True, "saved"))

    result = photos.refresh_single_place_photos({
        "place": {
            "id": "rec-publish-failure",
            "fields": {"Place": "Publish Failure", "Google Maps Place Id": "ChIJ-publish-failure"},
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": False,
            "force_provider": True,
        },
    })

    assert result["status"] == "failed_photo_publish"
    assert result["data_file_status"] == "updated"
    assert result["failed_uploads"] == 1


@pytest.mark.parametrize(
    ("force_provider", "expected_status", "expected_airtable_updates"),
    [
        (False, "failed_provider_regression", 0),
        (True, "updated_from_provider", 1),
    ],
)
def test_refresh_single_place_photos_allows_count_reduction_only_when_forced(
    monkeypatch,
    force_provider,
    expected_status,
    expected_airtable_updates,
):
    provider_urls = [
        f"https://lh5.googleusercontent.com/p/provider-photo-{index}"
        for index in range(MAX_SELECTED_PHOTOS)
    ]
    curator_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/curator-att1-photo.webp"
    curator_photo = _photo_manifest(curator_url)
    existing_provider_photos = [
        _photo_manifest(
            f"https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/{index:064x}.webp"
        )
        for index in range(MAX_SELECTED_PHOTOS + 1)
    ]
    selected_provider_photos = existing_provider_photos[:MAX_SELECTED_PHOTOS]
    provider_raw_data = {
        "photos_data": [
            {"photo_url_big": provider_url, "photo_tags": ["vibe"]}
            for provider_url in provider_urls
        ],
        "photo": "https://lh5.googleusercontent.com/p/provider-hero",
    }
    saved_payload = {}
    airtable_updates = []

    class CaptureAirtableService:
        def __init__(self, provider_type, initialize_provider=True):
            self.provider_type = provider_type

        def update_place_record(self, record_id, field_to_update, update_value, overwrite):
            airtable_updates.append({
                "record_id": record_id,
                "field_to_update": field_to_update,
                "update_value": update_value,
                "overwrite": overwrite,
            })
            return {"updated": True}

        def get_place_record_by_id(self, record_id):
            return {
                "id": record_id,
                "fields": {
                    "Place": "Test Place",
                    "Photos": json.dumps([curator_photo, *existing_provider_photos]),
                },
            }

    class DummyPhotoAssetService:
        def process_place(self, place, place_data, config):
            assert "json" in saved_payload
            assert place_data["photos"]["photo_urls"] == provider_urls
            assert place_data["photos"]["raw_data"] == provider_raw_data
            asset_photos = json.loads(place["fields"]["Photos"])
            assert asset_photos == (
                [curator_photo]
                if force_provider
                else [curator_photo, *existing_provider_photos]
            )
            return {
                "summary": {"selected_airtable_count": 1 + MAX_SELECTED_PHOTOS},
                "failures": [],
                "assets": [],
                "selected_source_urls": provider_urls,
                "selected_airtable_photos": [curator_photo, *selected_provider_photos],
                "selected_airtable_urls": [
                    curator_url,
                    *[photo["display"] for photo in selected_provider_photos],
                ],
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
        staticmethod(lambda provider_type: DummyProvider(provider_photos={"photo_urls": provider_urls, "raw_data": provider_raw_data})),
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
                "Photos": json.dumps([curator_photo, *existing_provider_photos]),
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": False,
            "upload": True,
            "write_airtable": True,
            "force_provider": force_provider,
        },
    }

    result = photos.refresh_single_place_photos(activity_input)

    assert result["status"] == expected_status
    assert result["photos_before"] == 2 + MAX_SELECTED_PHOTOS
    assert saved_payload["path"] == "data/places/charlotte/ChIJ123.json"
    assert saved_payload["json"]["photos"]["photo_urls"] == provider_urls
    assert saved_payload["json"]["photos"]["raw_data"] == provider_raw_data
    assert saved_payload["json"]["photos"]["selection_source"] == "provider"
    assert saved_payload["json"]["photos"]["selection_limit"] == MAX_SELECTED_PHOTOS
    assert len(airtable_updates) == expected_airtable_updates
    if force_provider:
        assert result["photos_after"] == 1 + MAX_SELECTED_PHOTOS
        assert result["provider_photos_after"] == MAX_SELECTED_PHOTOS
    else:
        assert "protected provider photo count" in result["message"]


@pytest.mark.parametrize(
    (
        "force_provider",
        "latest_matches_proposed",
        "latest_has_new_curator",
        "expected_status",
        "expected_airtable_status",
        "expected_writes",
    ),
    [
        (False, True, False, "updated_data_file", "no_change", 0),
        (False, False, False, "conflict", "conflict", 0),
        (True, False, False, "updated_from_provider", "updated", 1),
        (True, False, True, "updated_from_provider", "updated", 1),
    ],
)
def test_refresh_single_place_photos_handles_concurrent_airtable_changes(
    monkeypatch,
    force_provider,
    latest_matches_proposed,
    latest_has_new_curator,
    expected_status,
    expected_airtable_status,
    expected_writes,
):
    curator_photo = _photo_manifest(
        "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/curator-att1.webp"
    )
    concurrent_curator_photo = _photo_manifest(
        "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/curator-att2.webp"
    )
    provider_photo = _photo_manifest(
        "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/" + ("a" * 64) + ".webp"
    )
    concurrent_provider_photo = (
        provider_photo
        if latest_matches_proposed
        else _photo_manifest(
            "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ123/display/" + ("b" * 64) + ".webp"
        )
    )
    provider_urls = ["https://example.com/provider.jpg"]
    writes = []

    class ConflictAirtableService:
        def __init__(self, provider_type, initialize_provider=True):
            self.provider_type = provider_type

        def get_place_record_by_id(self, record_id):
            latest_photos = [curator_photo, concurrent_provider_photo]
            if latest_has_new_curator:
                latest_photos.insert(1, concurrent_curator_photo)
            return {
                "id": record_id,
                "fields": {"Photos": json.dumps(latest_photos)},
            }

        def update_place_record(self, *args, **kwargs):
            writes.append((args, kwargs))
            return {"updated": True}

    class SuccessfulPhotoAssetService:
        def process_place(self, place, place_data, config):
            return {
                "summary": {"selected_airtable_count": 2},
                "failures": [],
                "assets": [],
                "selected_airtable_photos": [curator_photo, provider_photo],
                "selected_airtable_urls": [curator_photo["display"], provider_photo["display"]],
            }

    monkeypatch.setattr(photos, "AirtableService", ConflictAirtableService)
    monkeypatch.setattr(photos, "PhotoAssetService", SuccessfulPhotoAssetService)
    monkeypatch.setattr(
        photos.PlaceDataProviderFactory,
        "get_provider",
        staticmethod(lambda provider_type: DummyProvider(provider_photos={
            "photo_urls": provider_urls,
            "raw_data": {"photos_data": [{"photo_url_big": provider_urls[0]}]},
        })),
    )
    monkeypatch.setattr(photos, "fetch_data_github", lambda path: (True, {"photos": {}}, "ok"))
    monkeypatch.setattr(photos, "save_data_github", lambda content, path: (True, "saved"))

    result = photos.refresh_single_place_photos({
        "place": {
            "id": "rec123",
            "fields": {
                "Place": "Test Place",
                "Google Maps Place Id": "ChIJ123",
                "Photos": json.dumps([curator_photo]),
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": False,
            "upload": True,
            "write_airtable": True,
            "force_provider": force_provider,
        },
    })

    assert result["status"] == expected_status
    assert result["airtable_status"] == expected_airtable_status
    assert len(writes) == expected_writes
    if force_provider and latest_has_new_curator:
        written_photos = json.loads(writes[0][1]["update_value"])
        assert written_photos[:2] == [curator_photo, concurrent_curator_photo]


def test_refresh_single_place_photos_cache_hit_is_idempotent(monkeypatch):
    provider = DummyProvider()
    provider_manifest = _photo_manifest(
        "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ-cache/display/" + ("a" * 64) + ".webp"
    )
    current_manifests = [provider_manifest]
    raw_records = [
        {"photo_url_big": f"https://example.com/photo-{index}.jpg", "photo_tags": ["vibe"]}
        for index in range(MAX_SELECTED_PHOTOS)
    ]
    selected_urls = [record["photo_url_big"] for record in raw_records]
    cached_place_data = {
        "data_source": "OutscraperProvider",
        "photos_provider_type": "outscraper",
        "photos": {
            "raw_data": {"photos_data": raw_records},
            "photo_urls": selected_urls,
            "provider_type": "outscraper",
            "selection_source": "cache",
            "selection_limit": MAX_SELECTED_PHOTOS,
            "raw_photo_count": MAX_SELECTED_PHOTOS,
            "selected_photo_count": MAX_SELECTED_PHOTOS,
            "last_refreshed": "2026-08-15T12:00:00",
            "message": "Photos reconciled using outscraper from cache",
        },
    }
    save_calls = []

    class NoChangeAirtableService:
        def __init__(self, provider_type, initialize_provider=True):
            self.provider_type = provider_type

        def get_place_record_by_id(self, record_id):
            return {"id": record_id, "fields": {"Photos": json.dumps(current_manifests)}}

        def update_place_record(self, record_id, field_to_update, update_value, overwrite):
            raise AssertionError("A matching Airtable manifest must not be updated")

    class NoChangePhotoAssetService:
        def process_place(self, place, place_data, config):
            return {
                "summary": {"selected_airtable_count": 1},
                "failures": [],
                "assets": [],
                "selected_airtable_photos": current_manifests,
                "selected_airtable_urls": [provider_manifest["display"]],
            }

    def save_data(content, path):
        save_calls.append((content, path))
        return True, "saved"

    monkeypatch.setattr(photos, "AirtableService", NoChangeAirtableService)
    monkeypatch.setattr(photos, "PhotoAssetService", NoChangePhotoAssetService)
    monkeypatch.setattr(photos.PlaceDataProviderFactory, "get_provider", staticmethod(lambda provider_type: provider))
    monkeypatch.setattr(
        photos,
        "fetch_data_github",
        lambda path: (True, cached_place_data, "ok"),
    )
    monkeypatch.setattr(photos, "save_data_github", save_data)

    result = photos.refresh_single_place_photos({
        "place": {
            "id": "rec-cache",
            "fields": {
                "Place": "Cache Place",
                "Google Maps Place Id": "ChIJ-cache",
                "Photos": json.dumps(current_manifests),
            },
        },
        "config": {
            "provider_type": "outscraper",
            "city": "charlotte",
            "dry_run": False,
            "upload": True,
            "write_airtable": True,
        },
    })

    assert result["status"] == "no_change"
    assert result["selection_source"] == "cache"
    assert result["provider_called"] is False
    assert result["data_file_status"] == "no_change"
    assert provider.get_place_photos_calls == []
    assert save_calls == []
