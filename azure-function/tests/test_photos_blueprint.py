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


def test_validate_refresh_all_photos_request_success_defaults():
    parsed, error_response = photos.validate_refresh_all_photos_request(
        DummyRequest({"provider_type": "outscraper"})
    )

    assert error_response is None
    assert parsed["provider_type"] == "outscraper"
    assert parsed["city"] == "charlotte"
    assert parsed["dry_run"] is True
    assert parsed["sequential_mode"] is False
    assert parsed["max_places"] is None
    assert parsed["photo_source_mode"] == "refresh_from_data_file_raw_data"


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
    assert result["photos_before"] == 2
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
