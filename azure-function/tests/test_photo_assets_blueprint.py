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