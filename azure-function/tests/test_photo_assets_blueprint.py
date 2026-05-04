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
                        "https://thirdplacesdata.blob.core.windows.net/photos/ChIJ5YHD3oOhVogRAV83qWzHmgg/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.jpg"
                    ]),
                },
            }
        ]


def test_base_config_defaults_for_audit():
    config = photo_assets._base_config_from_params({})

    assert config == {
        "city": "charlotte",
        "record_id": "",
        "place_id": "",
        "max_places": 0,
        "dry_run": True,
        "upload": False,
        "write_airtable": False,
    }


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


def test_filter_places_applies_record_place_and_limit():
    places = [
        {"id": "rec123", "fields": {"Google Maps Place Id": "ChIJ123"}},
        {"id": "rec456", "fields": {"Google Maps Place Id": "ChIJ456"}},
    ]

    assert photo_assets._filter_places(places, {"record_id": "rec123", "max_places": 0}) == [places[0]]
    assert photo_assets._filter_places(places, {"place_id": "ChIJ456", "max_places": 0}) == [places[1]]
    assert photo_assets._filter_places(places, {"max_places": 1}) == [places[0]]


def test_aggregate_audit_results_counts_findings():
    totals = photo_assets._aggregate_audit_results([
        {
            "status": "ok",
            "canonical_photo_url_count": 2,
            "canonical_curator_url_count": 1,
            "canonical_standard_url_count": 1,
            "legacy_airtable_url_count": 0,
            "non_azure_airtable_url_count": 0,
            "mappable_legacy_blob_count": 0,
            "unmappable_legacy_blob_count": 0,
            "new_container_blob_count": 2,
            "unserved_blob_count": 0,
        },
        {
            "status": "skipped",
            "skip_reason": "ignored_missing_place_id",
        },
        {
            "status": "error",
            "legacy_airtable_url_count": 1,
            "non_azure_airtable_url_count": 1,
            "mappable_legacy_blob_count": 1,
        },
    ])

    assert totals["total_places"] == 3
    assert totals["ignored_missing_place_id"] == 1
    assert totals["canonical_photo_url_count"] == 2
    assert totals["legacy_airtable_url_count"] == 1
    assert totals["non_azure_airtable_url_count"] == 1
    assert totals["mappable_legacy_blob_count"] == 1
    assert totals["errors"] == 1
    assert totals["success"] is False


def test_audit_single_place_reports_counts(monkeypatch):
    place_id = "ChIJ123"
    record_id = "rec123"
    canonical_url = f"https://thirdplacesdata.blob.core.windows.net/photos/{place_id}/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp"

    monkeypatch.setattr(
        photo_assets,
        "list_blobs_in_container",
        lambda container, prefix="": [
            f"{prefix}aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.webp"
        ] if container == photo_assets.PHOTOS_CONTAINER else [],
    )

    result = photo_assets.audit_single_place_photo_assets({
        "place": {
            "id": record_id,
            "fields": {
                "Place": "Audit Cafe",
                "Google Maps Place Id": place_id,
                "Photos": json.dumps([canonical_url, "https://example.com/photo.jpg"]),
            },
        },
        "config": {"city": "charlotte"},
    })

    assert result["status"] == "ok"
    assert result["canonical_standard_url_count"] == 1
    assert result["non_azure_airtable_url_count"] == 1
    assert result["new_container_blob_count"] == 1
    assert result["missing_blob_reference_count"] == 0


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