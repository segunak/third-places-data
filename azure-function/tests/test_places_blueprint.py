"""Tests for place refresh blueprint parameter propagation."""

from conftest import TEST_PLACE_ID, TEST_PLACE_NAME


class FakeOrchestrationContext:
    def __init__(self, input_data):
        self.input_data = input_data
        self.calls = []

    def get_input(self):
        return self.input_data

    def call_activity(self, name, input_data):
        call = {"name": name, "input": input_data}
        self.calls.append(call)
        return call


def test_get_place_data_passes_photos_provider_type(monkeypatch):
    from blueprints import places

    captured = {}

    def get_and_cache_place_data(**kwargs):
        captured.update(kwargs)
        return "succeeded", {"place_id": TEST_PLACE_ID}, "ok"

    monkeypatch.setattr(places.helpers, "get_and_cache_place_data", get_and_cache_place_data)

    result = places.get_place_data({
        "place": {
            "id": "recABC123",
            "fields": {
                "Place": TEST_PLACE_NAME,
                "Google Maps Place Id": TEST_PLACE_ID,
            },
        },
        "config": {
            "provider_type": "outscraper",
            "photos_provider_type": "google",
            "city": "charlotte",
            "force_refresh": True,
        },
    })

    assert result["status"] == "succeeded"
    assert captured["provider_type"] == "outscraper"
    assert captured["photos_provider_type"] == "google"


def test_refresh_single_place_orchestrator_passes_photos_provider_type_to_refresh_and_enrich():
    from blueprints import places

    context = FakeOrchestrationContext({
        "place_id": TEST_PLACE_ID,
        "provider_type": "outscraper",
        "photos_provider_type": "google",
        "city": "charlotte",
        "force_refresh": True,
    })

    place_record = {
        "id": "recABC123",
        "fields": {
            "Place": TEST_PLACE_NAME,
            "Google Maps Place Id": TEST_PLACE_ID,
        },
    }

    orchestrator = places.refresh_single_place_orchestrator._function._func.orchestrator_function(context)
    first_call = next(orchestrator)
    assert first_call["name"] == "find_place_by_id"

    second_call = orchestrator.send(place_record)
    assert second_call["name"] == "get_place_data"
    assert second_call["input"]["config"]["photos_provider_type"] == "google"

    third_call = orchestrator.send({"status": "succeeded", "place_name": TEST_PLACE_NAME, "message": "ok"})
    assert third_call["name"] == "enrich_single_place"
    assert third_call["input"]["photos_provider_type"] == "google"

    try:
        orchestrator.send({"status": "succeeded", "message": "ok", "field_updates": {}})
    except StopIteration as finished:
        result = finished.value

    assert result["success"] is True
    assert result["data"]["photos_provider_type"] == "google"
