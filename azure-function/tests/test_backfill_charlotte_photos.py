import json
import importlib.util
from pathlib import Path


def load_backfill_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "backfill_charlotte_photos.py"
    spec = importlib.util.spec_from_file_location("backfill_charlotte_photos", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_process_file_recomputes_from_raw_data(tmp_path):
    module = load_backfill_module()

    sample_payload = {
        "photos": {
            "photo_urls": [],
            "raw_data": {
                "photos_data": [
                    {
                        "photo_url_big": "https://lh5.googleusercontent.com/gps-cs-s/restricted-big",
                        "photo_date": "12/01/2024 10:00:00",
                        "photo_tags": ["vibe"]
                    },
                    {
                        "photo_url_big": "https://lh5.googleusercontent.com/p/photo-2-big",
                        "photo_date": "11/28/2024 14:30:00",
                        "photo_tags": ["front"]
                    }
                ]
            }
        }
    }

    file_path = tmp_path / "place.json"
    file_path.write_text(json.dumps(sample_payload), encoding="utf-8")

    result = module.process_file(file_path, dry_run=False)

    assert result["status"] == "updated"
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    assert len(payload["photos"]["photo_urls"]) == 2
    assert any("gps-cs-s" in url for url in payload["photos"]["photo_urls"])


def test_process_file_preserves_existing_when_no_raw_data(tmp_path):
    module = load_backfill_module()

    sample_payload = {
        "photos": {
            "photo_urls": ["https://example.com/already-present.jpg"],
            "raw_data": {}
        }
    }

    file_path = tmp_path / "place.json"
    file_path.write_text(json.dumps(sample_payload), encoding="utf-8")

    result = module.process_file(file_path, dry_run=False)

    assert result["status"] == "preserved_existing"
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    assert payload["photos"]["photo_urls"] == ["https://example.com/already-present.jpg"]


def test_select_prioritized_photos_respects_limit_and_dedupes():
    module = load_backfill_module()

    photos = []
    for index in range(50):
        photos.append(
            {
                "photo_url_big": f"https://example.com/photo-{index % 35}.jpg",
                "photo_date": "12/01/2024 10:00:00",
                "photo_tags": ["vibe"]
            }
        )

    selected = module.select_prioritized_photos(photos, max_photos=30)
    assert len(selected) <= 30
    assert len(selected) == len(set(selected))
