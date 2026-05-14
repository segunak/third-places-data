import json
from unittest import mock

from conftest import TEST_PLACE_ID, TEST_PLACE_NAME
from services.utils import get_and_cache_place_data, sanitize_blob_metadata


def test_sanitize_blob_metadata_returns_header_safe_ascii_values():
    metadata = {
        "place-name": "Caf\u00e9 Ros\u00e9\nCharlotte \U0001f95e",
        "1bad key": "value\twith\rcontrols",
        "": "ignored",
        "none": None,
    }

    assert sanitize_blob_metadata(metadata) == {
        "place_name": "Cafe Rose Charlotte",
        "m_1bad_key": "value with controls",
    }


class TestGetAndCachePlaceDataPhotosProvider:
    def test_fresh_fetch_uses_photo_provider_when_different(self, mock_env_vars):
        primary_provider = mock.MagicMock()
        primary_provider.get_all_place_data.return_value = {
            "place_id": TEST_PLACE_ID,
            "place_name": TEST_PLACE_NAME,
            "data_source": "OutscraperProvider",
            "details": {"place_id": TEST_PLACE_ID, "raw_data": {}},
            "photos": {"photo_urls": []},
        }
        photo_provider = mock.MagicMock()
        photo_provider.get_place_photos.return_value = {
            "place_id": TEST_PLACE_ID,
            "message": "Selected 1 photos",
            "photo_urls": ["https://lh3.googleusercontent.com/p/google-photo"],
        }

        def get_provider(provider_type):
            return {"outscraper": primary_provider, "google": photo_provider}[provider_type]

        airtable_instance = mock.MagicMock()
        airtable_instance.get_record.return_value = None

        with mock.patch("services.utils.PlaceDataProviderFactory.get_provider", side_effect=get_provider):
            with mock.patch("services.airtable_service.AirtableService", return_value=airtable_instance):
                with mock.patch("services.utils.fetch_data_github", return_value=(False, None, "not found")):
                    with mock.patch("services.utils.save_data_github", return_value=(True, "saved")):
                        status, place_data, _ = get_and_cache_place_data(
                            provider_type="outscraper",
                            photos_provider_type="google",
                            place_name=TEST_PLACE_NAME,
                            place_id=TEST_PLACE_ID,
                            city="charlotte",
                            force_refresh=False,
                        )

        assert status == "succeeded"
        primary_provider.get_all_place_data.assert_called_once_with(TEST_PLACE_ID, TEST_PLACE_NAME, skip_photos=True)
        photo_provider.get_place_photos.assert_called_once_with(TEST_PLACE_ID)
        assert place_data["photos"]["photo_urls"] == ["https://lh3.googleusercontent.com/p/google-photo"]
        assert place_data["photos_provider_type"] == "google"

    def test_cached_empty_photos_fetches_photo_provider(self, mock_env_vars):
        cached_place_data = {
            "place_id": TEST_PLACE_ID,
            "place_name": TEST_PLACE_NAME,
            "data_source": "OutscraperProvider",
            "details": {"place_id": TEST_PLACE_ID, "raw_data": {}},
            "photos": {"photo_urls": []},
        }
        primary_provider = mock.MagicMock()
        photo_provider = mock.MagicMock()
        photo_provider.get_place_photos.return_value = {
            "place_id": TEST_PLACE_ID,
            "message": "Selected 1 photos",
            "photo_urls": ["https://lh3.googleusercontent.com/p/google-photo"],
        }

        def get_provider(provider_type):
            return {"outscraper": primary_provider, "google": photo_provider}[provider_type]

        airtable_instance = mock.MagicMock()
        airtable_instance.get_record.return_value = {"id": "recABC", "fields": {"Place": TEST_PLACE_NAME}}

        with mock.patch("services.utils.PlaceDataProviderFactory.get_provider", side_effect=get_provider):
            with mock.patch("services.airtable_service.AirtableService", return_value=airtable_instance):
                with mock.patch("services.utils.fetch_data_github", return_value=(True, cached_place_data, "ok")):
                    with mock.patch("services.utils.save_data_github", return_value=(True, "saved")) as mock_save:
                        status, place_data, _ = get_and_cache_place_data(
                            provider_type="outscraper",
                            photos_provider_type="google",
                            place_name=TEST_PLACE_NAME,
                            place_id=TEST_PLACE_ID,
                            city="charlotte",
                            force_refresh=False,
                        )

        assert status == "cached"
        primary_provider.get_all_place_data.assert_not_called()
        photo_provider.get_place_photos.assert_called_once_with(TEST_PLACE_ID)
        assert place_data["photos"]["photo_urls"] == ["https://lh3.googleusercontent.com/p/google-photo"]
        saved_json = json.loads(mock_save.call_args.args[0])
        assert saved_json["photos"]["photo_urls"] == ["https://lh3.googleusercontent.com/p/google-photo"]

    def test_cached_airtable_photos_skip_photo_provider(self, mock_env_vars):
        cached_place_data = {
            "place_id": TEST_PLACE_ID,
            "place_name": TEST_PLACE_NAME,
            "data_source": "OutscraperProvider",
            "details": {"place_id": TEST_PLACE_ID, "raw_data": {}},
            "photos": {"photo_urls": []},
        }
        primary_provider = mock.MagicMock()
        photo_provider = mock.MagicMock()

        def get_provider(provider_type):
            return {"outscraper": primary_provider, "google": photo_provider}[provider_type]

        airtable_instance = mock.MagicMock()
        airtable_instance.get_record.return_value = {
            "id": "recABC",
            "fields": {"Place": TEST_PLACE_NAME, "Photos": '["https://existing.example/photo.jpg"]'},
        }

        with mock.patch("services.utils.PlaceDataProviderFactory.get_provider", side_effect=get_provider):
            with mock.patch("services.airtable_service.AirtableService", return_value=airtable_instance):
                with mock.patch("services.utils.fetch_data_github", return_value=(True, cached_place_data, "ok")):
                    with mock.patch("services.utils.save_data_github") as mock_save:
                        status, place_data, _ = get_and_cache_place_data(
                            provider_type="outscraper",
                            photos_provider_type="google",
                            place_name=TEST_PLACE_NAME,
                            place_id=TEST_PLACE_ID,
                            city="charlotte",
                            force_refresh=False,
                        )

        assert status == "cached"
        photo_provider.get_place_photos.assert_not_called()
        mock_save.assert_not_called()
        assert place_data["photos"]["photo_urls"] == []
