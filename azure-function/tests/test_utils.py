import base64
import json
from unittest import mock
from requests.exceptions import ChunkedEncodingError

from conftest import TEST_PLACE_ID, TEST_PLACE_NAME
from services.utils import fetch_data_github, get_and_cache_place_data, sanitize_blob_metadata


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


class BrokenGitHubResponse:
    @property
    def content(self):
        raise ChunkedEncodingError("incomplete response body")


def test_fetch_data_github_retries_incomplete_response(mock_env_vars, monkeypatch):
    monkeypatch.setenv("GITHUB_PERSONAL_ACCESS_TOKEN", "test-github-token")
    place_data = {"place_id": TEST_PLACE_ID}
    encoded_content = base64.b64encode(json.dumps(place_data).encode()).decode()
    complete_response = mock.MagicMock(status_code=200)
    complete_response.content = b"complete"
    complete_response.json.return_value = {
        "type": "file",
        "encoding": "base64",
        "content": encoded_content,
    }
    session = mock.MagicMock()
    session.get.side_effect = [BrokenGitHubResponse(), complete_response]

    with mock.patch("services.utils.requests.Session", return_value=session):
        with mock.patch("services.utils.time.sleep"):
            success, result, message = fetch_data_github("data/places/charlotte/test.json")

    assert success is True
    assert result == place_data
    assert message == "File fetched successfully"
    assert session.get.call_count == 2


def test_fetch_data_github_reports_exhausted_incomplete_response(mock_env_vars, monkeypatch):
    monkeypatch.setenv("GITHUB_PERSONAL_ACCESS_TOKEN", "test-github-token")
    session = mock.MagicMock()
    session.get.return_value = BrokenGitHubResponse()

    with mock.patch("services.utils.requests.Session", return_value=session):
        with mock.patch("services.utils.time.sleep"):
            success, result, message = fetch_data_github(
                "data/places/charlotte/test.json",
                max_retries=2,
            )

    assert success is False
    assert result is None
    assert "Network error while fetching from GitHub" in message
    assert session.get.call_count == 3


class TestGetAndCachePlaceDataPhotosProvider:
    @staticmethod
    def _fresh_place_data():
        return {
            "place_id": TEST_PLACE_ID,
            "place_name": TEST_PLACE_NAME,
            "data_source": "OutscraperProvider",
            "details": {"place_id": TEST_PLACE_ID, "raw_data": {}},
            "photos": {"photo_urls": []},
        }

    def test_fresh_save_sets_has_data_file_after_github_save(self, mock_env_vars):
        provider = mock.MagicMock()
        provider.get_all_place_data.return_value = self._fresh_place_data()
        airtable_instance = mock.MagicMock()
        airtable_instance.get_record.return_value = None
        events = []

        def save_data(*args):
            events.append("saved")
            return True, "saved"

        def update_record(record_id, field_name, value, overwrite):
            events.append((record_id, field_name, value, overwrite))
            return {"updated": True}

        airtable_instance.update_place_record.side_effect = update_record

        with mock.patch("services.utils.PlaceDataProviderFactory.get_provider", return_value=provider):
            with mock.patch("services.airtable_service.AirtableService", return_value=airtable_instance):
                with mock.patch("services.utils.fetch_data_github", return_value=(False, None, "not found")):
                    with mock.patch("services.utils.save_data_github", side_effect=save_data):
                        status, _, _ = get_and_cache_place_data(
                            provider_type="outscraper",
                            place_name=TEST_PLACE_NAME,
                            place_id=TEST_PLACE_ID,
                            city="charlotte",
                            airtable_record_id="recABC",
                        )

        assert status == "succeeded"
        assert events == [
            "saved",
            ("recABC", "Has Data File", "Yes", True),
        ]

    def test_fresh_save_failure_does_not_set_has_data_file(self, mock_env_vars):
        provider = mock.MagicMock()
        provider.get_all_place_data.return_value = self._fresh_place_data()
        airtable_instance = mock.MagicMock()
        airtable_instance.get_record.return_value = None

        with mock.patch("services.utils.PlaceDataProviderFactory.get_provider", return_value=provider):
            with mock.patch("services.airtable_service.AirtableService", return_value=airtable_instance):
                with mock.patch("services.utils.fetch_data_github", return_value=(False, None, "not found")):
                    with mock.patch(
                        "services.utils.save_data_github",
                        return_value=(False, "GitHub API returned status code 409"),
                    ):
                        status, place_data, message = get_and_cache_place_data(
                            provider_type="outscraper",
                            place_name=TEST_PLACE_NAME,
                            place_id=TEST_PLACE_ID,
                            city="charlotte",
                            airtable_record_id="recABC",
                        )

        assert status == "failed"
        assert place_data is None
        assert "Failed to save data file" in message
        airtable_instance.update_place_record.assert_not_called()

    def test_cached_file_sets_has_data_file_without_github_save(self, mock_env_vars):
        provider = mock.MagicMock()
        cached_place_data = self._fresh_place_data()
        airtable_instance = mock.MagicMock()
        airtable_instance.get_record.return_value = None

        with mock.patch("services.utils.PlaceDataProviderFactory.get_provider", return_value=provider):
            with mock.patch("services.airtable_service.AirtableService", return_value=airtable_instance):
                with mock.patch(
                    "services.utils.fetch_data_github",
                    return_value=(True, cached_place_data, "File fetched successfully"),
                ):
                    with mock.patch("services.utils.save_data_github") as mock_save:
                        status, _, _ = get_and_cache_place_data(
                            provider_type="outscraper",
                            place_name=TEST_PLACE_NAME,
                            place_id=TEST_PLACE_ID,
                            city="charlotte",
                            airtable_record_id="recABC",
                        )

        assert status == "cached"
        mock_save.assert_not_called()
        airtable_instance.update_place_record.assert_called_once_with(
            "recABC",
            "Has Data File",
            "Yes",
            overwrite=True,
        )

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
