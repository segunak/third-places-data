import json
import pytest
from unittest import mock

from blueprints import curator_photos
from tests.conftest import load_fixture


# =============================================================================
# Helpers
# =============================================================================

def _make_place(record_id="recABC123", place_name="Test Coffee Shop",
                curator_attachments=None, curator_photo_urls=""):
    """Build a minimal Airtable place record dict for activity input."""
    fields = {
        "Place": place_name,
        "Google Maps Place Id": "ChIJtest123",
    }
    if curator_attachments is not None:
        fields["Curator Photos"] = curator_attachments
    if curator_photo_urls:
        fields["Curator Photo URLs"] = curator_photo_urls
    return {"id": record_id, "fields": fields}


class DummyRequest:
    def __init__(self, params):
        self.params = params


# =============================================================================
# Validation Tests
# =============================================================================

class TestValidateSyncCuratorPhotosRequest:

    def test_defaults(self):
        parsed, error = curator_photos.validate_sync_curator_photos_request(
            DummyRequest({})
        )
        assert error is None
        assert parsed["city"] == "charlotte"

    def test_custom_city(self):
        parsed, error = curator_photos.validate_sync_curator_photos_request(
            DummyRequest({"city": "raleigh"})
        )
        assert error is None
        assert parsed["city"] == "raleigh"


# =============================================================================
# Activity Tests
# =============================================================================

class TestSyncSinglePlaceCuratorPhotos:

    def test_no_curator_photos_returns_skipped(self):
        place = _make_place(curator_attachments=[])
        activity_input = {"place": place, "config": {}}
        result = curator_photos.sync_single_place_curator_photos(activity_input)
        assert result["status"] == "skipped"
        assert "No curator photos" in result["message"]

    def test_no_curator_photos_field_returns_skipped(self):
        place = _make_place()  # No Curator Photos field at all
        activity_input = {"place": place, "config": {}}
        result = curator_photos.sync_single_place_curator_photos(activity_input)
        assert result["status"] == "skipped"

    def test_invalid_place_returns_error(self):
        activity_input = {"place": None, "config": {}}
        result = curator_photos.sync_single_place_curator_photos(activity_input)
        assert result["status"] == "error"

    def test_already_synced_returns_no_change(self, monkeypatch, airtable_attachment_objects):
        place = _make_place(curator_attachments=airtable_attachment_objects)

        # Simulate that both blobs already exist
        expected_blobs = [
            f"recABC123/attABC123_cafe-interior.jpg",
            f"recABC123/attDEF456_patio_seating.png",
        ]
        monkeypatch.setattr(
            curator_photos, "list_blobs",
            lambda prefix: expected_blobs
        )

        activity_input = {"place": place, "config": {}}
        result = curator_photos.sync_single_place_curator_photos(activity_input)

        assert result["status"] == "no_change"

    def test_new_attachments_upload(self, monkeypatch, airtable_attachment_objects):
        place = _make_place(curator_attachments=airtable_attachment_objects)

        monkeypatch.setattr(
            curator_photos, "list_blobs",
            lambda prefix: []
        )

        downloaded_images = []
        monkeypatch.setattr(
            curator_photos, "download_image",
            lambda url: (b"fake-image-bytes", "image/jpeg")
        )

        uploaded_blobs = []
        def mock_upload(blob_path, data, content_type):
            uploaded_blobs.append(blob_path)
            return f"https://thirdplacesdata.blob.core.windows.net/curator-photos/{blob_path}"

        monkeypatch.setattr(curator_photos, "upload_blob", mock_upload)

        # Mock Airtable API write
        mock_table = mock.MagicMock()
        mock_api = mock.MagicMock()
        mock_api.table.return_value = mock_table

        monkeypatch.setattr(
            "blueprints.curator_photos.AirtableApi",
            lambda token: mock_api
        )

        activity_input = {"place": place, "config": {}}
        result = curator_photos.sync_single_place_curator_photos(activity_input)

        assert result["status"] == "updated"
        assert result["photos_synced"] == 2
        assert len(uploaded_blobs) == 2
        mock_table.update.assert_called_once()

        # Verify the URLs written to Airtable
        update_args = mock_table.update.call_args
        assert update_args[0][0] == "recABC123"
        written_urls = json.loads(update_args[0][1]["Curator Photo URLs"])
        assert len(written_urls) == 2
        assert all("thirdplacesdata.blob.core.windows.net" in url for url in written_urls)

    def test_orphaned_blobs_deleted(self, monkeypatch, airtable_attachment_objects):
        # Only first attachment exists in Airtable
        place = _make_place(curator_attachments=[airtable_attachment_objects[0]])

        # But blob storage has both plus an extra orphan
        monkeypatch.setattr(
            curator_photos, "list_blobs",
            lambda prefix: [
                "recABC123/attABC123_cafe-interior.jpg",
                "recABC123/attOLD789_old-photo.jpg",  # orphaned
            ]
        )

        monkeypatch.setattr(curator_photos, "download_image", lambda url: (b"bytes", "image/jpeg"))
        monkeypatch.setattr(curator_photos, "upload_blob", lambda p, d, c: f"https://test/{p}")

        deleted_blobs = []
        monkeypatch.setattr(
            curator_photos, "delete_blob",
            lambda path: (deleted_blobs.append(path), True)[1]
        )

        mock_table = mock.MagicMock()
        mock_api = mock.MagicMock()
        mock_api.table.return_value = mock_table
        monkeypatch.setattr("blueprints.curator_photos.AirtableApi", lambda token: mock_api)

        activity_input = {"place": place, "config": {}}
        result = curator_photos.sync_single_place_curator_photos(activity_input)

        assert result["status"] == "updated"
        assert result["photos_deleted"] == 1
        assert "recABC123/attOLD789_old-photo.jpg" in deleted_blobs

    def test_download_failure_handled_gracefully(self, monkeypatch, airtable_attachment_objects):
        place = _make_place(curator_attachments=airtable_attachment_objects)

        monkeypatch.setattr(curator_photos, "list_blobs", lambda prefix: [])

        def failing_download(url):
            raise Exception("Network error")

        monkeypatch.setattr(curator_photos, "download_image", failing_download)
        monkeypatch.setattr(curator_photos, "upload_blob", lambda p, d, c: f"https://test/{p}")

        mock_table = mock.MagicMock()
        mock_api = mock.MagicMock()
        mock_api.table.return_value = mock_table
        monkeypatch.setattr("blueprints.curator_photos.AirtableApi", lambda token: mock_api)

        activity_input = {"place": place, "config": {}}
        result = curator_photos.sync_single_place_curator_photos(activity_input)

        # Should still succeed (updated) but with 0 photos synced due to download failures
        assert result["status"] == "updated"
        assert result["photos_synced"] == 0


# =============================================================================
# Blob Path Construction Tests
# =============================================================================

class TestBuildBlobPath:

    def test_basic_path(self):
        path = curator_photos._build_blob_path("recABC", "attXYZ", "photo.jpg")
        assert path == "recABC/attXYZ_photo.jpg"

    def test_spaces_replaced(self):
        path = curator_photos._build_blob_path("recABC", "attXYZ", "my photo file.jpg")
        assert path == "recABC/attXYZ_my_photo_file.jpg"
