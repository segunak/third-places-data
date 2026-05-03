import json
from unittest import mock

from blueprints import curator_photos


class DummyRequest:
    def __init__(self, params):
        self.params = params


def _make_place(record_id="recABC123", place_name="Test Coffee Shop", curator_attachments=None, photos="", place_id="ChIJtest123"):
    fields = {
        "Place": place_name,
        "Google Maps Place Id": place_id,
    }
    if curator_attachments is not None:
        fields["Curator Photos"] = curator_attachments
    if photos:
        fields["Photos"] = photos
    return {"id": record_id, "fields": fields}


def _expected_curator_urls(place_id="ChIJtest123"):
    return [
        f"https://thirdplacesdata.blob.core.windows.net/photos/{place_id}/curator-attABC123-cafe-interior.webp",
        f"https://thirdplacesdata.blob.core.windows.net/photos/{place_id}/curator-attDEF456-patio-seating.webp",
    ]


def _expected_curator_blobs(place_id="ChIJtest123"):
    return [
        f"{place_id}/curator-attABC123-cafe-interior.webp",
        f"{place_id}/curator-attDEF456-patio-seating.webp",
    ]


def _publisher_success(monkeypatch, place_id="ChIJtest123"):
    published = []

    def publish(self, attachment, place_id, record_id, place_name, dry_run=True, upload=False, try_url_variants=True):
        attachment_id = attachment["id"]
        filename_stem = "cafe-interior" if attachment_id == "attABC123" else "patio-seating"
        blob_path = f"{place_id}/curator-{attachment_id}-{filename_stem}.webp"
        azure_url = f"https://thirdplacesdata.blob.core.windows.net/photos/{blob_path}"
        published.append({"attachment_id": attachment_id, "blob_path": blob_path, "azure_url": azure_url})
        return {"success": True, "blob_path": blob_path, "azure_url": azure_url}

    monkeypatch.setattr(curator_photos.PhotoPublisherService, "publish_curator_attachment", publish)
    return published


class TestValidateSyncCuratorPhotosRequest:
    def test_defaults(self):
        parsed, error = curator_photos.validate_sync_curator_photos_request(DummyRequest({}))
        assert error is None
        assert parsed["city"] == "charlotte"

    def test_custom_city(self):
        parsed, error = curator_photos.validate_sync_curator_photos_request(DummyRequest({"city": "raleigh"}))
        assert error is None
        assert parsed["city"] == "raleigh"


class TestSyncSinglePlaceCuratorPhotos:
    def test_invalid_place_returns_error(self):
        result = curator_photos.sync_single_place_curator_photos({"place": None, "config": {}})
        assert result["status"] == "error"

    def test_missing_place_id_is_ignored(self, airtable_attachment_objects):
        place = _make_place(curator_attachments=airtable_attachment_objects, place_id="")
        result = curator_photos.sync_single_place_curator_photos({"place": place, "config": {}})
        assert result["status"] == "skipped"
        assert result["skip_reason"] == "ignored_missing_place_id"

    def test_no_curator_photos_returns_no_change(self, monkeypatch):
        place = _make_place(curator_attachments=[])
        monkeypatch.setattr(curator_photos, "list_blobs_in_container", lambda *args, **kwargs: [])
        monkeypatch.setattr("blueprints.curator_photos.AirtableApi", lambda token: (_ for _ in ()).throw(AssertionError("Airtable should not be updated")))

        result = curator_photos.sync_single_place_curator_photos({"place": place, "config": {}})

        assert result["status"] == "no_change"
        assert result["airtable_fields_updated"] == []

    def test_already_synced_returns_no_change(self, monkeypatch, airtable_attachment_objects):
        curator_urls = _expected_curator_urls()
        place_photo_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJtest123/" + ("a" * 64) + ".webp"
        place = _make_place(
            curator_attachments=airtable_attachment_objects,
            photos=json.dumps([*curator_urls, place_photo_url]),
        )
        _publisher_success(monkeypatch)
        monkeypatch.setattr(curator_photos, "list_blobs_in_container", lambda *args, **kwargs: _expected_curator_blobs())
        monkeypatch.setattr("blueprints.curator_photos.AirtableApi", lambda token: (_ for _ in ()).throw(AssertionError("Airtable should not be updated")))

        result = curator_photos.sync_single_place_curator_photos({"place": place, "config": {}})

        assert result["status"] == "no_change"
        assert result["photos_synced"] == 2

    def test_already_synced_repairs_photos_field_without_writing_legacy_field(self, monkeypatch, airtable_attachment_objects):
        curator_urls = _expected_curator_urls()
        place_photo_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJtest123/" + ("a" * 64) + ".webp"
        place = _make_place(curator_attachments=airtable_attachment_objects, photos=json.dumps([place_photo_url]))
        _publisher_success(monkeypatch)
        monkeypatch.setattr(curator_photos, "list_blobs_in_container", lambda *args, **kwargs: _expected_curator_blobs())

        mock_table = mock.MagicMock()
        mock_api = mock.MagicMock()
        mock_api.table.return_value = mock_table
        monkeypatch.setattr("blueprints.curator_photos.AirtableApi", lambda token: mock_api)

        result = curator_photos.sync_single_place_curator_photos({"place": place, "config": {}})

        assert result["status"] == "updated"
        assert result["photos_synced"] == 2
        assert result["airtable_fields_updated"] == ["Photos"]
        written_fields = mock_table.update.call_args[0][1]
        assert "Curator Photo URLs" not in written_fields
        assert json.loads(written_fields["Photos"]) == [*curator_urls, place_photo_url]

    def test_removed_curator_attachments_clears_curator_urls_from_photos(self, monkeypatch):
        curator_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJtest123/curator-attOLD-old-photo.webp"
        place_photo_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJtest123/" + ("a" * 64) + ".webp"
        place = _make_place(curator_attachments=[], photos=json.dumps([curator_url, place_photo_url]))

        monkeypatch.setattr(curator_photos, "list_blobs_in_container", lambda *args, **kwargs: ["ChIJtest123/curator-attOLD-old-photo.webp"])
        deleted_blobs = []
        monkeypatch.setattr(curator_photos, "delete_blob_from_container", lambda container, path: (deleted_blobs.append((container, path)), True)[1])

        mock_table = mock.MagicMock()
        mock_api = mock.MagicMock()
        mock_api.table.return_value = mock_table
        monkeypatch.setattr("blueprints.curator_photos.AirtableApi", lambda token: mock_api)

        result = curator_photos.sync_single_place_curator_photos({"place": place, "config": {}})

        assert result["status"] == "updated"
        assert result["photos_deleted"] == 1
        assert deleted_blobs == [("photos", "ChIJtest123/curator-attOLD-old-photo.webp")]
        assert result["airtable_fields_updated"] == ["Photos"]
        written_fields = mock_table.update.call_args[0][1]
        assert "Curator Photo URLs" not in written_fields
        assert json.loads(written_fields["Photos"]) == [place_photo_url]

    def test_new_attachments_upload(self, monkeypatch, airtable_attachment_objects):
        place = _make_place(curator_attachments=airtable_attachment_objects)
        published = _publisher_success(monkeypatch)
        monkeypatch.setattr(curator_photos, "list_blobs_in_container", lambda *args, **kwargs: [])

        mock_table = mock.MagicMock()
        mock_api = mock.MagicMock()
        mock_api.table.return_value = mock_table
        monkeypatch.setattr("blueprints.curator_photos.AirtableApi", lambda token: mock_api)

        result = curator_photos.sync_single_place_curator_photos({"place": place, "config": {}})

        assert result["status"] == "updated"
        assert result["photos_synced"] == 2
        assert len(published) == 2
        written_fields = mock_table.update.call_args[0][1]
        assert set(written_fields) == {"Photos"}
        written_photos = json.loads(written_fields["Photos"])
        assert written_photos == _expected_curator_urls()

    def test_orphaned_blobs_deleted(self, monkeypatch, airtable_attachment_objects):
        place = _make_place(curator_attachments=[airtable_attachment_objects[0]])
        published = _publisher_success(monkeypatch)
        monkeypatch.setattr(
            curator_photos,
            "list_blobs_in_container",
            lambda *args, **kwargs: [published[0]["blob_path"], "ChIJtest123/curator-attOLD789-old-photo.webp"],
        )
        deleted_blobs = []
        monkeypatch.setattr(curator_photos, "delete_blob_from_container", lambda container, path: (deleted_blobs.append(path), True)[1])

        mock_table = mock.MagicMock()
        mock_api = mock.MagicMock()
        mock_api.table.return_value = mock_table
        monkeypatch.setattr("blueprints.curator_photos.AirtableApi", lambda token: mock_api)

        result = curator_photos.sync_single_place_curator_photos({"place": place, "config": {}})

        assert result["status"] == "updated"
        assert result["photos_deleted"] == 1
        assert deleted_blobs == ["ChIJtest123/curator-attOLD789-old-photo.webp"]

    def test_publish_failure_handled_gracefully(self, monkeypatch, airtable_attachment_objects):
        place = _make_place(curator_attachments=airtable_attachment_objects)

        def fail_publish(self, attachment, place_id, record_id, place_name, dry_run=True, upload=False, try_url_variants=True):
            return {"success": False, "error": "Network error"}

        monkeypatch.setattr(curator_photos.PhotoPublisherService, "publish_curator_attachment", fail_publish)
        monkeypatch.setattr(curator_photos, "list_blobs_in_container", lambda *args, **kwargs: [])

        mock_table = mock.MagicMock()
        mock_api = mock.MagicMock()
        mock_api.table.return_value = mock_table
        monkeypatch.setattr("blueprints.curator_photos.AirtableApi", lambda token: mock_api)

        result = curator_photos.sync_single_place_curator_photos({"place": place, "config": {}})

        assert result["status"] == "failed"
        assert result["photos_synced"] == 0
        assert result["photos_failed"] == 2
        mock_table.update.assert_not_called()

    def test_publish_failure_does_not_delete_existing_curator_blobs(self, monkeypatch, airtable_attachment_objects):
        existing_curator_url = "https://thirdplacesdata.blob.core.windows.net/photos/ChIJtest123/curator-attABC123-cafe-interior.webp"
        place = _make_place(
            curator_attachments=airtable_attachment_objects,
            photos=json.dumps([existing_curator_url]),
        )

        def fail_publish(self, attachment, place_id, record_id, place_name, dry_run=True, upload=False, try_url_variants=True):
            return {"success": False, "error": "Network error"}

        monkeypatch.setattr(curator_photos.PhotoPublisherService, "publish_curator_attachment", fail_publish)
        monkeypatch.setattr(
            curator_photos,
            "list_blobs_in_container",
            lambda *args, **kwargs: ["ChIJtest123/curator-attABC123-cafe-interior.webp"],
        )
        deleted_blobs = []
        monkeypatch.setattr(curator_photos, "delete_blob_from_container", lambda container, path: (deleted_blobs.append(path), True)[1])

        mock_table = mock.MagicMock()
        mock_api = mock.MagicMock()
        mock_api.table.return_value = mock_table
        monkeypatch.setattr("blueprints.curator_photos.AirtableApi", lambda token: mock_api)

        result = curator_photos.sync_single_place_curator_photos({"place": place, "config": {}})

        assert result["status"] == "failed"
        assert result["photos_deleted"] == 0
        assert deleted_blobs == []
        mock_table.update.assert_not_called()
