from services.utils import sanitize_blob_metadata


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
