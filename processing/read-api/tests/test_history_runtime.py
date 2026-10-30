"""Runtime construction keeps RO source and RW metadata credentials separate."""

import json

import pytest
from processing_read_api.history_runtime import cursor_keys

from .test_history_settings import configured


def test_cursor_key_file_is_explicit_and_strict(tmp_path):
    env = configured(tmp_path)
    path = env["PROCESSING_HISTORY_CURSOR_KEY_FILE"]
    keys = cursor_keys(path)
    assert keys.active == "one"
    with open(path, "w") as stream:
        json.dump({"active": "one", "keys": {"one": "not-base64"}}, stream)
    with pytest.raises(ValueError):
        cursor_keys(path)
