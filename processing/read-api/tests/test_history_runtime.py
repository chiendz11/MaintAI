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


def test_catalog_pending_import_continues_without_sixty_second_idle():
    import threading
    from types import SimpleNamespace

    from processing_read_api.history_api import HistoryRuntime

    attempted = threading.Event()
    count = []

    def sync():
        count.append(1)
        if len(count) == 2:
            attempted.set()
        return None

    runtime = HistoryRuntime(
        SimpleNamespace(catalog=SimpleNamespace(sync=sync)),
        None,
        SimpleNamespace(run_once=lambda: None),
        None,
        None,
    )
    runtime.start()
    try:
        assert attempted.wait(2), "Pending catalog import slept for the idle interval"
    finally:
        runtime.close()
