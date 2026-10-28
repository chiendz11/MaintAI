"""SDK-only wheel checks must not import the optional owner HTTP application."""

import subprocess
import sys

import pytest


@pytest.mark.parametrize("command", ["maintai-processing-worker", "maintai-processing-relay"])
def test_console_help_does_not_import_owner_http_stack(command, tmp_path):
    code = """
import importlib.abc
from importlib.metadata import entry_points
import sys
class SDKOnly(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = {'fastapi','httpx','pydantic','uvicorn','prometheus_client','alembic'}
        if fullname.split('.')[0] in blocked:
            raise ImportError('HTTP application dependency is absent: ' + fullname)
sys.meta_path.insert(0, SDKOnly())
entry = next(e for e in entry_points(group='console_scripts') if e.name == sys.argv[1])
sys.argv = [sys.argv[1], '--help']
raise SystemExit(entry.load()())
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, command],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout
