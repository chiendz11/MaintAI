"""Opt-in relay image entrypoint and privilege verification."""

import json
import os
import subprocess

import pytest

pytestmark = pytest.mark.integration


def test_relay_image_has_installed_entrypoint_and_nonroot_user():
    if os.environ.get("MAINTAI_M2_CONTAINER_TEST") != "1":
        pytest.skip("M2 container checks were not explicitly enabled")
    image = os.environ.get("MAINTAI_M2_RELAY_IMAGE", "maintai-processing-relay:m2-local")
    config = subprocess.run(
        ["docker", "image", "inspect", image], capture_output=True, text=True, timeout=15
    )
    assert config.returncode == 0, config.stderr
    assert json.loads(config.stdout)[0]["Config"]["User"] == "10001:10001"
    result = subprocess.run(
        ["docker", "run", "--rm", "--network", "none", "--read-only", image, "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout
