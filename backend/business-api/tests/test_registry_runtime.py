"""Installed Business registry commands and optional image checks."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "command", ["maintai-business-api", "maintai-business-migrate", "maintai-business-provision"]
)
def test_installed_registry_commands_have_help_outside_checkout(command, tmp_path):
    result = subprocess.run(
        [str(Path(sys.executable).parent / command), "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout


@pytest.mark.integration
def test_registry_image_is_nonroot_installed_and_rejects_missing_config():
    if os.environ.get("MAINTAI_M2_CONTAINER_TEST") != "1":
        pytest.skip("Registry image checks were not enabled")
    image = "maintai-business-registry:m2-local"
    config = subprocess.run(
        ["docker", "image", "inspect", image], capture_output=True, text=True, timeout=15
    )
    assert config.returncode == 0, config.stderr
    assert json.loads(config.stdout)[0]["Config"]["User"] == "10001:10001"
    for command in (
        "maintai-business-api",
        "maintai-business-migrate",
        "maintai-business-provision",
    ):
        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--read-only",
                "--entrypoint",
                command,
                image,
                "--help",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stderr
    result = subprocess.run(
        ["docker", "run", "--rm", "--network", "none", image],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 1
    assert "Traceback" not in result.stderr
