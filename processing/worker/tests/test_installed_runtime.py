"""Opt-in installed image checks; do not mistake --help for provider integration."""

import json
import os
import subprocess

import pytest

pytestmark = pytest.mark.integration


def test_worker_image_contains_installed_commands_and_nonroot_user():
    if os.environ.get("MAINTAI_M2_CONTAINER_TEST") != "1":
        pytest.skip("M2 container checks were not explicitly enabled")
    image = os.environ.get("MAINTAI_M2_WORKER_IMAGE", "maintai-processing-worker:m2-local")
    config = subprocess.run(
        ["docker", "image", "inspect", image], capture_output=True, text=True, timeout=15
    )
    assert config.returncode == 0, config.stderr
    assert json.loads(config.stdout)[0]["Config"]["User"] == "10001:10001"
    installed = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--read-only",
            "--entrypoint",
            "python",
            image,
            "-c",
            "from importlib.metadata import version; "
            "from maintai_messaging.contracts import default_contracts; "
            "from consumer.handler import KeyedProcessingHandler; "
            "import importlib; "
            "assert version('maintai-messaging') == '0.3.0'; "
            "assert default_contracts().bundle_version == '2.0.0'; "
            "migration = importlib.import_module('processing.migrations.versions.0002_ordering'); "
            "assert migration.down_revision == 'processing_0001'",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert installed.returncode == 0, installed.stderr
    for command in ("maintai-processing-worker", "maintai-processing-migrate"):
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
        assert "usage:" in result.stdout


def test_worker_image_does_not_start_shared_with_missing_provider_handoff():
    if os.environ.get("MAINTAI_M2_CONTAINER_TEST") != "1":
        pytest.skip("M2 container checks were not explicitly enabled")
    image = os.environ.get("MAINTAI_M2_WORKER_IMAGE", "maintai-processing-worker:m2-local")
    result = subprocess.run(
        ["docker", "run", "--rm", "--network", "none", image],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 1
    assert "Traceback" not in result.stderr
