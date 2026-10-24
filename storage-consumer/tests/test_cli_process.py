"""Real CLI, Kafka/S3 clients and management HTTP, targeting closed loopback ports."""

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

import pytest


def test_real_cli_is_live_but_not_ready_offline_and_handles_sigterm():
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("AWS_", "KAFKA_", "S3_", "STORAGE_"))
        and key.lower() not in {"http_proxy", "https_proxy", "all_proxy"}
    }
    env.update(
        {
            "KAFKA_LOCAL_MODE": "true",
            "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
            "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:1",
            "S3_RAW_BUCKET": "fixture-bucket",
            "S3_ENDPOINT_URL": "http://127.0.0.1:1",
            "AWS_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": "fixture-access",
            "AWS_SECRET_ACCESS_KEY": "SECRET_DO_NOT_LOG",
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_CONFIG_FILE": os.devnull,
            "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
            "NO_PROXY": "*",
            "STORAGE_HTTP_HOST": "127.0.0.1",
            "STORAGE_HTTP_PORT": str(port),
            "S3_CONNECT_TIMEOUT_SECONDS": "0.1",
            "S3_READ_TIMEOUT_SECONDS": "0.1",
            "STORAGE_PROBE_TIMEOUT_SECONDS": "0.1",
            "STORAGE_PROBE_INTERVAL_SECONDS": "0.1",
        }
    )
    child = subprocess.Popen(
        [sys.executable, "-m", "maintai_storage", "serve"],
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            assert child.poll() is None, "CLI failed before management HTTP started"
            try:
                with urllib.request.urlopen(url + "/health/live", timeout=0.2) as response:
                    assert json.load(response)["live"]
                    break
            except (OSError, urllib.error.URLError):
                time.sleep(0.02)
        else:
            pytest.fail("CLI management server did not start")
        with pytest.raises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(url + "/health/ready", timeout=1)
        assert error.value.code == 503
        child.send_signal(signal.SIGTERM)
        output, errors = child.communicate(timeout=8)
        assert child.returncode == 0
        assert "SECRET_DO_NOT_LOG" not in output + errors
        assert "127.0.0.1:1" not in errors
        assert "Traceback" not in errors
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate(timeout=5)
