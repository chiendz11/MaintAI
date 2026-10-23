import os
import subprocess
import sys


def test_cli_configures_observable_json_logs():
    code = """
import logging
from unittest.mock import patch
from maintai_ingestion.cli import main

def run(app, **kwargs):
    logging.getLogger('maintai.ingestion').info('{"probe":"json-request-log"}')

with patch('maintai_ingestion.cli.uvicorn.run', run):
    main()
"""
    env = {
        **os.environ,
        "INGESTION_ENV": "test",
        "INGESTION_HOST": "127.0.0.1",
        "AUTH_MODE": "local-fixture",
        "LOCAL_SOURCE_SUBJECT": "s",
        "LOCAL_ALLOWED_MACHINE_IDS": "M-001",
        "BUSINESS_PUBLIC_ORIGIN": "http://127.0.0.1:8003",
        "KAFKA_LOCAL_MODE": "true",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
    }
    result = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True, timeout=5
    )
    assert result.returncode == 0
    assert '{"probe":"json-request-log"}' in result.stderr
