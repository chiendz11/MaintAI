"""CI-only Docker smoke checks. No published ports, registry push or deployment."""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]


def docker(*args, check=True, include_stderr=False):
    result = subprocess.run(
        ["docker", *args], text=True, capture_output=True, timeout=120, check=False
    )
    if check and result.returncode:
        raise RuntimeError(f"docker {args[0]} failed: {result.stderr.strip()}")
    return (result.stdout + (result.stderr if include_stderr else "")).strip()


def request(container, path, *, method="GET", body=None, key=None, status=200):
    script = """
import json,sys,urllib.request,urllib.error
path,method,body,key,expected=sys.argv[1:]
headers={'Content-Type':'application/json'}
if key: headers['Idempotency-Key']=key
request=urllib.request.Request('http://127.0.0.1:8005'+path,method=method,
 data=body.encode() if body else None,headers=headers)
try:
 response=urllib.request.urlopen(request,timeout=3)
except urllib.error.HTTPError as error:
 response=error
assert response.status==int(expected),(path,response.status)
print(response.read().decode())
"""
    return docker(
        "exec",
        container,
        "python",
        "-c",
        script,
        path,
        method,
        json.dumps(body) if body is not None else "",
        key or "",
        str(status),
    )


def wait_ready(container, *, ui=False):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        try:
            if ui:
                docker(
                    "exec",
                    container,
                    "wget",
                    "-qO-",
                    "http://127.0.0.1:8080/health/ready",
                )
            else:
                request(container, "/health/ready")
            return
        except RuntimeError:
            time.sleep(0.5)
    raise RuntimeError("Container did not become ready: " + docker("logs", container, check=False))


def journal(container):
    return json.loads(
        docker(
            "exec",
            container,
            "python",
            "-c",
            """
import json,os,sqlite3
with sqlite3.connect(os.environ['SIM_STATE_PATH']) as db:
 sequence=db.execute("SELECT sequence_number FROM machines WHERE machine_id='M-001'").fetchone()[0]
 spool=db.execute('SELECT event_id,payload FROM spool ORDER BY id').fetchall()
 print(json.dumps({'sequence':sequence,'spool':spool}))
""",
        )
    )


def sensor(image):
    name, volume = (
        "maintai-sensor-smoke-" + uuid4().hex[:10],
        "maintai-state-" + uuid4().hex[:10],
    )
    docker("volume", "create", volume)
    try:
        # Docker named-volume copy-up preserves /app/state ownership for UID10001.
        docker(
            "run",
            "--detach",
            "--name",
            name,
            "--network",
            "none",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--mount",
            f"type=bind,source={ROOT / 'sensor/tests/fixtures/ai4i_sample.csv'},"
            "target=/app/data/ai4i2020.csv,readonly",
            "--mount",
            f"type=volume,source={volume},target=/app/state",
            "--env",
            "SIM_AUTH_MODE=local",
            "--env",
            "SIM_INGESTION_AUTH=none",
            "--env",
            "BACKEND_BASE_URL=http://127.0.0.1:9",
            "--env",
            "SIM_INITIAL_STATE=PAUSED",
            "--env",
            "SIM_MACHINE_COUNT=2",
            image,
            "serve",
            "--host",
            "127.0.0.1",
        )
        wait_ready(name)
        assert docker("exec", name, "id", "-u") == "10001"
        base = "/api/v1/simulator/machines"
        state = json.loads(request(name, base + "/M-001/control"))
        assert state["stream_state"] == "PAUSED"
        resumed = json.loads(
            request(
                name,
                base + "/M-001/resume",
                method="POST",
                body={"expected_control_version": 0},
                key="resume",
            )
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and journal(name)["sequence"] == 0:
            time.sleep(0.1)
        assert journal(name)["sequence"] > 0
        paused = json.loads(
            request(
                name,
                base + "/M-001/pause",
                method="POST",
                body={"expected_control_version": resumed["control_version"]},
                key="pause",
            )
        )
        before = journal(name)
        assert before["spool"]  # Ingestion offline; readings remain durable.
        assert "maintai_simulator_spool_pending_count" in request(name, "/metrics")
        assert json.loads(request(name, base + "/M-002/control"))["control_version"] == 0
        docker("restart", "--time", "15", name)
        wait_ready(name)
        assert json.loads(request(name, base + "/M-001/control")) == paused
        assert journal(name) == before
        # The old request version still gets its persisted receipt after restart.
        assert (
            json.loads(
                request(
                    name,
                    base + "/M-001/resume",
                    method="POST",
                    body={"expected_control_version": 0},
                    key="resume",
                )
            )
            == resumed
        )
        assert json.loads(request(name, base + "/M-001/control")) == paused
        request(
            name,
            base + "/M-001/pause",
            method="POST",
            body={"expected_control_version": 0},
            key="different",
            status=409,
        )
        print(
            "Sensor image: non-root, probes, metrics, control receipts, "
            "persistent sequence/spool restart PASS"
        )
    finally:
        if sys.exc_info()[0] is not None:
            print(docker("logs", name, check=False, include_stderr=True), file=sys.stderr)
        docker("rm", "--force", name, check=False)
        docker("volume", "rm", "--force", volume, check=False)


def ui(image):
    name = "maintai-ui-smoke-" + uuid4().hex[:10]
    try:
        docker(
            "run",
            "--detach",
            "--name",
            name,
            "--network",
            "none",
            "--read-only",
            "--tmpfs",
            "/tmp:rw,nosuid,noexec,size=16m",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            image,
        )
        wait_ready(name, ui=True)
        assert docker("exec", name, "id", "-u") == "101"
        docker("exec", name, "nginx", "-t")
        index = docker("exec", name, "wget", "-qO-", "http://127.0.0.1:8080/")
        assert '<div id="root"></div>' in index and "/assets/" in index
        config = json.loads(
            docker(
                "exec",
                name,
                "wget",
                "-qO-",
                "http://127.0.0.1:8080/runtime-config.json",
            )
        )
        assert config["mode"] == "demo"
        headers = docker(
            "exec",
            name,
            "wget",
            "-S",
            "-O-",
            "http://127.0.0.1:8080/runtime-config.json",
            include_stderr=True,
        )
        # Verify the actual HTTP header and effective Nginx config.
        effective = docker("exec", name, "nginx", "-T")
        assert "no-store" in effective
        assert "cache-control: no-store" in headers.lower()
        missing = subprocess.run(
            [
                "docker",
                "exec",
                name,
                "wget",
                "-qO-",
                "http://127.0.0.1:8080/api/v1/simulator/machines",
            ],
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        assert missing.returncode != 0 and '<div id="root">' not in missing.stdout
        print(
            "Simulator UI image: non-root/read-only, Nginx, probes, built assets, "
            "runtime config, API404 PASS"
        )
    finally:
        if sys.exc_info()[0] is not None:
            print(docker("logs", name, check=False, include_stderr=True), file=sys.stderr)
        docker("rm", "--force", name, check=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("component", choices=["sensor", "ui"])
    parser.add_argument("--image", required=True)
    args = parser.parse_args()
    (sensor if args.component == "sensor" else ui)(args.image)
