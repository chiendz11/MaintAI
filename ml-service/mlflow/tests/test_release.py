"""Offline release checks. Fixtures never publish/sign/dispatch real artifacts."""

import importlib.util
import io
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

MODULE = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("maintai_mlflow_release", MODULE / "release.py")
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)
SHA = "a" * 40
REPOSITORY = "123456789012.dkr.ecr.ap-southeast-1.amazonaws.com/maintai/mlflow"
DIGEST = "sha256:" + "b" * 64


@pytest.fixture
def ready_lock():
    return {
        **release.read_lock(),
        "base_image": "ghcr.io/mlflow/mlflow@sha256:" + "c" * 64,
        "mlflow_version": "3.0.0",
        "compatibility_reviewed": True,
    }


def test_source_check_does_not_open_network_or_publish(monkeypatch, capsys):
    monkeypatch.setattr(release, "run", lambda *_a, **_k: pytest.fail("External command"))
    assert release.main(["check-source"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["source_repository"] == "chiendz11/MaintAI"
    assert output["network_checked"] is False


@pytest.mark.parametrize(
    "change",
    [
        {"source_repository": "chiendz11/MaintAI-gitops"},
        {"publisher_workflow": "other.yml"},
        {"build_context": ".."},
        {"base_image": "ghcr.io/mlflow/mlflow:latest"},
        {"base_image": "registry.invalid/foreign@sha256:" + "c" * 64},
        {"mlflow_version": "3.x"},
        {"compatibility_reviewed": "true"},
    ],
)
def test_invalid_source_or_mutable_image_lock_is_rejected(
    tmp_path, monkeypatch, ready_lock, change
):
    (tmp_path / "image-lock.json").write_text(json.dumps({**ready_lock, **change}))
    monkeypatch.setattr(release, "MODULE", tmp_path)
    with pytest.raises(ValueError):
        release.read_lock()


@pytest.mark.parametrize(
    "change",
    [
        {"base_image": None},
        {"mlflow_version": None},
        {"compatibility_reviewed": False},
    ],
)
def test_unconfigured_release_is_blocked_before_external_calls(
    tmp_path, monkeypatch, ready_lock, change
):
    (tmp_path / "image-lock.json").write_text(json.dumps({**ready_lock, **change}))
    monkeypatch.setattr(release, "MODULE", tmp_path)
    monkeypatch.setattr(release, "run", lambda *_a, **_k: pytest.fail("External command"))
    with pytest.raises(ValueError, match="not ready"):
        release.publish()


def test_intent_uses_existing_gitops_contract_without_enable_or_platform_config():
    intent = release.make_intent(SHA, REPOSITORY, DIGEST)
    assert intent["source_repository"] == "chiendz11/MaintAI"
    assert intent["image_group"] == "mlflow" and intent["image"].endswith("@" + DIGEST)
    assert intent["config_schema_sha256"] == {}
    assert "enable" not in intent and "runtime_configs" not in intent
    assert release.make_intent(SHA, REPOSITORY, DIGEST) == intent


@pytest.mark.parametrize(
    "sha,repo,digest",
    [
        ("not-a-sha", REPOSITORY, DIGEST),
        (SHA, "registry.invalid/image", DIGEST),
        (SHA, REPOSITORY, "latest"),
    ],
)
def test_invalid_artifact_identity_cannot_be_dispatched(sha, repo, digest):
    with pytest.raises(ValueError):
        release.make_intent(sha, repo, digest)


def setup_publisher(monkeypatch, tmp_path, ready_lock, *, existing=False, signature_fails=False):
    env = {
        "SOURCE_SHA": SHA,
        "GITHUB_SHA": SHA,
        "GITHUB_REPOSITORY": "chiendz11/MaintAI",
        "GITHUB_REF": "refs/heads/main",
        "IMAGE_GROUP": "mlflow",
        "IMAGE_REPOSITORIES_JSON": json.dumps({"mlflow": REPOSITORY}),
        "AWS_REGION": "ap-southeast-1",
        "RUNNER_TEMP": str(tmp_path),
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(release, "read_lock", lambda **_kw: ready_lock)
    commands = []

    def capture(command, **_kwargs):
        commands.append(command)
        if command[:2] == ["git", "rev-parse"]:
            return SHA
        if command[:3] == ["aws", "ecr", "get-login-password"]:
            return "secret-registry-password"
        if command[:3] == ["aws", "ecr", "describe-images"]:
            return json.dumps({"imageDetails": [{"imageDigest": DIGEST}]})
        raise AssertionError(command)

    def run(command, **_kwargs):
        commands.append(command)
        if signature_fails and command[:2] == ["cosign", "verify"]:
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(release, "capture", capture)
    monkeypatch.setattr(release, "run", run)
    monkeypatch.setattr(
        release, "smoke", lambda image, version: commands.append(["smoke", image, version])
    )
    monkeypatch.setattr(
        release.subprocess,
        "run",
        lambda *_a, **_kw: SimpleNamespace(
            returncode=0 if existing else 255,
            stdout=json.dumps({"imageDetails": [{"imageDigest": DIGEST}]}),
            stderr="" if existing else "ImageNotFoundException",
        ),
    )
    return commands


def test_new_image_is_smoked_before_push_and_signed_by_app_identity(
    monkeypatch, tmp_path, ready_lock
):
    commands = setup_publisher(monkeypatch, tmp_path, ready_lock)
    release.publish()
    build = next(command for command in commands if command[:3] == ["docker", "buildx", "build"])
    assert "MLFLOW_BASE_IMAGE=" + ready_lock["base_image"] in build
    assert build[-1] == "ml-service/mlflow" and "--load" in build
    smoke_at = next(i for i, c in enumerate(commands) if c[0] == "smoke")
    push_at = next(i for i, c in enumerate(commands) if c[:2] == ["docker", "push"])
    assert smoke_at < push_at
    sign = next(c for c in commands if c[:2] == ["cosign", "sign"])
    assert "source_repository=chiendz11/MaintAI" in sign
    assert "git_sha=" + SHA in sign and "image_group=mlflow" in sign
    intent = json.loads((tmp_path / "workload-intent.json").read_text())
    assert intent == release.make_intent(SHA, REPOSITORY, DIGEST)


def test_rerun_reuses_only_previously_verified_app_image(monkeypatch, tmp_path, ready_lock):
    commands = setup_publisher(monkeypatch, tmp_path, ready_lock, existing=True)
    release.publish()
    verify = next(c for c in commands if c[:2] == ["cosign", "verify"])
    assert release.IDENTITY in verify
    assert "source_repository=chiendz11/MaintAI" in verify
    assert not any(c[:2] in (["docker", "push"], ["cosign", "sign"]) for c in commands)
    assert (tmp_path / "workload-intent.json").exists()


def test_existing_unsigned_or_foreign_image_is_not_resigned(monkeypatch, tmp_path, ready_lock):
    commands = setup_publisher(
        monkeypatch, tmp_path, ready_lock, existing=True, signature_fails=True
    )
    with pytest.raises(subprocess.CalledProcessError):
        release.publish()
    assert not any(c[:2] == ["cosign", "sign"] for c in commands)
    assert not (tmp_path / "workload-intent.json").exists()


def test_source_sha_mismatch_stops_before_registry_login(monkeypatch, tmp_path, ready_lock):
    commands = setup_publisher(monkeypatch, tmp_path, ready_lock)
    monkeypatch.setenv("GITHUB_SHA", "d" * 40)
    with pytest.raises(ValueError, match="context"):
        release.publish()
    assert commands == []


def test_handoff_schema_tampering_fails_offline(tmp_path, monkeypatch):
    directory = tmp_path / "contracts"
    directory.mkdir()
    (directory / "lock.json").write_bytes((MODULE / "contracts/lock.json").read_bytes())
    (directory / "workload-release-v1.schema.json").write_text("{}")
    monkeypatch.setattr(release, "MODULE", tmp_path)
    with pytest.raises(ValueError, match="contract changed"):
        release.validate_contract()


def test_github_app_token_is_scoped_to_receiver_and_never_printed(monkeypatch, capsys):
    spec = importlib.util.spec_from_file_location(
        "maintai_mlflow_github_app", MODULE / "github_app.py"
    )
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    monkeypatch.setenv("GITHUB_APP_ID", "123")
    monkeypatch.setenv("GITHUB_APP_INSTALLATION_ID", "456")
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", "private-key-never-log")
    monkeypatch.setattr(helper.jwt, "encode", lambda *_a, **_kw: "fixture-jwt")
    calls = []

    class Response(io.StringIO):
        status = 204

    def open_request(request, *, timeout):
        assert timeout == 30
        calls.append(request)
        return Response('{"token":"fixture-installation-token"}')

    helper.dispatch_workload(release.make_intent(SHA, REPOSITORY, DIGEST), opener=open_request)
    payload = json.loads(calls[0].data)
    assert payload == {"repositories": ["MaintAI-gitops"], "permissions": {"actions": "write"}}
    assert calls[1].full_url.endswith(
        "/MaintAI-gitops/actions/workflows/workload-release.yml/dispatches"
    )
    assert json.loads(calls[1].data)["ref"] == "main"
    output = capsys.readouterr().out
    assert "fixture-installation-token" not in output and "private-key-never-log" not in output
