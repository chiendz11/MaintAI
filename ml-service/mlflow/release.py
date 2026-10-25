"""App-owned MLflow image publication. No cluster writes or automatic merge/enable."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import jsonschema

MODULE = Path(__file__).resolve().parent
ROOT = MODULE.parents[1]
SOURCE_REPOSITORY = "chiendz11/MaintAI"
TARGET_REPOSITORY = "chiendz11/MaintAI-gitops"
WORKFLOW = "maintai-publish.yml"
ARCHITECTURE = "maintai-1.3.1+simulator-ui.1"  # Existing GitOps handoff, not app wire version.
IDENTITY = f"https://github.com/{SOURCE_REPOSITORY}/.github/workflows/{WORKFLOW}@refs/heads/main"
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
ECR = re.compile(r"([0-9]{12})\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com/[a-z0-9/_-]+")
BASE = re.compile(r"ghcr\.io/mlflow/mlflow(?::[A-Za-z0-9_.-]+)?@sha256:[0-9a-f]{64}")


def read_lock(*, require_ready=False):
    lock = json.loads((MODULE / "image-lock.json").read_text())
    expected = {
        "schema_version": "1.0.0",
        "image_group": "mlflow",
        "source_repository": SOURCE_REPOSITORY,
        "publisher_workflow": WORKFLOW,
        "dockerfile": "ml-service/mlflow/Dockerfile",
        "build_context": "ml-service/mlflow",
    }
    if any(lock.get(key) != value for key, value in expected.items()):
        raise ValueError("Unexpected MLflow image ownership/build configuration")
    image, version = lock.get("base_image"), lock.get("mlflow_version")
    if image is not None and not BASE.fullmatch(image):
        raise ValueError("Base image must be an official upstream MLflow image digest")
    if version is not None and not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ValueError("Pin an exact stable server patch version")
    if type(lock.get("compatibility_reviewed")) is not bool:
        raise ValueError("Compatibility review must be an explicit boolean")
    if require_ready and (not image or not version or not lock["compatibility_reviewed"]):
        raise ValueError("MLflow base/version/compatibility pins are not ready for publication")
    if any(not (MODULE / name).is_file() for name in ("Dockerfile", "requirements.lock")):
        raise ValueError("MLflow image source and dependency lock are required")
    return lock


def validate_contract():
    path = MODULE / "contracts/workload-release-v1.schema.json"
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    expected = json.loads((MODULE / "contracts/lock.json").read_text())["sha256"]
    if actual != expected:
        raise ValueError("GitOps handoff contract changed without reviewed lock update")
    schema = json.loads(path.read_text())
    jsonschema.Draft202012Validator.check_schema(schema)
    return jsonschema.Draft202012Validator(schema)


def make_intent(source_sha, repository, digest):
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha) or not ECR.fullmatch(repository):
        raise ValueError("Invalid source SHA or ECR image repository")
    if not DIGEST.fullmatch(digest):
        raise ValueError("Registry did not return an immutable image digest")
    intent = {
        "schema_version": "1.0.0",
        "change_id": "mlflow-" + source_sha[:24],
        "source_repository": SOURCE_REPOSITORY,
        "source_sha": source_sha,
        "architecture_revision": ARCHITECTURE,
        "image_group": "mlflow",
        "image": repository + "@" + digest,
        "runtime_config_version": "2.0.0",
        "config_schema_sha256": {},  # Platform config remains receiver/operator owned.
    }
    validate_contract().validate(intent)
    return intent


def capture(command, **kwargs):
    return subprocess.check_output(command, cwd=ROOT, text=True, **kwargs).strip()


def run(command, **kwargs):
    subprocess.run(command, cwd=ROOT, check=True, **kwargs)


def signature_arguments(source_sha):
    return [
        "-a",
        "git_sha=" + source_sha,
        "-a",
        "image_group=mlflow",
        "-a",
        "source_repository=" + SOURCE_REPOSITORY,
    ]


def smoke(image, version):
    imports = (
        "import mlflow,boto3,psycopg2; "
        f"assert mlflow.__version__ == {version!r}; "
        "print('MLflow server version and PostgreSQL/S3 adapters imported')"
    )
    run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--read-only",
            "--tmpfs",
            "/tmp",
            "--entrypoint",
            "python",
            image,
            "-c",
            imports,
        ]
    )
    help_text = capture(["docker", "run", "--rm", "--network", "none", image, "server", "--help"])
    if any(flag not in help_text for flag in ("--serve-artifacts", "--allowed-hosts")):
        raise ValueError("MLflow server does not support the reviewed deployment flags")
    if (
        capture(["docker", "image", "inspect", "--format", "{{.Config.User}}", image])
        != "10001:10001"
    ):
        raise ValueError("MLflow image must run as the reviewed non-root UID")


def publish():
    lock = read_lock(require_ready=True)
    validate_contract()
    source_sha = os.environ.get("SOURCE_SHA", "")
    if (
        os.environ.get("GITHUB_REPOSITORY") != SOURCE_REPOSITORY
        or os.environ.get("GITHUB_REF") != "refs/heads/main"
        or os.environ.get("GITHUB_SHA") != source_sha
        or os.environ.get("IMAGE_GROUP") != "mlflow"
        or not re.fullmatch(r"[0-9a-f]{40}", source_sha)
    ):
        raise ValueError("Publication requires the reviewed MaintAI main workflow context")
    if capture(["git", "rev-parse", "HEAD"]) != source_sha:
        raise ValueError("Checkout differs from claimed source SHA")
    run(["git", "diff", "--exit-code", "HEAD", "--"])
    repository = json.loads(os.environ["IMAGE_REPOSITORIES_JSON"])["mlflow"]
    match = ECR.fullmatch(repository)
    if not match or match.group(2) != os.environ.get("AWS_REGION"):
        raise ValueError("Unexpected ECR target or region")
    password = capture(["aws", "ecr", "get-login-password"])
    run(
        ["docker", "login", "--username", "AWS", "--password-stdin", repository.split("/")[0]],
        input=password,
        text=True,
        stdout=subprocess.DEVNULL,
    )
    tag = repository + ":git-" + source_sha
    lookup_command = [
        "aws",
        "ecr",
        "describe-images",
        "--repository-name",
        repository.split("/", 1)[1],
        "--image-ids",
        "imageTag=git-" + source_sha,
        "--output",
        "json",
    ]
    existing = subprocess.run(lookup_command, cwd=ROOT, text=True, capture_output=True)
    if existing.returncode == 0:
        digest = json.loads(existing.stdout)["imageDetails"][0]["imageDigest"]
        image = make_intent(source_sha, repository, digest)["image"]
        # Do not bless an unsigned/foreign image merely because an immutable tag exists.
        run(
            [
                "cosign",
                "verify",
                "--certificate-identity",
                IDENTITY,
                "--certificate-oidc-issuer",
                "https://token.actions.githubusercontent.com",
                *signature_arguments(source_sha),
                image,
            ],
            stdout=subprocess.DEVNULL,
        )
    elif "ImageNotFoundException" not in existing.stderr:
        raise ValueError("Registry image lookup failed; no new image was published")
    else:
        run(
            [
                "docker",
                "buildx",
                "build",
                "--load",
                "--platform",
                "linux/amd64",
                "--build-arg",
                "MLFLOW_BASE_IMAGE=" + lock["base_image"],
                "--label",
                "org.opencontainers.image.source=https://github.com/" + SOURCE_REPOSITORY,
                "--label",
                "org.opencontainers.image.revision=" + source_sha,
                "--file",
                lock["dockerfile"],
                "--tag",
                tag,
                lock["build_context"],
            ]
        )
        smoke(tag, lock["mlflow_version"])
        run(["docker", "push", tag])
        digest = json.loads(capture(lookup_command))["imageDetails"][0]["imageDigest"]
        image = make_intent(source_sha, repository, digest)["image"]
        run(["cosign", "sign", "--yes", *signature_arguments(source_sha), image])
    intent = make_intent(source_sha, repository, digest)
    output = Path(os.environ["RUNNER_TEMP"]) / "workload-intent.json"
    output.write_text(json.dumps(intent, indent=2) + "\n")
    print("Signed MLflow image ready; GitOps proposal/merge/enablement are separate stages.")


def dispatch():
    from github_app import dispatch_workload

    path = Path(os.environ["RUNNER_TEMP"]) / "workload-intent.json"
    intent = json.loads(path.read_text())
    validate_contract().validate(intent)
    expected = make_intent(
        intent["source_sha"], intent["image"].split("@", 1)[0], intent["image"].split("@", 1)[1]
    )
    if intent != expected:
        raise ValueError("Dispatch must use the exact MLflow app-owned intent")
    dispatch_workload(intent)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["check-source", "publish", "dispatch"])
    args = parser.parse_args(argv)
    try:
        if args.command == "check-source":
            lock = read_lock()
            validate_contract()
            print(
                json.dumps(
                    {
                        "image_group": "mlflow",
                        "source_repository": SOURCE_REPOSITORY,
                        "publication_ready": bool(
                            lock["base_image"]
                            and lock["mlflow_version"]
                            and lock["compatibility_reviewed"]
                        ),
                        "network_checked": False,
                    }
                )
            )
        elif args.command == "publish":
            publish()
        else:
            dispatch()
        return 0
    except Exception as exc:
        print(
            json.dumps({"event": "mlflow_release_failed", "error_type": type(exc).__name__}),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
