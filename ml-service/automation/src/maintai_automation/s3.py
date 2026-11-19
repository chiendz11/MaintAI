"""Fail-closed MinIO / S3 helpers enforcing VersionId and SHA-256 verification."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path

import boto3
from botocore.config import Config

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class S3VerificationError(RuntimeError):
    """Raised when S3/MinIO VersionId or SHA-256 verification fails."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1048576):
            digest.update(chunk)
    return digest.hexdigest()


def validate_sha256(value: str, label: str = "sha256") -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise S3VerificationError(f"Invalid {label}: expected 64-char lowercase hex SHA-256")
    return value


def validate_version_id(value: object, label: str = "version_id") -> str:
    if not isinstance(value, str) or not value or value == "null":
        raise S3VerificationError(f"Missing or invalid S3 {label} (bucket versioning required)")
    return value


def make_s3_client(
    *,
    endpoint_url: str | None = None,
    region_name: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
):
    endpoint = endpoint_url if endpoint_url is not None else os.environ.get("S3_ENDPOINT_URL")
    region = (
        region_name
        or os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "us-east-1"
    )
    cfg = Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3, "mode": "standard"})
    kwargs: dict = {
        "region_name": region,
        "config": cfg,
    }
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    if access_key is not None and secret_key is not None:
        kwargs["aws_access_key_id"] = access_key
        kwargs["aws_secret_access_key"] = secret_key
    return boto3.client("s3", **kwargs)


def _stream_and_hash(body, dest: Path) -> str:
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".download-", dir=dest.parent)
    tmp_path = Path(tmp_name)
    digest = hashlib.sha256()
    try:
        with os.fdopen(fd, "wb") as out:
            while chunk := body.read(1048576):
                digest.update(chunk)
                out.write(chunk)
        os.chmod(tmp_path, 0o644)
        actual = digest.hexdigest()
        os.replace(tmp_path, dest)
        return actual
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    finally:
        body.close()


def download_verified(
    client,
    bucket: str,
    key: str,
    dest: Path,
    *,
    expected_version_id: str,
    expected_sha256: str,
) -> dict[str, str]:
    version_id = validate_version_id(expected_version_id, "expected_version_id")
    expected_hex = validate_sha256(expected_sha256, "expected_sha256")
    response = client.get_object(Bucket=bucket, Key=key, VersionId=version_id)
    actual_version = validate_version_id(response.get("VersionId"), "response VersionId")
    if actual_version != version_id:
        response["Body"].close()
        raise S3VerificationError(
            f"S3 VersionId mismatch for s3://{bucket}/{key}: "
            f"expected {version_id}, got {actual_version}"
        )
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".verified-", dir=dest.parent)
    tmp_path = Path(tmp_name)
    digest = hashlib.sha256()
    try:
        body = response["Body"]
        try:
            with os.fdopen(fd, "wb") as out:
                while chunk := body.read(1048576):
                    digest.update(chunk)
                    out.write(chunk)
        finally:
            body.close()
        actual_hex = digest.hexdigest()
        if actual_hex != expected_hex:
            raise S3VerificationError(
                f"SHA-256 mismatch for s3://{bucket}/{key}@{version_id}: "
                f"expected {expected_hex}, got {actual_hex}"
            )
        os.chmod(tmp_path, 0o644)
        os.replace(tmp_path, dest)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return {"bucket": bucket, "key": key, "version_id": actual_version, "sha256": actual_hex}


def download_object_locking_version(
    client,
    bucket: str,
    key: str,
    dest: Path,
    *,
    expected_version_id: str | None = None,
) -> dict[str, str]:
    kwargs = {"Bucket": bucket, "Key": key}
    if expected_version_id:
        kwargs["VersionId"] = validate_version_id(expected_version_id, "manifest_version_id")
    response = client.get_object(**kwargs)
    actual_version = validate_version_id(response.get("VersionId"), "manifest VersionId")
    if expected_version_id and actual_version != expected_version_id:
        response["Body"].close()
        raise S3VerificationError(
            f"Manifest VersionId mismatch: expected {expected_version_id}, got {actual_version}"
        )
    actual_sha = _stream_and_hash(response["Body"], dest)
    return {"bucket": bucket, "key": key, "version_id": actual_version, "sha256": actual_sha}


def upload_file_versioned(client, bucket: str, key: str, source: Path) -> dict[str, str]:
    data = source.read_bytes()
    actual_sha = sha256_bytes(data)
    response = client.put_object(Bucket=bucket, Key=key, Body=data)
    version_id = validate_version_id(response.get("VersionId"), f"put_object({key}) VersionId")
    return {
        "bucket": bucket,
        "key": key,
        "version_id": version_id,
        "sha256": actual_sha,
    }
