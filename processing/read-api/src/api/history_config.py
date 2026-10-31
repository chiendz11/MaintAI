"""Explicit M6 opt-in; metadata credentials never replace processing_ro."""

import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse


@dataclass(frozen=True)
class HistorySettings:
    metadata_url: str = field(repr=False)
    delegation_issuer: str
    delegation_jwks_url: str
    cursor_key_file: str = field(repr=False)
    meta_schema: str = "history_meta"
    raw_bucket: str | None = None
    export_bucket: str | None = None
    s3_endpoint: str | None = None
    s3_region: str | None = None

    @classmethod
    def from_env(cls, env, source_url, *, local):
        from .config import loopback

        if (
            env.get("PROCESSING_RETENTION_ENABLED", "false").lower() != "false"
            or env.get("PROCESSING_CLEANER_DRY_RUN", "true").lower() != "true"
        ):
            raise ValueError("M6 only supports disabled source retention and read-only dry-run")
        enabled = env.get("PROCESSING_HISTORY_ENABLED", "false").lower()
        if enabled not in {"true", "false"}:
            raise ValueError("History enable flag must be true or false")
        if enabled == "false":
            return None
        required = [
            "PROCESSING_HISTORY_METADATA_DATABASE_URL",
            "PROCESSING_HISTORY_DELEGATION_ISSUER",
            "PROCESSING_HISTORY_DELEGATION_JWKS_URL",
            "PROCESSING_HISTORY_CURSOR_KEY_FILE",
        ]
        if any(not env.get(key, "").strip() for key in required):
            raise ValueError(
                "M6 requires metadata credential, delegation issuer/keys and cursor key file"
            )
        metadata, issuer, jwks, key_file = (env[key] for key in required)
        source, target = urlparse(source_url), urlparse(metadata)
        if (
            target.scheme not in {"postgres", "postgresql"}
            or not target.username
            or target.username == source.username
            or (target.hostname, target.port, target.path)
            != (source.hostname, source.port, source.path)
        ):
            raise ValueError(
                "History metadata needs a distinct credential for the same Processing database"
            )
        schema = env.get("PROCESSING_HISTORY_META_SCHEMA", "history_meta")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema):
            raise ValueError("Invalid metadata schema")
        for value in (issuer, jwks):
            url = urlparse(value)
            if (
                not url.hostname
                or url.username
                or url.password
                or url.query
                or url.fragment
                or url.scheme not in {"http", "https"}
            ):
                raise ValueError("Invalid history delegation endpoint")
            if local and not loopback(url.hostname) or not local and url.scheme != "https":
                raise ValueError("Delegation endpoints must follow the selected TLS/local profile")
        if not Path(key_file).is_file() or Path(key_file).stat().st_size > 16384:
            raise ValueError("Cursor key file must be explicit and bounded")
        raw, export = (
            env.get("PROCESSING_HISTORY_RAW_BUCKET"),
            env.get("PROCESSING_HISTORY_EXPORT_BUCKET"),
        )
        if bool(raw) != bool(export):
            raise ValueError("Object storage requires both raw and private export buckets")
        endpoint, region = env.get("PROCESSING_HISTORY_S3_ENDPOINT"), env.get("AWS_REGION")
        if raw:
            if not region:
                raise ValueError("Explicit AWS_REGION is required for history object storage")
            if endpoint:
                parsed = urlparse(endpoint)
                if (
                    not parsed.hostname
                    or parsed.username
                    or parsed.password
                    or parsed.query
                    or parsed.fragment
                    or parsed.scheme not in {"http", "https"}
                    or (local and not loopback(parsed.hostname))
                    or (not local and parsed.scheme != "https")
                ):
                    raise ValueError("Unsafe history S3 endpoint")
        elif endpoint:
            raise ValueError("S3 endpoint requires explicit buckets")
        return cls(metadata, issuer, jwks, key_file, schema, raw, export, endpoint, region)
