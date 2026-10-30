"""Opt-in Business history configuration and owner-local runtime construction."""

import base64
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

import httpx
from backend.shared.persistence.history_markers import MarkerSnapshots
from business_processing_client.delegation import DelegationSigner
from business_processing_client.history import ProcessingHistoryClient
from business_processing_client.history_schemas import HistorySchemas
from maintai_messaging.contracts import strict_json

from .history import HistoryFacade
from .history_charts import CompositeCharts


@dataclass(frozen=True)
class HistorySettings:
    signing_key_file: str = field(repr=False)
    signing_key_id: str
    delegation_issuer: str
    service_subject: str
    cursor_key_file: str = field(repr=False)

    @classmethod
    def from_env(cls, env, *, processing_url, local):
        from .config import loopback

        enabled = env.get("BUSINESS_HISTORY_ENABLED", "false").lower()
        if enabled not in {"true", "false"}:
            raise ValueError("History enable flag must be true or false")
        if enabled == "false":
            return None
        names = (
            "BUSINESS_HISTORY_SIGNING_KEY_FILE",
            "BUSINESS_HISTORY_SIGNING_KEY_ID",
            "BUSINESS_HISTORY_DELEGATION_ISSUER",
            "BUSINESS_HISTORY_SERVICE_SUBJECT",
            "BUSINESS_HISTORY_CURSOR_KEY_FILE",
        )
        if not processing_url or any(not env.get(name, "").strip() for name in names):
            raise ValueError(
                "M6 requires Processing credentials, explicit signing identity and cursor keys"
            )
        key, kid, issuer, subject, cursors = (env[name] for name in names)
        parsed = urlparse(issuer)
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
            raise ValueError("Delegation issuer must follow the selected TLS/local profile")
        for path in (key, cursors):
            if not Path(path).is_file() or Path(path).stat().st_size > 16384:
                raise ValueError("History key files must be explicit and bounded")
        return cls(key, kid, issuer, subject, cursors)


def build(settings, connection_factory):
    config = settings.history
    raw = Path(config.cursor_key_file).read_bytes()
    private = Path(config.signing_key_file).read_bytes()
    if len(raw) > 16384 or len(private) > 16384:
        raise ValueError("History key file exceeds bound")
    cursor = strict_json(raw)
    if set(cursor) != {"active", "keys"}:
        raise ValueError("Invalid cursor key document")
    keys = {key: base64.b64decode(value, validate=True) for key, value in cursor["keys"].items()}
    repository = MarkerSnapshots(connection_factory, keys, active=cursor["active"])
    signer = DelegationSigner(
        private,
        kid=config.signing_key_id,
        issuer=config.delegation_issuer,
        service_subject=config.service_subject,
    )
    schemas = HistorySchemas()
    with connection_factory() as connection:
        connection.execute("SELECT snapshot_id FROM history_marker_snapshots LIMIT 0")
    client = ProcessingHistoryClient(
        httpx.AsyncClient(trust_env=False, follow_redirects=False),
        base_url=settings.processing_read_url,
        token_file=settings.processing_token_file,
        signer=signer,
    )
    return HistoryFacade(client, schemas, markers=CompositeCharts(client, schemas, repository))
