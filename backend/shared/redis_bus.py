"""Optional Redis/Valkey hot state, Pub/Sub invalidations, and Brief Streams bus."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import redis
from maintai_messaging.contracts import canonical_json

from backend.shared.notifications import machine_key
from backend.shared.service_auth import loopback, read_credential_file

BRIEF_STREAM = "maintai:m7:brief_jobs"
BRIEF_GROUP = "maintai-brief-workers"
LIVE_HASH_PREFIX = "maintai:business:live:"
LIVE_HASH_TTL_SECONDS = 86400

_UPDATE_LIVE_LUA = """
local key = KEYS[1]
local ttl = tonumber(ARGV[1])
local as_of = ARGV[2]

local function seq_cmp(a, b)
  if #a ~= #b then
    if #a > #b then return 1 else return -1 end
  end
  if a == b then return 0 end
  if a > b then return 1 else return -1 end
end

local updated = 0

if ARGV[3] == '1' then
  local new_seq = ARGV[4]
  local cur_seq = redis.call('HGET', key, 'telemetry_seq')
  if (not cur_seq) or seq_cmp(new_seq, cur_seq) >= 0 then
    redis.call('HSET', key, 'telemetry', ARGV[5], 'telemetry_seq', new_seq)
    updated = 1
  end
end

if ARGV[6] == '1' then
  local new_seq = ARGV[7]
  local new_status = ARGV[8]
  local cur_seq = redis.call('HGET', key, 'processing_seq')
  local cur_status = redis.call('HGET', key, 'processing_status')
  local apply = false
  if not cur_seq then
    apply = true
  else
    local cmp = seq_cmp(new_seq, cur_seq)
    if cmp > 0 then
      apply = true
    elseif cmp == 0 and cur_status == 'PENDING' and new_status ~= 'PENDING' then
      apply = true
    end
  end
  if apply then
    redis.call(
      'HSET',
      key,
      'latest_processing',
      ARGV[9],
      'processing_seq',
      new_seq,
      'processing_status',
      new_status
    )
    updated = 1
  end
end

if ARGV[10] == '1' then
  local new_seq = ARGV[11]
  local cur_seq = redis.call('HGET', key, 'prediction_seq')
  if (not cur_seq) or seq_cmp(new_seq, cur_seq) > 0 then
    redis.call('HSET', key, 'latest_valid_prediction', ARGV[12], 'prediction_seq', new_seq)
    updated = 1
  end
end

if updated == 1 then
  redis.call('HSET', key, 'as_of', as_of)
  redis.call('EXPIRE', key, ttl)
end

return updated
"""


def validate_redis_url(raw: str, *, local: bool) -> str:
    """Validate optional BUSINESS_REDIS_URL against local vs shared TLS boundaries."""
    value = raw.strip()
    if not value:
        raise ValueError("Redis URL cannot be empty")
    parsed = urlparse(value)
    if parsed.scheme not in {"redis", "rediss"} or not parsed.hostname or parsed.fragment:
        raise ValueError("Invalid Redis URL")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Invalid Redis port") from exc
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("Invalid Redis port")
    if parsed.path not in {"", "/"}:
        db_part = parsed.path.lstrip("/")
        if not re.fullmatch(r"[0-9]+", db_part):
            raise ValueError("Redis DB path must be an integer")
    if local:
        if not loopback(parsed.hostname):
            raise ValueError("Local test Redis URL must use loopback or cluster-internal host")
    elif parsed.scheme != "rediss":
        raise ValueError("Shared Redis requires rediss:// TLS URL")
    return value


def _read_password(env: Mapping[str, str]) -> str | None:
    file_path = env.get("BUSINESS_REDIS_PASSWORD_FILE", "").strip()
    if file_path:
        return read_credential_file(Path(file_path))
    raw = env.get("BUSINESS_REDIS_PASSWORD", "")
    if not raw.strip():
        return None
    if "\n" in raw or "\r" in raw:
        raise ValueError("Invalid Redis password")
    return raw


@dataclass(frozen=True)
class RedisSettings:
    """Bounded Redis/Valkey connection and stream configuration for Business runtimes."""

    url: str = field(repr=False)
    password: str | None = field(default=None, repr=False)
    brief_stream: str = BRIEF_STREAM
    brief_group: str = BRIEF_GROUP
    live_ttl_seconds: int = LIVE_HASH_TTL_SECONDS

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        local: bool,
    ) -> RedisSettings | None:
        env = os.environ if env is None else env
        raw_url = env.get("BUSINESS_REDIS_URL", "").strip()
        if not raw_url:
            return None
        url = validate_redis_url(raw_url, local=local)
        stream = env.get("BUSINESS_REDIS_STREAM", BRIEF_STREAM).strip() or BRIEF_STREAM
        group = env.get("BUSINESS_REDIS_STREAM_GROUP", BRIEF_GROUP).strip() or BRIEF_GROUP
        return cls(
            url=url,
            password=_read_password(env),
            brief_stream=stream,
            brief_group=group,
        )

    def client_kwargs(self) -> dict:
        kwargs: dict = {
            "socket_connect_timeout": 2,
            "socket_timeout": 5,
            "decode_responses": False,
        }
        if self.password is not None:
            kwargs["password"] = self.password
        return kwargs


def live_hash_key(machine_id: str) -> str:
    return f"{LIVE_HASH_PREFIX}{machine_id}"


def _decode_json(raw):
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        return json.loads(bytes(raw).decode("utf-8"))
    if isinstance(raw, str):
        return json.loads(raw)
    if isinstance(raw, dict):
        return raw
    raise ValueError("Unsupported cached snapshot type")


class BusinessRedisBus:
    """Synchronous Redis client helper for hot projection hashes, Pub/Sub, and Brief Streams."""

    def __init__(self, settings: RedisSettings | None = None, *, client=None):
        if settings is None and client is None:
            raise ValueError("Either RedisSettings or explicit Redis client is required")
        self.settings = settings or RedisSettings(url="redis://127.0.0.1:6379/0")
        self._owns_client = client is None
        self.client = (
            client
            if client is not None
            else redis.Redis.from_url(self.settings.url, **self.settings.client_kwargs())
        )
        self._update_script = None

    def ping(self) -> bool:
        try:
            return bool(self.client.ping())
        except Exception:
            return False

    def close(self) -> None:
        if self._owns_client and hasattr(self.client, "close"):
            try:
                self.client.close()
            except Exception:
                pass

    def update_live(
        self,
        machine_id: str,
        rows: Mapping[str, dict | None],
        changed: frozenset[str] | set[str],
        *,
        as_of: str | None = None,
    ) -> int:
        if not changed:
            return 0
        timestamp = as_of or datetime.now(UTC).isoformat()

        def pack(table: str):
            if table not in changed or rows.get(table) is None:
                return ("0", "0", "", "")
            row = rows[table]
            raw = bytes(row["snapshot"]).decode("utf-8")
            status = ""
            if table == "latest_processing":
                status = str(json.loads(raw)["processing_status"])
            return ("1", str(int(row["sequence_number"])), status, raw)

        t_flag, t_seq, _, t_raw = pack("latest_telemetry")
        p_flag, p_seq, p_status, p_raw = pack("latest_processing")
        v_flag, v_seq, _, v_raw = pack("latest_valid_prediction")
        args = [
            str(self.settings.live_ttl_seconds),
            timestamp,
            t_flag,
            t_seq,
            t_raw,
            p_flag,
            p_seq,
            p_status,
            p_raw,
            v_flag,
            v_seq,
            v_raw,
        ]
        key = live_hash_key(machine_id)
        if hasattr(self.client, "register_script"):
            if self._update_script is None:
                self._update_script = self.client.register_script(_UPDATE_LIVE_LUA)
            return int(self._update_script(keys=[key], args=args))
        return int(self.client.eval(_UPDATE_LIVE_LUA, 1, key, *args))

    def read_live_many(self, machine_ids: list[str] | tuple[str, ...]) -> dict[str, dict | None]:
        ids = list(dict.fromkeys(machine_ids))
        if not ids:
            return {}
        pipe = self.client.pipeline(transaction=False)
        fields = ("telemetry", "latest_processing", "latest_valid_prediction", "as_of")
        for machine_id in ids:
            pipe.hmget(live_hash_key(machine_id), *fields)
        results = pipe.execute()
        snapshots: dict[str, dict | None] = {}
        fallback_as_of = datetime.now(UTC).isoformat()
        for machine_id, values in zip(ids, results, strict=True):
            telemetry_raw, proc_raw, pred_raw, as_of_raw = values
            if telemetry_raw is None and proc_raw is None and pred_raw is None:
                snapshots[machine_id] = None
                continue
            as_of = (
                bytes(as_of_raw).decode("utf-8")
                if isinstance(as_of_raw, (bytes, bytearray))
                else str(as_of_raw)
                if as_of_raw is not None
                else fallback_as_of
            )
            snapshots[machine_id] = {
                "machine_id": machine_id,
                "telemetry": _decode_json(telemetry_raw),
                "latest_processing": _decode_json(proc_raw),
                "latest_valid_prediction": _decode_json(pred_raw),
                "as_of": as_of,
            }
        return snapshots

    def publish_live(self, channel: str, machine_id: str) -> None:
        payload = json.dumps({"version": 1, "machine_key": machine_key(machine_id)})
        self.client.publish(channel, payload)

    def publish_alert(self, channel: str, payload: str | bytes) -> None:
        body = (
            payload
            if isinstance(payload, (str, bytes))
            else canonical_json(payload).decode("utf-8")
        )
        self.client.publish(channel, body)

    def publish_m7(self, channel: str, payload: str | bytes) -> None:
        body = (
            payload
            if isinstance(payload, (str, bytes))
            else canonical_json(payload).decode("utf-8")
        )
        self.client.publish(channel, body)

    def ensure_brief_group(self) -> None:
        try:
            self.client.xgroup_create(
                self.settings.brief_stream,
                self.settings.brief_group,
                id="0",
                mkstream=True,
            )
        except redis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def enqueue_brief_job(self, job_id: str) -> str:
        entry = self.client.xadd(
            self.settings.brief_stream,
            {"job_id": job_id},
            maxlen=10000,
            approximate=True,
        )
        return bytes(entry).decode("utf-8") if isinstance(entry, (bytes, bytearray)) else str(entry)

    def read_brief_jobs(
        self,
        consumer: str,
        *,
        count: int = 1,
        block_ms: int = 1000,
    ) -> list[tuple[str, dict[str, str]]]:
        response = self.client.xreadgroup(
            self.settings.brief_group,
            consumer,
            {self.settings.brief_stream: ">"},
            count=count,
            block=block_ms,
        )
        return _normalize_stream_entries(response)

    def autoclaim_brief_jobs(
        self,
        consumer: str,
        *,
        min_idle_ms: int = 90000,
        count: int = 4,
    ) -> list[tuple[str, dict[str, str]]]:
        response = self.client.xautoclaim(
            self.settings.brief_stream,
            self.settings.brief_group,
            consumer,
            min_idle_ms,
            start_id="0-0",
            count=count,
        )
        if not response:
            return []
        entries = response[1] if len(response) >= 2 else []
        return _decode_entries(entries)

    def ack_brief_job(self, entry_id: str) -> int:
        return int(
            self.client.xack(self.settings.brief_stream, self.settings.brief_group, entry_id)
        )


def _decode_text(value) -> str:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8")
    return str(value)


def _decode_entries(entries) -> list[tuple[str, dict[str, str]]]:
    parsed: list[tuple[str, dict[str, str]]] = []
    for item in entries or ():
        if not item:
            continue
        entry_id, fields = item[0], item[1]
        if fields is None:
            continue
        parsed.append(
            (
                _decode_text(entry_id),
                {_decode_text(k): _decode_text(v) for k, v in fields.items()},
            )
        )
    return parsed


def _normalize_stream_entries(response) -> list[tuple[str, dict[str, str]]]:
    if not response:
        return []
    parsed: list[tuple[str, dict[str, str]]] = []
    for _stream_name, entries in response:
        parsed.extend(_decode_entries(entries))
    return parsed
