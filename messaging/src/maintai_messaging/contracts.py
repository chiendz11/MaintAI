"""Strict offline schemas, compatibility readers and immutable event identities."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from urllib.parse import unquote, urldefrag, urljoin, urlparse

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from .errors import ContractViolation, MalformedJSON


def canonical_json(value) -> bytes:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode("utf-8")
    except (ValueError, TypeError, RecursionError) as exc:
        raise ContractViolation("Payload is not finite, serializable JSON") from exc


def strict_json(raw: bytes) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise MalformedJSON("Duplicate JSON object key")
            result[key] = value
        return result

    def nonfinite(_):
        raise MalformedJSON("Non-finite JSON number")

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=nonfinite)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise MalformedJSON("Malformed JSON document") from exc
    if not isinstance(value, dict):
        raise ContractViolation("JSON envelope must be an object")
    canonical_json(value)  # Reject float overflow and unpaired surrogate strings too.
    return value


def contract_root() -> Path:
    bundled = Path(str(files("maintai_messaging").joinpath("contracts")))
    if (bundled / "kafka/topology.json").is_file():
        return bundled
    repository = Path(__file__).resolve().parents[3] / "contracts"
    if (repository / "kafka/topology.json").is_file():
        return repository
    raise ContractViolation("Kafka contracts are missing from this distribution")


@dataclass(frozen=True)
class DecodedEvent:
    topic: str
    payload: dict
    wire_bytes: bytes

    @property
    def schema_version(self) -> str:
        return self.payload["schema_version"]

    @property
    def legacy(self) -> bool:
        return self.topic == "inference.results" and self.schema_version == "2.0.0"

    @property
    def kind(self) -> str:
        if self.topic == "inference.results":
            return self.payload["result_kind" if self.legacy else "kind"]
        return self.topic

    @property
    def message_id(self) -> str:
        field = {
            "sensor.readings": "event_id",
            "sensor.rejected": "rejection_id",
            "inference.results": "result_id",
            "alert.signals": "signal_id",
        }[self.topic]
        return self.payload[field]

    @property
    def source_event_id(self) -> str | None:
        if self.topic == "inference.results":
            return self.payload["source_event_id" if self.legacy else "event_id"]
        return self.payload.get("event_id")

    @property
    def key(self) -> str:
        if self.topic == "sensor.rejected":
            return self.payload["source_identity"]
        return self.payload["machine_id"]

    @property
    def identity_sha256(self) -> str:
        value = self.payload
        if self.topic == "sensor.readings":
            # Delivery metadata may change across retries; the logical reading must not.
            value = {
                k: v for k, v in value.items() if k not in {"server_accepted_at", "correlation_id"}
            }
        elif self.topic == "sensor.rejected":
            value = {k: v for k, v in value.items() if k not in {"rejected_at", "correlation_id"}}
        return hashlib.sha256(canonical_json(value)).hexdigest()


class ContractRegistry:
    def __init__(self, root: Path | None = None):
        self.root = (root or contract_root()).resolve()
        self.directory = self.root / "kafka"
        manifest = strict_json((self.directory / "manifest.json").read_bytes())
        for relative, digest in manifest["sha256"].items():
            path = (self.root / relative).resolve()
            if not path.is_relative_to(self.root) or not path.is_file():
                raise ContractViolation("Missing or unsafe contract bundle member")
            if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                raise ContractViolation("Contract bundle checksum mismatch")
        self.bundle_version = manifest["bundle_version"]
        self.topology = strict_json((self.directory / "topology.json").read_bytes())
        paths = list(self.directory.rglob("*.schema.json")) + [
            self.root / "v1.3/schemas/runtime.schema.json",
            self.root / "v1.3/events/alert-signal.schema.json",
        ]
        self.documents = {p.resolve(): strict_json(p.read_bytes()) for p in paths}
        resources = []
        for path, document in self.documents.items():
            Draft202012Validator.check_schema(document)
            resource = Resource.from_contents(document, default_specification=DRAFT202012)
            resources.append((path.as_uri(), resource))
            if "$id" in document:
                resources.append((document["$id"], resource))
        self.registry = Registry().with_resources(resources)
        self._check_local_references()

    def _check_local_references(self):
        def walk(value):
            if isinstance(value, dict):
                yield value
                for child in value.values():
                    yield from walk(child)
            elif isinstance(value, list):
                for child in value:
                    yield from walk(child)

        for path, document in self.documents.items():
            for node in walk(document):
                if "$ref" not in node:
                    continue
                uri, fragment = urldefrag(urljoin(path.as_uri(), node["$ref"]))
                target = Path(unquote(urlparse(uri).path)).resolve()
                if urlparse(uri).scheme != "file" or not target.is_relative_to(self.root):
                    raise ContractViolation("Non-local schema reference")
                if target not in self.documents:
                    raise ContractViolation("Unbundled schema reference")
                value = self.documents[target]
                for part in fragment.removeprefix("/").split("/") if fragment else []:
                    token = unquote(part).replace("~1", "/").replace("~0", "~")
                    value = value[int(token)] if isinstance(value, list) else value[token]

    def validator(self, relative: str) -> Draft202012Validator:
        path = (self.directory / relative).resolve()
        if not path.is_relative_to(self.directory) or path not in self.documents:
            raise ContractViolation("Unknown contract entrypoint")
        return Draft202012Validator(
            {"$ref": path.as_uri()}, registry=self.registry, format_checker=FormatChecker()
        )

    def request_issues(self, payload: dict) -> list[dict]:
        errors = self.validator("events/sensor-request.schema.json").iter_errors(payload)
        return [
            {
                "field": ".".join(map(str, error.absolute_path))[:256],
                "code": str(error.validator).upper(),
            }
            for error in list(errors)[:128]
        ]

    def decode(self, topic: str, raw: bytes, *, writer: bool = False) -> DecodedEvent:
        if topic not in self.topology["topics"]:
            raise ContractViolation("Unknown Kafka topic")
        payload = strict_json(raw)
        entry = self.topology["topics"][topic]
        version = payload.get("schema_version")
        if version not in entry["reader_schemas"]:
            raise ContractViolation("Unsupported Kafka event schema version")
        if writer and version != entry["writer_version"]:
            raise ContractViolation("Legacy wire versions are read-only")
        validator = self.validator(entry["reader_schemas"][version])
        if not validator.is_valid(payload):
            # jsonschema messages can echo sensitive input. Do not put them in logs/errors.
            raise ContractViolation("Event does not satisfy the published schema")
        event = DecodedEvent(topic, payload, raw)
        if topic == "inference.results" and event.kind == "PREDICTION_COMPLETED":
            expected = (
                "FAILURE"
                if payload["failure_probability"] >= payload["model_threshold"]
                else "NORMAL"
            )
            if payload["prediction"] != expected:
                raise ContractViolation("Prediction label contradicts its threshold")
        return event

    def encode(self, topic: str, payload: dict) -> DecodedEvent:
        return self.decode(topic, canonical_json(payload), writer=True)

    def check_examples(self) -> int:
        directory = self.directory / "examples"
        manifest = strict_json((directory / "manifest.json").read_bytes())
        for case in manifest["cases"]:
            try:
                self.decode(case["topic"], (directory / case["file"]).read_bytes())
                valid = True
            except ContractViolation:
                valid = False
            if valid != case["valid"]:
                raise ContractViolation(f"Fixture has unexpected result: {case['file']}")
        if self.request_issues(strict_json((directory / "sensor-request.json").read_bytes())):
            raise ContractViolation("Request fixture is invalid")
        return len(manifest["cases"])


@lru_cache(maxsize=1)
def default_contracts() -> ContractRegistry:
    """Release-pinned schemas are immutable; parse/check them once per process."""
    return ContractRegistry()
