"""Validate target API documentation offline; this does not test application runtimes."""

import json
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urldefrag, urljoin, urlparse

import yaml
from jsonschema import Draft202012Validator, FormatChecker
from openapi_spec_validator import validate as validate_openapi
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

ROOT = Path(__file__).resolve().parent
CONTRACTS = ROOT.parent


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate key: {key}")
        result[key] = value
    return result


def read_json(path):
    def invalid_constant(value):
        raise ValueError(f"Non-finite JSON number: {value}")

    return json.loads(
        path.read_text(),
        object_pairs_hook=unique_pairs,
        parse_constant=invalid_constant,
    )


class UniqueLoader(yaml.SafeLoader):
    pass


def yaml_mapping(loader, node):
    return unique_pairs(
        (loader.construct_object(key), loader.construct_object(value))
        for key, value in node.value
    )


UniqueLoader.add_constructor("tag:yaml.org,2002:map", yaml_mapping)


def walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def locations(value, parts=()):
    if isinstance(value, dict):
        yield parts, value
        for key, child in value.items():
            yield from locations(child, parts + (key,))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from locations(child, parts + (index,))


def schema_uri(path, parts):
    pointer = "/".join(
        str(part).replace("~", "~0").replace("/", "~1") for part in parts
    )
    return path.as_uri() + ("#/" + pointer if parts else "")


def resolve_reference(base, reference, documents):
    uri, fragment = urldefrag(urljoin(base.as_uri(), reference))
    if urlparse(uri).scheme != "file":
        raise ValueError(f"Nonlocal reference not allowed: {reference}")
    target = Path(unquote(urlparse(uri).path)).resolve()
    if not target.is_relative_to(CONTRACTS.resolve()):
        raise ValueError(f"Reference outside current contract bundle: {reference}")
    if target not in documents:
        raise ValueError(f"Missing local reference: {target}")
    value = documents[target]
    if fragment:
        for part in fragment.removeprefix("/").split("/"):
            token = unquote(part).replace("~1", "/").replace("~0", "~")
            value = value[int(token)] if isinstance(value, list) else value[token]
    return value


def semantic_checks(value):
    for node in walk(value):
        m6_semantic_checks(node)
        if {"checklist", "citations", "retrieval", "grounded_status"} <= node.keys():
            citations = {item["citation_id"]: item for item in node["citations"]}
            if len(citations) != len(node["citations"]):
                raise ValueError("Duplicate citation ID")
            retrieved = set(node["retrieval"]["retrieved_chunk_ids"])
            if any(item["chunk_id"] not in retrieved for item in citations.values()):
                raise ValueError("Citation points outside the retrieved context")
            for step in node["checklist"]:
                if any(
                    identifier not in citations for identifier in step["citation_ids"]
                ):
                    raise ValueError("Checklist points to an unknown citation")
        if (
            {"failure_probability", "model_threshold", "prediction"} <= node.keys()
            and node["failure_probability"] is not None
            and node["model_threshold"] is not None
        ):
            expected = (
                "FAILURE"
                if node["failure_probability"] >= node["model_threshold"]
                else "NORMAL"
            )
            if node["prediction"] != expected:
                raise ValueError("Prediction label disagrees with threshold")
        if "outcome" in node and "result_id" in node:
            for key in ("result_id", "source_event_id", "machine_id"):
                outcome_key = "event_id" if key == "source_event_id" else key
                if node[key] != node["outcome"][outcome_key]:
                    raise ValueError(f"History lineage mismatch: {key}")
        if {
            "overall_status",
            "current_window",
            "summary",
            "features",
            "policy",
        } <= node.keys():
            window = node["current_window"]
            if datetime.fromisoformat(window["start_at"]) > datetime.fromisoformat(
                window["end_at"]
            ):
                raise ValueError("Reversed drift report window")
            warning = sum(item["status"] == "WARNING" for item in node["features"])
            critical = sum(item["status"] == "CRITICAL" for item in node["features"])
            summary = node["summary"]
            if (warning, critical) != (
                summary["warning_features"],
                summary["critical_features"],
            ):
                raise ValueError("Drift summary disagrees with feature results")
            if node["overall_status"] in {"ERROR", "INSUFFICIENT_DATA"}:
                if node["features"]:
                    raise ValueError(
                        "Unavailable drift result contains feature conclusions"
                    )
            elif len({item["feature"] for item in node["features"]}) != 6:
                raise ValueError("Conclusive drift report needs six distinct features")


def m6_semantic_checks(node):
    """Cross-field checks for design fixtures; no signature/S3/runtime validation."""
    if node.get("profile") == "M6-HISTORY-01/1.0.0":
        if "from" in node and datetime.fromisoformat(
            node["from"]
        ) >= datetime.fromisoformat(node["to"]):
            raise ValueError("M6 range must have from before to")
        if "as_of" in node:
            if datetime.fromisoformat(node["expires_at"]) <= datetime.fromisoformat(
                node["as_of"]
            ):
                raise ValueError("M6 snapshot expires before its as_of")
            first, last = node["available_from"], node["available_through"]
            if (
                first is not None
                and last is not None
                and datetime.fromisoformat(first) > datetime.fromisoformat(last)
            ):
                raise ValueError("M6 available range is reversed")
        if node.get("collection") == "RAW":
            keys = [(p["topic"], p["partition"]) for p in node["partition_coverage"]]
            if len(set(keys)) != len(keys):
                raise ValueError("Raw page repeats partition coverage")
            if any(p["status"] != "COMPLETE" for p in node["partition_coverage"]):
                raise ValueError("Incomplete raw partition cannot claim COMPLETE page")
    if node.get("aud") == "processing-history" and "azp" in node:
        if not 0 < node["exp"] - node["iat"] <= 60 or node["nbf"] > node["exp"]:
            raise ValueError("M6 delegation exceeds its lifetime budget")
    if "catalog_schema_version" in node:
        if "receipt_id" in node:
            identity = f"{node['topic']}:{node['partition']}:{node['offset']}"
            if node["receipt_id"] != identity:
                raise ValueError("Catalog receipt differs from Kafka locator")
        if "entries" in node:
            ids = [entry["receipt_id"] for entry in node["entries"]]
            if len(set(ids)) != len(ids):
                raise ValueError("Catalog segment repeats a receipt")
        if "partitions" in node:
            keys = [(p["topic"], p["partition"]) for p in node["partitions"]]
            if len(set(keys)) != len(keys):
                raise ValueError("Catalog generation repeats a partition")
            partitions = {
                key: p for key, p in zip(keys, node["partitions"], strict=True)
            }
            for segment in node["segments"]:
                if (segment["topic"], segment["partition"]) not in partitions:
                    raise ValueError(
                        "Catalog segment is outside declared partition scope"
                    )
        if "min_received_at" in node:
            first, last = node["min_received_at"], node["max_received_at"]
            if (first is None) != (last is None) or (
                first is not None
                and datetime.fromisoformat(first) > datetime.fromisoformat(last)
            ):
                raise ValueError("Catalog segment time bounds differ")
    if {
        "start_offset",
        "archived_next_offset",
        "indexed_next_offset",
        "gaps",
        "status",
    } <= node.keys():
        if (
            not node["start_offset"]
            <= node["indexed_next_offset"]
            <= node["archived_next_offset"]
        ):
            raise ValueError("Catalog frontiers are reversed")
        if (node["captured_log_end_offset"] is None) != (node["captured_at"] is None):
            raise ValueError("Catalog capture time/frontier must be present together")
        if node["status"] == "COMPLETE" and node["gaps"]:
            raise ValueError("COMPLETE catalog partition contains a gap")
        if node["status"] == "GAP" and not node["gaps"]:
            raise ValueError("GAP catalog partition must describe its gap")
        for gap in node["gaps"]:
            if gap["from_offset"] >= gap["to_offset"]:
                raise ValueError("Catalog gap must have positive exclusive range")


def main():
    documents = {}
    for path in [
        *ROOT.rglob("*.json"),
        CONTRACTS / "kafka/events/inference-result.schema.json",
    ]:
        documents[path.resolve()] = read_json(path)
    for path in ROOT.rglob("*.yaml"):
        documents[path.resolve()] = yaml.load(path.read_text(), Loader=UniqueLoader)
    resources = []
    for path, value in documents.items():
        if path.name.endswith(".schema.json") or path.suffix == ".yaml":
            resource = Resource.from_contents(value, default_specification=DRAFT202012)
            resources.append((path.as_uri(), resource))
            if "$id" in value:
                resources.append((value["$id"], resource))
    # No remote retriever: the current bundle must resolve without legacy files.
    registry = Registry().with_resources(resources)

    def check_value(uri, value):
        Draft202012Validator(
            {"$ref": uri}, registry=registry, format_checker=FormatChecker()
        ).validate(value)
        semantic_checks(value)

    refs = 0
    for path, document in documents.items():
        if not path.is_relative_to(ROOT):
            continue
        if path.name.endswith(".schema.json"):
            Draft202012Validator.check_schema(document)
        for node in walk(document):
            if "$ref" in node:
                resolve_reference(path, node["$ref"], documents)
                refs += 1
            if "externalValue" in node:
                target = (path.parent / node["externalValue"]).resolve()
                if (
                    not target.is_relative_to(CONTRACTS.resolve())
                    or not target.is_file()
                ):
                    raise ValueError(f"Missing or unsafe example: {target}")

    specs = list((ROOT / "openapi").glob("*.yaml"))
    operations = 0
    wire_checks = 0
    used_examples = set()
    for path in specs:
        spec = documents[path.resolve()]
        validate_openapi(spec, base_uri=path.as_uri())
        for parts, node in locations(spec):
            if "schema" not in node:
                continue
            uri = schema_uri(path, parts + ("schema",))
            for name, example in node.get("examples", {}).items():
                while "$ref" in example:
                    example = resolve_reference(path, example["$ref"], documents)
                if "externalValue" in example:
                    target = (path.parent / example["externalValue"]).resolve()
                    used_examples.add(target)
                    value = (
                        documents[target]
                        if target.suffix == ".json"
                        else target.read_text()
                    )
                else:
                    value = example["value"]
                try:
                    check_value(uri, value)
                except ValueError as exc:
                    raise ValueError(
                        f"Invalid HTTP example {path.name}:{name}: {exc}"
                    ) from exc
                wire_checks += 1
            if "example" in node:
                check_value(uri, node["example"])
                wire_checks += 1
            if "default" in node["schema"]:
                check_value(uri, node["schema"]["default"])
                wire_checks += 1
        for route, item in spec["paths"].items():
            for method, operation in item.items():
                if method not in {"get", "post", "put", "patch", "delete"}:
                    continue
                operations += 1
                if route.startswith("/api/"):
                    if not operation.get("security", spec.get("security")):
                        raise ValueError(f"Anonymous target API: {route}")
                    if any(
                        not requirement for requirement in operation.get("security", [])
                    ):
                        raise ValueError(f"Optional auth in target API: {route}")
        if "business" in path.name:
            if "/api/v1/sensor-readings" in spec["paths"]:
                raise ValueError("Ingestion leaked into Business API")
            if any("/simulator/" in route for route in spec["paths"]):
                raise ValueError("Simulator facade leaked into Business API")

    manifest = read_json(ROOT / "examples/manifest.json")
    passed = 0
    for case in manifest["cases"]:
        target = (ROOT / "examples" / case["file"]).resolve()
        if not target.is_relative_to(ROOT / "examples"):
            raise ValueError(f"Unsafe fixture path: {case['file']}")
        used_examples.add(target)
        value = read_json(target)
        uri = urljoin((ROOT / "examples/manifest.json").as_uri(), case["schema"])
        validator = Draft202012Validator(
            {"$ref": uri}, registry=registry, format_checker=FormatChecker()
        )
        valid = validator.is_valid(value)
        if valid:
            try:
                semantic_checks(value)
            except ValueError:
                valid = False
        if valid != case["valid"]:
            errors = [error.message for error in validator.iter_errors(value)]
            raise ValueError(f"Unexpected fixture result: {case['file']}: {errors[:2]}")
        passed += 1
    orphaned = set((ROOT / "examples").rglob("*.json")) - used_examples
    orphaned.discard(ROOT / "examples/manifest.json")
    if orphaned:
        raise ValueError(f"Unvalidated example files: {sorted(map(str, orphaned))}")
    for path in (ROOT / "examples").glob("*.sse"):
        for frame in path.read_text().strip().split("\n\n"):
            fields = dict(
                line.split(": ", 1) for line in frame.splitlines() if ": " in line
            )
            value = json.loads(fields["data"])
            if fields["event"] != value["type"]:
                raise ValueError(f"SSE event name/envelope mismatch: {path}")
            Draft202012Validator(
                {"$ref": (ROOT / "events/sse-event.schema.json").as_uri()},
                registry=registry,
                format_checker=FormatChecker(),
            ).validate(value)
    print(
        f"HTTP contracts: {len(specs)} OpenAPIs, {operations} operations, "
        f"{refs} local refs, {wire_checks} HTTP example/default checks, "
        f"{passed} positive/negative cases passed. "
        "No application runtime tested."
    )


if __name__ == "__main__":
    main()
