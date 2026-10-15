"""Validate API contracts and examples without importing any application service."""

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urldefrag, urlsplit

import yaml
from jsonschema import Draft202012Validator, FormatChecker
from openapi_spec_validator import validate as validate_openapi
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

ROOT = Path(__file__).resolve().parent
METHODS = {"get", "post", "put", "patch", "delete", "options", "head", "trace"}


class ContractError(ValueError):
    pass


class UniqueLoader(yaml.SafeLoader):
    pass


def unique_mapping(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ContractError(f"Duplicate mapping key: {key}")
        result[key] = value
    return result


def yaml_mapping(loader, node):
    return unique_mapping(
        (loader.construct_object(key), loader.construct_object(value))
        for key, value in node.value
    )


UniqueLoader.add_constructor("tag:yaml.org,2002:map", yaml_mapping)


def invalid_constant(value):
    raise ContractError(f"Non-finite JSON number: {value}")


def load_json(path):
    return json.loads(
        path.read_text(),
        object_pairs_hook=unique_mapping,
        parse_constant=invalid_constant,
    )


def pointer(parts):
    return "/" + "/".join(
        str(part).replace("~", "~0").replace("/", "~1") for part in parts
    )


@dataclass(frozen=True)
class Location:
    path: Path
    parts: tuple = ()

    @property
    def uri(self):
        return self.path.as_uri() + ("#" + pointer(self.parts) if self.parts else "")

    def child(self, *parts):
        return Location(self.path, self.parts + parts)


class Contracts:
    def __init__(self, root=ROOT):
        self.root = root.resolve()
        self.documents = {}
        self.ids = {}
        self.used_examples = set()
        self.case_count = 0
        self.negative_count = 0
        self.operation_count = 0
        for path in sorted(self.root.glob("*.schema.json")) + sorted(
            (self.root / "schemas").glob("*.json")
        ):
            schema = load_json(path)
            Draft202012Validator.check_schema(schema)
            self.documents[path] = schema
            if "$id" in schema:
                self.ids[schema["$id"]] = path
        for path in sorted(self.root.glob("openapi-*.yaml")):
            self.documents[path] = yaml.load(path.read_text(), Loader=UniqueLoader)
        resources = []
        for path, document in self.documents.items():
            resource = Resource.from_contents(
                document, default_specification=DRAFT202012
            )
            resources.append((path.as_uri(), resource))
            if "$id" in document:
                resources.append((document["$id"], resource))
        # No retriever: unknown references fail instead of fetching remote schemas.
        self.registry = Registry().with_resources(resources)

    def node(self, location):
        value = self.documents[location.path]
        for part in location.parts:
            value = value[int(part)] if isinstance(value, list) else value[part]
        return value

    def local_path(self, base, reference):
        if urlsplit(reference).scheme:
            raise ContractError(f"Expected a repository-local path: {reference}")
        path = (base.parent / unquote(reference)).resolve()
        if not path.is_relative_to(self.root) or not path.is_file():
            raise ContractError(f"Missing or non-local contract file: {reference}")
        return path

    def reference(self, base, reference):
        document, fragment = urldefrag(reference)
        if document in self.ids:
            path = self.ids[document]
        elif document:
            path = self.local_path(base, document)
        else:
            path = base
        if fragment and not fragment.startswith("/"):
            raise ContractError(
                f"Only JSON Pointer fragments are used here: {reference}"
            )
        parts = (
            tuple(
                part.replace("~1", "/").replace("~0", "~")
                for part in unquote(fragment).lstrip("/").split("/")
            )
            if fragment
            else ()
        )
        location = Location(path, parts)
        self.node(location)
        return location

    def follow(self, location):
        seen = set()
        while isinstance(self.node(location), dict) and "$ref" in self.node(location):
            if location in seen:
                raise ContractError(f"Reference cycle: {location.uri}")
            seen.add(location)
            location = self.reference(location.path, self.node(location)["$ref"])
        return location

    def walk_references(self, value, base):
        if isinstance(value, dict):
            if "$ref" in value:
                self.reference(base, value["$ref"])
            for child in value.values():
                self.walk_references(child, base)
        elif isinstance(value, list):
            for child in value:
                self.walk_references(child, base)

    def check_value(self, location, value, *, valid=True, label="example"):
        validator = Draft202012Validator(
            {"$ref": location.uri},
            registry=self.registry,
            format_checker=FormatChecker(),
        )
        violations = [error.message for error in validator.iter_errors(value)]
        if not violations:
            try:
                semantic_checks(value)
            except ContractError as exc:
                violations.append(str(exc))
        if valid and violations:
            raise ContractError(f"{label}: {violations[0]}")
        if not valid and not violations:
            raise ContractError(f"Invalid fixture unexpectedly accepted: {label}")
        self.case_count += 1
        self.negative_count += int(not valid)

    def check_media(self, location):
        media = self.node(location)
        if "schema" not in media or not media.get("examples"):
            raise ContractError(f"Missing schema or examples at {location.uri}")
        for name, item in media["examples"].items():
            item = self.node(self.follow(location.child("examples", name)))
            if "externalValue" not in item:
                raise ContractError(
                    f"Expected a concrete external example: {location.uri}"
                )
            path = self.local_path(location.path, item["externalValue"])
            self.used_examples.add(path)
            content = path.read_text()
            if path.suffix == ".json":
                self.check_value(
                    location.child("schema"), load_json(path), label=str(path)
                )
            else:
                self.check_value(location.child("schema"), content, label=str(path))
                if "x-parsed-event-schema" in media:
                    for frame in parse_sse(content):
                        self.check_value(
                            location.child("x-parsed-event-schema"),
                            frame,
                            label=str(path),
                        )

    def check_api(self, path):
        document = self.documents[path]
        validate_openapi(document, base_uri=path.as_uri())
        ids = set()
        for route, path_item in document["paths"].items():
            for method in METHODS & path_item.keys():
                location = Location(path, ("paths", route, method))
                operation = self.node(location)
                operation_id = operation["operationId"]
                if operation_id in ids or not operation.get("x-maintai-source"):
                    raise ContractError(
                        f"Duplicate operationId or missing provenance: {route}"
                    )
                ids.add(operation_id)
                self.operation_count += 1
                if "requestBody" in operation:
                    body = self.follow(location.child("requestBody"))
                    for media_type in self.node(body)["content"]:
                        self.check_media(body.child("content", media_type))
                for status in operation["responses"]:
                    response = self.follow(location.child("responses", status))
                    for media_type in self.node(response).get("content", {}):
                        self.check_media(response.child("content", media_type))
                for index in range(len(operation.get("parameters", []))):
                    parameter = self.follow(location.child("parameters", index))
                    value = self.node(parameter)
                    if "default" in value.get("schema", {}):
                        self.check_value(
                            parameter.child("schema"), value["schema"]["default"]
                        )
        print(
            f"PASS {path.name}: {len(ids)} operations, "
            "OpenAPI structure and wire examples"
        )

    def check_baseline(self):
        baseline = (
            self.root.parent / "docs/MaintAI_System_Contracts_v1_1_DRIFT.md"
        ).read_text()
        provider = {
            "/api/v1/predict": "openapi-ml.yaml",
            "/api/v1/briefs": "openapi-rag.yaml",
        }
        for method, raw_route in re.findall(
            r"^(GET|POST) (\S+)$", baseline, re.MULTILINE
        ):
            route = raw_route.split("?", 1)[0]
            filenames = (
                ["openapi-backend.yaml", "openapi-ml.yaml", "openapi-rag.yaml"]
                if route.startswith("/health/")
                else [provider.get(route, "openapi-backend.yaml")]
            )
            for filename in filenames:
                if method.lower() not in self.documents[self.root / filename][
                    "paths"
                ].get(route, {}):
                    raise ContractError(
                        f"Baseline operation missing: {filename}: {method} {route}"
                    )
        enums = self.documents[self.root / "schemas/common.schema.json"]["$defs"]
        names = [
            "ValidationStatus",
            "MachineState",
            "PredictionLabel",
            "AlertSeverity",
            "AlertStatus",
            "GroundedStatus",
            "DependencyStatus",
        ]
        for number, name in enumerate(names, 1):
            block = re.search(
                rf"## 3\.{number}\..*?```text\n(.*?)```", baseline, re.DOTALL
            )
            expected = block.group(1).strip().splitlines()
            if enums[name]["enum"] != expected:
                raise ContractError(f"Enum differs from baseline: {name}")
        drift_block = re.search(r"## 3\.8\..*?```text\n(.*?)```", baseline, re.DOTALL)
        drift_statuses = self.documents[self.root / "drift-report.schema.json"][
            "$defs"
        ]["DriftStatus"]["enum"]
        if drift_statuses != drift_block.group(1).strip().splitlines():
            raise ContractError("Runtime DriftStatus differs from baseline")
        print("PASS baseline: existing HTTP operations and common enums preserved")

    def run(self):
        for path, document in self.documents.items():
            self.walk_references(document, path)
        for path in sorted(self.root.glob("openapi-*.yaml")):
            self.check_api(path)
        manifest = load_json(self.root / "examples/manifest.json")
        for case in manifest["cases"]:
            path = self.local_path(self.root / "index", case["file"])
            location = self.reference(self.root / "index", case["schema"])
            self.used_examples.add(path)
            self.check_value(
                location, load_json(path), valid=case["valid"], label=str(path)
            )
        orphaned = set((self.root / "examples").rglob("*.json")) - self.used_examples
        orphaned.discard(self.root / "examples/manifest.json")
        if orphaned:
            raise ContractError(
                f"Unvalidated example files: {sorted(map(str, orphaned))}"
            )
        self.check_baseline()
        print(
            f"PASS {self.operation_count} operations; "
            f"{self.case_count} example/default checks, "
            f"including {self.negative_count} intentionally invalid fixtures"
        )

    def bundle(self, destination):
        def expand(location, stack):
            value = self.node(location)
            if isinstance(value, dict):
                if "$ref" in value:
                    target = self.reference(location.path, value["$ref"])
                    if target in stack:
                        raise ContractError(
                            f"Cannot inline recursive reference: {target.uri}"
                        )
                    resolved = expand(target, stack | {target})
                    siblings = {
                        key: expand(location.child(key), stack)
                        for key in value
                        if key not in {"$ref", "$id", "$schema"}
                    }
                    return {"allOf": [resolved], **siblings} if siblings else resolved
                return {
                    key: expand(location.child(key), stack)
                    for key in value
                    if key not in {"$id", "$schema"}
                }
            if isinstance(value, list):
                return [
                    expand(location.child(index), stack) for index in range(len(value))
                ]
            return value

        destination.mkdir(parents=True, exist_ok=True)
        for path in sorted(self.root.glob("openapi-*.yaml")):
            document = expand(Location(path), set())

            # Keep examples portable when importing one bundled JSON file.
            def inline_examples(value, base=path):
                if isinstance(value, dict):
                    if "externalValue" in value:
                        target = self.local_path(base, value.pop("externalValue"))
                        value["value"] = (
                            load_json(target)
                            if target.suffix == ".json"
                            else target.read_text()
                        )
                    for child in value.values():
                        inline_examples(child, base)
                elif isinstance(value, list):
                    for child in value:
                        inline_examples(child, base)

            inline_examples(document)
            validate_openapi(document)
            (destination / (path.stem + ".json")).write_text(
                json.dumps(document, ensure_ascii=False, indent=2, allow_nan=False)
                + "\n"
            )
        print(f"PASS portable OpenAPI bundles: {destination}")


def semantic_checks(value):
    if isinstance(value, list):
        for child in value:
            semantic_checks(child)
    elif isinstance(value, dict):
        if {"checklist", "citations", "retrieval", "grounded_status"} <= value.keys():
            citations = {item["citation_id"]: item for item in value["citations"]}
            if len(citations) != len(value["citations"]):
                raise ContractError("Duplicate citation ID")
            retrieved = set(value["retrieval"]["retrieved_chunk_ids"])
            if any(item["chunk_id"] not in retrieved for item in citations.values()):
                raise ContractError("Citation points outside the retrieved context")
            for step in value["checklist"]:
                if any(
                    identifier not in citations for identifier in step["citation_ids"]
                ):
                    raise ContractError("Checklist points to an unknown citation")
        if {"failure_probability", "model_threshold", "prediction"} <= value.keys():
            expected = (
                "FAILURE"
                if value["failure_probability"] >= value["model_threshold"]
                else "NORMAL"
            )
            if value["prediction"] != expected:
                raise ContractError("Prediction label contradicts its model threshold")
        if {
            "overall_status",
            "current_window",
            "summary",
            "features",
            "policy",
        } <= value.keys():
            if datetime.fromisoformat(
                value["current_window"]["start_at"]
            ) > datetime.fromisoformat(value["current_window"]["end_at"]):
                raise ContractError("Reversed report window")
            warning = sum(item["status"] == "WARNING" for item in value["features"])
            critical = sum(item["status"] == "CRITICAL" for item in value["features"])
            if (warning, critical) != (
                value["summary"]["warning_features"],
                value["summary"]["critical_features"],
            ):
                raise ContractError("Report summary disagrees with feature results")
            if value["overall_status"] in {"ERROR", "INSUFFICIENT_DATA"}:
                if value["features"]:
                    raise ContractError(
                        "Unavailable drift result contains feature conclusions"
                    )
            elif len({item["feature"] for item in value["features"]}) != 6:
                raise ContractError(
                    "Conclusive report needs six distinct feature results"
                )
        for child in value.values():
            semantic_checks(child)


def parse_sse(content):
    frames = []
    for block in content.replace("\r\n", "\n").split("\n\n"):
        fields, data = {}, []
        for line in block.splitlines():
            if not line or line.startswith(":"):
                continue
            key, _, text = line.partition(":")
            text = text.removeprefix(" ")
            if key == "data":
                data.append(text)
            elif key in {"event", "id"}:
                fields[key] = text
        if data:
            fields["data"] = json.loads(
                "\n".join(data), parse_constant=invalid_constant
            )
            frames.append(fields)
    if not frames:
        raise ContractError("SSE fixture has no data frame")
    return frames


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bundle-dir",
        type=Path,
        help="Export standalone JSON OpenAPI files for viewers/codegen",
    )
    args = parser.parse_args()
    contracts = Contracts()
    contracts.run()
    if args.bundle_dir:
        contracts.bundle(args.bundle_dir)


if __name__ == "__main__":
    main()
