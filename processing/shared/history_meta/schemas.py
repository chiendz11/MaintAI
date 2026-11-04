"""Load M6 package data explicitly, alongside the immutable SDK schema registry."""

import os
import sys
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from maintai_messaging.contracts import default_contracts, strict_json
from referencing import Resource

from .security import HistoryError


class Schemas:
    def __init__(self, directory=None):
        root = Path(
            directory
            or os.environ.get(
                "PROCESSING_SCHEMA_DIRECTORY",
                Path(sys.prefix) / "share/maintai-processing/contracts",
            )
        )
        contracts = default_contracts()
        virtual = contracts.root / "v1.3/schemas"
        self.registry = contracts.registry
        self.locations = {}
        for name in ("history-m6", "raw-catalog-m6"):
            value = strict_json((root / (name + ".schema.json")).read_bytes())
            uri = (virtual / (name + ".schema.json")).as_uri()
            self.registry = self.registry.with_resource(uri, Resource.from_contents(value))
            for definition in value["$defs"]:
                self.locations[definition] = uri + "#/$defs/" + definition

    def validate(self, definition, value, *, status=503):
        validator = Draft202012Validator(
            {"$ref": self.locations[definition]},
            registry=self.registry,
            format_checker=FormatChecker(),
        )
        if not validator.is_valid(value):
            raise HistoryError(
                status, "VALIDATION_FAILED" if status == 422 else "HISTORY_UNAVAILABLE"
            )
        return value
