"""Business validates the M6 provider bundle without importing Processing code."""

import os
import sys
from pathlib import Path

from api.auth import RegistryFailure
from jsonschema import Draft202012Validator, FormatChecker
from maintai_messaging.contracts import default_contracts, strict_json
from referencing import Resource


class HistorySchemas:
    def __init__(self, directory=None):
        root = Path(
            directory
            or os.environ.get(
                "BUSINESS_HISTORY_SCHEMA_DIRECTORY",
                Path(sys.prefix) / "share/maintai-business/contracts",
            )
        )
        bundle = default_contracts()
        self.registry = bundle.registry
        self.locations = {}
        self.locations["HistoryRecord"] = (
            bundle.root / "v1.3/schemas/runtime.schema.json"
        ).as_uri() + "#/$defs/HistoryRecord"
        for name in ("history-m6", "raw-catalog-m6"):
            value = strict_json((root / (name + ".schema.json")).read_bytes())
            uri = (bundle.root / "v1.3/schemas" / (name + ".schema.json")).as_uri()
            self.registry = self.registry.with_resource(uri, Resource.from_contents(value))
            self.locations.update({key: uri + "#/$defs/" + key for key in value["$defs"]})

    def validate(self, definition, value, *, request=False):
        validator = Draft202012Validator(
            {"$ref": self.locations[definition]},
            registry=self.registry,
            format_checker=FormatChecker(),
        )
        if not validator.is_valid(value):
            raise RegistryFailure(
                422 if request else 503,
                "VALIDATION_FAILED" if request else "DEPENDENCY_UNAVAILABLE",
            )
        return value
