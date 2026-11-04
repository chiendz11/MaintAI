"""Validate the canonical HTTP bundle in both checkout and installed releases."""

import sys
from functools import lru_cache
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from maintai_messaging.contracts import strict_json
from referencing import Registry, Resource


@lru_cache(maxsize=1)
def bundle():
    root = Path(sys.prefix) / "share/maintai-business/contracts"
    source = Path(__file__).resolve().parents[3] / "contracts/v1.3/schemas"
    if source.is_dir():
        root = source
    registry = Registry()
    for path in root.glob("*.schema.json"):
        registry = registry.with_resource(
            path.as_uri(), Resource.from_contents(strict_json(path.read_bytes()))
        )
    return root, registry


def valid(name, value, definition=None):
    root, registry = bundle()
    uri = (root / (name + ".schema.json")).as_uri()
    if definition:
        uri += "#/$defs/" + definition
    return Draft202012Validator(
        {"$ref": uri}, registry=registry, format_checker=FormatChecker()
    ).is_valid(value)
