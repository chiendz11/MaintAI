"""Validate a separately verified identity without inventing a provider DTO.

The caller must obtain identity from an approved, serving-bound source. Passing
schema checks proves shape and version consistency, not artifact authenticity.
Until provider/release handoff is frozen this boundary is exercised with fixtures
only; the baseline ML response cannot supply the missing fields itself.
"""

from dataclasses import asdict, fields

from jsonschema import Draft202012Validator
from maintai_messaging.contracts import default_contracts
from maintai_messaging.outcomes import ModelIdentity

from .client import PredictionResponse


def resolve_model_identity(
    response: PredictionResponse,
    *,
    verified_identity: ModelIdentity | None,
) -> ModelIdentity:
    validate_model_identity(verified_identity)
    if (
        response.model_version != verified_identity.model_version
        or response.preprocessor_version != verified_identity.preprocessor_version
    ):
        raise ValueError("Serving model identity does not match prediction response")
    return verified_identity


def validate_model_identity(verified_identity):
    """Validate identity shape only; this does not attest artifact authenticity."""
    if not isinstance(verified_identity, ModelIdentity):
        raise ValueError("Verified serving model identity is required")
    contracts = default_contracts()
    document = contracts.documents[
        (contracts.directory / "events/inference-result.schema.json").resolve()
    ]
    completed = next(
        branch
        for branch in document["oneOf"]
        if branch["properties"]["kind"]["const"] == "PREDICTION_COMPLETED"
    )
    names = [field.name for field in fields(ModelIdentity)]
    schema = {
        "type": "object",
        "required": names,
        "additionalProperties": False,
        "properties": {name: completed["properties"][name] for name in names},
    }
    if not Draft202012Validator(schema).is_valid(asdict(verified_identity)):
        raise ValueError("Incomplete or invalid model identity")
