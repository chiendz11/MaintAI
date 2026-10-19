"""Construct Kafka outcomes from real DQ/ML/risk outputs and source coordinates."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass

from .contracts import default_contracts
from .errors import ContractViolation
from .ingestion import utc_now


@dataclass(frozen=True)
class ModelIdentity:
    model_version: str
    preprocessor_version: str
    dataset_version: str
    dataset_manifest_sha256: str
    split_sha256: str
    mlflow_run_id: str
    model_artifact_sha256: str
    preprocessor_sha256: str
    feature_schema_version: str
    feature_schema_sha256: str
    release_id: str
    inference_image_digest: str


@dataclass(frozen=True)
class Prediction:
    prediction_id: str
    prediction: str
    failure_probability: float
    model_threshold: float
    completed_at: str
    model: ModelIdentity


@dataclass(frozen=True)
class RiskDecision:
    risk_decision_id: str
    policy_version: str
    policy_sha256: str
    machine_state: str
    risk_score: float


class OutcomeFactory:
    def __init__(
        self,
        source,
        record,
        *,
        dq_ruleset_version: str,
        dq_ruleset_sha256: str,
        producer_git_sha: str,
        contracts=None,
    ):
        if (
            source.topic != "sensor.readings"
            or record.topic != source.topic
            or record.value != source.wire_bytes
        ):
            raise ContractViolation("Outcome source must be the actual consumed sensor reading")
        self.contracts = contracts or default_contracts()
        payload = source.payload
        self.common = {
            "schema_version": "1.4.0",
            "result_id": "res_" + hashlib.sha256(payload["event_id"].encode()).hexdigest(),
            "event_id": payload["event_id"],
            "machine_id": payload["machine_id"],
            "stream_epoch": payload["stream_epoch"],
            "sequence_number": payload["sequence_number"],
            "source_schema_version": source.schema_version,
            "source_topic": record.topic,
            "source_partition": record.partition,
            "source_offset": record.offset,
            "source_server_accepted_at": payload["server_accepted_at"],
            "correlation_id": payload["correlation_id"],
            "producer_git_sha": producer_git_sha,
            "dq_ruleset_version": dq_ruleset_version,
            "dq_ruleset_sha256": dq_ruleset_sha256,
        }

    def _checked(self, value):
        self.contracts.encode("inference.results", value)
        return value

    def completed(self, prediction: Prediction, risk: RiskDecision):
        fields = asdict(prediction)
        fields.pop("model")
        return self._checked(
            {
                **self.common,
                "kind": "PREDICTION_COMPLETED",
                "occurred_at": utc_now(),
                "validation_status": "VALID",
                "processing_status": "COMPLETED",
                **fields,
                **asdict(prediction.model),
                **asdict(risk),
            }
        )

    def invalid(self, issues: list[dict]):
        return self._checked(
            {
                **self.common,
                "kind": "DATA_INVALID",
                "occurred_at": utc_now(),
                "validation_status": "INVALID",
                "processing_status": "INVALID",
                "validation_errors": issues,
            }
        )

    def unavailable(self, failure_code: str, attempts: int):
        return self._checked(
            {
                **self.common,
                "kind": "INFERENCE_UNAVAILABLE",
                "occurred_at": utc_now(),
                "validation_status": "VALID",
                "processing_status": "FAILED",
                "failure": {"code": failure_code, "attempts": attempts},
            }
        )

    def alert_signal(self, completed):
        event = self.contracts.encode("inference.results", completed)
        if event.kind != "PREDICTION_COMPLETED":
            raise ContractViolation("Only a completed prediction may produce an alert signal")
        if completed["machine_state"] == "NORMAL":
            return None
        signal = {
            "schema_version": "1.0.0",
            "signal_id": "sig_"
            + hashlib.sha256(completed["risk_decision_id"].encode()).hexdigest(),
            "event_id": completed["event_id"],
            "machine_id": completed["machine_id"],
            "prediction_id": completed["prediction_id"],
            "severity": completed["machine_state"],
            "model_version": completed["model_version"],
            "policy_version": completed["policy_version"],
            "correlation_id": completed["correlation_id"],
            "risk_score": completed["risk_score"],
            "decided_at": completed["occurred_at"],
        }
        self.contracts.encode("alert.signals", signal)
        return signal
