

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BriefIntent(str, Enum):
    MAINTENANCE_BRIEF = "MAINTENANCE_BRIEF"
    TECHNICIAN_CHECKLIST = "TECHNICIAN_CHECKLIST"
    ALERT_EXPLANATION = "ALERT_EXPLANATION"


class GroundedStatus(str, Enum):
    GROUNDED = "GROUNDED"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    VALIDATION_FAILED = "VALIDATION_FAILED"


class AlertContext(ContractModel):
    alert_id: str = Field(min_length=1)
    machine_id: str = Field(min_length=1)
    severity: str = Field(min_length=1)
    status: str = Field(min_length=1)
    risk_score: float = Field(ge=0, le=1)
    policy_version: str = Field(min_length=1)


class PredictionContext(ContractModel):
    prediction_id: str = Field(min_length=1)
    prediction: str = Field(min_length=1)
    failure_probability: float = Field(ge=0, le=1)
    model_version: str = Field(min_length=1)


class SensorContext(ContractModel):
    product_type: str = Field(min_length=1)
    air_temperature_k: float
    process_temperature_k: float
    rotational_speed_rpm: float
    torque_nm: float
    tool_wear_min: float


class MaintenanceBriefRequest(ContractModel):
    alert: AlertContext
    prediction: PredictionContext
    sensor_context: SensorContext
    intent: BriefIntent


class ChecklistItem(ContractModel):
    step: int = Field(ge=1)
    action: str = Field(min_length=1)
    citation_ids: list[str]


class Citation(ContractModel):
    citation_id: str = Field(min_length=1)
    document_id: str = Field(min_length=1)
    document_version: str = Field(min_length=1)
    section: str = Field(min_length=1)
    page: int = Field(ge=1)
    chunk_id: str = Field(min_length=1)


class RetrievalInfo(ContractModel):
    top_k: int = Field(ge=1)
    retrieved_chunk_ids: list[str]


class GeneratorInfo(ContractModel):
    provider: str = Field(min_length=1)
    model: str = Field(min_length=1)


class MaintenanceBriefResponse(ContractModel):
    brief_id: str = Field(min_length=1)
    alert_id: str = Field(min_length=1)
    grounded_status: GroundedStatus
    summary: str
    uncertainty: str
    checklist: list[ChecklistItem]
    citations: list[Citation]
    retrieval: RetrievalInfo
    generator: GeneratorInfo | None = None
    generated_at: datetime | None = None

    @model_validator(mode="after")
    def validate_grounding_structure(self):
        """Validate structural references, not factual grounding."""

        if self.grounded_status != GroundedStatus.GROUNDED:
            if self.checklist or self.citations:
                raise ValueError(
                    "Non-grounded results must have empty checklist and citations"
                )
            return self

        if not self.checklist or not self.citations:
            raise ValueError(
                "GROUNDED requires nonempty checklist and citations"
            )

        if self.generator is None or self.generated_at is None:
            raise ValueError(
                "GROUNDED requires generator and generated_at"
            )

        citation_ids = [
            citation.citation_id for citation in self.citations
        ]

        if len(citation_ids) != len(set(citation_ids)):
            raise ValueError("Duplicate citation_id")

        retrieved_ids = set(self.retrieval.retrieved_chunk_ids)

        for citation in self.citations:
            if citation.chunk_id not in retrieved_ids:
                raise ValueError(
                    "Citation chunk_id not present in retrieved_chunk_ids"
                )

        known_citations = set(citation_ids)

        for item in self.checklist:
            if not item.citation_ids:
                raise ValueError(
                    "Each checklist item must cite at least one source"
                )

            if not set(item.citation_ids).issubset(known_citations):
                raise ValueError(
                    "Checklist references unknown citation_id"
                )

        return self
