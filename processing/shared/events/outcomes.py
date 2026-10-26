"""Map validated provider output into the canonical SDK prediction/risk DTOs."""

from maintai_messaging.outcomes import Prediction, RiskDecision

from processing.shared.inference_client.model_identity import resolve_model_identity
from processing.shared.ordering.identity import risk_decision_id
from processing.shared.risk_engine.engine import evaluate_risk


def completed_outcome(factory, response, *, verified_identity, policy):
    identity = resolve_model_identity(response, verified_identity=verified_identity)
    evaluation = evaluate_risk(failure_probability=response.failure_probability, policy=policy)
    prediction = Prediction(
        prediction_id=response.prediction_id,
        prediction=response.prediction,
        failure_probability=response.failure_probability,
        model_threshold=response.model_threshold,
        completed_at=response.predicted_at.isoformat(),
        model=identity,
    )
    risk = RiskDecision(
        risk_decision_id=risk_decision_id(response.event_id),
        policy_version=evaluation.policy_version,
        policy_sha256=evaluation.policy_sha256,
        machine_state=evaluation.machine_state,
        risk_score=evaluation.risk_score,
    )
    return factory.completed(prediction, risk), evaluation
