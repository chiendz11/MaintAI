"""Read-only HTTP client for Processing Read API `/api/v1/internal/runtime-windows`."""

from datetime import UTC, datetime, timedelta

import httpx
from pydantic import ValidationError

from drift_monitor.config import DriftConfig
from drift_monitor.models import Features, RuntimeSample


class ProcessingApiError(RuntimeError):
    """Raised when Processing Read API or OIDC token retrieval fails safely."""

    def __init__(self, code: str, message: str = "Processing runtime window request failed"):
        self.code = code
        super().__init__(message)


def _format_utc(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat().replace("+00:00", "Z")


class ProcessingApiWindowSource:
    """Paginates frozen `RuntimeWindow` payloads into sorted `RuntimeSample` records."""

    def __init__(self, config: DriftConfig, *, client: httpx.Client | None = None):
        if not config.processing_read_url:
            raise ValueError("processing_read_url is required for ProcessingApiWindowSource")
        self.config = config
        self._base_url = config.processing_read_url.rstrip("/")
        self._client = client

    def _acquire_token(self, client: httpx.Client) -> str:
        if self.config.processing_token_file is not None:
            try:
                token = self.config.processing_token_file.read_text(encoding="utf-8").strip()
            except OSError as exc:
                raise ProcessingApiError("PROCESSING_AUTH_FAILED", "Token file unreadable") from exc
            if not token:
                raise ProcessingApiError("PROCESSING_AUTH_FAILED", "Token file is empty")
            return token

        assert self.config.oidc_client_secret_file is not None
        try:
            secret = self.config.oidc_client_secret_file.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ProcessingApiError("PROCESSING_AUTH_FAILED", "OIDC secret unreadable") from exc
        if not secret:
            raise ProcessingApiError("PROCESSING_AUTH_FAILED", "OIDC client secret is empty")

        try:
            response = client.post(
                self.config.oidc_token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.config.oidc_client_id,
                    "client_secret": secret,
                },
            )
            response.raise_for_status()
            payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise ProcessingApiError("PROCESSING_AUTH_FAILED", "OIDC token request failed") from exc

        token = payload.get("access_token") if isinstance(payload, dict) else None
        if not isinstance(token, str) or not token:
            raise ProcessingApiError("PROCESSING_AUTH_FAILED", "Missing access_token in OIDC reply")
        return token

    def _to_sample(
        self, item: object, *, expected_model_version: str, expected_dataset_version: str
    ) -> RuntimeSample | None:
        if not isinstance(item, dict):
            raise ProcessingApiError("PROCESSING_API_INVALID_PAYLOAD")
        outcome = item.get("outcome")
        if not isinstance(outcome, dict):
            raise ProcessingApiError("PROCESSING_API_INVALID_PAYLOAD")
        if (
            outcome.get("kind") != "PREDICTION_COMPLETED"
            or outcome.get("validation_status") != "VALID"
            or outcome.get("model_version") != expected_model_version
            or outcome.get("dataset_version") != expected_dataset_version
        ):
            return None
        try:
            event_id = item["source_event_id"]
            accepted_raw = item["source_server_accepted_at"]
            if not isinstance(event_id, str) or not isinstance(accepted_raw, str):
                raise ValueError("Invalid event identity fields")
            received_at = datetime.fromisoformat(accepted_raw.replace("Z", "+00:00"))
            if received_at.utcoffset() is None:
                raise ValueError("Timezone offset required")
            features = Features.model_validate(item["features_raw"])
            prediction_label = outcome.get("prediction")
            if prediction_label not in ("NORMAL", "FAILURE"):
                prediction_label = None
            return RuntimeSample(
                event_id=event_id,
                received_at=received_at.astimezone(UTC),
                features=features,
                prediction_label=prediction_label,
            )
        except (KeyError, TypeError, ValueError, ValidationError) as exc:
            raise ProcessingApiError("PROCESSING_API_INVALID_PAYLOAD") from exc

    def load_window(
        self,
        *,
        model_version: str,
        dataset_version: str,
        window_size: int,
        window_end: datetime,
    ) -> list[RuntimeSample]:
        window_end_utc = window_end.astimezone(UTC)
        window_start_utc = window_end_utc - timedelta(hours=self.config.window_lookback_hours)

        own_client = self._client is None
        client = httpx.Client(timeout=self.config.timeout_seconds) if own_client else self._client
        assert client is not None
        try:
            token = self._acquire_token(client)
            headers = {"Authorization": f"Bearer {token}"}
            collected: dict[str, RuntimeSample] = {}
            before: str | None = None
            seen_cursors: set[str] = set()

            while len(collected) < window_size:
                remaining = window_size - len(collected)
                params: dict[str, str | int] = {
                    "model_version": model_version,
                    "dataset_version": dataset_version,
                    "window_start": _format_utc(window_start_utc),
                    "window_end": _format_utc(window_end_utc),
                    "limit": min(500, remaining),
                }
                if before is not None:
                    params["before"] = before
                try:
                    response = client.get(
                        f"{self._base_url}/api/v1/internal/runtime-windows",
                        params=params,
                        headers=headers,
                    )
                    response.raise_for_status()
                    payload = response.json()
                except (httpx.HTTPError, ValueError) as exc:
                    raise ProcessingApiError("PROCESSING_API_UNAVAILABLE") from exc

                if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
                    raise ProcessingApiError("PROCESSING_API_INVALID_PAYLOAD")
                items = payload["items"]
                if not items:
                    break
                for item in items:
                    sample = self._to_sample(
                        item,
                        expected_model_version=model_version,
                        expected_dataset_version=dataset_version,
                    )
                    if sample is None:
                        continue
                    existing = collected.get(sample.event_id)
                    if existing is not None and existing != sample:
                        raise ProcessingApiError("PROCESSING_API_INVALID_PAYLOAD")
                    collected[sample.event_id] = sample
                    if len(collected) >= window_size:
                        break
                next_before = payload.get("next_before")
                if not next_before:
                    break
                if not isinstance(next_before, str) or next_before in seen_cursors:
                    raise ProcessingApiError("PROCESSING_API_INVALID_PAYLOAD")
                seen_cursors.add(next_before)
                before = next_before

            return sorted(
                collected.values(),
                key=lambda row: (row.received_at, row.event_id),
            )[:window_size]
        finally:
            if own_client:
                client.close()
