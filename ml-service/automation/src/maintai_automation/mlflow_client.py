"""Thin MLflow 2.x REST API v2.0 client registering runs and @shadow model versions."""

from __future__ import annotations

import os
import time

import httpx


class MlflowApiError(RuntimeError):
    """Raised when MLflow tracking or registry REST calls fail."""


class MlflowRestClient:
    """HTTP client talking directly to MLflow REST API `/api/2.0/mlflow/...`."""

    def __init__(
        self,
        tracking_uri: str | None = None,
        *,
        public_url: str | None = None,
        client: httpx.Client | None = None,
        timeout: float = 15.0,
    ):
        uri = (tracking_uri or os.environ.get("MLFLOW_TRACKING_URI") or "").rstrip("/")
        if not uri:
            raise ValueError("MLFLOW_TRACKING_URI is required")
        self.base_url = uri
        self.public_url = (public_url or os.environ.get("MLFLOW_PUBLIC_URL") or uri).rstrip("/")
        self._client = client
        self._timeout = timeout

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict | None = None,
        params: dict | None = None,
    ) -> dict:
        own = self._client is None
        client = httpx.Client(timeout=self._timeout) if own else self._client
        assert client is not None
        try:
            response = client.request(
                method,
                f"{self.base_url}{path}",
                json=json_body,
                params=params,
            )
            if response.status_code >= 400:
                raise MlflowApiError(
                    f"MLflow {method} {path} failed ({response.status_code}): {response.text[:300]}"
                )
            return response.json() if response.content else {}
        except httpx.HTTPError as exc:
            raise MlflowApiError(f"MLflow request error on {path}: {exc}") from exc
        finally:
            if own:
                client.close()

    def ensure_experiment(self, name: str) -> str:
        own = self._client is None
        client = httpx.Client(timeout=self._timeout) if own else self._client
        assert client is not None
        try:
            resp = client.get(
                f"{self.base_url}/api/2.0/mlflow/experiments/get-by-name",
                params={"experiment_name": name},
            )
            if resp.status_code == 200:
                return str(resp.json()["experiment"]["experiment_id"])
            if resp.status_code != 404:
                raise MlflowApiError(f"MLflow get experiment failed ({resp.status_code})")
        except httpx.HTTPError as exc:
            raise MlflowApiError(f"MLflow experiment lookup failed: {exc}") from exc
        finally:
            if own:
                client.close()

        created = self._request(
            "POST",
            "/api/2.0/mlflow/experiments/create",
            json_body={"name": name},
        )
        return str(created["experiment_id"])

    def create_run(self, experiment_id: str, *, run_name: str, tags: dict[str, str]) -> str:
        now_ms = int(time.time() * 1000)
        payload = {
            "experiment_id": experiment_id,
            "run_name": run_name,
            "start_time": now_ms,
            "tags": [{"key": k, "value": str(v)} for k, v in tags.items()],
        }
        created = self._request("POST", "/api/2.0/mlflow/runs/create", json_body=payload)
        return str(created["run"]["info"]["run_id"])

    def log_batch(
        self,
        run_id: str,
        *,
        metrics: dict[str, float],
        params: dict[str, str],
        tags: dict[str, str] | None = None,
    ) -> None:
        now_ms = int(time.time() * 1000)
        payload: dict = {
            "run_id": run_id,
            "metrics": [
                {"key": k, "value": float(v), "timestamp": now_ms, "step": 0}
                for k, v in metrics.items()
            ],
            "params": [{"key": k, "value": str(v)} for k, v in params.items()],
        }
        if tags:
            payload["tags"] = [{"key": k, "value": str(v)} for k, v in tags.items()]
        self._request("POST", "/api/2.0/mlflow/runs/log-batch", json_body=payload)

    def finish_run(self, run_id: str, *, status: str = "FINISHED") -> None:
        self._request(
            "POST",
            "/api/2.0/mlflow/runs/update",
            json_body={"run_id": run_id, "status": status, "end_time": int(time.time() * 1000)},
        )

    def ensure_registered_model(self, name: str) -> None:
        own = self._client is None
        client = httpx.Client(timeout=self._timeout) if own else self._client
        assert client is not None
        try:
            resp = client.get(
                f"{self.base_url}/api/2.0/mlflow/registered-models/get",
                params={"name": name},
            )
            if resp.status_code == 200:
                return
            if resp.status_code != 404:
                raise MlflowApiError(f"MLflow get registered model failed ({resp.status_code})")
        except httpx.HTTPError as exc:
            raise MlflowApiError(f"MLflow registered model lookup failed: {exc}") from exc
        finally:
            if own:
                client.close()

        self._request(
            "POST",
            "/api/2.0/mlflow/registered-models/create",
            json_body={"name": name},
        )

    def register_shadow_version(
        self,
        *,
        experiment_name: str,
        model_name: str,
        model_version_label: str,
        artifact_source_uri: str,
        metrics: dict[str, float],
        params: dict[str, str],
        tags: dict[str, str],
    ) -> dict[str, str]:
        experiment_id = self.ensure_experiment(experiment_name)
        run_id = self.create_run(
            experiment_id,
            run_name=model_version_label,
            tags={"mlflow.runName": model_version_label, **tags},
        )
        try:
            self.log_batch(run_id, metrics=metrics, params=params)
        except Exception:
            try:
                self.finish_run(run_id, status="FAILED")
            except Exception:
                pass
            raise
        self.finish_run(run_id, status="FINISHED")

        self.ensure_registered_model(model_name)
        version_resp = self._request(
            "POST",
            "/api/2.0/mlflow/model-versions/create",
            json_body={
                "name": model_name,
                "source": artifact_source_uri,
                "run_id": run_id,
                "tags": [{"key": k, "value": str(v)} for k, v in tags.items()],
            },
        )
        registered_version = str(version_resp["model_version"]["version"])
        self._request(
            "POST",
            "/api/2.0/mlflow/registered-models/alias",
            json_body={
                "name": model_name,
                "alias": "shadow",
                "version": registered_version,
            },
        )
        run_url = f"{self.public_url}/#/experiments/{experiment_id}/runs/{run_id}"
        return {
            "experiment_id": experiment_id,
            "run_id": run_id,
            "run_url": run_url,
            "registered_model_version": registered_version,
            "alias": "shadow",
        }
