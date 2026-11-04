"""Explicit RAG and Drift publishers for browser acceptance; real HTTP/S3/Business SQL."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from fastapi import HTTPException, Request
from maintai_messaging.contracts import canonical_json

ROOT = Path(__file__).resolve().parents[3]


def prepare(app, archive, directory):
    mode = {"value": "GROUNDED"}
    token = directory / "rag-token"
    token.write_text("m7-provider-fixture-token")
    key = directory / "drift-cursor"
    key.write_bytes(hashlib.sha256(str(directory).encode()).digest())

    @app.post("/fixture/rag-mode")
    async def set_mode(request: Request):
        mode["value"] = (await request.json())["value"]
        return {"status": "OK"}

    @app.post("/api/v1/briefs")
    async def rag(request: Request):
        if request.headers.get("Authorization") != "Bearer m7-provider-fixture-token":
            raise HTTPException(401)
        if mode["value"] == "UNAVAILABLE":
            raise HTTPException(503)
        payload = await request.json()
        value = json.loads(
            (ROOT / "contracts/v1.3/examples/http/backend/copilot-grounded.json").read_text()
        )["brief"]
        value["alert_id"] = payload["alert"]["alert_id"]
        value["brief_id"] = (
            "fixture_" + hashlib.sha256(request.headers["Idempotency-Key"].encode()).hexdigest()
        )
        if mode["value"] != "GROUNDED":
            value.update(grounded_status=mode["value"], checklist=[], citations=[])
        return value

    report = json.loads(
        (ROOT / "contracts/v1.3/examples/http/drift/report-stable.json").read_text()
    )
    raw = canonical_json(report)
    report_key = "drift-reports/" + report["report_id"] + ".json"
    published = archive.client.put_object(Bucket=archive.export_bucket, Key=report_key, Body=raw)
    now = datetime.now(UTC).isoformat()
    manifest = {
        "schema_version": "1.0.0",
        "report_id": report["report_id"],
        "computed_at": report["computed_at"],
        "publication_at": now,
        **report["reference"],
        "overall_status": report["overall_status"],
        "report_ref": {
            "bucket": archive.export_bucket,
            "key": report_key,
            "version_id": published["VersionId"],
            "sha256": hashlib.sha256(raw).hexdigest(),
            "size_bytes": len(raw),
        },
    }
    archive.client.put_object(
        Bucket=archive.export_bucket,
        Key="drift-report-manifests/published/" + now[:10] + "/" + report["report_id"] + ".json",
        Body=canonical_json(manifest),
    )
    return {
        "BUSINESS_M7_ENABLED": "true",
        "BUSINESS_RAG_URL": "http://127.0.0.1:18033",
        "BUSINESS_RAG_TOKEN_FILE": str(token),
        "BUSINESS_DRIFT_CURSOR_KEY_FILE": str(key),
        "BUSINESS_DRIFT_BUCKET": archive.export_bucket,
        "BUSINESS_DRIFT_S3_REGION": "us-east-1",
        "BUSINESS_DRIFT_S3_ENDPOINT": "http://127.0.0.1:39000",
        "BUSINESS_MONITORING_MODEL_VERSION": report["reference"]["model_version"],
        "AWS_ACCESS_KEY_ID": "maintai_m6_test",
        "AWS_SECRET_ACCESS_KEY": "maintai_m6_test_only",
    }


def monitoring(factory, archive, env):
    from backend.shared.monitoring.catalog import DriftCatalog
    from backend.shared.monitoring.s3 import S3Reports

    return DriftCatalog(
        factory,
        S3Reports(archive.client, archive.export_bucket),
        Path(env["BUSINESS_DRIFT_CURSOR_KEY_FILE"]).read_bytes(),
        model_version=env["BUSINESS_MONITORING_MODEL_VERSION"],
    )
