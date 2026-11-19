import base64
import json
import tempfile
import unittest
from pathlib import Path

import httpx
import yaml
from maintai_automation.github_pr import GitHubPullRequestClient
from maintai_automation.gitops_yaml import GitOpsContractError
from maintai_automation.promote import run_promote_shadow
from maintai_automation.publish import publish_shadow_pr

SAMPLE_KUST = """apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: maintai-local
resources:
  - ../../../base
  - ml-service-shadow.yaml
  - ml-traffic-mirror.yaml
  - business-edge.yaml
configMapGenerator:
  - name: ml-service-artifacts
    literals:
      - >-
        artifacts.json=[{"bucket":"maintai-models","key":"models/old/pipeline.joblib",
        "version_id":"v-old","path":"/local/models/old/pipeline.joblib",
        "sha256":"1111111111111111111111111111111111111111111111111111111111111111"}]
  - name: ml-service-config
    literals:
      - MODEL_DIR=/local/models/old
      - SERVICE_ROLE=champion
      - PRODUCER_GIT_SHA=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      - S3_ENDPOINT_URL=http://minio:9000
  - name: ml-service-shadow-artifacts
    literals:
      - >-
        artifacts.json=[{"bucket":"maintai-models","key":"models/old-shadow/pipeline.joblib",
        "version_id":"BOOTSTRAP_UPLOAD_REQUIRED","path":"/local/models/old-shadow/pipeline.joblib",
        "sha256":"2222222222222222222222222222222222222222222222222222222222222222"}]
  - name: ml-service-shadow-config
    literals:
      - MODEL_DIR=/local/models/old-shadow
      - SERVICE_ROLE=shadow
      - PRODUCER_GIT_SHA=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
      - S3_ENDPOINT_URL=http://minio:9000
  - name: processing-worker-config
    literals:
      - ML_BASE_URL=http://ml-service:8001
"""

SAMPLE_HPA = """apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: business-api-hpa
  namespace: maintai-local
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: business-api
  minReplicas: 1
  maxReplicas: 4
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: ml-service-shadow-hpa
  namespace: maintai-local
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: ml-service-shadow
  minReplicas: 1
  maxReplicas: 2
"""


def _mock_github(initial_files: dict[str, str]):
    files = dict(initial_files)
    created_prs: list[dict] = []
    branches: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "GET" and path.endswith("/git/ref/heads/main"):
            return httpx.Response(200, json={"object": {"sha": "base-sha-123"}})
        if request.method == "POST" and path.endswith("/git/refs"):
            payload = json.loads(request.content.decode("utf-8"))
            branches.append(payload["ref"])
            return httpx.Response(201, json=payload)
        if request.method == "GET" and "/contents/" in path:
            file_path = path.split("/contents/", 1)[1]
            encoded = base64.b64encode(files[file_path].encode("utf-8")).decode("ascii")
            return httpx.Response(200, json={"content": encoded, "sha": f"sha-{file_path}"})
        if request.method == "PUT" and "/contents/" in path:
            file_path = path.split("/contents/", 1)[1]
            payload = json.loads(request.content.decode("utf-8"))
            files[file_path] = base64.b64decode(payload["content"]).decode("utf-8")
            return httpx.Response(200, json={"commit": {"sha": "new-sha"}})
        if request.method == "POST" and path.endswith("/pulls"):
            payload = json.loads(request.content.decode("utf-8"))
            created_prs.append(payload)
            pr_num = len(created_prs)
            return httpx.Response(
                201,
                json={
                    "number": pr_num,
                    "html_url": f"https://github.com/chiendz11/MaintAI-gitops/pull/{pr_num}",
                },
            )
        return httpx.Response(404)

    return httpx.Client(transport=httpx.MockTransport(handler)), files, created_prs, branches


def _published_fixture() -> dict:
    mv = "hgb-interactions-1234abcd"
    names = ("pipeline.joblib", "metadata.json", "evaluation.json", "drift_reference.json")
    arts = []
    for i, name in enumerate(names):
        arts.append(
            {
                "bucket": "maintai-models",
                "key": f"models/{mv}/{name}",
                "version_id": f"vid-{i + 1}",
                "path": f"/local/models/{mv}/{name}",
                "sha256": chr(ord("a") + i) * 64,
            }
        )
    return {
        "schema_version": "1.0.0",
        "model_version": mv,
        "dataset_version": "ai4i2020-approved-v1",
        "manifest_key": "datasets/approved/ai4i-v1.manifest.json",
        "manifest_version_id": "man-v1",
        "dataset_version_id": "ds-v1",
        "source_git_sha": "c" * 40,
        "threshold": 0.089434,
        "validation_metrics": {
            "recall_failure": 0.8529,
            "precision_failure": 0.8788,
            "f1": 0.865,
            "f2": 0.858,
            "average_precision": 0.91,
        },
        "test_metrics": {
            "recall_failure": 0.8824,
            "precision_failure": 0.7792,
            "f1": 0.827,
            "f2": 0.859,
            "average_precision": 0.89,
        },
        "artifacts": arts,
        "mlflow": {
            "run_id": "run-42",
            "run_url": "http://127.0.0.1:15000/#/experiments/1/runs/run-42",
            "registered_model_version": "4",
        },
    }


def _cm_literals(kust_doc: dict, name: str) -> dict[str, str]:
    entry = next(x for x in kust_doc["configMapGenerator"] if x["name"] == name)
    return dict(line.split("=", 1) for line in entry["literals"])


class PublishAndPromoteTest(unittest.TestCase):
    def test_publish_shadow_pr_then_promote_with_decommission(self):
        client, repo_files, prs, _branches = _mock_github(
            {
                "environments/local/apps/kustomization.yaml": SAMPLE_KUST,
                "environments/local/apps/hpa.yaml": SAMPLE_HPA,
            }
        )
        gh = GitHubPullRequestClient(token="ghp_testtoken", client=client)

        with tempfile.TemporaryDirectory() as tmp:
            slot = Path(tmp)
            published = _published_fixture()
            (slot / "published-artifacts.json").write_text(json.dumps(published), encoding="utf-8")
            res1 = publish_shadow_pr(directory=slot, github_client=gh)

        self.assertEqual(res1["pr_number"], 1)
        self.assertIn("Recall (Failure)", prs[0]["body"])
        self.assertIn("http://127.0.0.1:15000/#/experiments/1/runs/run-42", prs[0]["body"])
        kust_after_shadow = yaml.safe_load(repo_files["environments/local/apps/kustomization.yaml"])
        proc_lits = _cm_literals(kust_after_shadow, "processing-worker-config")
        self.assertEqual(proc_lits["ML_BASE_URL"], "http://ml-service-mirror:8001")

        # Now run promote-shadow with decommission_shadow=True
        res2 = run_promote_shadow(decommission_shadow=True, github_client=gh)
        self.assertEqual(res2["pr_number"], 2)
        kust_after_promote = yaml.safe_load(
            repo_files["environments/local/apps/kustomization.yaml"]
        )
        self.assertNotIn("ml-service-shadow.yaml", kust_after_promote["resources"])
        self.assertNotIn("ml-traffic-mirror.yaml", kust_after_promote["resources"])

        champ_cfg = _cm_literals(kust_after_promote, "ml-service-config")
        self.assertEqual(champ_cfg["MODEL_DIR"], "/local/models/hgb-interactions-1234abcd")
        self.assertEqual(champ_cfg["SERVICE_ROLE"], "champion")
        self.assertEqual(champ_cfg["PRODUCER_GIT_SHA"], "c" * 40)

        proc_after_promote = _cm_literals(kust_after_promote, "processing-worker-config")
        self.assertEqual(proc_after_promote["ML_BASE_URL"], "http://ml-service:8001")

        hpa_docs = list(yaml.safe_load_all(repo_files["environments/local/apps/hpa.yaml"]))
        self.assertNotIn("ml-service-shadow-hpa", [d["metadata"]["name"] for d in hpa_docs])

    def test_promote_rejects_placeholder_version_id(self):
        client, _, _, _ = _mock_github(
            {
                "environments/local/apps/kustomization.yaml": SAMPLE_KUST,
                "environments/local/apps/hpa.yaml": SAMPLE_HPA,
            }
        )
        gh = GitHubPullRequestClient(token="ghp_testtoken", client=client)
        with self.assertRaises(GitOpsContractError):
            run_promote_shadow(decommission_shadow=True, github_client=gh)
