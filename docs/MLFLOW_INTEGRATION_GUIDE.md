# MLFLOW-01 — Training, Registry và phục hồi MLflow

**Revision:** `1.0.0`, quyết định ngày2026-10-20, bổ sung System Contracts v1.4
§41 và Guide §43.2/§49. Đây là hướng dẫn implementation mục tiêu, chưa triển khai
trainer, ledger adapter hoặc MLflow deployment. Không thay schema bằng ví dụ code.

Đọc cùng [Versioning & Provenance](VERSIONING_AND_PROVENANCE.md),
[Metrics catalog](OBSERVABILITY_IMPLEMENTATION.md), [readiness](implementation-readiness.md).

## 1. Ai làm gì và khi nào cần MLflow

| Thành phần | Trách nhiệm |
| --- | --- |
| App/ml-service/training — Người2 | Data/split, fit/evaluate, log runs/params/metrics/artifacts, register selected candidate, xuất provenance thật |
| App/ml-service/inference — Người2 | Load approved release, xác minh model/preprocessor/reference, xuất effective model info; không gọi MLflow trên mỗi predict |
| App/ml-service/mlflow — ML + Platform | Upstream server image packaging, dependency/base pins, local dev và signed image publisher |
| GitOps/platform — Người4 | MLflow production manifests/config, namespace provisioning, DB/S3 connectivity/auth, verified digest PR và vận hành |
| Infrastructure — Người4 | PostgreSQL instance, S3 versioning/encryption/IAM, network và durable training ledger infrastructure |
| Drift — Người4, ML review Người2 | Chỉ đọc đúng reference/model release, xuất report/metrics; không tự train/register/promote |

Một MLflow Server cung cấp Tracking + Registry + UI. Training chỉ gọi SDK/API,
không SQL vào mlflow_db. Server quản lý schema/migrations của DB đó. Grafana
theo dõi runtime health/latency/drift; MLflow giữ experiment/evaluation/model
evidence theo run. Không đưa run_id thành Prometheus label hoặc gửi sensor record
tới MLflow trên mỗi inference request.

```text
Approved dataset manifest + evaluation protocol + training context
 -> orchestration claim training_id -> app training image
 -> prepare/split/train/evaluate -> Tracking runs + verified artifacts
 -> offline gate -> Registry selected candidate version
 -> immutable model-result/provenance -> approved GitOps PR
 -> inference loads pinned release -> verify effective identities
```

UI mở hay đóng không ảnh hưởng SDK logging. MLflow outage chặn việc hoàn tất
candidate/release có đủ evidence; không chặn inference đã Ready với verified
local model. Cold start còn cần artifact store nếu chưa tải bundle.

## 2. Compatibility và cấu hình

`ml-service/mlflow/image-lock.json` trong app giữ base/server-version pins; cả
hai hiện chưa cấu hình, publisher bị chặn. GitOps giữ production image digest
và config, không build server. App training
còn skeleton. Ví dụ SDK dưới đây theo API OSS MLflow3.x đã đối chiếu20/10/2026;
không coi `3.x` là dependency lock. Trước PR source, Người2+Người4 phải chọn và
ghi **exact server/client patch versions**, Python/sklearn/serialization versions,
training image digest và lockfile SHA; chạy client/server/artifact compatibility
tests trên đúng cặp đó. Không dùng `latest` hoặc ngầm chuyển sang managed registry.
Server image phải hỗ trợ các flags GitOps đang dùng như --serve-artifacts và
--allowed-hosts; SDK pair phải có synchronous log_batch và tags-at-create theo
snippets. Không chọn một patch chỉ vì cùng major3 mà bỏ qua capability tests.

### 2.1. Quyết định nguồn image và local dev — 21/10/2026

Source Dockerfile/requirements lock/local Compose/README/publisher nằm ở
[`ml-service/mlflow/`](../ml-service/mlflow/README.md), cùng ML domain nhưng
image/runtime riêng với training và inference. Server/UI dùng upstream MLflow.
Production Deployment/Service/DB/S3/IAM/auth/config vẫn do GitOps/hạ tầng quản lý.
App runtime map giữ14 application descriptors +1 platform descriptor MLflow;
không tạo thêm service/DB/UI chỉ vì chuyển source.

Workflow `chiendz11/MaintAI/.github/workflows/maintai-publish.yml` build/smoke,
push ECR và ký exact digest. GitOps chỉ tin certificate identity workflow này
ở main cùng GitHub OIDC issuer, source SHA/image_group/source_repository annotations
và app publisher actor. Nguồn GitOps cũ bị từ chối cho group mlflow; không tự ký
lại image cũ để đổi danh tính. Intent dùng workload-release1.0.0 hiện hành,
architecture handoff1.3.1+simulator-ui.1 và empty config-schema map cho platform
group; không giả platform đã migrate toàn bộ1.4. Receiver chỉ mở PR đổi digest,
không bật MLflow, đổi credentials hoặc auto merge.

Local Compose dùng cùng Dockerfile với PostgreSQL17 và filesystem artifact volume,
bind UI127.0.0.1. Nó không chứng minh production S3/IRSA/auth đã hoạt động. Muốn dev
giống object store production cần integration S3-compatible riêng; không tự nối AWS.
Chưa có base/server/client compatibility pins hoặc real container evidence; phải
điền reviewed digests/version và kiểm cặp SDK/server trước khi publish.

IAM trong repo infrastructure hiện còn cấp ECR mlflow cho GitOps publisher;
phải chuyển grant sang app publisher trước khi bật workflow mới, đồng thời kiểm
app GitHub App actor/installation. Lượt chuyển source ở app/GitOps không thay
Terraform, không publish, apply hay deploy. Xem README module và GitOps
`docs/MLFLOW_IMAGE_MIGRATION.md` cho thứ tự thực hiện.

| Cấu hình mục tiêu | Nơi dùng / quy tắc |
| --- | --- |
| MLFLOW_TRACKING_URI | Trainer HTTP endpoint từ workflow context; không chứa username/password/query token |
| MLFLOW_REGISTRY_URI | Cùng approved MLflow service trong baseline; không tùy tiện trỏ registry khác |
| MLFLOW_EXPERIMENT_ID | Experiment đã provision, đối chiếu tên/ID trước create run; không tự tạo experiment mới khi GET bị lỗi |
| registered model name | `maintai-failure-classifier`, namespace được Platform provision; app chỉ tạo versions theo quyền |
| MLFLOW_HTTP_REQUEST_TIMEOUT | Initial policy30s/request; artifact transfer có bounded budget riêng và workflow deadline |
| MLFLOW_HTTP_REQUEST_MAX_RETRIES | **0 cho trainer profile này**; tránh hidden retries của create-run/create-version. Retry theo operation-aware policy ở §7 |
| Credentials | Secret injection/workload identity/auth proxy theo platform; không log tokens, không credentials trong artifact URI |

Đặt env trước khi import/khởi tạo SDK. Các biến/app settings này là yêu cầu cho
PR triển khai trainer, không tự thêm keys vào runtime config JSON schema đã
freeze. Cấu hình HTTP timeout/retries không thay thế kiểm soát retry của S3 SDK,
proxy hoặc workflow. Owner phải kiểm tra không tầng nào tự replay create POST.
Tắt autolog cho flow chính hoặc review rõ side effects trước khi dùng: không để
autolog tự tạo child runs/register model ngoài ledger.

## 3. Experiment, run và identity bất biến

- Một experiment cho bài toán, ví dụ `maintai-ai4i-failure`; không experiment
  mới theo mỗi Pod, ngày chạy hoặc machine_id.
- Một parent run cho mỗi logical training_id. Baseline/candidate có child runs
  riêng, tag `mlflow.parentRunId` từ parent thật. Rule-based baseline có thể log
  metrics-only; không đăng ký thành ML model giả.
- Mỗi training context quyết định candidate slots/hyperparameters/split/seed.
  Hyperparameter search tạo run_key riêng cho từng trial, không overwrite params
  trên cùng run. `model-result.mlflow_run_id` là run **selected candidate**, không
  parent run chứa summary hoặc run mới nhất.
- `training_id` dùng identity đã có của orchestration: SHA256 canonical JSON của
  approved manifest bucket/key/VersionId/SHA và training context SHA. App kiểm tra
  identity/context nhận vào, không tự bỏ VersionId hoặc tạo ID mới mỗi retry.
- `run_key = SHA256(canonical JSON(training_id, candidate_key, params_sha256,
  split_sha256, feature_schema_sha256))`. Đây là key của ledger/tag, không phải
  run_id do MLflow tự sinh. Cùng run_key chỉ được một coordinated create operation.
- Source SHA là SHA thật của MaintAI monorepo; không tạo Git SHA riêng cho folder.

### 3.1. Metadata phải log

| Nhóm | Tags/params/artifacts bắt buộc |
| --- | --- |
| Identity | training_id, run_key, candidate_key, parent run ID nếu có, real MLflow run_id |
| Data | dataset_version, manifest SHA + S3 VersionId ref, data SHA, evaluation protocol SHA, split_version/split SHA, approved_by/approved_at |
| Reproducibility | training_code_git_sha, training_image_digest, lockfile SHA, seed, algorithm, hyperparameters, feature schema version/SHA/order |
| Evaluation | validation/test metrics tách namespace, threshold_source=validation, selected threshold, calibration method nếu dùng, baseline/rule comparisons và quality gate version |
| Registry | registered model name + numeric version + source URI + selected run_id + registry payload SHA; không chỉ alias |
| Serving artifacts | model/preprocessor/metadata/reference refs gồm bucket,key,VersionId,SHA; reference khớp model/dataset, không tạo từ runtime window/test |

Tags/params là metadata, không model features. MLflow tags phù hợp giữ IDs/hashes;
đó không phải lý do đưa các giá trị này vào Prometheus labels. Params trên một run
không đổi; mismatch => provenance error, không ghi đè hoặc tạo run mới để né lỗi.

### 3.2. Metric keys và ý nghĩa

| Prefix/key | Quy tắc |
| --- | --- |
| validation.recall_failure / precision_failure / f1 / pr_auc / false_negative_rate | Đo trên validation; dùng chọn threshold/model theo frozen protocol |
| test.recall_failure / precision_failure / f1 / pr_auc / false_negative_rate | Đo held-out test sau khi chọn; không dùng test chọn hyperparameters/threshold |
| threshold | Threshold đã chọn từ validation, probability[0,1] |
| false_alarms_per_1000_valid | Tính theo denominator và nhãn thật trong evaluation protocol; không giả từ machine IDs |
| human_review_ratio | Phải có protocol/nguồn đo; thiếu ground truth không điền0 cho đủ schema |

Metric finite, ratio trong[0,1], cùng frozen evaluation/test split cho baseline
và candidate. Undefined PR-AUC/denominator không hợp lệ => gate chưa đạt, lưu
reason trong evaluation report; không log NaN/Infinity hoặc fabricate score.
Initial GitOps quality gate hiện yêu cầu Recall_failure≥0.85, Precision_failure≥0.50
và baseline/rule comparison. Đây là tiêu chí dự án đang cấu hình, không kết quả
đã đo hoặc ngưỡng production mặc định; thay gate phải bump/version/review.

## 4. Artifact contract và tránh vòng lặp hash

1. Fit chỉ trên train; freeze split/protocol, threshold từ validation. Các features
   phục vụ ML chỉ gồm sáu allowlisted features; IDs/timestamps/fault labels bị loại.
2. Serialize model và fitted preprocessor một lần, ghi checksum từ bytes thật.
   Khi retry publication, tải lại **đúng bytes/version**, không serialize lại rồi
   cho rằng seed giống nhau sẽ tạo cùng hash.
3. Tạo riêng `registry-model/` là MLflow flavor directory của fitted sklearn
   Pipeline gồm preprocessor+classifier với signature của raw features. Có thể
   dùng mlflow.sklearn.save_model một lần với lockfile/input example đã review.
   Inference bundle tách model.joblib/preprocessor.joblib vẫn dùng cùng fitted
   components; không double-transform và không xem hai formats là cùng bytes.
4. Freeze `registry_payload_manifest.json`: sorted relative paths + file SHA,
   không có absolute path/secrets; registry_payload_sha256 là hash canonical của
   manifest, **không gồm chính manifest hash**. Log payload và manifest vào run.
5. Tạo Registry version tham chiếu `runs:/<selected-run-id>/model`; lưu source,
   numeric version, payload SHA và run_id. Xác minh registry READY và download
   bytes/manifest từ **registered source** trước khi cho qua bước này.
6. Sau khi biết registry version thật mới finalize serving `metadata.json` chứa
   run/model registry identities + dataset/split/code/model/preprocessor/reference
   identities. Metadata hash được tính sau cùng; không cần rewrite registry payload
   để thêm metadata hash rồi tạo vòng tham chiếu hash.
7. Upload serving model/preprocessor/reference/metadata và evaluation report vào
   versioned S3, thu VersionId/SHA thật. Final release manifest nối tất cả refs;
   MLflow registry tags có thể link serving metadata SHA nhưng không là root of trust.

S3 keys content-addressed hoặc identity-bound; không overwrite `current/model`
trong lúc training. MLflow artifact upload có thể ghi lại cùng path: chỉ retry
khi byte-for-byte bằng manifest đã freeze. Download/verify phải kiểm tra mọi file,
không chỉ tên URI/ETag. Artifact directory có thể chứa nhiều files; bucket/key/
VersionId/SHA của serving bundle không suy ra từ `runs:/...` một cách đoán mò.

Model Registry version là assigned numeric identity, còn model_version trong
MaintAI là logical bundle identity được dùng cùng drift reference. Hai giá trị
phải liên kết rõ trong metadata/release, không ép chúng bằng nhau. Model version
tags/aliases và backing artifacts có thể bị sửa nếu ACL cho phép: chỉ version
number không đủ để bảo đảm immutable content; cần pin object VersionId+SHA, ACL
và read-back verification. Không mutate payload của version đã phát hành.

## 5. Ví dụ MLflow SDK

Các hàm dưới là **reference snippets trong docs**, không trainer chạy ngay. Chúng
nhận data đã validate/fit và ledger-owned identities; không cài dependency, không
khởi chạy training hoặc tạo bucket. Durable ledger adapter ở §6 chưa có source.
Không copy create calls vào blanket retry/context manager tự tạo active run.

### 5.1. Khởi tạo client và create run một lần

```python
import os
from mlflow import MlflowClient

def configured_client():
    tracking = os.environ["MLFLOW_TRACKING_URI"]
    registry = os.environ.get("MLFLOW_REGISTRY_URI", tracking)
    return MlflowClient(tracking_uri=tracking, registry_uri=registry)

def create_candidate_run_once(client, experiment_id, tags):
    # Caller already committed RUN_CREATE_INTENT with ownership/generation.
    # No automatic retry here; timeout means RUN_UNKNOWN and requires reconciliation.
    run = client.create_run(
        experiment_id=experiment_id,
        tags=tags,
        run_name=f"{tags['candidate_key']}-{tags['run_key'][:12]}",
    )
    # Persist this real ID with conditional ledger update before the next side effect.
    return run.info.run_id
```

`tags` phải chứa identity/data/source entries của §3; parent ID nếu có được đặt
ngay trong create call. Experiment tồn tại/đúng namespace trước khi chạy. Không
gọi get-or-create mỗi lần lỗi mà coi mọi exception là experiment/run chưa tồn tại.

### 5.2. Ghi params, metrics và artifacts cho run đã resolve

```python
from mlflow.entities import Metric, Param, RunTag

def log_candidate(client, run_id, run_key, params, values, recorded_at_ms,
                  registry_model_dir, evidence_dir):
    # Caller checks owner/lease and verifies frozen local file manifests first.
    run = client.get_run(run_id)
    if run.data.tags.get("run_key") != run_key:
        raise ValueError("MLflow run identity mismatch")
    for key, value in params.items():
        old = run.data.params.get(key)
        if old is not None and old != str(value):
            raise ValueError("Immutable parameter mismatch")
    client.log_batch(
        run_id=run_id,
        params=[Param(key, str(value)) for key, value in params.items()],
        metrics=[Metric(key, float(value), recorded_at_ms, step=0)
                 for key, value in values.items()],
        tags=[RunTag("tracking_policy", "MLFLOW-01/1.0.0")],
        synchronous=True,
    )
    # Same paths may be retried only with identical frozen bytes.
    client.log_artifacts(run_id, str(registry_model_dir), artifact_path="model")
    client.log_artifacts(run_id, str(evidence_dir), artifact_path="evidence")
```

`values` dùng keys ở §3.2 và phải được check finite/ranges; recorded_at_ms là
timestamp evaluation đã persist, giữ nguyên khi retry, không Date.now mới mỗi
lần replay. Một batch có thể partial/unknown khi mất mạng: đọc params/metric
history/artifact manifests trước khi tiếp tục. Duplicate metric samples có thể
tồn tại dù cùng timestamp/step/value; evaluation JSON + SHA là evidence chuẩn,
không khẳng định log_batch exactly-once. Không dùng asynchronous logging mà bỏ
future chưa flush khi job exit. Đọc lại run và download artifacts để verify trước
khi checkpoint TRACKING_VERIFIED; set_terminated(FINISHED) sau successful evidence
verification, không vì subprocess vừa exit0.

### 5.3. Tạo model version và resolve bằng ID thật

```python
def create_registry_version_once(client, model_name, run_id, run_key, payload_sha256):
    # Registered model namespace already provisioned. REGISTRY_CREATE_INTENT committed.
    version = client.create_model_version(
        name=model_name,
        source=f"runs:/{run_id}/model",
        run_id=run_id,
        tags={"run_key": run_key, "registry_payload_sha256": payload_sha256},
        await_creation_for=0,
    )
    return {"registered_model_name": version.name,
            "registered_model_version": str(version.version),
            "mlflow_run_id": run_id,
            "registry_source_uri": version.source,
            "registry_payload_sha256": payload_sha256}

def verify_registry_receipt(client, receipt):
    version = client.get_model_version(
        receipt["registered_model_name"], receipt["registered_model_version"]
    )
    if version.status != "READY":
        raise RuntimeError("Registry is pending or failed; do not release")
    if version.run_id != receipt["mlflow_run_id"]:
        raise ValueError("Registry run mismatch")
    if version.tags.get("registry_payload_sha256") != receipt["registry_payload_sha256"]:
        raise ValueError("Registry payload mismatch")
    # Caller also downloads the resolved source and verifies every file hash.
    return version
```

Receipt ở đây là **internal metadata**, không tự thêm keys vào model-result-v1
đang `additionalProperties:false`. Không catch timeout rồi gọi create lần nữa;
numeric version có thể đã được tạo. READY chỉ chứng minh registration thành công,
không quality gate, artifact hash hoặc deployment đã đúng.

### 5.4. Reconciliation chỉ đọc, không get-or-create

```python
from mlflow.entities import ViewType

def find_runs_by_key(client, experiment_id, run_key):
    if len(run_key) != 64 or any(c not in "0123456789abcdef" for c in run_key):
        raise ValueError("Expected canonical SHA256 key")
    found, token = [], None
    while True:
        page = client.search_runs(
            experiment_ids=[experiment_id],
            filter_string=f"tags.run_key = '{run_key}'",
            run_view_type=ViewType.ALL,
            max_results=100, page_token=token,
        )
        found.extend(page)
        token = page.token
        if not token:
            return found
```

Registry reconciliation dùng search_model_versions theo approved model name và
tag run_key, duyệt **tất cả pages** rồi verify run_id/source/payload SHA. Không
search `latest`, không chọn version lớn nhất khi nhiều matches. Đúng1 match,
identity/content hợp lệ và run chưa bị delete mới có thể conditional checkpoint
ID về ledger.0 hoặc nhiều matches khi thao tác create trước đó unknown thì giữ
UNKNOWN, operator kiểm tra; không tạo mới hoặc xóa bản trùng tự động.

## 6. Durable ledger và bước checkpoint

App không tự thêm training DB thứ6 hoặc query mlflow_db. Nơi lưu orchestration
hiện được thiết kế là DynamoDB ledger trong platform. Người2+Người4 bổ sung
per-run/per-operation records hoặc versioned checkpoint artifacts được ledger
tham chiếu; đây là **target schema migration cần làm**, chưa có trong app/GitOps.

| Nội dung durable checkpoint | Yêu cầu |
| --- | --- |
| Identity | training_id, run_key, candidate_key, context/params/split hashes |
| Ownership | owner, lease_until, generation/fencing token, expected previous phase |
| Tracking | experiment_id, real parent/child run IDs, frozen params/metric timestamp/manifest refs |
| Registration | operation intent timestamp/identity, selected run ID, registry payload SHA, name/version/source/status |
| Output | S3 model-result/evaluation/metadata refs, result SHA, release change ID nếu dispatch |
| Failure | retry class, bounded error code, last phase, attempts, next retry/deadline; không credentials/raw exception |

```text
NEW -> RUN_CREATE_INTENT -> RUN_KNOWN -> TRAINED_ARTIFACTS_FROZEN
 -> TRACKING_VERIFIED -> REGISTRY_CREATE_INTENT -> REGISTRY_KNOWN
 -> ARTIFACTS_AND_REGISTRY_VERIFIED -> RESULT_COMMITTED -> RELEASE_DISPATCHED
```

RUN_UNKNOWN/REGISTRY_UNKNOWN là nhánh **chưa xác định side effect**. Tracking
outage có thể là TRACKING_PENDING khi đã có run_id và frozen bytes. Failed gate
là REJECTED, có evidence nhưng không dispatch release. Các tên phase này mô tả
logical state cần migration; không tuyên bố enum mới đã tồn tại trong DynamoDB
helper, Kafka hoặc model-result JSON.

- Trước mỗi side-effect create, commit intent bằng conditional write; chỉ một
  owner/generation cho cùng run_key. Checkpoint real ID trước side effect kế tiếp.
- Heartbeat lease trong lúc training/upload; mọi update dùng owner+generation+
  expected phase, không chỉ owner string. Lease hết hạn không chứng minh worker
  cũ đã dừng. Owner mất lease dừng phát remote calls và không dispatch release.
- MLflow APIs không hiểu fencing token của DynamoDB. Một in-flight request cũ
  vẫn có thể commit sau khi lease hết hạn; do đó **không chuyển quyền tạo run/
  version mới khi intent cũ unresolved**. Serial registrar là một task/step của
  workflow, không đòi thêm microservice; platform phải dừng/triage writer cũ.
- Tags/run names không có unique constraint cho run_key. Search-before-create
  không atomic và không bảo đảm exactly-once. Mục tiêu là một verified canonical
  run/version được release, detect/quarantine duplicates, không tự nhận tuyệt đối
  không bao giờ có orphan side effects trên MLflow.

GitOps helper hiện có training_id, owner/lease và checkpoint model-result, nhưng
chưa có run/version step checkpoint, generation fencing/lease heartbeat của flow
này. Không dùng snapshot đó như bằng chứng đã có crash-safe MLflow registration.

## 7. Retry, unknown outcome và outage policy

| Operation/lỗi | Hành vi bắt buộc |
| --- | --- |
| GET/search/download bị timeout/429/5xx | Bounded read retry, initial tối đa3 lần, backoff1/2/4s+jitter; honor Retry-After trong deadline tối đa60s; không tạo resource khi search lỗi |
| Log params/metrics/artifacts cho run_id đã biết | Giữ run ID, key/value/step/timestamp/path/bytes; retry tối đa3 lần khi chứng minh immutable input; verify partial writes trước checkpoint |
| Create run/create model version timeout, disconnect,5xx hoặc job crash | Coi remote result unknown; không blanket retry, không tạo run/version khác. Read reconciliation theo intent;0 match không chứng minh request cũ chưa commit |
| 401/403 | Dừng, sửa credential/role; không loop retry hoặc chuyển sang anonymous/local tracking store |
| 400/422, param/hash/provenance mismatch | Terminal contract/data error; lưu reason, không đổi input/hashes cho pass |
| Registry PENDING_REGISTRATION | Poll existing numeric ID trong bounded deadline; không create lại. FAILED_REGISTRATION cần triage và evidence riêng |
| MLflow unavailable sau khi đã train | Lưu frozen artifacts/evaluation/checkpoint bền vững, mark TRACKING_PENDING, resume publication; không tự retrain hoặc phát hành model thiếu run ID |
| Artifact upload timeout | GET/HEAD đúng content-addressed key/version, verify bytes/SHA; retries có thể tạo S3 versions thừa nhưng chỉ chọn một verified VersionId, không giả exactly-one upload |
| Result đã commit nhưng release dispatch lỗi | Retry delivery cùng immutable model-result object và change identity; không train/register lại |
| GitHub dispatch204 / RELEASE_DISPATCHED | Chỉ dispatch ACK, không khẳng định PR đã tạo/merge hoặc model đã serving; theo dõi receiver/PR/Argo riêng |
| Rollback inference | Redeploy approved immutable release cũ; không sửa historical metrics/predictions hoặc đổi alias để né PR |

Các giới hạn trên là initial retry policy của project, phải đi cùng workflow
deadline và exact SDK retry configuration. Hết budget trả failure/pending rõ,
không giữ worker ngủ vô hạn. Sau create UNKNOWN không có phép thử chỉ bằng tag
search để chứng minh an toàn tạo lại; cần operator xác minh remote request đã
kết thúc/không có side effect từ server audit/trace và record quyết định, hoặc
backend có idempotency guarantee được chứng minh. Chưa đủ evidence thì giữ
UNKNOWN và chặn release. Không xóa duplicate version đang được release tham chiếu.

Tag sau khi mutate cũng không đủ cho recovery nếu crash trước set_tag; bởi vậy
run_key/payload SHA được đưa vào **create call**. Trước reclaim, đối chiếu cả
deleted runs/versions nếu backend hỗ trợ; delete không phải lý do tạo lại mù.

## 8. Output và compatibility với contracts hiện có

Platform [model-result-v1.schema.json](../../MaintAI-gitops/contracts/model-result-v1.schema.json)
đang bắt buộc: schema_version, release_id, logical model_version, dataset_version,
source_sha, training_image, artifacts(model/preprocessor/drift_reference/metadata),
evaluation/evaluation_report, mlflow_run_id và quality_gate_version. Validate đúng
schema trước dispatch; không thêm training_id/registry fields tùy ý vào body.

- `mlflow_run_id` lấy từ selected run thật.
- `artifacts.metadata` trỏ metadata.json đã finalize ở §4, chứa registry name/
  numeric version/source và full dataset/split/model provenance.
- evaluation giữ chính xác candidate/baseline/rule_based metrics và frozen
  protocol/test hashes theo schema; keys MLflow `test.*` cần map rõ sang DTO.
- Logical model_version do bundle export contract quyết định, giữ cùng drift
  reference; không dùng numeric Registry version để ghi đè semantic version.
- `registered_model_version`, inference effective release/model identity và các
  fields provenance v1.4 trong HTTP/Kafka cần mapping/schema review tương ứng.
  Kafka canonical outcome1.4 hiện có mlflow_run_id nhưng chưa có explicit registry
  name/version fields. Không tự chèn additional properties; metadata/release
  reference giữ identity, PR contract riêng mới thêm wire fields nếu cần.
- App target `contracts/provenance/release_manifest.schema.json` chưa được cung
  cấp. Platform model-result schema và app target không tự trở thành cùng contract;
  cần freeze release manifest/mapping/fixtures trước khi implement gate toàn v1.4.

Internal registration receipt trong ví dụ không phải mẫu HTTP/Kafka DTO hoàn
chỉnh. Raw dataset và full PDF không commit Git hoặc log tràn lan vào run;
MLflow giữ manifests/hashes/approved samples, data lớn ở storage có ACL/retention.

## 9. PR checklist và bằng chứng nghiệm thu

| Owner | Bằng chứng phải bàn giao |
| --- | --- |
| Người2 | Exact SDK/dependency lock, experiment/candidate identity, params+split reproducibility, metrics JSON, frozen artifact manifests, run/version read-back, metadata/output-schema mapping |
| Người4 | Exact server image/DB migration, credentials/ACL, experiment/model namespace, workflow ledger conditional writes/heartbeat/fencing/unknown policy, S3 VersionId refs, release receiver/PR evidence |
| Người1 | Predictions/alerts giữ effective model/run/release provenance; không query MLflow trên mỗi UI request hoặc dùng alias hiện tại sửa lịch sử |
| Người3 | RAG evidence giữ ownership riêng; nếu log RAG experiments vào MLflow phải có contract/experiment scope riêng, không tự trộn với training gate này |

Phải có tests tương ứng khi implement: duplicate dataset trigger; same seed/
split identity; concurrent same run_key; crash sau remote create nhưng trước
checkpoint; search unavailable/0/multiple pages/multiple matches; partial logging;
upload timeout; mutated artifact/hash; lease expiry với old writer còn chạy;
auth403; MLflow outage; result committed rồi dispatch lỗi; successful tracking
không auto-promote; Registry version chỉ đúng khi source bytes đúng; rollback
giữ historical lineage. Integration dùng đúng server/client pinned versions,
real DB/store và logs đã sanitize. Các checklist này chưa được thực thi chỉ vì
viết docs; readiness phân biệt source, fixture checks và runtime evidence.

## 10. Tài liệu chính thức đã đối chiếu

- API signatures/create/read/log/search: [MlflowClient](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.client.html).
- Flavor serialization của fitted sklearn model: [mlflow.sklearn](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.sklearn.html).
- SDK HTTP timeouts/retries cần khóa theo phiên bản: [MLflow environment variables](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.environment_variables.html).
- Server/API/UI và backend/artifact stores: [MLflow Tracking Server](https://mlflow.org/docs/latest/self-hosting/architecture/tracking-server/).

Ví dụ SDK trong lượt docs chỉ kiểm tra Python syntax; chưa chạy với MLflow thật,
chưa pin dependency/server, chưa claim crash recovery hoặc image/deploy readiness.
