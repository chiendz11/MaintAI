# Nghiệm thu ML — bằng chứng và phần việc còn lại

## 1. Mục tiêu

Xây dựng model phân loại Machine failure có thể giải thích được quyết định
lựa chọn, tái lập thí nghiệm và kiểm soát phiên bản khi phục vụ dự đoán.

Phân biệt:
- Đánh giá chất lượng model.
- Kiểm chứng phần mềm.
- Nghiệm thu tích hợp và release.

PASS ở một nhóm không tự động chứng minh hai nhóm còn lại.

## 2. Trạng thái hiện tại

- Candidate: hgb-interactions-candidate-v1.
- Threshold: 0.08943418823732646.
- Validation: precision 0.87879; recall 0.85294.
- Previously used test: precision 0.77922; recall 0.88235.
- Independent new holdout: NO.
- Approved for release: NO.

Không thay model hoặc threshold trong quá trình hoàn thiện deployment.
Nếu mở một vòng nghiên cứu mới, phải ghi protocol và lý do thay đổi riêng.

## 3. Bảy tiêu chí đánh giá

| Tiêu chí | Bằng chứng hiện có | Phần cần chốt |
| --- | --- | --- |
| Correctness and fitness for purpose | Bài toán, feature allowlist, target, gate và giới hạn trong model card | Liên kết với quyết định nghiệm thu và phạm vi sử dụng |
| Validation versus software verification | Metrics model tách riêng test code/API/container | Tổng hợp kết quả theo đúng loại kiểm chứng |
| Metrics, baselines, thresholds, operating points | Dummy, rule, LR, LR balanced, DT, RF, HGB; threshold từ validation | Bảng so sánh đầy đủ, PR curve và trade-off tại các operating point |
| Offline-evaluation pitfalls | Split/hash cố định, fit train-only, CV và ablation cùng folds | Ghi lịch sử lựa chọn và giới hạn test reuse trong báo cáo cuối |
| Evaluation slices and behavioral testing | Slices Type/RPM/torque/wear; input và lỗi API | Phân tích các nhóm lỗi validation; kiểm thử robustness có protocol rõ |
| Invariants and metamorphic relations | Column/row order, single/batch, save/load, probability và threshold | Bổ sung kiểm tra đường tải và nạp artifact |
| Test-data adequacy | Số mẫu/lớp/slice, Wilson intervals | Kết luận đủ/thiếu bằng chứng theo phạm vi; không giả định có holdout mới |

## 4. Nghiên cứu và tái lập

Đã có:
- Dataset và split identity.
- Train-only cross-validation.
- Ablation raw features so với nhóm interaction features.
- Threshold selection và frozen candidate.
- Báo cáo RF v1 không đạt test gate.
- Báo cáo HGB trên reused test, công khai giới hạn.
- Training Docker chạy thật không cần mạng.
- Metrics, threshold và prediction validation của RF/HGB khớp giữa
  hai lượt Windows và Linux với rtol=1e-10, atol=1e-12.

Cần chốt:
- Báo cáo so sánh đầy đủ sinh từ artifacts đã lưu.
- Liên kết mỗi bảng/biểu đồ với source experiment và phạm vi dữ liệu.
- Hướng dẫn tái lập từ đầu, gồm cách tạo các đầu vào ablation.
- Lưu bằng chứng kiểm tra cùng source/image/dependency identity.

Không yêu cầu artifact train lại phải giống từng byte.
Không diễn giải std giữa các fold thành khoảng tin cậy độc lập.
Không chọn thêm thuật toán chỉ để tăng số lượng thí nghiệm.

## 5. Kiểm soát inference và release

Đã kiểm chứng local:
- API và pipeline thật; contract, startup, metrics, failure handling.
- Checksum model và transformer, feature/class identity.
- Drift reference từ train và loader compatibility.
- MLflow tracking, Registry, artifact download và hash verification.
- Persistence sau restart local; chưa phải backup/restore.

Cần triển khai hoặc phối hợp nghiệm thu:
- Approved release manifest và mapping với contract Platform.
- Tải artifact đúng identity/version, kiểm tra trước deserialize.
- Thiếu file, sai hash, sai version hoặc tải dở phải chặn readiness.
- Authentication/authorization theo contract shared deployment.
- Effective model identity và Processing -> ML thực tế.
- Rollback image/bundle/reference tương thích, giữ lịch sử provenance.
- MLflow retry/reconciliation theo MLFLOW-01.
- Image publication và release qua GitOps.
- Training task nhận approved dataset/context theo môi trường nhóm.

## 6. Điều kiện không được tự tuyên bố PASS

- Registry READY không phải phê duyệt release.
- Hash khớp không phải chữ ký phê duyệt.
- CI build/CLI không thay thế một lượt training thật.
- API nhận input hợp lệ không chứng minh prediction đúng.
- Reused test không phải holdout độc lập mới.
- Benchmark HTTP local không chứng minh production capacity.
- Restart persistence không chứng minh backup/restore.
- Không bịa metric, object VersionId, approval hoặc release identity.

## 7. Phân công nghiệm thu

ML chịu trách nhiệm code training/evaluation/inference, bằng chứng thí nghiệm,
artifact compatibility, SDK integration và provenance do ML tạo.

Platform phối hợp cung cấp storage/versioning/IAM, namespace MLflow,
workflow/ledger infrastructure và GitOps deployment.

Processing phối hợp xác thực service, gọi inference, giữ provenance và
kiểm thử luồng nghiệp vụ thực tế.

Mỗi mục chỉ được đóng khi có kết quả kiểm chứng hoặc quyết định phạm vi
được ghi rõ. Mục phụ thuộc chưa hoàn thành phải tiếp tục được theo dõi.