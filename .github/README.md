# MaintAI CI

Workflow: [workflows/ci.yml](workflows/ci.yml), theo
[cú pháp GitHub Actions](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax).

- Chạy khi push hoặc mở/cập nhật pull request; có khai báo `workflow_dispatch`
  để chạy thủ công khi workflow đã có trên default branch.
- Job `structure` kiểm tra README và 6 folder chính.
- Job `sensor-ci` chạy trên Python 3.11/3.12: lint/format, unit/contract tests,
  mock HTTP integration, build wheel/sdist; Python 3.12 thêm runtime dependency audit.
- Job `sensor-container` build image rồi chạy dry-run offline với CSV fixture.
- Job `component-ci` dùng matrix cho `backend`, `frontend`,
  `ml-service`, `rag-service`, `drift-worker`; chỉ bật khi repository variable
  `ENABLE_COMPONENT_CI` có giá trị `true` (hiện chưa được đặt).
- Quyền workflow là `contents: read`; chưa cần secrets.

Với các component còn lại, thêm setup runtime, cache, install dependencies, lint,
tests và build khi có code. Có thể dùng điều kiện `matrix.component` hoặc tách thành job
riêng nếu stack khác nhau. Thay bước placeholder, sau đó đặt repository variable
`ENABLE_COMPONENT_CI=true` trong Settings → Secrets and variables → Actions → Variables
để bật job. Placeholder chủ động trả lỗi nếu bị bật khi chưa cấu hình.

CI xanh xác nhận cấu trúc repo và các kiểm tra sensor đã cấu hình; không kiểm tra
nghiệp vụ của các component còn lại. Chưa có publish image, cập nhật GitOps hoặc deploy.

Với `drift-worker`, bổ sung unit/integration tests khi có implementation;
lịch chạy định kỳ của worker thuộc repo GitOps/hạ tầng, không phải lịch CI.
