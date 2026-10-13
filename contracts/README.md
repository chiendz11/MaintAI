# System Contracts v1.1 DRIFT

`sensor-event.schema.json` cụ thể hóa **System Contracts v1.1 DRIFT**, mục 2 và 5.
Các field của request nằm trong `features`, có `source=AI4I_SIMULATOR`, timezone
trong `received_at`, rpm > 0 và tool wear >= 0. Feature ngoài allowlist không được gửi.
Các range nghiệp vụ khác do backend validator quản lý; schema không tự thêm range.

Model runtime: `sensor/src/simulator/models.py`. Xuất lại schema từ root:

```bash
.venv/bin/maintai-sensor export-schema --output contracts/sensor-event.schema.json
```

Contract tests kiểm tra schema đã commit khớp model và kiểm tra payload/scenario
thực tế bằng Draft 2020-12 + date-time format checker. JSON chuẩn không cho phép
NaN/Infinity; runtime cũng kiểm tra finite number trước khi serialize.

Guide §5.3 có ví dụ feature ở cấp ngoài; simulator dùng cấu trúc lồng trong
System Contracts §5.2 làm chuẩn. Contract tests của backend thật sẽ được thêm
khi backend được triển khai; hiện sensor dùng backend giả lập để kiểm thử giao tiếp.

## Drift reference và report

`drift-reference.schema.json` và `drift-report.schema.json` cụ thể hóa payload
`schema_version=1.0.0` trong phần Drift Monitoring của System Contracts v1.1.
Models nằm trong `drift-worker/src/drift_monitor/models.py`.

Reference bắt buộc đủ sáu feature, numeric bins/probabilities, categorical
categories/probabilities, model và dataset version. Runtime kiểm tra thêm bins
tăng nghiêm ngặt, tổng probabilities bằng 1, danh mục L/M/H và version khớp model.
Report gồm window, tổng kết, feature PSI, prediction drift, reason và snapshot
policy; trường policy/provenance là phần mở rộng phục vụ audit. Timestamp phải có
timezone; số phải finite; feature ngoài allowlist không được chấp nhận.

```bash
.venv/bin/maintai-drift export-schemas --directory contracts
```

Tests kiểm tra schema đã commit khớp model và validate reports từ cả sensor
scenarios và PostgreSQL/container integration. Fixture reference chỉ dành cho
test; production cần reference artifact của model triển khai.
