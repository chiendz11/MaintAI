# System contract 1.1.0 — SensorEvent

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
