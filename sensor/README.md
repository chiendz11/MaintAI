# MaintAI Sensor Simulator

**Target 1.3.1:** xem [Dockerfile](Dockerfile). Code/commands bên dưới là simulator CLI v1.1 interface (package 0.2.0); Kafka ACK 202, internal control API, durable ordering và retry spool chưa implement. Các folder mới chỉ là skeleton; không dùng CLI hiện tại làm bằng chứng target long-running Deployment đã chạy.

Triển khai theo **MaintAI Implementation Development Guide v1.1 DRIFT §5** và
**MaintAI System Contracts v1.1 DRIFT §2, §5, §25A.5**. Folder giữ tên `sensor/`
theo cấu trúc repo đã thống nhất; package Python dùng `src/simulator/` như guide.

## Cài đặt và chạy nhanh

Từ root repo, dùng Python 3.11+ (CI kiểm tra 3.11 và 3.12):

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r sensor/requirements-dev.lock
.venv/bin/python -m pip install --no-deps -e ./sensor
.venv/bin/maintai-sensor download-data
.venv/bin/maintai-sensor replay --dry-run --limit 3
```

Dataset tải từ [UCI AI4I 2020](https://doi.org/10.24432/C5HS5C), kiểm tra SHA-256;
không commit CSV đầy đủ. Không tải lại nếu file hiện tại đúng checksum.
Nếu đã có dataset, có thể dùng `--dataset /path/to/ai4i2020.csv` mà không tải.
Test dùng fixture local, không phụ thuộc UCI hoặc backend thật.

Backend nhận `POST /api/v1/sensor-readings`:

```bash
.venv/bin/maintai-sensor replay --backend-url http://localhost:8000 --limit 20 --speed 4
```

Backend hiện vẫn là skeleton. Dùng dry-run hoặc mock HTTP backend trong tests
để kiểm tra sensor trước khi backend thật được triển khai.

## Wire payload

```json
{
  "event_id": "evt_opaque-uuid",
  "machine_id": "M-001",
  "received_at": "2026-10-12T04:30:15Z",
  "source": "AI4I_SIMULATOR",
  "features": {
    "product_type": "L",
    "air_temperature_k": 298.1,
    "process_temperature_k": 308.6,
    "rotational_speed_rpm": 1551.0,
    "torque_nm": 42.8,
    "tool_wear_min": 0.0
  }
}
```

Contract dùng `features` lồng, thay cho ví dụ flat trong guide.
Chỉ gửi 6 feature theo allowlist. `UDI`, `Product ID`, `Machine failure`,
`TWF/HDF/PWF/OSF/RNF` và ID/timestamp không được đưa vào features.
`Machine failure` chỉ giữ trong `DatasetRecord.ground_truth` cho harness offline.
Không dùng label gốc để kết luận failure cho các record đã biến đổi bằng drift.

Features được kiểm tra kiểu, finite number, category L/M/H, rpm > 0 và wear >= 0.
Không tự thêm ngưỡng torque/temperature ngoài contract. Frozen schema cho wire
synchronous hiện có nằm ở [tests/fixtures/sensor-wire.schema.json](tests/fixtures/sensor-wire.schema.json).
Đây là fixture kiểm thử độc lập của module. Contract Ingestion mục tiêu nằm trong
[contracts/v1.3/](../contracts/v1.3/README.md); code Sensor chưa migrate sang ACK202.

## Replay và tính lặp lại

- Mỗi lượt chạy tạo run ID mới, tránh trùng event với lượt trước.
- Các máy gửi luân phiên: mỗi vòng đi qua đủ `machine_count` máy đúng một lần.
  Seed quyết định thứ tự máy cố định cho các vòng và categorical transforms bằng
  hai RNG riêng. Mặc định dùng seed trong scenario; `--seed`/`SIM_SEED` có thể override.
- Để tái tạo toàn bộ payload, giữ cùng dataset/scenario/seed, `--run-id` và
  `--start-time`. Tái dùng run ID với payload khác có thể dẫn tới `409 EVENT_CONFLICT`.
- Từ sensor **0.2.0**, `interval_ms` là chu kỳ logical **của mỗi máy**. Các event
  toàn luồng cách nhau `interval_ms / machine_count` milliseconds; thời gian chờ
  gửi là `interval_ms / machine_count / speed`. Mỗi máy có chu kỳ gửi mục tiêu
  `interval_ms / speed`, tăng số máy không làm giảm tốc độ mục tiêu của từng máy.
  Tăng speed không làm đổi timestamp/features. HTTP gửi tuần tự; backend chậm hoặc
  retry có thể làm tốc độ thực tế thấp hơn mục tiêu. `interval_ms=0` tắt chờ gửi.
- Replay dừng khi hết dataset, hết phases hoặc đạt `--limit`. Chỉ lặp CSV nếu có
  `--loop-dataset`; ngay cả khi lặp, số event vẫn bị giới hạn bởi scenario/limit.
- Dry-run không gọi HTTP, không sleep; in JSONL ra stdout, logs JSON ra stderr.
  `--output` ghi JSONL ra file, mỗi dòng là một logical delivery (gồm duplicate,
  không gồm HTTP retry); file được ghi lại từ đầu mỗi lượt chạy.

```bash
.venv/bin/maintai-sensor replay --dry-run --limit 10 --seed 42 \
  --run-id demo-001 --start-time 2026-10-12T04:30:15Z \
  --output sensor/output/events.jsonl --metrics-file sensor/output/metrics.prom
```

Với `--interval-ms 1000 --speed 1`, 5 máy có tổng tốc độ mục tiêu 5 bản ghi/giây;
đổi `--machine-count 10` thì thành 10 bản ghi/giây. Đây là nhiều sensor/máy giả
lập trong một tiến trình; không phải dữ liệu lịch sử thật của nhiều máy trong AI4I.
Mỗi event đọc một dòng CSV tiếp theo, không nhân cùng một dòng cho tất cả máy.
`--limit` và số event trong scenario vẫn tính cho toàn luồng, không phải từng máy.

```bash
.venv/bin/maintai-sensor replay --machine-count 5 --interval-ms 1000 --speed 1 --limit 100
.venv/bin/maintai-sensor replay --machine-count 10 --interval-ms 1000 --speed 1 --limit 100
```

**Chuyển từ 0.1.0:** trước đây interval áp dụng cho toàn luồng và máy được chọn
ngẫu nhiên ở mỗi event. Để giữ tổng tốc độ cũ, đặt interval mới bằng interval cũ
nhân số máy (ví dụ 5 máy, `--interval-ms 5000` giữ khoảng 1 event/giây ở speed 1).
Không tái dùng `run_id` của bản cũ để gửi payload mới. Wire payload vẫn giữ nguyên
System Contracts 1.1.0; GitOps dùng schema cấu hình mới `sensor-config-2.0.0` để
ghi rõ thay đổi ý nghĩa interval, đồng thời giữ schema 1.0.0 cho lịch sử.

## Drift scenarios

| Scenario | Phases / biến đổi |
| --- | --- |
| `normal` | 10.000 baseline events; giữ nguyên features của CSV |
| `sudden-drift` | 300 baseline + 700 shifted; torque +8 Nm, rpm ×0,93 |
| `gradual-drift` | 300 baseline + 700 ramp; torque từ +0 đến +8, rpm từ ×1 đến ×0,93 |
| `categorical-drift` | 300 baseline + 700 mix; L/M/H = 0,05/0,15/0,80 |

```bash
.venv/bin/maintai-sensor replay --scenario gradual-drift --dry-run \
  --output sensor/output/gradual.jsonl
```

YAML trong `scenarios/` theo phase contract, hỗ trợ `shift`, `scale`,
`linear_shift`, `linear_scale` cho numeric fields và `categorical_mix` cho
`product_type`. Ramp bao gồm cả start/end; phase một event dùng giá trị end.
Custom YAML chọn qua `--scenario-file /path/to/scenario.yaml`.
Transform tạo invalid feature sẽ dừng với lỗi cấu hình; không clip/impute.
Tests kiểm tra validity và distribution shift trên fixture. Kết luận
STABLE/WARNING/CRITICAL và calibration với model reference thuộc Drift Worker.

## Fault injection và resilience

| `--fault-mode` | Hành vi |
| --- | --- |
| `none` | Không inject lỗi |
| `missing_field` | Xóa feature được chọn; mong đợi backend 422 |
| `invalid_type` | Numeric thành string (category thành number); mong đợi 422 |
| `out_of_range` | rpm hoặc wear = -1; mong đợi 422 |
| `duplicate_event` | Gửi 2 lần cùng payload/event ID/correlation ID |
| `network_timeout` | Giả lập timeout ở attempt đầu, rồi dùng retry policy |
| `network_disconnect` | Giả lập disconnect ở attempt đầu, rồi dùng retry policy |

`--fault-every N` inject mỗi event thứ N; `--fault-field` mặc định rpm.
Fault không sửa dataset gốc và áp dụng sau valid drift transforms.

```bash
.venv/bin/maintai-sensor replay --limit 10 --fault-mode missing_field --fault-every 2
.venv/bin/maintai-sensor replay --limit 10 --fault-mode duplicate_event
```

HTTP client gửi `Accept: application/json`, `X-Request-ID`, `X-Correlation-ID`.
Retries giữ nguyên payload/event ID/correlation ID, tạo request ID mới mỗi attempt.
Retry tối đa 2 lần mặc định (3 attempts), backoff 100/200 ms, capped ở 5 giây;
áp dụng cho transport errors, 429/500/502/503/504, tôn trọng `retryable=false`.
Không retry 422/409. Response 2xx sai contract/trace identity được báo là lỗi.
422 có thể là invalid-event DTO (§5.5) hoặc error envelope `VALIDATION_FAILED` (§4).

Mặc định `--on-error continue`: hoàn tất các event còn lại, rồi exit 1 nếu có lỗi
ngoài dự kiến. `--on-error stop` dừng tại kết quả ngoài dự kiến đầu tiên.
Các fault payload bị reject đúng 422 là kết quả mong đợi, không phải lỗi lượt chạy.
Backend chấp nhận một payload fault-invalid cũng được báo là kết quả ngoài dự kiến.
Timeout/disconnect injection chỉ giả lập lỗi tại client; tests riêng kiểm tra
trường hợp backend đã commit nhưng response bị mất, đảm bảo retry không tạo bản sao.

Exit codes: `0` hoàn tất đúng kỳ vọng, `1` kết quả HTTP ngoài dự kiến,
`2` lỗi dataset/scenario/config/file, `130` bị ngắt bởi Ctrl+C.

## Config và log/metrics

CLI override environment. `.env` không tự nạp; có thể export từ shell:

```bash
cp sensor/.env.example sensor/.env
set -a
source sensor/.env
set +a
```

Các biến trong `.env.example` tương ứng flag CLI; thêm `SIM_LIMIT`,
`SIM_LOOP_DATASET` nếu cần. `--dry-run`, `--output`, `--metrics-file` là flag CLI.

JSON logs ghi event/machine ID, request/correlation ID, status, latency, attempts
và error code. Summary gồm generated, accepted/rejected/failed, retries, duplicates,
expected rejections và unexpected results. `--metrics-file` xuất batch text artifact
theo Prometheus format, không dùng ID làm metric label; simulator không chạy HTTP daemon.

## Test, build và container

```bash
.venv/bin/ruff check sensor
.venv/bin/ruff format --check sensor
.venv/bin/python -m pytest sensor/tests --cov=simulator
.venv/bin/python -m build sensor
.venv/bin/pip-audit --disable-pip --no-deps -r sensor/requirements.lock
docker build -t maintai-sensor:dev sensor
docker run --rm --network none \
  -v "$PWD/sensor/data/ai4i2020.csv:/app/data/ai4i2020.csv:ro" \
  maintai-sensor:dev replay --dry-run --limit 3
```

Image dùng user không phải root; CSV được mount runtime, không bake vào image.
Gửi tới backend trong Docker qua `--backend-url` hoặc `BACKEND_BASE_URL`
và network do repo deployment quản lý.

CI sensor chạy lint/format, unit/contract tests, mock HTTP integration, wheel/sdist
build, dependency audit, Docker build và container dry-run smoke test. Không publish
image hay deploy. Các component còn lại tiếp tục giữ skeleton.


> **Thiết kế v1.4:** Xem [docs index](../docs/README.md),
> [Guide v1.4](../docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md) và
> [readiness](../docs/implementation-readiness.md). Runtime metadata hiện có vẫn
> baseline1.3.2; source chưa tự migrate theo việc nhập tài liệu.
