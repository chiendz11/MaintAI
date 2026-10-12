# Drift Worker

Skeleton thư mục theo [chat bổ sung Drift Monitoring](https://chatgpt.com/s/t_6accf009315081918266e3664cba5a6d).
Hiện chỉ có thư mục và `.gitkeep`; chưa có implementation, dependency hay test thật.

```text
drift-worker/
├── src/
│   └── drift_monitor/
├── tests/
│   ├── unit/
│   └── integration/
└── README.md
```

## Trách nhiệm dự kiến

Worker chạy định kỳ, độc lập với luồng ML inference:

1. Load reference artifact có version gắn với phiên bản model.
2. Đọc cửa sổ sensor readings hợp lệ từ database.
3. So sánh phân phối feature hiện tại với reference để phát hiện drift.
4. Lưu report gồm drift score, feature bị drift, window và model/reference version.
5. Cung cấp kết quả đã lưu cho monitoring và cảnh báo drift.
6. Lưu checkpoint của window đã xử lý để tránh xử lý lại ngoài ý muốn.

Khi triển khai, các module dự kiến trong `src/drift_monitor/` gồm:

| Module dự kiến | Trách nhiệm |
| --- | --- |
| `main.py` | Điều phối một lượt chạy worker |
| `reference_loader.py` | Load reference artifact theo version |
| `data_loader.py` | Load cửa sổ sensor readings hợp lệ |
| `detector.py` | Tính drift score và xác định feature bị drift |
| `report.py` | Lưu report, checkpoint và kết quả phục vụ monitoring |
| `config.py` | Cấu hình window, reference, detector và database |

Các file trên sẽ được thêm khi triển khai. Worker không có REST API riêng,
không cung cấp `/predict` và không tự động retrain hay promote model trong MVP.

## Các phần tích hợp tiếp theo

- `sensor/scenarios/`: bổ sung sudden, gradual và categorical drift scenarios.
- Database: lưu drift reports và checkpoint; reference có thể là artifact riêng.
- Contract: chốt schema drift report trước khi nối backend/monitoring.
- Tests: unit cho detector/loaders; integration cho report/checkpoint;
  E2E kiểm tra scenario drift đi tới report và monitoring.
- CI: thêm setup/install/unit/integration vào job của component `drift-worker`.

CronJob/lịch chạy, Prometheus alert rules và Grafana dashboard sẽ nằm ở repo
GitOps/hạ tầng riêng. Vì worker chạy xong rồi kết thúc, monitoring nên đọc kết
quả đã lưu qua một thành phần chạy liên tục thay vì scrape trực tiếp worker.
