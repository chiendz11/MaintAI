# Runbook Rollback An toàn — Hybrid Ordering (`MaintAI`)

- **Phạm vi:** Tắt `Hybrid Ordering (Keyed Parallel Scheduling)` trên `processing-worker`, quay về hành vi FIFO mỗi máy (`max_inflight_per_key = 1`) mà **không làm mất dữ liệu**, **không phá cursor**, và **không chạy destructive DDL**.
- **Tài liệu liên quan:** [HYBRID_ORDERING_ADR.md](../architecture/HYBRID_ORDERING_ADR.md) · [COMPARISON_50RPS.md](../benchmarks/hybrid-ordering/COMPARISON_50RPS.md) · [handler.py](../../processing/worker/src/consumer/handler.py) · [resequencer.py](../../processing/shared/persistence/resequencer.py).

---

## 1. Khi nào cần Rollback?

Thực hiện rollback ngay về chế độ baseline FIFO mỗi máy (`per_key = 1`) nếu xảy ra một trong các dấu hiệu sau:
- `maintai_alerts_stale_signals_total` hoặc `maintai_outbox_order_conflicts_total` tăng bất thường.
- Hàng đợi `maintai_processing_alert_resequencer_depth{disposition="PENDING"}` tăng kéo dài hoặc xuất hiện nhiều `BLOCKED` ngoài dự kiến (`maintai_processing_alert_frontier_stalled_seconds` > SLO).
- Tải uniform (`Workload A`) xuất hiện suy giảm thông lượng Processing / Business DB hoặc lỗi PG lock/CPU bất thường do overhead `TxA + TxC`.

---

## 2. Nguyên lý An toàn khi Rollback

1. **Schema hoàn toàn backward-compatible (Additive Only):**
   - Các bảng `processing_admission_cursor`, `processing_alert_ledger` (migration `processing_0005_hybrid_ordering`) và 2 cột nullable `alerts.latest_source_sequence`, `alerts.latest_source_stream_epoch` (migration `business_0008_alert_seq_guard`) **không bao giờ được `DROP` hay `TRUNCATE` khi rollback**.
   - Code baseline (`PROCESSING_HYBRID_ORDERING_ENABLED=false`) bỏ qua các bảng hybrid mới nhưng vẫn giữ nguyên toàn bộ `maintai_processing_machine_cursor` và `processing_source_cursor` ở **contiguous completed frontier**.
2. **Không bao giờ gây `IdentityConflict` khi rollback sau drain:**
   - Do Hybrid mode chỉ tiến `maintai_processing_machine_cursor` và `processing_source_cursor` tại `AlertResequencer.drain_frontier()` khi tiền tố liên tiếp đã hoàn tất 100%, không có reading chưa xong nào mang `sequence_number` nhỏ hơn cursor đã ghi.

---

## 3. Quy trình Rollback Chi tiết (Từng bước)

### Bước 1 — Tạm dừng nguồn phát / Kiểm tra và xả sạch Ledger Frontier

Trước khi chuyển cờ về `false`, kiểm tra trong `processing_db` xem còn reading nào đã nhận (`PENDING`, `SIGNAL_READY`, `NO_SIGNAL`) hoặc đang bị chặn (`BLOCKED`) trong `processing_alert_ledger` hay không:

```sql
-- 1. Kiểm tra còn dòng nào chưa hoàn tất frontier không
SELECT consumer_group, machine_id, disposition, count(*) AS cnt,
       min(admission_seq) AS min_adm_seq, max(sequence_number) AS max_src_seq
FROM processing_alert_ledger
WHERE disposition IN ('PENDING', 'SIGNAL_READY', 'NO_SIGNAL', 'BLOCKED')
GROUP BY consumer_group, machine_id, disposition
ORDER BY cnt DESC;

-- 2. Kiểm tra độ lệch giữa admission và completed alert frontier theo từng máy
SELECT c.consumer_group, c.machine_id,
       (c.next_admission_seq - 1) AS total_admitted,
       c.alert_frontier_seq,
       c.last_admitted_sequence,
       mc.last_sequence AS machine_cursor_seq
FROM processing_admission_cursor c
LEFT JOIN maintai_processing_machine_cursor mc
  ON mc.consumer_group = c.consumer_group AND mc.machine_id = c.machine_id
WHERE (c.next_admission_seq - 1) > c.alert_frontier_seq;
```

- Nếu còn `PENDING` hoặc `SIGNAL_READY` / `NO_SIGNAL` chưa drain: giữ worker chạy thêm vài giây trong trạng thái dừng input mới (`SIM_INITIAL_STATE=PAUSED` hoặc ngừng gửi mới) để `drain_frontier()` hoàn tất và `next_admission_seq - 1 = alert_frontier_seq`.
- Nếu còn dòng `BLOCKED` do `INFERENCE_UNAVAILABLE` đang chặn frontier của một máy:

```sql
-- Liệt kê các dòng đang BLOCKED để kỹ sư quyết định
SELECT consumer_group, machine_id, admission_seq, message_id,
       sequence_number, disposition_reason, decided_at
FROM processing_alert_ledger
WHERE disposition = 'BLOCKED'
ORDER BY decided_at, machine_id, admission_seq;
```

  Sau khi xác minh máy an toàn hoặc đã kiểm tra manual raw telemetry, gọi helper `AlertResequencer.resolve_blocked(...)` từ job/script bảo trì (`action='SKIP_NO_SIGNAL'`, ghi rõ `actor` và `note`) để ghi audit và kích hoạt `drain_frontier()` nhả các `SIGNAL_READY` phía sau theo đúng source order. **Không** sửa SQL trực tiếp để chuyển `BLOCKED -> RESOLVED_NO_SIGNAL` mà bỏ qua `drain_frontier()`.

### Bước 2 — Chuyển cấu hình GitOps / Env về `hybrid=false`

Trong ConfigMap `processing-worker-config` (ví dụ `MaintAI-gitops/environments/local/apps/kustomization.yaml`):

```yaml
- PROCESSING_EXECUTION_MODE=keyed
- PROCESSING_HYBRID_ORDERING_ENABLED=false
- PROCESSING_MAX_ACTIVE_KEYS=8
- PROCESSING_MAX_INFLIGHT_PER_KEY=1
- PROCESSING_MAX_TOTAL_INFLIGHT=8
- PROCESSING_UNAVAILABLE_ALERT_DISPOSITION=BLOCKED
```

Áp dụng cấu hình (hoặc `kubectl rollout restart deployment/processing-worker`):
- Pod cũ sẽ drain các task đang chạy (`PROCESSING_DRAIN_TIMEOUT_SECONDS`), giải phóng token (`release()`), rồi exit sạch.
- Pod mới khởi động với `PROCESSING_HYBRID_ORDERING_ENABLED=false` và `PROCESSING_MAX_INFLIGHT_PER_KEY=1` — tiếp tục nhận từ Kafka safe-prefix offset cuối cùng và kiểm tra `maintai_processing_machine_cursor` theo đúng FIFO mỗi máy.

*(Không cần thay đổi `processing-outbox-relay`, `business-consumer`, hay `business-api`: `AlertRepository` sequence guard trên `business_db` hoàn toàn tương thích ngược với chế độ baseline và vẫn bảo vệ chống stale signal.)*

### Bước 3 — Xác minh Sau Rollback (Zero-Data-Loss Checklist)

1. Kiểm tra pod `processing-worker` báo `Ready` (`GET /health/ready` trả `200 UP`).
2. Kiểm tra logs không có `ConfigurationError`, `IdentityConflict`, hoặc `LeaseLost` lặp lại.
3. Kiểm tra outbox và backlog:

```sql
-- Outbox không còn tồn đọng
SELECT topic, status, count(*)
FROM maintai_kafka_outbox
WHERE status != 'PUBLISHED'
GROUP BY topic, status;

-- Không có sai lệch cursor giữa owner và consumer group
SELECT mc.consumer_group, mc.machine_id,
       mc.last_sequence AS sdk_seq, sc.last_sequence AS owner_seq
FROM maintai_processing_machine_cursor mc
JOIN processing_source_cursor sc ON sc.machine_id = mc.machine_id
WHERE mc.last_sequence <> sc.last_sequence;
```

4. Kiểm tra Business UI / `alerts`: mọi alert đang `OPEN` tiếp tục dedupe và transition bình thường.
