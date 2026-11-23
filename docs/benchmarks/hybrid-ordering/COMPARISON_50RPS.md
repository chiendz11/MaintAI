# Before / After Benchmark Comparison — Official 50 RPS (`baseline-50rps` vs `candidate-50rps`)

- **Mục tiêu:** Đánh giá trung thực tác động của **Hybrid Ordering (`Keyed Parallel Scheduling`)** với ngân sách tổng concurrency giữ nguyên (`PROCESSING_MAX_ACTIVE_KEYS=8`, `PROCESSING_MAX_TOTAL_INFLIGHT=8`), chỉ thay đổi `per_key` từ `1` (`baseline-50rps`) lên `2` (`candidate-50rps`).
- **Tài liệu nguồn:** [Baseline 50 RPS](./baseline-50rps/REPORT.md) ([summary.csv](./baseline-50rps/summary.csv)) vs [Candidate 50 RPS](./candidate-50rps/REPORT.md) ([summary.csv](./candidate-50rps/summary.csv)) · Lịch sử cũ 50/100/65 RPS: [./baseline/REPORT.md](./baseline/REPORT.md).
- **ADR & Rollback:** [HYBRID_ORDERING_ADR.md](../../architecture/HYBRID_ORDERING_ADR.md) · [HYBRID_ORDERING_ROLLBACK_RUNBOOK.md](../../operations/HYBRID_ORDERING_ROLLBACK_RUNBOOK.md).

> [!IMPORTANT]
> **Giới hạn phạm vi:** Cả hai bộ số liệu đều chạy trên **Controlled PostgreSQL 17 (`synchronous_commit=on`, 1 instance chung cho `proc_db` và `biz_db`) + Kafka 4.0.0 + `ControlledMLProvider`** trên 1 worker process. Kết quả này chứng minh cơ chế scheduler, transaction, Outbox và ordering; **không** đại diện trực tiếp cho throughput của `ml-service`/GPU thật hoặc cụm k3d 6 worker pods (`synchronous_commit=off`).

---

## 1. Bảng So sánh Toàn diện (Khoảng Min–Max qua 3 Runs)

### 1.1. Official Workload A (`Normal Load`: 50 machines × 1.0 RPS = `50.0 RPS`)

| Metric | Baseline (`per_key=1, total=8`) | Candidate A (`per_key=2, total=8`) | Chênh lệch / Nhận định |
|---|---:|---:|---|
| **Admitted RPS** | `50.00 – 50.00` | `44.87 – 47.80` | **Giảm `-4.4%` đến `-10.3%`** (poll-thread Stage A WAL commit overhead) |
| **Outcomes RPS (Processing)** | `49.93 – 50.00` | `44.80 – 47.40` | Giảm theo tỷ lệ admitted |
| **Predictions RPS (Business)** | `44.47 – 45.93` | `33.00 – 36.00` | **Giảm `-19%` đến `-28%`** (tranh chấp WAL `synchronous_commit=on` với 2× TX từ Processing) |
| **Inference p50 / p95 / p99 (ms)** | `26.11–26.40` / `300.92–301.89` / `304.44–304.60` | `26.05–26.60` / `301.11–302.27` / `304.63–304.90` | Không đổi (~0%) |
| **Hot (`M-001`) E2E p50 / p95 / p99 (ms)** | `1231.6–1936.0` / `1931.3–2224.0` / `1982.4–2248.9` | `2513.7–3958.8` / `3375.1–4839.6` / `3460.9–5244.0` | **Tăng `+1.4s` đến `+2.6s` (p95)** |
| **Background E2E p50 / p95 / p99 (ms)** | `1476.1–1784.3` / `2405.5–5634.5` / `3144.1–8499.4` | `2743.7–4290.3` / `13934.9–16230.6` / `16790.7–19538.3` | **Suy giảm mạnh** (do `kafka_publish_to_business_db p95` tăng từ `5.16s` lên `13.04s` ở Run 1) |
| **Processing DB Tx p95 (ms)** | `39.22 – 41.13` | `54.76 – 63.90` | Tăng `~+35%–55%` (chi phí `TxC` + ledger/frontier) |
| **Outbox Publish p95 (ms)** | `92.45 – 139.83` | `90.26 – 160.39` | Tương đương |
| **Alert Delivery p95 (ms)** | `30.89 – 34.62` | `33.50 – 42.83` | Tăng nhẹ `+3–8 ms` |
| **Alert Frontier Wait p95 (Run 1)** | N/A (`per_key=1`) | `20.581 ms` (`mean=9.763 ms`) | Rất nhỏ |
| **ML Peak Concurrency / Budget** | `8 / 12` | `8 / 12` | Giống nhau (budget = 8) |
| **Max Pending / Max Outbox** | `44–50` / `10–15` | `12–25` / `7–9` | Pending giảm một nửa ở worker (do Stage A kìm nhịp vào) |
| **PG Conns / Lock Waits** | `14–15` / `0` | `14–15` / `0–1` | Rất thấp, `0` deadlock |
| **Mean CPU (%) / Peak RSS (MB)** | `97.50–100.20%` / `131.80–137.21` | `121.89–125.94%` / `132.44–139.06` | CPU `+~24%` điểm do thêm TxA & Resequencer SQL |
| **CPU Throttled Periods** | `0` | `0` | `0` |
| **Correctness Violations** | **`0`** | **`0`** | **100% đúng ở cả hai chế độ** |

### 1.2. Official Workload C (`Hot Machine`: `M-001` `16.0 RPS` + `34` background × `1.0 RPS` = `50.0 RPS`)

| Metric | Baseline (`per_key=1, total=8`) | Candidate A (`per_key=2, total=8`) | Chênh lệch / Nhận định |
|---|---:|---:|---|
| **Admitted RPS** | `35.33 – 35.67` | `42.60 – 46.80` | **Tăng `+19.4%` đến `+32.5%`** (giải tỏa tắc nghẽn `M-001` trên partition) |
| **Outcomes RPS (Processing)** | `35.33 – 35.67` | `39.47 – 45.93` | **Tăng `+11.7%` đến `+29.0%`** |
| **Predictions RPS (Business)** | `35.07 – 35.67` | `33.07 – 37.40` | Tương đương trong giới hạn PG fsync của Business Consumer |
| **Inference p50 / p95 / p99 (ms)** | `26.49–26.61` / `302.45–302.75` / `304.55–304.90` | `26.42–26.72` / `302.20–303.15` / `304.92–304.93` | Không đổi (~0%) |
| **Hot (`M-001`) E2E p50 (ms)** | `20939.9 – 22484.6` | `6428.3 – 8979.7` | **Giảm `-60%` đến `-70%`** |
| **Hot (`M-001`) E2E p95 (ms)** | **`28108.4 – 30716.2`** | **`7394.4 – 9901.4`** | **Giảm `-68%` đến `-74%` (~`3×`–`4×` nhanh hơn!)** |
| **Hot (`M-001`) E2E p99 (ms)** | `28550.9 – 31192.8` | `7569.1 – 10290.5` | **Giảm `-67%` đến `-74%`** |
| **Background E2E p50 (ms)** | `452.5 – 546.6` | `2824.9 – 5041.2` | Tăng do `kafka_publish_to_business_db p50 = 1.62s` khi thông lượng lên `46 RPS` |
| **Background E2E p95 / p99 (ms)** | `16356.5–18709.8` / `20830.3–22433.3` | `10728.8–12982.7` / `14265.0–16931.5` | **Cải thiện `-23%` đến `-41%` (p95)** (không còn bị `M-001` pause partition lâu) |
| **Processing DB Tx p95 (ms)** | `36.21 – 42.44` | `58.54 – 69.84` | Tăng chi phí `TxC` + `AlertResequencer` |
| **Outbox Publish p95 (ms)** | `161.58 – 164.42` | `139.10 – 291.15` | Hợp lệ, drain sạch |
| **Alert Delivery p95 (ms)** | `41.15 – 46.13` | `35.71 – 45.23` | Tương đương / cải thiện nhẹ |
| **Alert Frontier Wait (Run 1)** | N/A (`per_key=1`) | `p50=11.64ms / p95=197.85ms / p99=264.22ms` | Khớp đúng cơ chế giữ signal sau khi gặp đọc `300 ms` chậm |
| **ML Peak Concurrency / Max Per-Key** | `8` / `1` | `8` / `2` | `M-001` tận dụng được đúng `per_key=2` |
| **Max Pending / Max Outbox** | `102–112` / `11–14` | `86–92` / `9–14` | Pending queue thấp hơn nhờ xả `M-001` gấp đôi |
| **PG Conns / Lock Waits** | `14–16` / `0` | `14–15` / `1–2` | Lock wait nhỏ (`<=2`), `0` deadlock |
| **Mean CPU (%) / Peak RSS (MB)** | `80.96–92.77%` / `139.55–143.54` | `119.56–125.48%` / `141.93–146.23` | CPU tăng khi gánh thêm ~10 RPS + TxA |
| **CPU Throttled Periods** | `0` | `0` | `0` |
| **Correctness Violations** | **`0`** | **`0`** | **100% đúng ở cả hai chế độ** |

---

## 2. Phân tích Nguyên nhân & Giải thích Kỹ thuật Trung thực

1. **Tại sao Workload C cải thiện mạnh khi vẫn giữ tổng `max_total_inflight=8`?**
   - Ở baseline (`per_key=1`), `M-001` bị kẹt 1 slot và tích tụ backlog qua ngưỡng `max_pending_per_key=64`, khiến runner phải `pause()` Kafka partition chứa `M-001`, vừa đẩy `M-001 p95` lên `28.1–30.7s` vừa làm tắc lây các máy background chung partition (`bg p95 = 16.4–18.7s`) và tụt `admitted_rps` còn `35.3–35.7 RPS`.
   - Khi bật Hybrid với `per_key=2`, `M-001` xử lý song song 2 readings/lần -> vượt qua được các tail `300 ms`, cắt **`~71%` độ trễ `M-001 E2E p95`** (`7.39–9.90s`), giảm luôn `bg E2E p95` xuống `10.73–12.98s` và phục hồi thông lượng lên `42.60–46.80 admitted RPS`.

2. **Tại sao Workload A (`50×1 RPS`) bị suy giảm trên harness Compose đơn-PG (`synchronous_commit=on`)?**
   - Ở tải đều `1 rps/machine`, hầu như không có backlog nội tại trên cùng machine (`max_per_key_inflight = 1` ở cả Run 1 lẫn baseline), nên song song per-key không tạo thêm tốc độ nào.
   - Ngược lại, kiến trúc 3 giai đoạn phải thực thi **2 database transactions tuần tự/bổ sung cho mỗi reading** (Stage A `TxA` trên poll thread + Stage C `TxC` trên worker thread) thay vì 1 transaction như baseline:
     - Vì **Docker Compose Postgres (`postgres:17`) mặc định `synchronous_commit=on`** và cả `proc_db` + `biz_db` nằm trên **cùng 1 WAL disk**, lượng fsync tăng gấp đôi từ Processing đã cạnh tranh trực tiếp với single serial `business-prediction` consumer (`preds_rps` giảm từ `44.5–45.9` xuống `33.0–36.0`, tạo hàng đợi `kafka_publish_to_business_db p95 = 13.04s`).
     - Ngoài ra Stage A `TxA` chạy từng record đồng bộ trên 1 poll thread làm tốc độ nhận vào tự nhiên bị giới hạn quanh `~45–48 rps` trên single-worker.

---

## 3. Đánh giá theo Acceptance Criteria (Mục 8 Yêu cầu)

| Tiêu chí Acceptance | Kết quả | Bằng chứng / Ghi chú |
|---|---|---|
| **Zero missing terminal outcomes** | **PASS** | `0 / 6` runs baseline, `0 / 6` runs candidate (`1000/1000` outcomes mỗi run) |
| **Zero duplicate logical outcomes** | **PASS** | `0` duplicate outcomes/alerts across `12/12` runs & `223/223` tests |
| **Zero stale latest-prediction updates** | **PASS** | `stale_prediction_updates=0`, `wrong_latest_sequence=0`, `Projector` sequence guard pass |
| **Zero alert ordering violations** | **PASS** | `alert_sequence_violations=0` qua broker readback + `maintai_kafka_outbox` |
| **Zero incorrect Alert Lifecycle transitions** | **PASS** | `test_alert_sequence_guard.py` verify `STALE_IGNORED`, `STALE_AFTER_TERMINAL`, legacy bootstrap, conflict fail-closed |
| **Zero unsafe Kafka offset commits** | **PASS** | `offset_commit_violations=0`, `InspectingRunner` & `VerifiedKeyedRunner` pass |
| **Duy trì gần 50 admitted RPS** | **Đạt một phần** | Workload C cải thiện lên **`42.60–46.80`** (Baseline `35.33–35.67`); Workload A giảm nhẹ xuống **`44.87–47.80`** (Baseline `50.00`) |
| **Hot machine E2E p95 giảm rõ rệt** | **PASS (Mạnh)** | Workload C giảm từ **`28.11–30.72s` → `7.39–9.90s`** (**`-68%` đến `-74%`**) |
| **Background E2E p95 không suy giảm đáng kể** | **Đạt trên C / FAIL trên A** | Workload C cải thiện (`16.36–18.71s → 10.73–12.98s`); Workload A suy giảm (`2.41–5.63s → 13.93–16.23s`) do tắc chặng Business DB fsync khi Processing commit `2×` trên cùng instance Postgres |
| **Queue và Outbox drain hết** | **PASS** | Toàn bộ 6 runs candidate drain sạch (`0` pending, `0` outbox, `0` ledger unresolved) |
| **Không tạo resource pressure bất thường** | **PASS có lưu ý** | `PG Conns = 14–15`, `PG Lock Waits <= 2`, `CPU throttling = 0`, `RSS ~132–146 MB`; `CPU%` tăng `~+24%` điểm do thêm `TxA` & `AlertResequencer` |

### Kết luận & Khuyến nghị Trung thực
- **Kết luận thẳng thắn:** Ở cấu hình `per_key=2, total=8` trên môi trường **đơn-worker + Postgres đơn-instance `synchronous_commit=on`**, Hybrid Ordering **cải thiện rất lớn bài toán Head-of-Line ở Hot Machine (Workload C: p95 nhanh hơn ~3–4×)** nhưng **tạo regression rõ rệt về throughput Business DB và BG p95 trên tải đều đồng nhất không có hot key (Workload A)**.
- **Vì vậy:**
  1. **Giữ mặc định `PROCESSING_HYBRID_ORDERING_ENABLED=false`, `PROCESSING_MAX_INFLIGHT_PER_KEY=1`** trong `WorkerSettings` và GitOps (`environments/local/apps`).
  2. Chỉ kích hoạt có chủ đích khi workload có hot machine lệch tải hoặc sau khi tối ưu thêm (batch admission `TxA` theo batch poll trên poll thread để bỏ chi phí 1 fsync/record) và chạy validation trên cụm `k3d` đa-replica (`6` workers chia nhau tải `TxA` + `ml-service` thật).
