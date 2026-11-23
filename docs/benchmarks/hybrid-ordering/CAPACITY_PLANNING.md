# Hybrid Ordering Capacity Planning Reference (50 RPS Verified + Scaling Formulas)

- **Date:** 2026-11-23
- **Branch:** `feat/hybrid-ordering`
- **Source Constants Inspected:** [messaging/config.py](../../../messaging/src/maintai_messaging/config.py), [processing/worker/main.py](../../../processing/worker/src/consumer/main.py), [processing/shared/persistence/pool.py](../../../processing/shared/persistence/pool.py), [processing/outbox-relay/main.py](../../../processing/outbox-relay/src/relay/main.py), [processing/read-api/history_runtime.py](../../../processing/read-api/src/api/history_runtime.py), [backend/shared/persistence/pool.py](../../../backend/shared/persistence/pool.py), [backend/business-api/main.py](../../../backend/business-api/src/api/main.py), [backend/business-consumer/runtime.py](../../../backend/business-consumer/src/runtime.py)

---

## 1. Analytical Concurrency Model (Little's Law & Bottleneck Bounds)

For any component with sustained arrival rate $\lambda$ (`req/s` or `events/s`) and mean service time $W$ (`s`), required average concurrency is $L = \lambda \cdot W$. End-to-end cluster throughput is strictly bounded by the minimum ceiling across all 5 pipeline stages:

$$\text{Cluster Max RPS} = \min\Big(C_{\text{ing}},\; C_{\text{proc}},\; C_{\text{ml}},\; C_{\text{db}},\; C_{\text{biz}}\Big)$$

Per-machine concurrency safety constraint (Head-of-Line threshold on hot machine `M-001` at $\lambda_{\text{hot}}$):

$$L_{\text{hot,needed}} = \lambda_{\text{hot}} \cdot W_{\text{ML,p95}}$$

- With **Real CPU HGB** on native x86_64 ($W_{\text{ML,p95}} \approx 0.013\text{ s}$ at $c=1..2$): at $\lambda_{\text{hot}} = 16\text{ RPS}$, $L_{\text{hot,needed}} = 16 \times 0.013 = 0.21 \ll 1.0$. Baseline (`per_key=1`) has ~`5×` latency margin per machine.
- With **Controlled Tail ML** ($25\text{ ms}$ mean, $10\text{–}28\%$ at $300\text{ ms}$, $W_{\text{ML,avg}} \approx 0.052\text{–}0.102\text{ s}$): at $\lambda_{\text{hot}} = 16\text{ RPS}$, $L_{\text{hot,needed}} = 0.83\text{–}1.63 > 1.0$ — `per_key=1` saturates and pauses Kafka partition, whereas `per_key=2..4` breaks the HoL barrier.

---

## 2. Tier-by-Tier Capacity Profiles (Measured)

### 2.1 ML Service (`ml-service` LogReg & `ml-service-shadow` HGB)

| Metric / Concurrency | `1` HGB Pod (Unbounded CPU) | `1` HGB Pod (`1 CPU` Limit) | `2` HGB Pods (Unbounded CPU) | Safe Guideline |
| --- | ---: | ---: | ---: | --- |
| `c=1` Throughput / p95 | `90.05 RPS` / `12.83 ms` | `94.77 RPS` / `12.16 ms` (`0` throttled) | — | Optimal latency (`p50=10.68ms`) |
| `c=2` Throughput / p95 | `97.80 RPS` / `24.71 ms` | `97.45 RPS` / `27.32 ms` (**`24` throttled**) | `188.07 RPS` / `12.90 ms` (`1/pod`) | **Sweet spot (`~1–2 in-flight / ML pod`)** |
| `c=4` Throughput / p95 | `101.41 RPS` / `54.67 ms` | `92.75 RPS` / `59.40 ms` (**`25` throttled**) | `202.47 RPS` / `25.06 ms` (`2/pod`) | Upper safe saturation ceiling |
| `c=8..12` Throughput / p95 | `97.38 RPS` / `178.05 ms` (`c=12`) | — | `205.46 RPS` / `83.65 ms` (`6/pod`) | **Do NOT exceed** (pure GIL queue growth) |
| Peak RSS / Mean CPU | `135.6–138.0 MiB` / `89–115%` | `132–134 MiB` / `1 CPU` cap | `~265 MiB` total | Request **`256 Mi`**, limit **`512 Mi`** |
| **Safe Sustained Capacity** | **`65–70 RPS / pod`** (~70% ceiling) | **`< 60 RPS / pod`** (throttles >`c=2`) | **`130–140 RPS`** total (`50 RPS N+1` OK) | GPU `NVIDIA RTX 2050` **not used** (CPU sklearn) |

- Under k3d real HGB sweeps (`50 RPS` WL=C, `pk=4`): **`1` pod** achieved only `48.00 RPS` (`hot_p95 = 3,108 ms`), proving 1 pod is unsafe even at 50 RPS under skewed bursts; **`2` pods** sustained `50.0 RPS` (`138–176 ms` `pk=4`); **`4` pods** achieved `89.91 ms`.

### 2.2 Processing Worker & Outbox Relay

- **Measured Worker Scaling (`k3d` Real HGB, 50 RPS WL=C, `pk=4`):**
  - `1` pod (`infl=16`): capped at **`47.07 RPS`** (`hot_p95 = 1,601.19 ms`, `cpu = 339.7m / 637m`, `92 Mi`).
  - `2` pods: achieved **`50.00 RPS`** (`hot_p95 = 95.55 ms`, `824.1m / 893m`, `180 Mi`).
  - `4` pods: **`50.00 RPS`** (`hot_p95 = 82.48 ms`, `829.6m / 915m`, `353 Mi`).
  - `6` pods: **`50.07 RPS`** (`hot_p95 = 76.73 ms`, `726.8m / 833m`, `530 Mi`).
- **Safe Worker Capacity:** **`~35–40 RPS / pod`** (`~47 RPS` hard ceiling per pod on Ryzen vCPU; ~`120–160m` mean CPU per pod under active load, `~88 Mi` RSS/pod steady state).
- **Safe Relay Capacity:** `1` relay pod sustained `50.0 RPS` (`outbox_p95 = 48.25 ms`, `183.3m CPU`, `87 Mi`); `2` relay pods lowered `outbox_p95` to `40.19 ms` and provide active-passive `N+1` failover without blocking outbox dispatch.

### 2.3 Cluster-Wide `PROCESSING_MAX_TOTAL_INFLIGHT` Formula

To prevent `processing-worker` from flooding `ml-service` Starlette worker threads (40/pod) into GIL queueing (`k3d` proved `pk=4` into `2` HGB pods doubled `hot_p95` from `57.16 -> 138.37 ms`), bind worker inflight to ML replica capacity:

$$\text{Cluster\_Total\_ML\_Concurrency} = R_{\text{pw}} \times \text{PROCESSING\_MAX\_TOTAL\_INFLIGHT}_{\text{pod}} \le R_{\text{ml}} \times C_{\text{ml\_safe\_per\_pod}}$$

With $C_{\text{ml\_safe\_per\_pod}} = 2$ (optimal) to $4$ (upper cap):
- At `2` ML pods (`c_safe = 4` -> `8` cluster ceiling) and `4–6` worker pods:
  - Set `PROCESSING_MAX_TOTAL_INFLIGHT=2..4` per worker pod (**`8–24` cluster max**, average `~1–4` in-flight per ML pod) when running fast HGB, OR set `8` ONLY if ML is `>= 4` pods.
  - Under **Baseline** (`PROCESSING_HYBRID_ORDERING_ENABLED=false`), `max_inflight_per_key=1` and `PROCESSING_MAX_ACTIVE_KEYS=4..8` naturally keep per-pod ML concurrency bounded at `~1–2` at `50 RPS`.

### 2.4 Database Capacity & Connection Pool Formula

Exact per-pod `psycopg` pool sizes configured in source code:

| Service | Pool Code Formula / Default | Max Conns / Pod | At Local Replicas (`6 pw, 2 rly, 1 read, 4 biz, 1 bapi`) | Peak Measured (`k3d` 50 RPS) |
| --- | --- | ---: | ---: | --- |
| `processing-worker` | `max(12, min(32, effective_max_total_inflight + 4))` | `12` (`infl<=8`) / `20` (`infl=16`) | `72` (`infl=8`) / `120` (`infl=16`) | `27–44` total on `proc_db` |
| `processing-outbox-relay` | `ConnectionPool(size=4)` | `4` | `8` | Included in `proc_db` (`<=44`) |
| `processing-read-api` | `source_pool(4) + history(4+2+2)` | `12` (`4` if no history) | `12` | Included in `proc_db` (`<=44`) |
| **`proc_db` Subtotal** | — | — | **`92` (`140` at `infl=16`)** | **`27–44`** |
| `business-consumer` | `ConnectionPool(size=8)` (shared across 3 topics) | `8` | `32` | `25–30` total on `biz_db` |
| `business-api` | `ConnectionPool(size=12)` + `1` `LISTEN` | `13` | `13` (up to `52` at HPA `4`) | Included in `biz_db` (`<=30`) |
| **`biz_db` Subtotal** | — | — | **`45` (`84` if `bapi=4`)** | **`25–30`** |
| `rag-service`, `drift-worker`, `mlflow` | Per-request / single-conn / SQLAlchemy pool | `~2–10` / pod | `~10–20` | Idle/cron (`1` job/min) |
| **PostgreSQL Cluster Worst-Case Max** | Shared `max_connections=300` (local) / ~`450` (RDS `db.t4g.medium`) | — | **`~149–246` theoretical max** (`137 + ~10–20` if `bapi=1`, or `176 + ~10–20` if `bapi=4`) | **`52–74` combined observed peak** |

> [!WARNING]
> **Critical Scaling Guard:** Scaling `processing-worker` to `12` pods (the Kafka partition max) with `PROCESSING_MAX_TOTAL_INFLIGHT=16` creates **`12 × 20 = 240` max connections from `processing-worker` alone**, plus `business-api` (at 4 pods = `52`) + `business-consumer` (`<= 96` at 12 pods) -> **`~396` connections**, **exceeding `max_connections=300`!** Keep `PROCESSING_MAX_TOTAL_INFLIGHT <= 8` (`pool size = 12`) AND cap `processing-worker` at `<= 6–8` pods unless PG `max_connections` is raised (`450+`) or pool caps are lowered (`size=6–8` per worker).

### 2.5 Business Consumer Capacity & Kafka Partition Bounds

- **Before Opt 3:** `1` pod (`batch=1`, `poll=10ms`) capped at **`40.82–43.56 RPS`** (`5.7 ms` sync Kafka commit + `4.9 ms` wake delay per record).
- **After Opt 3 (`Wake + batch=16`):**
  - `1` pod sustained **`50.13 RPS`** on k3d (`k2b_p50=1.80 ms`, `p95=10.05 ms`, `hot_e2e_p95=84.08 ms`, `cpu_mean=658m`), corresponding to a **`~75–90 RPS/pod` ceiling** (and `85–110 RPS` single-topic in isolated compose).
  - `2` pods hit `50.07 RPS` (`k2b_p95=7.23 ms`, `569.9m`, `150 Mi`); `4` pods hit `50.00 RPS` (`k2b_p95=5.44 ms`, `602.5m`, `300 Mi`).
- **Kafka Partition Hard Ceiling:** All 4 topics (`sensor.readings`, `sensor.rejected`, `inference.results`, `alert.signals`) have **`12` partitions** (`allowIdleConsumers: false`). Replicas `> 12` for `processing-worker`, `business-consumer`, or `storage-consumer` sit **100% idle** with zero throughput gain.

---

## 3. Recommended Baseline & Hybrid Concurrency Configurations

| Parameter | Default Recommended (`50 RPS`, Fast ML / HGB) | Conditional Hybrid (`When ML p95 >= 45–50ms`) | Rationale |
| --- | --- | --- | --- |
| `PROCESSING_HYBRID_ORDERING_ENABLED` | **`false`** | **`true`** (opt-in) | Zero HoL advantage on fast HGB (`13ms`); wins massively (`-96%` p95) on slow/tailed ML |
| `PROCESSING_MAX_INFLIGHT_PER_KEY` | **`1`** | **`2`** (max `4` if ML >= `4` pods) | Prevents GIL surge (`pk=4` on 2 HGB pods hurt `57 -> 138 ms`) |
| `PROCESSING_MAX_ACTIVE_KEYS` | **`8`** | **`8`** | Fair multi-machine multiplexing |
| `PROCESSING_MAX_TOTAL_INFLIGHT` | **`8`** (pool size `12`) | **`8`** (pool size `12`, cap `<= 16`) | Caps total worker-to-ML concurrency and PG connections |
| `KAFKA_COMMIT_BATCH_SIZE` (`business-consumer`) | **`16`** | **`16`** | `-92.7%` `k2b_p95`; cuts sync broker commits ~`67%` safely behind idempotent inbox |
| `KAFKA_COMMIT_MAX_WAIT_MS` (`business-consumer`) | **`25`** | **`25`** | Bounds max commit wait when traffic is low |
| `KAFKA_COMMIT_BATCH_SIZE` (`storage-consumer`) | **`1`** | **`1`** | Untouched default; no lag observed at 50 RPS |
