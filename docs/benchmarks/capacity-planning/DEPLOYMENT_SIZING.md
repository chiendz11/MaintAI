# Production-Grade Deployment Replica & Resource Sizing (`MaintAI`)

- **Date:** 2026-11-23
- **Git Branch (`MaintAI`):** `feat/hybrid-ordering` (`MaintAI-gitops`: `feat/hybrid-ordering@4ea6821`, `Maint-infrastructure`: `main` — untouched)
- **Target Baseline Workload:** `50 RPS` (`Workload A` `50 × 1 RPS`; `Workload C` `M-001 16 RPS + 34 × 1 RPS`) with calculated projections to `100` & `200 RPS`
- **Source Inventory Inspected:** [MaintAI/architecture/runtime-map.json](../../../architecture/runtime-map.json), [MaintAI-gitops/base](../../../../MaintAI-gitops/base), [MaintAI-gitops/environments/local](../../../../MaintAI-gitops/environments/local), [MaintAI-gitops/environments/production](../../../../MaintAI-gitops/environments/production), [MaintAI-gitops/platform](../../../../MaintAI-gitops/platform), [Maint-infrastructure/terraform/platform/variables.tf](../../../../Maint-infrastructure/terraform/platform/variables.tf)
- **Measured Evidence Corpus:** [k3d-baseline/REPORT.md](../hybrid-ordering/k3d-baseline/REPORT.md), [k3d-hybrid/REPORT.md](../hybrid-ordering/k3d-hybrid/REPORT.md), [replica_sweeps.json](../hybrid-ordering/k3d-hybrid/replica_sweeps.json), [pod_kill_tests.json](../hybrid-ordering/k3d-hybrid/pod_kill_tests.json), [ml-service/REPORT.md](../hybrid-ordering/ml-service/REPORT.md)

> [!CAUTION]
> **Evidence Discipline:** Every row below explicitly labels whether numbers are **`Benchmark Validated`** (directly measured on `k3d-maintai-local` at `50 RPS` or direct HTTP micro-benchmarks), **`Calculated Estimate`** (analytical extrapolation from per-pod ceilings / Little's Law), or **`Not Yet Tested`** (unexercised components such as `rag-service`, `frontend` under user concurrency, `100/200 RPS` cluster loads, or AWS RDS Multi-AZ `synchronous_commit=on`). Do **NOT** modify Production GitOps manifests until staging validation with `synchronous_commit=on` passes.

---

## 1. Deployment & Workload Inventory

MaintAI comprises `14` application runtimes plus `4` core platform components (`PostgreSQL`, `Kafka`, `MinIO/S3`, `Keycloak`). Edge Ingress (`ingress-nginx` local / AWS ALB external in production) runs outside the `maintai` application pods and must be sized separately at the ingress controller / load balancer layer.

| Component | Workload Kind | Stateless / Stateful / Partition-Bound | Current GitOps Replicas (Local HPA/KEDA) | Current GitOps CPU req / lim | Current GitOps Mem req / lim | Measured k3d Idle / 50 RPS Active Total (Peak CPU, Mem) | Notes / Constraints |
| --- | --- | --- | ---: | --- | --- | --- | --- |
| `ingestion-api` | `Deployment` | **Stateless** (REST -> Kafka ACK) | `2` (`HPA 2..2` @ 70% CPU) | `100m` / none | `128Mi` / `1Gi` | `210.2–233.8m / 352m` pk, `154–166 Mi` (2 pods) | No PG DB; HTTP keepalive + OIDC JWKS cache |
| `processing-worker` | `Deployment` | **Partition-bound (`12` max)** | `6` (`KEDA 6..6`, lag `20` or CPU `70%`) | `100m` / none (`init: 25m/none`) | `128Mi` / `1Gi` | `605.1–883.8m / 1,184m` pk, `522–538 Mi` (6 pods) | Consumer `sensor.readings`; PG `proc_db` pool `12–20`/pod |
| `ml-service` (LogReg) | `Deployment` | **Stateless CPU** | `2` (`HPA 2..2` @ 70% CPU) | `100m` / none (`init: 50m/500m`) | `128Mi` / `1Gi` | `203.2–320.0m / 363m` pk, `252–266 Mi` (2 pods) | `1` uvicorn worker; HTTP `/api/v1/predict` |
| `ml-service-shadow` (HGB) | `Deployment` | **Stateless CPU (GIL-bound)** | `1` (`HPA 1..1` @ 80% CPU) | `100m` / none (`init: 50m/500m`) | `128Mi` / `1Gi` | `565.7–962.9m / 1,337m` pk (`2–4` pods), `127 Mi/pod` | **CPU HGB only; `RTX 2050` unused**; `1` pod fails 50 RPS HA! |
| `ml-service-mirror` | `Deployment` | **Stateless Nginx mirror** | `1` (fixed) | `25m` / `1000m` | `32Mi` / `128Mi` | `1.0m / 1m`, `12–13 Mi` | `proxy_read_timeout 50ms` drops HGB spikes `>50ms` |
| `processing-outbox-relay` | `Deployment` | **Stateless DB Lease Relay** (`FOR UPDATE SKIP LOCKED`) | `1` base (`2` during bench) | `100m` / none | `128Mi` / `1Gi` | `236.9–392.6m / 416m` pk, `174–179 Mi` (2 pods) | Publishes ordered `inference.results` + `alert.signals`; PG pool `4` |
| `processing-read-api` | `Deployment` | **Stateless HTTP + S3 Export** | `1` (no HPA) | `100m` / none | `128Mi` / `1Gi` | `29.6–448.6m / 841m` pk, `98–104 Mi` | Polled by bench + Evidence/History clients; PG pool `4–12` |
| `business-consumer` | `Deployment` | **Partition-bound (`12` max)** | `4` (`KEDA 4..4`, lag `20` or CPU `70%`) | `100m` / none | `128Mi` / `1Gi` | `349.5–715.5m / 878m` pk, `293–312 Mi` (4 pods) | Runs 3 groups (`live`, `prediction`, `alert`); PG pool `8` |
| `business-api` | `Deployment` | **Stateless HTTP + PG `LISTEN` SSE** | `1` (`HPA 1..4` @ 70% CPU) | `100m` / none | `128Mi` / `1Gi` | `13.3–35.7m / 43m` pk, `75–78 Mi` (idle UI) | `NotificationHub` uses PG `LISTEN` + `pool=12` (`13` conns/pod) |
| `storage-consumer` | `Deployment` | **Partition-bound (`12` max)** | `1` (fixed) | `100m` / none | `128Mi` / `1Gi` | `339.7–576.5m / 659m` pk, `92–99 Mi` | Consumes 3 raw/rejected/outcome topics; writes S3/MinIO |
| `rag-service` | `Deployment` | **Stateless API + Heavy Embedding Model** | `1` (`HPA 1..3` @ 75% CPU) | `250m` / `2` | `512Mi` / `2Gi` | Scaled `0` during bench (`Not Yet Tested` under load) | Requires pgvector `knowledge_db` + Groq HTTPS egress |
| `frontend` | `Deployment` | **Stateless Static Nginx/SPA** | `1` (`HPA 1..4` @ 70% CPU) | `100m` / none | `128Mi` / `1Gi` | Scaled `0` during bench (`~15–40 Mi` idle Nginx) | Browser SPA calling `business-api` REST/SSE only |
| `sensor` | `Deployment` | **Stateful Single Controller (`Recreate`, SQLite PVC)** | `1` (MUST stay `1`) | `100m` / none (`init: 25m/none`) | `128Mi` / `1Gi` | Scaled `0` during 50 RPS external generator bench | SQLite WAL PVC lock; **cannot scale horizontally (`Recreate`)** |
| `simulator-ui` | `Deployment` | **Stateless Static Nginx/SPA** | `1` (fixed) | `100m` / none | `128Mi` / `1Gi` | Scaled `0` during bench | Admin simulator UI only |
| `drift-worker` | `CronJob` (`*/1 * * * *`, `Forbid`) | **Batch Job** (`Forbid` overlap) | `0–1` transient pod | `50m` / none | `128Mi` / `512Mi` | Suspended cleanly during 50 RPS bench | Polls `processing-read-api` + writes `drift_db` + Pushgateway |
| `mlflow` | `Deployment` | **Platform Service (`Recreate`)** | `1` | `100m` / `500m` | `256Mi` / `1Gi` | Scaled `0` during bench | Local SQLite `/tmp`; Prod uses `mlflow_db` RDS + S3 (`mlops`) |
| `postgres` | `StatefulSet` (local) / AWS RDS | **Stateful Single Primary** | `1` (`local`) / Multi-AZ RDS (`prod`) | `100m` / none | `256Mi` / `2Gi` | `274.3–629.5m / 752m` pk, `228–839 Mi` | Local: `sync_commit=off, max_conns=300, shared_buffers=512MB`. Prod var default: `db.t4g.medium` |
| `kafka` | `StatefulSet` (local) / Strimzi `3` brokers | **Stateful (`12` partitions/topic)** | `1` (`local`, `rf=1`) / `3` (`prod`, `rf=3`) | `250m` (`local`) / `500m` (`prod`) | `768Mi/2Gi` (`local`) / `2Gi/4Gi` (`prod`) | `106.1–204.4m / 270m` pk, `1,001–1,036 Mi` | `3` KRaft dual-role brokers (`maintai-gp3 50Gi`) in prod GitOps |
| `minio` / S3 | `StatefulSet` (`local`) / AWS S3 | **Stateful Object Store** | `1` (`local`) | `100m` / none | `256Mi` / `1Gi` | `171.9–342.7m / 377m` pk, `359–534 Mi` | Handles `storage-consumer` raw parquet/json writes during 50 RPS |
| `keycloak` | `StatefulSet` | **Stateful Auth IdP** | `1` | `250m` / none | `512Mi` / `2Gi` | `2.8–16.0m / 34m` pk, `506–530 Mi` | Cached JWKS keeps CPU near zero post-warmup |

---

## 2. Replica Benchmark Results (`replica_sweeps.json` — `50 RPS` WL=C, Real HGB)

All 15 single-variable sweeps (`pk=4, inflight=16, batch=16` default unless noted) drained with `0` lag and `0` violations:

| Component Varied | Replicas Tested (Cfg / Active) | Sustained RPS (`proc / biz`) | `hot_e2e_p95` (ms) | `bg_e2e_p95` (ms) | Measured Group CPU (`mean / peak`) | Measured Group Mem Peak | Saturation / Scaling Verdict |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| `ml-service-shadow` (HGB) | **`1`** | **48.00 / 48.13** | **3,108.00** | 615.01 | `282.8m / 354m` | `127 Mi` | **FAIL** under `pk=4` hot burst (`p95 > 3s`, fails `50 RPS`) |
| `ml-service-shadow` (HGB) | **`2`** | 49.93 / 50.00 | 175.79 | 124.48 | `959.4m / 1,258m` | `255 Mi` | Meets 50 RPS; high `pk=4` jitter (`629m/pod` pk) |
| `ml-service-shadow` (HGB) | **`3` -> `4` (HPA)** | **50.00 / 50.07** | **89.91** | 77.42 | `962.9m / 1,070m` | `510 Mi` | Linear latency halving (`175.8 -> 89.9ms`) |
| `processing-worker` | **`1`** | **47.07 / 47.20** | **1,601.19** | **1,842.01** | `339.7m / 637m` | `92 Mi` | **FAIL** at 50 RPS (`proc_p95 = 1,669.6ms`) |
| `processing-worker` | **`2`** | 50.00 / 50.00 | 95.55 | 86.02 | `824.1m / 893m` | `180 Mi` | Sustains `50 RPS`, tight margin (`~446m/pod` peak) |
| `processing-worker` | **`4`** | 50.00 / 50.13 | 82.48 | 75.74 | `829.6m / 915m` | `353 Mi` | Comfortable headroom (`3` partitions/pod) |
| `processing-worker` | **`6`** | **50.07 / 50.07** | **76.73** | 68.25 | `726.8m / 833m` | `530 Mi` | Optimal low jitter (`2` partitions/pod) |
| `processing-outbox-relay` | **`1`** | 50.00 / 50.00 | 118.65 | 93.85 | `183.3m / 192m` | `87 Mi` | Capacity OK (`outbox_p95=48.25ms`), zero HA |
| `processing-outbox-relay` | **`2`** | 50.00 / 50.07 | 106.72 | 91.10 | `392.6m / 412m` | `171 Mi` | Recommended (`outbox_p95=40.19ms` + `N+1` HA) |
| `business-consumer` | **`1` (`batch=1`)** | 50.13 / 50.13 | **230.08** | 205.19 | `437.7m / 778m` | `84 Mi` | `k2b_p95 = 138.24ms` (saturates near `50` in single-broker sync) |
| `business-consumer` | **`1` (`batch=16`)** | 50.00 / 50.13 | **84.08** | 76.57 | `658.0m / 658m` | `75 Mi` | **`batch=16` cuts `k2b_p95` `138.2 -> 10.05ms`** on 1 pod |
| `business-consumer` | **`2` (`batch=16`)** | 50.00 / 50.07 | 75.41 | 71.84 | `569.9m / 632m` | `150 Mi` | **`N+1` HA + `k2b_p95=7.23ms`** (`6` partitions/pod) |
| `business-consumer` | **`4` (`batch=16`)** | **50.00 / 50.00** | **68.93** | 63.39 | `602.5m / 633m` | `300 Mi` | Best tail (`k2b_p95=5.44ms`, `3` partitions/pod) |
| `ingestion-api` | **`1`** | 50.00 / 49.93 | 82.44 | 72.98 | `147.3m / 202m` | `77 Mi` | `http_p95 = 13.81ms` (zero HA) |
| `ingestion-api` | **`2`** | **50.00 / 50.07** | **66.88** | 62.98 | `246.2m / 352m` | `154 Mi` | Recommended (`http_p95 = 11.00ms` + `N+1` HA) |

---

## 3. CPU / Memory Consumption & Request vs Limit Analysis

> **Critical Audit Finding on Current GitOps Manifests (`base/*/deployment.yaml`):**
> Almost all base workloads specify **`requests.cpu: 100m, requests.memory: 128Mi` with NO `limits.cpu` and `limits.memory: 1Gi`**. Two immediate hazards exist:
> 1. **Scheduling under-allocation:** Under real `50 RPS` load, `processing-worker`, `ml-service`, `ml-service-shadow`, `ingestion-api`, `business-consumer`, and `storage-consumer` consume **`120m–630m` per pod** (exceeding `100m` by `1.2–6.3×`), and **`ml-service` (LogReg) / `ml-service-shadow` (HGB) / `storage-consumer`** hold **`127–138 MiB` RSS per pod** (higher than `128Mi` memory request -> causes K8s pod eviction under memory pressure!).
> 2. **HPA Distortion:** Because `ml-service-shadow-hpa` targets `80%` CPU against the tiny `100m` request (`80m`), even light HGB inference immediately saturates `> 100%` of request and triggers aggressive autoscaling (`2 -> 4` pods). Raising `requests.cpu` accurately fixes HPA stability.

Measured per-pod resource envelopes across `k3d` runs:

| Component | Per-Pod Mean CPU at 50 RPS (m) | Per-Pod Peak CPU Observed (m) | Per-Pod Working Set RSS (Mi) | Recommended `req.cpu / lim.cpu` | Recommended `req.mem / lim.mem` |
| --- | ---: | ---: | ---: | --- | --- |
| `ingestion-api` (`2` pods) | `66–123m` | `176m` | `77–83 Mi` | `150m / 500m` | `192Mi / 384Mi` |
| `processing-worker` (`4–6` pods) | `100–181m` (at `2` pods: `412m`) | `199m` (`6` pods) / `637m` (`1` pod) | `88–92 Mi` | `250m / 1000m` (`500m` at `2–3` pods) | `256Mi / 512Mi` |
| `ml-service` (LogReg, `2` pods) | `102–160m` | `182m` | `126–133 Mi` | `200m / 1000m` | `256Mi / 512Mi` |
| `ml-service-shadow` (HGB, `2–4` pods) | `121–480m` | **`669m`** (`2` pods `pk=4`) | **`127–138 Mi`** | **`500m / 1500m`** (`1000m` causes throttling at `c>=2`) | **`256Mi / 512Mi`** |
| `processing-outbox-relay` (`2` pods) | `118–196m` | `208m` | `86–90 Mi` | `200m / 500m` | `192Mi / 384Mi` |
| `business-consumer` (`2–4` pods) | `87–179m` (`1` pod: `658m`) | `220m` (`4` pods) / `778m` (`1` pod) | `75–84 Mi` | `250m / 1000m` (`500m` at `2` pods) | `256Mi / 512Mi` |
| `business-api` (`1–2` pods) | `25–36m` (idle UI) | `43m` | `75–78 Mi` | `200m / 1000m` | `256Mi / 512Mi` |
| `processing-read-api` (`1` pod) | `30–449m` | `841m` (poll burst) | `98–104 Mi` | `200m / 1000m` | `256Mi / 512Mi` |
| `storage-consumer` (`1` pod) | `340–577m` | `659m` | `92–99 Mi` | `500m / 1500m` (if `1` pod) or `250m` (`2` pods) | `256Mi / 512Mi` |
| `postgres` (local single pod) | `274–630m` | `752m` | `345–839 Mi` | `500m / 2000m` (`prod`: RDS) | `1Gi / 2Gi` (`local`) |
| `kafka` (local single pod) | `106–204m` | `270m` | `1,001–1,036 Mi` | `500m / 2000m` | `1.5Gi / 2Gi` (`prod`: `2Gi/4Gi`) |

---

## 4 & 5. Per-Pod Capacity & Cluster-Wide `N_required` (`ceil(Target / safe_per_pod)`)

Using a **~`70–75%` safe sustained utilization factor** against measured saturation ceilings (except `N_required = ceil(Target / safe_per_pod)` + **`N+1`** minimum HA for zero-downtime rolling updates / pod loss):

| Deployment | Measured Hard Ceiling / Pod | Safe Capacity / Pod (~70%) | Evidence Tag | `50 RPS`: `N_req` -> `N+1` HA | `100 RPS` (`Est.`) `N+1` | `200 RPS` (`Est.`) `N+1` | Non-Linear Scaling Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `ingestion-api` | `> 100 RPS` (`50 RPS` @ `202m` peak) | `~100–120 RPS` | `50`: Bench (`ing=1` `50` OK); `100/200`: Calc | `1` -> **`2`** | `2` -> `3` | `3` -> `4` | Verify OIDC JWKS cache & Keep-Alive limits above `100 RPS` |
| `processing-worker` | `47.07 RPS` (`Sweep[3]`) | **`~35 RPS`** (`pk=1/2`) | `50`: **Benchmark Validated** (`pw=2` `50 RPS`); `100/200`: Calc | `2` -> **`3–4`** (`6` local OK) | `3` -> **`4–6`** | `6` -> **`8–10`** (cap `12`) | **Hard Kafka partition limit = `12` pods** (`allowIdleConsumers=false`) |
| `ml-service` (LogReg) | `> 200 RPS` (trivial linear) | `~150+ RPS` | `50`: Bench; `100/200`: Calc | `1` -> **`2`** | `1` -> `2` | `2` -> `3` | Very lightweight on CPU |
| `ml-service` (HGB CPU) | `~90–101 RPS` (`~48` under `pk=4` burst) | **`~65 RPS`** (`c=1–2/pod`) | Direct HGB bench + `50` k3d; `100/200`: Calc | `1` -> **`2–3`** (`3` safe burst `pk=4`) | `2` -> **`3–4`** | `4` -> **`5–6`** | GIL plateaus above `c=4`; do **not** run 1 CPU limit if `c>=2` |
| `processing-outbox-relay` | `> 60–80 RPS` (`50` @ `192m`) | `~55–65 RPS` | `50`: **Benchmark Validated** (`relay=1` OK); `100/200`: Calc | `1` -> **`2`** | `2` -> **`3`** | `4` -> **`5`** | `FOR UPDATE SKIP LOCKED` causes lock contention if scaled `> 4` |
| `business-consumer` (`batch=16`) | `~85–110 RPS` (`Opt 3` compose/k3d) | **`~65–75 RPS`** | `50`: **Benchmark Validated** (`biz=1,b=16` `50` OK); `100/200`: Calc | `1` -> **`2–4`** | `2` -> **`3–4`** | `3` -> **`4–6`** (cap `12`) | **Hard Kafka partition limit = `12` pods**; `business-live` shares same pool |
| `business-api` | Untested at load | Est. `~100+ req/s` + `128` SSE/pod | **Calculated Estimate / Not Yet Tested** | **`2`** (HA minimum, local `1`) | `2–3` | `3–4` | Each pod holds `12` pool + `1` persistent PG `LISTEN` conn |
| `storage-consumer` | `~60+ RPS` (`50 RPS` @ `659m` pk) | `~40–50 RPS` | `50`: Single-pod observed; `100/200`: Calc | `1` -> **`2`** | `3` -> `4` | `5` -> `6` (cap `12`) | Consumes 3 topics; scaling to `2` eliminates 1-pod bottleneck |

> **Local Cluster (15 GiB RAM Laptop) Budget Check:** At `50 RPS` (`30–33` running pods), entire k3d namespace consumed **`~3.8–5.0 vCPU` mean (`5.88 vCPU` peak)** and **`~4.4–5.4 GiB` RAM sum** (`~2.0–2.3 GiB` pipeline apps + `~2.4–3.1 GiB` Postgres/Kafka/MinIO/Keycloak/Observability). On a `15 GiB` laptop with OS + desktop overhead (~`3–4 GiB`), running `mlflow + rag-service + sensor + frontend + simulator-ui` simultaneously leaves **`~1–2 GiB` swap/RAM margin**, explaining why production HA counts (`N+2`) cannot all run at peak load on a single local node.

---

## 6. `N+1` / `N+2` Availability & Chaos Failure Analysis

Empirical pod-kill validation at **`50.0 RPS` Workload C (`Real HGB`, `pk=4, inflight=16, batch=16`)** demonstrated:

1. **Kill 1 `processing-worker` (`6 -> 5 -> 6` pods):**
   - Recovery: **`10.675 s`**, Drain: `0.118 s`, throughput sustained at **`50.20 RPS`**, `hot_e2e_p95 = 222.79 ms` (`max 407.65 ms`, `bg_max = 1,449.18 ms` during rebalance), **`0` violations**.
   - **Under `N_required` (`2` pods -> drops to `1`):** Remaining `1` pod saturates at `47.07 RPS` -> Kafka lag grows until Kubernetes reschedules the killed pod (`~10–30 s`).
   - **Under `N+1` (`3` pods -> `2` survive) / `N+2` (`4+` pods -> `3+` survive):** Remaining pods absorb `100%` of `50 RPS` immediately without backlog accumulation (**Recommended minimum `>= 3` pods for Prod 50 RPS; `4` optimal**).
2. **Kill 1 `ml-service-shadow` (`4 -> 3 -> 4` pods, or `N+1` `2` pods -> `1`):**
   - Observed k3d recovery (`4 -> 3 -> 4`): **`10.808 s`**, `hot_e2e_p95 = 114.85 ms`, `1,000/1,000` `PREDICTION_COMPLETED`, **`0` violations** (retry handles transient connection reset on HTTP call).
   - **CRITICAL HA WARNING (`2` pods -> drops to `1`):** Sweep[0] proved **`1` HGB pod fails `50 RPS` WL=C (`48.00 RPS`, `hot_p95 = 3,108 ms`)**! Therefore **`2` HGB pods is NOT `N+1` fault-tolerant against WL=C pod loss** — losing 1 pod forces backlog until pod restart (~`15–25s` initContainer model download + Ready). Production HGB `50 RPS` therefore requires **`3` pods minimum (`N+1`)** or **`4` pods (`N+2`)**!
3. **Kill 1 `business-consumer` (`4 -> 3 -> 4` pods):**
   - Recovery: **`7.545 s`**, `hot_e2e_p95 = 155.48 ms` (`k2b_p99 = 1,523.4 ms`, `max = 2,423.25 ms` across `~2.4 s` Kafka consumer-group rebalance), **`0` stale predictions / duplicate alerts**.
   - Note that with `Opt 3 batch=16`, **`2` pods (`N+1`, drops to `1`) STILL sustain `50.0 RPS`** cleanly (`Sweep[10]`: `1` pod `batch=16` = `50.13 RPS`, `hot_p95 = 84.08 ms`). Keeping **`3–4` pods** gives `N+2` tolerance across `business-live`, `business-prediction`, and `business-alert`.
4. **Non-HA Single-Pod Stateful Risks:**
   - `sensor` (`SQLite PVC`, `Recreate`) -> `1`-pod SPOF (`5–15 s` downtime on restart, zero backlog loss due to durable spool).
   - `postgres` (`local StatefulSet`) -> Production mitigates via **AWS RDS Multi-AZ** (`db_multi_az` in Terraform).
   - `kafka` (`local 1 broker`) -> Production mitigates via **3-broker Strimzi KRaft (`rf=3, min.insync.replicas=2`)**.

---

## 7 & 8 & 9. Dependency Capacity Constraints (Kafka, PostgreSQL, ML Service)

- **Kafka (`12` Partitions Fixed):**
  - All 4 topics (`sensor.readings`, `sensor.rejected`, `inference.results`, `alert.signals`) have `12` partitions ([topics.sh](../../../../MaintAI-gitops/local/platform/topics.sh), [topic-sensor-readings.yaml](../../../../MaintAI-gitops/platform/kafka/topic-sensor-readings.yaml)).
  - At `50 RPS`, aggregate broker CPU is only `106–204m` (`~1.0 GiB` heap/pagecache), negligible compared to Strimzi Prod `3 × (500m..2000m, 2Gi..4Gi)`.
  - `processing-worker`, `business-consumer`, and `storage-consumer` must **never** configure `maxReplicas > 12`.
- **PostgreSQL Connection Budget (`max_connections=300` Local / `~450` `db.t4g.medium` Est.):**
  - Observed peak connections on k3d (`6 pw, 2 relay, 1 read, 4 biz, 1 bapi`): `proc_db = 44`, `biz_db = 30` -> **`74` total active+idle connections** (`~24.7%` of `300`).
  - Theoretical maximum (if every `ConnectionPool` reaches `size` concurrently):
    `6×12 (pw infl<=8) + 2×4 (relay) + 12 (read-api) + 4×8 (biz-cons) + 2×13 (bapi HA)` = **`150` connections** (`+ ~15` for `rag-service`, `drift-worker`, `mlflow` = **`~165 / 300`**, `55.0%` safe ceiling).
  - **100 / 200 RPS Hazard:** If scaling `processing-worker=10..12` with `inflight=16` (`pool=20 -> 240` conns) plus `business-api=4` (`52`) and `business-consumer=8` (`64`), theoretical demand reaches **`~368–400` connections**, **breaking `max_connections=300`** and saturating `db.t4g.medium` (2 vCPU / 4 GiB RAM) `synchronous_commit=on` Multi-AZ IOPS! Production scaling past `100 RPS` requires **RDS Proxy / PgBouncer transaction pooling** (or upgrading `db.t4g.medium -> db.m6i.large/xlarge` and `max_connections >= 500`, plus lowering `processing-worker` pool cap from `12–32` down to `8`).
- **ML Service & Mirror Hazard:**
  - Real `hgb-interactions-candidate-v1` executes 100% on CPU (`0` GPU usage on RTX 2050).
  - `ml-service-mirror` sets `proxy_read_timeout 50ms` ([ml-traffic-mirror.yaml](../../../../MaintAI-gitops/environments/local/apps/ml-traffic-mirror.yaml)); under HGB `c>=4` (`p95 = 54.67 ms`) or `pk=4` overload (`proc_p95 = 73.81 ms`), Nginx drops mirrored shadow traffic silently. Keep `ml-service` concurrency `<= 2–4/pod` or bump mirror timeout to `150ms` for slow models.

---

## 10. Final Recommended Configuration Matrix

| Deployment | Env Profile | Replicas (Min / Target / HA) | CPU Req | CPU Lim | Mem Req | Mem Lim | Concurrency / Key Settings | Evidence Status |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| **`ingestion-api`** | **Prod 50 RPS** (`N+1`) | **`2`** (`HPA 2..4`) | `150m` | `500m` | `192Mi` | `384Mi` | Async FastAPI, `max_inflight=8` (local demo: `2`) | `50`: Bench; `limits`: Adjusted from meas peak (`176m/83Mi`) |
| **`processing-worker`** | **Prod 50 RPS** (`N+2`) | **`4`** (`KEDA 3..8`, cap `12`) | `300m` | `1000m` | `256Mi` | `512Mi` | `hybrid=false, pk=1, infl=4–8, pool=12` (or `hybrid=true, pk=2` if ML p95 >= `45ms`) | **Bench Validated** (`pw=4` `82ms` p95; `pk=4` tested to `pw=6`) |
| **`ml-service` (LogReg)** | **Prod 50 RPS** (`N+1`) | **`2`** (`HPA 2..4`) | `200m` | `1000m` | `256Mi` | `512Mi` | `uvicorn --workers 1`, `OMP=1`, `<= 4` in-flight/pod | **Bench Validated** (`2` pods `p95=49ms`) |
| **`ml-service` (HGB CPU)** | **Prod 50 RPS** (`N+1` HA) | **`3`** (`HPA 3..6`; `2` min bare) | **`500m`** | **`1500m`** | **`256Mi`** | **`512Mi`** | `uvicorn --workers 1`, `OMP=1`, **`1–2` safe (`4` max) in-flight/pod** | **Bench Validated** (`1` pod fails; `3–4` pods `89.9ms` & survives 1 pod kill) |
| **`processing-outbox-relay`** | **Prod 50 RPS** (`N+1`) | **`2`** (fixed `2..3`) | `200m` | `500m` | `192Mi` | `384Mi` | `OUTBOX_BATCH_SIZE=1` (ordered), PG `pool=4` | **Bench Validated** (`outbox_p95=40.2ms`) |
| **`business-consumer`** | **Prod 50 RPS** (`N+2`) | **`3`** (`KEDA 2..6`, cap `12`) | `250m` | `1000m` | `256Mi` | `512Mi` | **`KAFKA_COMMIT_BATCH_SIZE=16, MAX_WAIT_MS=25`**, PG `pool=8` | **Bench Validated** (`biz=2` `75ms`; `1` pod `batch=16` sustains `50`!) |
| **`business-api`** | **Prod 50 RPS** (`N+1`) | **`2`** (`HPA 2..4`) | `200m` | `1000m` | `256Mi` | `512Mi` | `128` SSE conns/pod, PG `pool=12` + `1 LISTEN` | **Calculated Estimate / Not Yet Tested under multi-SSE load** |
| **`processing-read-api`** | **Prod 50 RPS** (`N+1`) | **`2`** (`HPA 2..3`) | `200m` | `1000m` | `256Mi` | `512Mi` | PG `source_pool=4 + meta=8` (`4` w/o history) | Observed poll load; failover **Calculated Estimate** |
| **`storage-consumer`** | **Prod 50 RPS** (`N+1`) | **`2`** (cap `12`) | `300m` | `1000m` | `256Mi` | `512Mi` | `KAFKA_COMMIT_BATCH_SIZE=1` (default), S3/MinIO writes | Single pod observed `659m` pk (`50 RPS`); `N+1=2` **Calc Estimate** |
| **`rag-service` / `frontend` / `simulator-ui`** | **Prod 50 RPS** | `2` / `2` / `1` | `250m`/`50m`/`50m` | `2`/`250m`/`250m` | `512Mi`/`64Mi`/`64Mi` | `2Gi`/`256Mi`/`256Mi` | On-demand alert enrichment / static SPA | **Not Yet Tested under sustained load** |
| **`sensor` / `drift-worker` / `mlflow`** | **Prod 50 RPS** | `1` / `CronJob` / `1` | `100m`/`100m`/`250m` | `500m`/`500m`/`1000m` | `192Mi`/`256Mi`/`512Mi` | `512Mi`/`512Mi`/`1Gi` | `Recreate` PVC (`sensor`), `Forbid` (`drift`), `mlflow_db` RDS | `drift`/`mlflow` **Not Yet Tested** in multi-pod bench |

**Projected Cluster Totals (`Prod 50 RPS HA`, app pods excluding RDS/MSK):** `~28–30` app pods requesting **`~6.65–7.05 vCPU` / `~7.04–7.55 GiB` RAM** (depending on champion LogReg vs CPU HGB cutover, with CPU/mem limits allowing burst headroom; fits comfortably on default `3 × m6i.xlarge` = `12 vCPU / 48 GiB` EKS nodes).
**Local 15 GiB k3d Profile Recommendation:** Keep `ingestion=2, pw=4–6, ml=2 (LogReg) / 2–3 (HGB), relay=2, biz=2–4` with lower `requests.cpu` (`100–250m`) when sharing `15 GiB` with Postgres/Kafka/MinIO/Keycloak/Obs (`~4.5–5.4 GiB` total cluster RSS).

---

## 11. Autoscaling Recommendations (`HPA` & `KEDA` — Report Only, No GitOps Edits)

Do **not** scale event-driven Kafka consumers by CPU alone (`ml-service` bursts or DB waits lower worker CPU even while lag rises). Configure:

1. **`processing-worker` (`KEDA ScaledObject`):**
   - `minReplicaCount: 3` (`Prod 50 RPS` `N+1`; `6` max local / `12` hard partition cap matching `12` Kafka partitions!).
   - Dual triggers: `kafka` (`topic: sensor.readings`, `consumerGroup: processing-v1`, **`lagThreshold: '20'`** (lower from `50` in prod `keda.yaml` so scale-up reacts within ~1s burst!), `allowIdleConsumers: 'false'`) **OR** `cpu` `70%` against `300m` request.
   - `scaleUp`: stabilization `15–30s`, `+2` pods/60s. `scaleDown`: stabilization `300s`, `-1` pod/120s (avoids frequent rebalance pauses!).
2. **`ml-service` (`HPA v2` — Must Scale Before/With Workers):**
   - `minReplicas: 2` (LogReg) / **`3` (HGB CPU)**, `maxReplicas: 6–8`.
   - CPU target `65%` against realistic `requests.cpu` (`200m` LogReg / **`500m` HGB**, NOT `100m`!). Better yet, scale on custom Prometheus RPS/in-flight gauge (`ml_in_flight_requests_per_pod >= 1.5–2`) so GIL throttling never triggers `100%` spike lag.
3. **`business-consumer` (`KEDA ScaledObject`):**
   - `minReplicaCount: 2` (`3` recommended), `maxReplicaCount: 6` (cap `<= 12` partitions).
   - Dual Kafka triggers (`sensor.readings` + `inference.results`, **`lagThreshold: '25'`**) + CPU `70%`. Note Opt 3 `batch=16` means lag stays `< 5` up to `~140 RPS` on 2 pods.
4. **`ingestion-api` & `business-api` (`HPA v2`):**
   - `minReplicas: 2`, `maxReplicas: 6`, CPU `65–70%` (`business-api` should also track active SSE connections `< 100/pod`).

---

## 12. Known Limitations, Untested Assumptions & Answers to the 10 Addendum Questions

### Untested Assumptions Blocking Immediate Production Rollout
1. **`synchronous_commit=off` on local k3d PG:** Real AWS RDS (`db.t4g.medium`, 2 vCPU, 4 GiB RAM) runs `synchronous_commit=on` across Multi-AZ EBS/network links, adding `~1–3 ms` WAL flush latency per commit — unbenchmarked on multi-pod K8s!
2. **Database Connection & IOPS Ceiling at `100 / 200 RPS`:** Above `100 RPS`, RDS `db.t4g.medium` IOPS and `max_connections` require PgBouncer/RDS Proxy + instance sizing validation.
3. **Sustained Duration (`15s` vs multi-hour soak) & Non-Pipeline Workloads:** Memory leak/autovacuum bloat, `drift-worker` heavy table scans, `rag-service` LLM bursts, and `100–200 RPS` k3d runs remain **Not Yet Tested**.
4. **HPA Scale-Up Under Sudden Traffic Spike (`0 -> 200 RPS`):** `ml-service-hpa` cold pod pull + model download/load time (~`10–25 s`) was seen during `ml-service-shadow` scale-ups (`890m` spike in Exp B) and requires pre-warmed `minReplicas`.

### Direct Answers to the 10 Addendum Questions (§10)

| # | Addendum Question | Answer from Measured Evidence |
|---|---|---|
| **1** | **Mỗi Deployment cần bao nhiêu replicas?** | **Prod 50 RPS (`N+1`/`N+2` HA):** `ingestion-api=2`, `processing-worker=4` (min `3`), `ml-service=2` (LogReg) or **`3–4` (HGB CPU)**, `outbox-relay=2`, `business-consumer=3` (min `2`), `business-api=2`, `processing-read-api=2`, `storage-consumer=2`, `sensor=1` (`Recreate`). *(See Section 10 table for Local vs 100/200 RPS).* |
| **2** | **Mỗi Pod cần bao nhiêu CPU và RAM?** | See Section 3 & 10: raise base `requests.memory` from `128Mi -> 256Mi` on workers/ML/consumers/storage (`ml-service` RSS reaches `138 MiB` > `128Mi`!); set `requests.cpu` realistically (`200–500m`, **`500m/1500m`** for CPU HGB) to prevent false HPA thrashing & `100%` throttle spikes. |
| **3** | **Processing Worker cần bao nhiêu tasks/Pod?** | **Baseline (`pk=1`):** `PROCESSING_MAX_ACTIVE_KEYS=8`, `MAX_INFLIGHT_PER_KEY=1`, `MAX_TOTAL_INFLIGHT=4–8/pod`. **Hybrid (conditional):** `per_key=2`, `MAX_TOTAL_INFLIGHT=4–8/pod` (avoid `pk=4, infl=16` on `< 4` HGB pods, which regressed `hot_p95` `57 -> 138ms`). |
| **4** | **ML Service cần bao nhiêu concurrent requests?** | **HGB CPU:** Sweet spot **`1–2` concurrent requests/pod** (`c=2` `2-pod p95 = 12.9ms`); hard cap **`<= 4/pod`** (`p95=54.7ms`); never `>= 8–12` (GIL queue `178ms`). For `50 RPS` cluster (`2–3` HGB pods), keep cluster-wide in-flight `<= 4–8`. |
| **5** | **Business Consumer cần bao nhiêu replicas?** | With **Opt 3 (`KAFKA_COMMIT_BATCH_SIZE=16`)**, even `1` pod sustains `50.13 RPS` (`hot_p95 = 84.08ms`); **`2` pods** achieve `N+1` HA (`75.41ms`), and **`3–4` pods** give `N+2` resilience (`68.93ms`). Never exceed `12` Kafka partitions. |
| **6** | **Tổng tài nguyên toàn cluster là bao nhiêu?** | **Measured `k3d` 50 RPS (`30–33` pods):** Mean CPU **`3.8–5.0 cores`** (Peak **`5.88 cores`**), Peak RAM **`4.4–5.4 GiB`** (`~2.0–2.3 GiB` pipeline + `~2.4–3.1 GiB` PG/Kafka/MinIO/Keycloak/Obs). **Prod 50 RPS HA Estimate:** `~28–30` app pods requesting **`~6.65–7.05 vCPU / ~7.04–7.55 GiB`** RAM (fits `3 × m6i.xlarge` EKS nodes). |
| **7** | **Hệ thống chịu được mất một Pod không?** | **YES** across `processing-worker` (`10.675s`), `business-consumer` (`7.545s`), and `ml-service-shadow` (`10.808s` with `4->3`) at `50 RPS` with **`0` violations** and `1,000/1,000` outcomes — **PROVIDED** `ml-service` HGB has `>= 3` pods (`2 -> 1` drops HGB below `50 RPS` WL=C at `48.0 RPS / 3,108ms` p95!) and `sensor`/`postgres`/`kafka` SPOFs are handled via durable spool/RDS Multi-AZ/Strimzi `rf=3`. |
| **8** | **Cấu hình nào đã được benchmark xác nhận?** | `50 RPS` WL=A & WL=C on `k3d` (**`45` runs** = `12` baseline + `15` hybrid + `15` sweeps + `3` chaos): `ingestion=2`, `processing-worker=1/2/4/6`, `ml-service=2` (LogReg), `ml-service-shadow=1/2/4` (HGB), `relay=1/2`, `business-consumer=1/2/4` (`batch=1 & 16`), `pk=1/2/4`, `pod_kill` chaos (`3` runs), + Opt 1 (`11f9e9f`) & Opt 3 (`8f3a88d`) compose tests (`516` passed, `10` skipped). |
| **9** | **Cấu hình nào mới chỉ là ước lượng?** | `100` & `200 RPS` replica counts, `business-api` / `rag-service` / `frontend` / `drift-worker` / `mlflow` load sizing, `storage-consumer=2` scaling, `processing-read-api` HA failover, AWS RDS `db.t4g.medium` (`synchronous_commit=on`) latency & Multi-AZ IOPS headroom, and multi-hour soak memory stability. |
| **10** | **Có đủ bằng chứng để đưa cấu hình vào Production hay chưa?** | **CHƯA đủ để đẩy thẳng toàn bộ cấu hình Hybrid lên Production, NHƯNG ĐỦ bằng chứng để:** (1) áp dụng điều chỉnh `requests.cpu/memory` (`256Mi`, `HGB 500m`), `Opt 1` + `Opt 3` (`KAFKA_COMMIT_BATCH_SIZE=16` cho `business-consumer`) và replica `N+1` tối thiểu (`HGB >= 3` pods) vào staging; (2) trước Production bắt buộc phải **soak-test trên Staging với `synchronous_commit=on`**, PgBouncer/RDS `db.t4g.medium` tuning, và xác minh Model Identity release pipeline thật (`not local-fixture`). |
