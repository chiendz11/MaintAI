#!/usr/bin/env python3
"""Real multi-pod k3d benchmark, scaling sweeps, and pod-kill chaos suite for MaintAI Hybrid Ordering."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import random
import socket
import statistics
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import httpx
import psycopg
from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[3]
EXAMPLE_REQ = json.loads((ROOT / "contracts/v1.3/examples/sensor-request.json").read_text())

DEFAULT_KUBECONFIG = "/tmp/k3d-bench-kubeconfig.yaml"
DEFAULT_NAMESPACE = "maintai-local"

PIPELINE_GROUPS = [
    "local-processing-v1",
    "local-business-prediction-v1",
    "local-business-live-v1",
    "local-business-alert-v1",
]
TOPICS = ["sensor.readings", "inference.results", "alert.signals"]


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    idx = (pct / 100.0) * (len(ordered) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(ordered) - 1)
    frac = idx - lo
    return round(ordered[lo] * (1.0 - frac) + ordered[hi] * frac, 3)


def summarize(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "mean": None, "p50": None, "p95": None, "p99": None, "max": None}
    return {
        "count": len(values),
        "mean": round(statistics.fmean(values), 3),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": round(max(values), 3),
    }


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def kube_cmd(kubeconfig: str, namespace: str, *args: str, check: bool = True) -> str:
    res = subprocess.run(
        ["kubectl", f"--kubeconfig={kubeconfig}", "-n", namespace, *args],
        capture_output=True,
        text=True,
    )
    if check and res.returncode != 0:
        raise RuntimeError(f"kubectl {' '.join(args)} failed: {res.stderr.strip()}")
    return res.stdout


def workload_rates(workload: str) -> dict[str, float]:
    w = workload.upper()
    if w == "A":
        return {f"M-{i:03d}": 1.0 for i in range(1, 51)}
    if w == "C":
        rates = {"M-001": 16.0}
        for i in range(2, 36):
            rates[f"M-{i:03d}"] = 1.0
        return rates
    raise ValueError(f"Unsupported official workload {workload!r} (must be A or C)")


class PortForwardPool:
    """Manages local kubectl port-forward processes and cleans them up reliably."""

    def __init__(self, kubeconfig: str, namespace: str):
        self.kubeconfig = kubeconfig
        self.namespace = namespace
        self.procs: list[subprocess.Popen] = []

    def open(self, target: str, remote_port: int, timeout: float = 15.0, *, local_port: int | None = None) -> int:
        import tempfile

        local_port = local_port if local_port is not None else free_port()
        log_fh = tempfile.TemporaryFile(mode="w+b")
        proc = subprocess.Popen(
            [
                "kubectl",
                f"--kubeconfig={self.kubeconfig}",
                "-n",
                self.namespace,
                "port-forward",
                target,
                f"{local_port}:{remote_port}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=log_fh,
        )
        self.procs.append((proc, log_fh))
        dl = time.monotonic() + timeout
        while time.monotonic() < dl:
            if proc.poll() is not None:
                log_fh.seek(0)
                err = log_fh.read().decode(errors="replace")
                raise RuntimeError(f"port-forward {target} exited early: {err}")
            try:
                with socket.create_connection(("127.0.0.1", local_port), timeout=0.3):
                    return local_port
            except OSError:
                time.sleep(0.1)
        raise RuntimeError(f"Timed out waiting for port-forward {target} -> {local_port}")

    def close_all(self) -> None:
        for p, fh in self.procs:
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()
            try:
                fh.close()
            except Exception:
                pass
        self.procs.clear()


def rewrite_local_pg_dsn(cluster_dsn: str, local_pg_port: int) -> str:
    # e.g. postgresql://user:pass@postgres:5432/processing_db -> 127.0.0.1:<local_pg_port>
    parsed = urllib.parse.urlsplit(cluster_dsn)
    userinfo = ""
    if parsed.username:
        userinfo = urllib.parse.quote(parsed.username, safe="")
        if parsed.password is not None:
            userinfo += f":{urllib.parse.quote(parsed.password, safe='')}"
        userinfo += "@"
    netloc = f"{userinfo}127.0.0.1:{local_pg_port}"
    return urllib.parse.urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


def read_k3d_secrets(kubeconfig: str, namespace: str) -> dict[str, dict[str, str]]:
    out = {}
    for sname in ("local-processing-owner", "local-business-owner", "local-oidc-sensor"):
        raw = json.loads(kube_cmd(kubeconfig, namespace, "get", "secret", sname, "-o", "json"))
        out[sname] = {k: base64.b64decode(v).decode() for k, v in raw["data"].items()}
    return out


def fetch_oidc_token(keycloak_port: int, client_id: str, client_secret: str) -> str:
    url = f"http://127.0.0.1:{keycloak_port}/realms/maintai/protocol/openid-connect/token"
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "sensor:write",
        }
    ).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Host": "auth.maintai.localhost", "Content-Type": "application/x-www-form-urlencoded"},
    )
    resp = json.loads(urllib.request.urlopen(req, timeout=10).read().decode())
    return resp["access_token"]


def sample_kubectl_top_pods(kubeconfig: str, namespace: str) -> dict[str, dict]:
    res = subprocess.run(
        ["kubectl", f"--kubeconfig={kubeconfig}", "-n", namespace, "top", "pods", "--no-headers"],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        return {}
    by_app: dict[str, dict] = defaultdict(lambda: {"pods": 0, "cpu_m": 0, "mem_mi": 0, "pod_details": []})
    for line in res.stdout.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        pod_name, cpu_s, mem_s = parts[0], parts[1], parts[2]
        cpu_m = int(cpu_s[:-1]) if cpu_s.endswith("m") else int(float(cpu_s) * 1000)
        mem_mi = int(mem_s[:-2]) if mem_s.endswith("Mi") else (int(mem_s[:-2]) * 1024 if mem_s.endswith("Gi") else int(mem_s[:-2]) // 1024 if mem_s.endswith("Ki") else 0)
        # Group deployment prefix
        if pod_name.startswith(("postgres-", "kafka-", "minio-", "keycloak-", "prometheus-", "grafana-")):
            app = pod_name.rsplit("-", 1)[0]
        else:
            segs = pod_name.split("-")
            app = "-".join(segs[:-2]) if len(segs) >= 3 else pod_name
        d = by_app[app]
        d["pods"] += 1
        d["cpu_m"] += cpu_m
        d["mem_mi"] += mem_mi
        d["pod_details"].append({"pod": pod_name, "cpu_m": cpu_m, "mem_mi": mem_mi})
    return dict(by_app)


def snapshot_pod_health(kubeconfig: str, namespace: str) -> dict[str, dict]:
    raw = json.loads(kube_cmd(kubeconfig, namespace, "get", "pods", "-o", "json"))
    info = {}
    for item in raw.get("items", []):
        name = item["metadata"]["name"]
        statuses = item.get("status", {}).get("containerStatuses", [])
        restarts = sum(int(cs.get("restartCount", 0)) for cs in statuses)
        oom = any(
            (cs.get("lastState", {}).get("terminated", {}) or {}).get("reason") == "OOMKilled"
            for cs in statuses
        )
        ready = bool(statuses) and all(bool(cs.get("ready")) for cs in statuses)
        info[name] = {"restarts": restarts, "oom": oom, "ready": ready}
    return info


def apply_cluster_profile(
    kubeconfig: str,
    namespace: str,
    *,
    hybrid_enabled: bool,
    per_key: int,
    max_total_inflight: int,
    ml_base_url: str,
    biz_commit_batch_size: int = 1,
    biz_commit_max_wait_ms: int = 25,
    worker_replicas: int = 6,
    relay_replicas: int = 2,
    biz_replicas: int = 4,
    ingestion_replicas: int = 2,
    ml_champion_replicas: int = 2,
    ml_shadow_replicas: int = 1,
) -> None:
    """Updates runtime env vars & replica counts for a controlled benchmark step and waits Ready."""
    # Update KEDA & HPA limits cleanly first so autoscalers never fight target counts
    kube_cmd(
        kubeconfig,
        namespace,
        "annotate",
        "scaledobject",
        "processing-worker-scaledobject",
        f"autoscaling.keda.sh/paused-replicas={worker_replicas}",
        "--overwrite",
    )
    kube_cmd(
        kubeconfig,
        namespace,
        "annotate",
        "scaledobject",
        "business-consumer-scaledobject",
        f"autoscaling.keda.sh/paused-replicas={biz_replicas}",
        "--overwrite",
    )
    # Patch HPA min/max so HPA keeps exact count
    for hpa, cnt in [
        ("ingestion-api-hpa", ingestion_replicas),
        ("ml-service-hpa", ml_champion_replicas),
        ("ml-service-shadow-hpa", ml_shadow_replicas),
    ]:
        kube_cmd(
            kubeconfig,
            namespace,
            "patch",
            "hpa",
            hpa,
            "--type=merge",
            "-p",
            json.dumps({"spec": {"minReplicas": cnt, "maxReplicas": max(cnt, 4 if "shadow" in hpa else cnt)}}),
        )

    fixture_path = (
        "/bench-fixtures/model-identity.json"
        if "ml-service-shadow" in ml_base_url
        else "/local/fixtures/model-identity.json"
    )
    kube_cmd(
        kubeconfig,
        namespace,
        "set",
        "env",
        "deploy/processing-worker",
        "-c",
        "processing-worker",
        f"PROCESSING_HYBRID_ORDERING_ENABLED={'true' if hybrid_enabled else 'false'}",
        f"PROCESSING_MAX_INFLIGHT_PER_KEY={per_key}",
        f"PROCESSING_MAX_TOTAL_INFLIGHT={max_total_inflight}",
        "PROCESSING_MAX_ACTIVE_KEYS=8",
        f"ML_BASE_URL={ml_base_url}",
        f"PROCESSING_MODEL_IDENTITY_FIXTURE={fixture_path}",
    )
    kube_cmd(
        kubeconfig,
        namespace,
        "set",
        "env",
        "deploy/business-consumer",
        "-c",
        "business-consumer",
        f"KAFKA_COMMIT_BATCH_SIZE={biz_commit_batch_size}",
        f"KAFKA_COMMIT_MAX_WAIT_MS={biz_commit_max_wait_ms}",
    )

    for dep, reps in [
        ("ml-service", ml_champion_replicas),
        ("ml-service-shadow", ml_shadow_replicas),
        ("ingestion-api", ingestion_replicas),
        ("processing-worker", worker_replicas),
        ("processing-outbox-relay", relay_replicas),
        ("business-consumer", biz_replicas),
    ]:
        kube_cmd(kubeconfig, namespace, "scale", f"deploy/{dep}", f"--replicas={reps}")

    for dep in (
        "ml-service",
        "ml-service-shadow",
        "ingestion-api",
        "processing-worker",
        "processing-outbox-relay",
        "business-consumer",
    ):
        kube_cmd(kubeconfig, namespace, "rollout", "status", f"deploy/{dep}", "--timeout=180s")
    # Brief settle for Kafka consumer group rebalance completion
    time.sleep(3.0)


def sample_kafka_lag(admin: AdminClient) -> dict:
    tps_by_topic = {t: [TopicPartition(t, p) for p in range(12)] for t in TOPICS}
    hi_futs = admin.list_offsets({tp: OffsetSpec.latest() for tps in tps_by_topic.values() for tp in tps})
    cg_reqs = [
        ConsumerGroupTopicPartitions(
            g,
            tps_by_topic[
                "sensor.readings"
                if g in ("local-processing-v1", "local-business-live-v1")
                else ("inference.results" if g == "local-business-prediction-v1" else "alert.signals")
            ],
        )
        for g in PIPELINE_GROUPS
    ]
    cg_res = admin.list_consumer_group_offsets(cg_reqs)
    out = {}
    for g in PIPELINE_GROUPS:
        topic = (
            "sensor.readings"
            if g in ("local-processing-v1", "local-business-live-v1")
            else ("inference.results" if g == "local-business-prediction-v1" else "alert.signals")
        )
        per_part = {}
        try:
            parts = cg_res[g].result(timeout=3).topic_partitions
            for pr in parts:
                hi = hi_futs[TopicPartition(topic, pr.partition)].result(timeout=3).offset
                off = pr.offset if pr.offset is not None and pr.offset >= 0 else hi
                per_part[pr.partition] = max(0, hi - off)
        except Exception:
            pass
        out[g] = {
            "total_lag": sum(per_part.values()),
            "max_partition_lag": max(per_part.values(), default=0),
        }
    return out


async def run_http_load(
    *,
    urls: list[str],
    token: str,
    rates: dict[str, float],
    seq_start: dict[str, int],
    run_id: str,
    epoch: str,
    warmup_seconds: int,
    measure_seconds: int,
    chaos_cb=None,
) -> dict:
    base_features = dict(EXAMPLE_REQ["features"])
    seq_counter = dict(seq_start)
    sent_records: dict[str, dict] = {}
    status_counts: Counter = Counter()
    errors_detail: list[str] = []
    http_lat_window_ms: list[float] = []
    rr_idx = 0
    sem = asyncio.Semaphore(128)
    tasks: list[asyncio.Task] = []

    t_start = time.monotonic()
    t_warmup_end = t_start + warmup_seconds
    t_measure_end = t_warmup_end + measure_seconds
    t_win_utc0 = None
    t_win_utc1 = None
    chaos_triggered = False

    limits = httpx.Limits(max_connections=64, max_keepalive_connections=32)
    async with httpx.AsyncClient(limits=limits, verify=False, timeout=10.0) as client:

        async def send_one(url: str, payload: dict, in_window: bool, machine: str) -> None:
            async with sem:
                send_utc = datetime.now(timezone.utc)
                t0 = time.perf_counter()
                try:
                    resp = await client.post(
                        url,
                        json=payload,
                        headers={"Authorization": f"Bearer {token}", "Host": "ingest.maintai.localhost"},
                    )
                    dt_ms = (time.perf_counter() - t0) * 1000.0
                    status_counts[resp.status_code] += 1
                    if resp.status_code not in (200, 202):
                        if len(errors_detail) < 10:
                            errors_detail.append(f"HTTP {resp.status_code}: {resp.text[:200]}")
                    else:
                        sent_records[payload["event_id"]] = {
                            "machine_id": machine,
                            "seq": payload["sequence_number"],
                            "in_window": in_window,
                            "send_utc": send_utc,
                            "http_ms": dt_ms,
                        }
                        if in_window:
                            http_lat_window_ms.append(dt_ms)
                except Exception as exc:
                    status_counts["exception"] += 1
                    if len(errors_detail) < 10:
                        errors_detail.append(f"EXC: {exc!r}")

        # One FIFO worker coroutine per machine guarantees seq N receives Kafka 202 ACK
        # before seq N+1 is posted across multi-replica ingestion-api, while all machines run concurrently.
        machine_queues: dict[str, asyncio.Queue] = {m: asyncio.Queue() for m in rates}

        async def machine_sender(q: asyncio.Queue) -> None:
            while True:
                item = await q.get()
                if item is None:
                    q.task_done()
                    break
                url, payload, in_window, machine = item
                await send_one(url, payload, in_window, machine)
                q.task_done()

        sender_tasks = [asyncio.create_task(machine_sender(q)) for q in machine_queues.values()]

        n_machines = max(1, len(rates))
        next_due = {
            m: t_start + (idx / n_machines) * (1.0 / rates[m])
            for idx, m in enumerate(sorted(rates))
        }
        while True:
            now = time.monotonic()
            if t_win_utc0 is None and now >= t_warmup_end:
                t_win_utc0 = datetime.now(timezone.utc)
            if chaos_cb is not None and not chaos_triggered and now >= t_warmup_end + (measure_seconds * 0.5):
                chaos_triggered = True
                threading.Thread(target=chaos_cb, daemon=True).start()
            if now >= t_measure_end:
                t_win_utc1 = datetime.now(timezone.utc)
                break
            due = [m for m, ts in next_due.items() if ts <= now]
            if not due:
                soonest = min(next_due.values())
                await asyncio.sleep(min(0.005, max(0.0004, soonest - now)))
                continue
            for m in sorted(due):
                seq_counter[m] += 1
                seq = seq_counter[m]
                event_id = f"k3db-{run_id}-{m}-{seq}"
                features = dict(base_features)
                # Inject ~10% high-torque readings matching controlled_benchmark.py so real HIGH/CRITICAL alerts fire
                if (seq + hash(m)) % 10 == 0:
                    features["torque_nm"] = 62.5
                iso_now = datetime.now(timezone.utc).isoformat()
                payload = {
                    **EXAMPLE_REQ,
                    "event_id": event_id,
                    "machine_id": m,
                    "stream_epoch": epoch,
                    "sequence_number": seq,
                    "received_at": iso_now,
                    "correlation_id": f"corr-{event_id}",
                    "features": features,
                }
                url = urls[rr_idx % len(urls)]
                rr_idx += 1
                in_window = t_warmup_end <= now <= t_measure_end
                machine_queues[m].put_nowait((url, payload, in_window, m))
                next_due[m] += 1.0 / rates[m]
                if next_due[m] < now - 0.5:
                    next_due[m] = now + (1.0 / rates[m])

        for q in machine_queues.values():
            q.put_nowait(None)
        await asyncio.gather(*sender_tasks)

    return {
        "sent_records": sent_records,
        "status_counts": {str(k): v for k, v in status_counts.items()},
        "errors_detail": errors_detail,
        "http_lat_window_ms": http_lat_window_ms,
        "window_seconds": measure_seconds,
        "t_win_utc0": t_win_utc0,
        "t_win_utc1": t_win_utc1,
    }


def query_pg_stats(conn) -> dict:
    db_row = conn.execute(
        "SELECT xact_commit, xact_rollback, blks_read, blks_hit, tup_inserted, tup_updated, tup_fetched "
        "FROM pg_stat_database WHERE datname = current_database()"
    ).fetchone()
    act_rows = conn.execute(
        "SELECT usename, state, count(*) AS n FROM pg_stat_activity WHERE datname = current_database() GROUP BY usename, state"
    ).fetchall()
    return {
        "db": dict(db_row) if db_row else {},
        "activity": [dict(r) for r in act_rows],
    }


def run_single_k3d_trial(
    *,
    kubeconfig: str,
    namespace: str,
    proc_dsn: str,
    biz_dsn: str,
    client_id: str,
    client_secret: str,
    label: str,
    mode: str,
    ml_target: str,
    workload: str,
    run_idx: int,
    config_snapshot: dict,
    warmup_seconds: int = 5,
    measure_seconds: int = 15,
    drain_timeout: int = 90,
    chaos_kind: str | None = None,
) -> dict:
    run_id = uuid4().hex[:8]
    epoch = f"k3d-bench-{run_id}"
    rates = workload_rates(workload)
    pf = PortForwardPool(kubeconfig, namespace)

    try:
        # Verify ingestion-api pods are Running and route through real k3d TLS Ingress -> Service LB
        ing_pods = kube_cmd(
            kubeconfig,
            namespace,
            "get",
            "pods",
            "-l",
            "app.kubernetes.io/name=ingestion-api",
            "--field-selector=status.phase=Running",
            "-o",
            "jsonpath={.items[*].metadata.name}",
        ).split()
        ing_pods = [p for p in ing_pods if p]
        if not ing_pods:
            raise RuntimeError("No running ingestion-api pods found")
        ing_urls = ["https://127.0.0.1:443/api/v1/sensor-readings"]

        kc_port = pf.open("svc/keycloak", 8080)
        pg_port = pf.open("svc/postgres", 5432)
        kafka_port = pf.open("pod/kafka-0", 19092, local_port=19092)

        local_proc_dsn = rewrite_local_pg_dsn(proc_dsn, pg_port)
        local_biz_dsn = rewrite_local_pg_dsn(biz_dsn, pg_port)
        token = fetch_oidc_token(kc_port, client_id, client_secret)
        admin = AdminClient({"bootstrap.servers": f"127.0.0.1:{kafka_port}"})

        # 2. Compute safe per-machine starting sequence strictly above all existing cursors/events
        machines_sorted = sorted(rates)
        with psycopg.connect(local_proc_dsn, row_factory=dict_row) as pc, psycopg.connect(local_biz_dsn, row_factory=dict_row) as bc:
            seq_start = {m: 0 for m in machines_sorted}
            for sql_txt, conn in [
                ("SELECT machine_id, MAX(sequence_number) AS s FROM sensor_events WHERE machine_id = ANY(%s) GROUP BY machine_id", pc),
                ("SELECT machine_id, MAX(last_sequence) AS s FROM maintai_processing_machine_cursor WHERE machine_id = ANY(%s) GROUP BY machine_id", pc),
                ("SELECT machine_id, MAX(last_sequence) AS s FROM processing_source_cursor WHERE machine_id = ANY(%s) GROUP BY machine_id", pc),
                ("SELECT machine_id, MAX(last_admitted_sequence) AS s FROM processing_admission_cursor WHERE machine_id = ANY(%s) GROUP BY machine_id", pc),
                ("SELECT machine_id, MAX(sequence_number) AS s FROM business_source_inbox WHERE machine_id = ANY(%s) GROUP BY machine_id", bc),
            ]:
                for r in conn.execute(sql_txt, (machines_sorted,)).fetchall():
                    if r["s"] is not None:
                        seq_start[r["machine_id"]] = max(seq_start[r["machine_id"]], int(r["s"]))

            proc_stats_before = query_pg_stats(pc)
            biz_stats_before = query_pg_stats(bc)
            proc_conflicts_before = pc.execute("SELECT count(*) AS n FROM processing_conflicts").fetchone()["n"]
            biz_conflicts_before = bc.execute("SELECT count(*) AS n FROM business_projection_conflicts").fetchone()["n"]
            biz_alert_inbox_before = bc.execute("SELECT count(*) AS n FROM maintai_kafka_inbox WHERE topic='alert.signals'").fetchone()["n"]
            proc_alert_outbox_before = pc.execute("SELECT count(*) AS n FROM maintai_kafka_outbox WHERE topic='alert.signals'").fetchone()["n"]
            biz_pred_inbox_before = bc.execute("SELECT count(*) AS n FROM maintai_kafka_inbox WHERE topic='inference.results'").fetchone()["n"]

        pod_health_before = snapshot_pod_health(kubeconfig, namespace)

        # 3. Background monitor thread for Kafka lag, kubectl top pods, and PG active conns
        stop_mon = threading.Event()
        lag_samples: list[dict] = []
        resource_samples: list[dict] = []
        pg_conn_samples: list[dict] = []
        chaos_events: list[dict] = []

        def monitor_loop():
            pc_m = bc_m = None
            try:
                while not stop_mon.is_set():
                    t0 = time.monotonic()
                    try:
                        lag = sample_kafka_lag(admin)
                        lag_samples.append({"t_epoch": round(time.time(), 2), "groups": lag})
                    except Exception:
                        pass
                    top = sample_kubectl_top_pods(kubeconfig, namespace)
                    if top:
                        resource_samples.append({"t_epoch": round(time.time(), 2), "apps": top})
                    try:
                        if pc_m is None or pc_m.closed:
                            pc_m = psycopg.connect(local_proc_dsn, row_factory=dict_row, autocommit=True, connect_timeout=5)
                        if bc_m is None or bc_m.closed:
                            bc_m = psycopg.connect(local_biz_dsn, row_factory=dict_row, autocommit=True, connect_timeout=5)
                        p_tot = pc_m.execute("SELECT count(*) AS total, count(*) FILTER (WHERE state='active') AS active FROM pg_stat_activity WHERE datname='processing_db'").fetchone()
                        b_tot = bc_m.execute("SELECT count(*) AS total, count(*) FILTER (WHERE state='active') AS active FROM pg_stat_activity WHERE datname='business_db'").fetchone()
                        pg_conn_samples.append({"proc": dict(p_tot), "biz": dict(b_tot)})
                    except Exception:
                        for c in (pc_m, bc_m):
                            try:
                                if c is not None:
                                    c.close()
                            except Exception:
                                pass
                        pc_m = bc_m = None
                    dt = time.monotonic() - t0
                    stop_mon.wait(max(0.2, 1.0 - dt))
            finally:
                for c in (pc_m, bc_m):
                    try:
                        if c is not None:
                            c.close()
                    except Exception:
                        pass

        mon_thread = threading.Thread(target=monitor_loop, daemon=True)
        mon_thread.start()

        # Optional pod-kill trigger at mid-window
        def trigger_chaos():
            if not chaos_kind:
                return
            sel_map = {
                "processing-worker": "app.kubernetes.io/name=processing-worker",
                "ml-service-shadow": "app.kubernetes.io/name=ml-service-shadow",
                "business-consumer": "app.kubernetes.io/name=business-consumer",
            }
            sel = sel_map[chaos_kind]
            pods = kube_cmd(
                kubeconfig,
                namespace,
                "get",
                "pods",
                "-l",
                sel,
                "--field-selector=status.phase=Running",
                "-o",
                "jsonpath={.items[*].metadata.name}",
            ).split()
            if pods:
                victim = pods[0]
                ts_kill = time.monotonic()
                kube_cmd(kubeconfig, namespace, "delete", "pod", victim, "--wait=false")
                chaos_events.append(
                    {
                        "kind": chaos_kind,
                        "victim": victim,
                        "triggered_at_epoch": round(time.time(), 3),
                        "triggered_mono": ts_kill,
                    }
                )

        # 4. Run real 50 RPS HTTP workload
        load_res = asyncio.run(
            run_http_load(
                urls=ing_urls,
                token=token,
                rates=rates,
                seq_start=seq_start,
                run_id=run_id,
                epoch=epoch,
                warmup_seconds=warmup_seconds,
                measure_seconds=measure_seconds,
                chaos_cb=trigger_chaos if chaos_kind else None,
            )
        )
        t_http_done = time.monotonic()
        sent_records = load_res["sent_records"]
        total_sent = len(sent_records)
        sent_eids = list(sent_records.keys())
        window_eids = {eid for eid, r in sent_records.items() if r["in_window"]}

        # 5. Wait for full asynchronous Kafka -> Processing -> Outbox -> Business drain
        dl = time.monotonic() + drain_timeout
        drained = False
        last_counts = {}
        with psycopg.connect(local_proc_dsn, row_factory=dict_row, autocommit=True, connect_timeout=10) as pc, psycopg.connect(local_biz_dsn, row_factory=dict_row, autocommit=True, connect_timeout=10) as bc:
            while time.monotonic() < dl:
                pr_done = pc.execute(
                    "SELECT count(*) AS n FROM processing_results pr JOIN sensor_events se ON se.event_id = pr.event_id WHERE se.stream_epoch=%s",
                    (epoch,),
                ).fetchone()["n"]
                ob_pend = pc.execute("SELECT count(*) AS n FROM maintai_kafka_outbox WHERE status != 'PUBLISHED'").fetchone()["n"]
                ledger_pend = pc.execute(
                    """
                    SELECT count(*) AS n FROM processing_alert_ledger pal
                    WHERE pal.stream_epoch=%s AND (
                        pal.disposition = 'PENDING'
                        OR (
                            pal.disposition = 'SIGNAL_READY'
                            AND NOT EXISTS (
                                SELECT 1 FROM processing_alert_ledger b
                                WHERE b.consumer_group = pal.consumer_group
                                  AND b.machine_id = pal.machine_id
                                  AND b.disposition = 'BLOCKED'
                                  AND b.admission_seq < pal.admission_seq
                            )
                        )
                    )
                    """,
                    (epoch,),
                ).fetchone()["n"]
                biz_pred_delta = bc.execute("SELECT count(*) AS n FROM maintai_kafka_inbox WHERE topic='inference.results'").fetchone()["n"] - biz_pred_inbox_before
                biz_alert_delta = bc.execute("SELECT count(*) AS n FROM maintai_kafka_inbox WHERE topic='alert.signals'").fetchone()["n"] - biz_alert_inbox_before
                proc_alert_delta = pc.execute("SELECT count(*) AS n FROM maintai_kafka_outbox WHERE topic='alert.signals'").fetchone()["n"] - proc_alert_outbox_before
                last_counts = {
                    "pr_done": pr_done,
                    "ob_pend": ob_pend,
                    "ledger_pend": ledger_pend,
                    "biz_pred_delta": biz_pred_delta,
                    "biz_alert_delta": biz_alert_delta,
                    "proc_alert_delta": proc_alert_delta,
                }
                if pr_done >= total_sent and ob_pend == 0 and ledger_pend == 0 and biz_pred_delta >= total_sent and biz_alert_delta >= proc_alert_delta:
                    drained = True
                    break
                time.sleep(0.25)

        drain_seconds = round(time.monotonic() - t_http_done, 3)

        # If chaos test, wait for Deployment replacement pod Ready & record recovery time
        recovery_seconds = None
        if chaos_events:
            dep_name = chaos_kind
            kube_cmd(kubeconfig, namespace, "rollout", "status", f"deploy/{dep_name}", "--timeout=120s")
            recovery_seconds = round(time.monotonic() - chaos_events[0]["triggered_mono"], 3)

        stop_mon.set()
        mon_thread.join(timeout=5)
        pod_health_after = snapshot_pod_health(kubeconfig, namespace)

        # 6. Query exact DB latencies, throughputs, and invariants
        with psycopg.connect(local_proc_dsn, row_factory=dict_row) as pc, psycopg.connect(local_biz_dsn, row_factory=dict_row) as bc:
            proc_stats_after = query_pg_stats(pc)
            biz_stats_after = query_pg_stats(bc)
            proc_conflicts_delta = pc.execute("SELECT count(*) AS n FROM processing_conflicts").fetchone()["n"] - proc_conflicts_before
            biz_conflicts_delta = bc.execute("SELECT count(*) AS n FROM business_projection_conflicts").fetchone()["n"] - biz_conflicts_before

            rows_proc = pc.execute(
                """
                SELECT se.event_id, se.machine_id, se.sequence_number,
                       se.source_server_accepted_at, se.created_at AS admit_at,
                       pr.result_id, pr.kind, pr.completed_at,
                       ob.created_at AS ob_created_at, ob.published_at AS ob_pub_at,
                       pal.disposition, pal.outcome_kind
                FROM sensor_events se
                JOIN processing_results pr ON pr.event_id = se.event_id
                LEFT JOIN maintai_kafka_outbox ob ON ob.topic='inference.results' AND ob.message_key=se.machine_id
                  AND ob.source_sequence_number = se.sequence_number
                LEFT JOIN processing_alert_ledger pal ON pal.stream_epoch = se.stream_epoch AND pal.machine_id = se.machine_id AND pal.sequence_number = se.sequence_number
                WHERE se.stream_epoch = %s
                """,
                (epoch,),
            ).fetchall()

            rows_biz = bc.execute(
                """
                SELECT bsi.source_event_id AS event_id, bsi.machine_id, bsi.sequence_number,
                       ki.processed_at AS biz_pred_at
                FROM business_source_inbox bsi
                LEFT JOIN maintai_kafka_inbox ki
                  ON ki.topic='inference.results' AND ki.consumer_group='local-business-prediction-v1'
                 AND ki.message_id IN (
                     SELECT message_id FROM maintai_kafka_inbox WHERE topic='inference.results' AND consumer_group='local-business-prediction-v1'
                 )
                WHERE bsi.stream_epoch = %s
                """,
                (epoch,),
            ).fetchall() if False else []
            # Clean exact lookup for biz inbox processed_at via predictions result_id (message_id of inference.results is outcome message_id or result_id? Query via outcome/source_event_id!)
            lvp_rows = bc.execute(
                "SELECT machine_id, sequence_number, stream_epoch, source_event_id FROM latest_valid_prediction WHERE machine_id = ANY(%s)",
                (machines_sorted,),
            ).fetchall()
            exp_rows = pc.execute(
                """
                SELECT se.machine_id, MAX(se.sequence_number) AS max_seq
                FROM sensor_events se
                JOIN processing_results pr ON pr.event_id = se.event_id
                WHERE se.stream_epoch = %s AND pr.kind = 'PREDICTION_COMPLETED'
                GROUP BY se.machine_id
                """,
                (epoch,),
            ).fetchall()
            dup_active_alerts = bc.execute(
                """
                SELECT a.machine_id, a.severity, count(*) AS n
                FROM alerts a
                WHERE a.status IN ('OPEN','ACKNOWLEDGED','IN_PROGRESS','ESCALATED')
                  AND EXISTS (
                      SELECT 1 FROM alerts b
                      WHERE b.machine_id = a.machine_id
                        AND b.severity = a.severity
                        AND b.status IN ('OPEN','ACKNOWLEDGED','IN_PROGRESS','ESCALATED')
                        AND b.alert_id <> a.alert_id
                        AND ABS(EXTRACT(EPOCH FROM (a.created_at - b.created_at))) < 300
                        AND (b.latest_source_stream_epoch = %s OR a.latest_source_stream_epoch = %s)
                  )
                GROUP BY a.machine_id, a.severity
                """,
                (epoch, epoch),
            ).fetchall()
            alerts_in_epoch = bc.execute(
                "SELECT count(*) AS n FROM alerts WHERE latest_source_stream_epoch = %s", (epoch,)
            ).fetchone()["n"]
            alert_links_in_epoch = bc.execute(
                """
                SELECT disposition, count(*) AS n
                FROM alert_signal_links
                WHERE source_event_id = ANY(%s)
                GROUP BY disposition
                """,
                (sent_eids,),
            ).fetchall()
            # Measure latest_valid_prediction timestamps or projection_journal / alerts / updated_at;
            # Actually business_source_inbox doesn't have timestamp; query projection_journal OR fallback to processing outbox->latest_telemetry!
            # Wait: maintai_kafka_inbox message_id on inference.results == result_id? Check!
            msg_ids_check = pc.execute(
                """
                SELECT pr.event_id, ob.message_id
                FROM processing_results pr
                JOIN sensor_events se ON se.event_id = pr.event_id
                JOIN maintai_kafka_outbox ob ON ob.topic='inference.results' AND ob.message_key=se.machine_id AND ob.source_sequence_number=se.sequence_number
                WHERE se.stream_epoch = %s
                """,
                (epoch,),
            ).fetchall()
            ob_msg_to_eid = {r["message_id"]: r["event_id"] for r in msg_ids_check}
            biz_pred_ts_by_eid = {}
            if ob_msg_to_eid:
                for br in bc.execute(
                    "SELECT message_id, processed_at FROM maintai_kafka_inbox WHERE topic='inference.results' AND message_id = ANY(%s)",
                    (list(ob_msg_to_eid.keys()),),
                ).fetchall():
                    biz_pred_ts_by_eid[ob_msg_to_eid[br["message_id"]]] = br["processed_at"]

        # Verify correctness invariants
        lvp_map = {r["machine_id"]: int(r["sequence_number"]) for r in lvp_rows}
        exp_map = {r["machine_id"]: int(r["max_seq"]) for r in exp_rows}
        wrong_latest_seq = sum(1 for m, exp in exp_map.items() if lvp_map.get(m, -1) < exp)
        # Note: since we incremented seq monotonically, if lvp_map[m] == exp (for our latest epoch!) it matches;
        # specifically for machines that received >=1 PREDICTION_COMPLETED in THIS epoch, lvp_map[m] MUST equal exp!
        exact_latest_mismatch = sum(1 for m, exp in exp_map.items() if lvp_map.get(m) != exp)

        kind_counts = Counter(r["kind"] for r in rows_proc)
        disp_counts = Counter(r["disposition"] for r in rows_proc if r["disposition"])
        # Check no INFERENCE_UNAVAILABLE was silently turned into NO_SIGNAL or EMITTED
        bad_unavail = sum(
            1
            for r in rows_proc
            if r["kind"] == "INFERENCE_UNAVAILABLE" and r["disposition"] in ("NO_SIGNAL", "EMITTED", "RESOLVED_NO_SIGNAL")
        )

        # Compute latencies & windowed throughput rates
        win_proc_lat_ms, win_outbox_ms, win_k2b_ms = [], [], []
        hot_e2e_ms, bg_e2e_ms = [], []
        hot_proc_ms, bg_proc_ms = [], []
        completed_win_count = 0
        biz_pred_win_count = 0
        # Derive exact measurement wall UTC bounds from window events sent timestamps
        win_send_times = [sent_records[e]["send_utc"] for e in window_eids]
        t_win_utc0 = load_res.get("t_win_utc0") or (min(win_send_times) if win_send_times else None)
        t_win_utc1 = load_res.get("t_win_utc1") or (max(win_send_times) if win_send_times else None)

        for r in rows_proc:
            eid = r["event_id"]
            acc = r["source_server_accepted_at"]
            comp = r["completed_at"]
            pub = r["ob_pub_at"]
            bpred = biz_pred_ts_by_eid.get(eid)
            if t_win_utc0 and t_win_utc1 and comp and t_win_utc0 <= comp <= t_win_utc1:
                completed_win_count += 1
            if t_win_utc0 and t_win_utc1 and bpred and t_win_utc0 <= bpred <= t_win_utc1:
                biz_pred_win_count += 1
            if eid in window_eids:
                p_ms = (comp - acc).total_seconds() * 1000.0 if (acc and comp) else None
                if p_ms is not None:
                    win_proc_lat_ms.append(p_ms)
                    if r["machine_id"] == "M-001":
                        hot_proc_ms.append(p_ms)
                    else:
                        bg_proc_ms.append(p_ms)
                if pub and comp:
                    win_outbox_ms.append(max(0.0, (pub - comp).total_seconds() * 1000.0))
                if bpred and pub:
                    win_k2b_ms.append(max(0.0, (bpred - pub).total_seconds() * 1000.0))
                if bpred and acc:
                    e2e_ms = max(0.0, (bpred - acc).total_seconds() * 1000.0)
                    if r["machine_id"] == "M-001":
                        hot_e2e_ms.append(e2e_ms)
                    else:
                        bg_e2e_ms.append(e2e_ms)

        # Pod restarts/OOM deltas & peak resource usage
        restart_deltas = {}
        oom_deltas = 0
        for p, st in pod_health_after.items():
            prev = pod_health_before.get(p, {"restarts": 0, "oom": False})
            d_r = st["restarts"] - prev["restarts"]
            if d_r > 0 and (chaos_events is None or not any(p.startswith(c["kind"]) for c in chaos_events)):
                restart_deltas[p] = d_r
            if st["oom"] and not prev["oom"]:
                oom_deltas += 1

        # Peak & mean CPU/RAM by deployment from resource_samples
        res_summary = {}
        apps_seen = set()
        for rs in resource_samples:
            apps_seen.update(rs["apps"].keys())
        for app in sorted(apps_seen):
            cpus = [rs["apps"][app]["cpu_m"] for rs in resource_samples if app in rs["apps"]]
            mems = [rs["apps"][app]["mem_mi"] for rs in resource_samples if app in rs["apps"]]
            pods_n = max((rs["apps"][app]["pods"] for rs in resource_samples if app in rs["apps"]), default=0)
            res_summary[app] = {
                "pods": pods_n,
                "cpu_m_mean": round(statistics.fmean(cpus), 1) if cpus else None,
                "cpu_m_peak": max(cpus) if cpus else None,
                "mem_mi_mean": round(statistics.fmean(mems), 1) if mems else None,
                "mem_mi_peak": max(mems) if mems else None,
            }

        def stat_delta(a: dict, b: dict) -> dict:
            return {k: (a["db"].get(k, 0) - b["db"].get(k, 0)) for k in b.get("db", {})}

        max_lag_by_group = {
            g: max((s["groups"][g]["total_lag"] for s in lag_samples if g in s["groups"]), default=0)
            for g in PIPELINE_GROUPS
        }

        return {
            "label": label,
            "mode": mode,
            "ml_target": ml_target,
            "workload": workload,
            "run_index": run_idx,
            "stream_epoch": epoch,
            "config": config_snapshot,
            "ingestion": {
                "total_sent": total_sent,
                "window_sent": len(window_eids),
                "sent_rps": round(len(window_eids) / measure_seconds, 2),
                "status_counts": load_res["status_counts"],
                "errors_sample": load_res["errors_detail"],
                "http_latency_ms": summarize(load_res["http_lat_window_ms"]),
            },
            "throughput": {
                "drained": drained,
                "drain_seconds": drain_seconds,
                "last_counts": last_counts,
                "proc_outcomes_total": len(rows_proc),
                "proc_outcomes_win_rps": round(completed_win_count / measure_seconds, 2),
                "biz_pred_total": len(biz_pred_ts_by_eid),
                "biz_pred_win_rps": round(biz_pred_win_count / measure_seconds, 2),
                "outcome_kinds": dict(kind_counts),
                "alert_dispositions": dict(disp_counts),
                "alerts_created_or_updated": alerts_in_epoch,
                "alert_signal_links": {r["disposition"]: int(r["n"]) for r in alert_links_in_epoch},
            },
            "latency_ms": {
                "accept_to_proc": summarize(win_proc_lat_ms),
                "hot_accept_to_proc": summarize(hot_proc_ms),
                "bg_accept_to_proc": summarize(bg_proc_ms),
                "proc_to_outbox": summarize(win_outbox_ms),
                "outbox_to_biz_pred": summarize(win_k2b_ms),
                "hot_e2e_accept_to_biz": summarize(hot_e2e_ms),
                "bg_e2e_accept_to_biz": summarize(bg_e2e_ms),
            },
            "kafka_max_lag": max_lag_by_group,
            "pg": {
                "proc_stat_delta": stat_delta(proc_stats_after, proc_stats_before),
                "biz_stat_delta": stat_delta(biz_stats_after, biz_stats_before),
                "proc_conns_peak": max((s["proc"]["total"] for s in pg_conn_samples), default=0),
                "proc_active_peak": max((s["proc"]["active"] for s in pg_conn_samples), default=0),
                "biz_conns_peak": max((s["biz"]["total"] for s in pg_conn_samples), default=0),
                "biz_active_peak": max((s["biz"]["active"] for s in pg_conn_samples), default=0),
            },
            "k8s_resources": res_summary,
            "invariants": {
                "wrong_latest_sequence": exact_latest_mismatch,
                "stale_lvp_lt_exp": wrong_latest_seq,
                "stuck_ledger_rows": int(last_counts.get("ledger_pend", -1)),
                "blocked_rows": int(disp_counts.get("BLOCKED", 0)),
                "blocked_behind_unavailable": (
                    int(disp_counts.get("SIGNAL_READY", 0))
                    if int(last_counts.get("ledger_pend", -1)) == 0
                    else 0
                ),
                "pending_outbox_after_drain": int(last_counts.get("ob_pend", -1)),
                "duplicate_active_alerts": len(dup_active_alerts),
                "proc_conflicts_delta": int(proc_conflicts_delta),
                "biz_conflicts_delta": int(biz_conflicts_delta),
                "bad_unavailable_downgrade": bad_unavail,
                "unexpected_pod_restarts": restart_deltas,
                "oom_killed_delta": oom_deltas,
                "total_violations": (
                    exact_latest_mismatch
                    + max(0, int(last_counts.get("ledger_pend", 0)))
                    + max(0, int(last_counts.get("ob_pend", 0)))
                    + len(dup_active_alerts)
                    + int(proc_conflicts_delta)
                    + int(biz_conflicts_delta)
                    + bad_unavail
                    + oom_deltas
                ),
            },
            "chaos": {"events": chaos_events, "recovery_seconds": recovery_seconds},
        }
    finally:
        admin = None
        import gc
        gc.collect()
        pf.close_all()


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def run_suite_exp(args, *, ml_target: str, ml_url: str, mode: str, per_key: int, max_inflight: int, batch_size: int, shadow_reps: int, workloads: list[str], out_file: Path, append: bool = False) -> None:
    secrets = read_k3d_secrets(args.kubeconfig, args.namespace)
    proc_dsn = secrets["local-processing-owner"]["url"]
    biz_dsn = secrets["local-business-owner"]["url"]
    cid = secrets["local-oidc-sensor"]["client_id"]
    csec = secrets["local-oidc-sensor"]["client_secret"]

    cfg = {
        "mode": mode,
        "ml_target": ml_target,
        "ml_url": ml_url,
        "hybrid_enabled": mode == "hybrid",
        "per_key": per_key,
        "max_total_inflight": max_inflight,
        "biz_commit_batch_size": batch_size,
        "worker_replicas": 6,
        "relay_replicas": 2,
        "biz_replicas": 4,
        "ingestion_replicas": 2,
        "ml_champion_replicas": 2,
        "ml_shadow_replicas": shadow_reps,
    }
    print(f"\n[CLUSTER] Applying profile: {cfg}", flush=True)
    apply_cluster_profile(
        args.kubeconfig,
        args.namespace,
        hybrid_enabled=(mode == "hybrid"),
        per_key=per_key,
        max_total_inflight=max_inflight,
        ml_base_url=ml_url,
        biz_commit_batch_size=batch_size,
        ml_shadow_replicas=shadow_reps,
    )

    runs = []
    if append and out_file.is_file():
        runs = json.loads(out_file.read_text()).get("runs", [])

    for wl in workloads:
        for r in range(1, args.runs + 1):
            lbl = f"{mode}-{ml_target}-wl{wl}-pk{per_key}"
            print(f"[RUN] {lbl} run {r}/{args.runs}...", flush=True)
            res = run_single_k3d_trial(
                kubeconfig=args.kubeconfig,
                namespace=args.namespace,
                proc_dsn=proc_dsn,
                biz_dsn=biz_dsn,
                client_id=cid,
                client_secret=csec,
                label=lbl,
                mode=mode,
                ml_target=ml_target,
                workload=wl,
                run_idx=r,
                config_snapshot=cfg,
                warmup_seconds=args.warmup_seconds,
                measure_seconds=args.measure_seconds,
            )
            runs.append(res)
            save_json(out_file, {"generated_at": datetime.now(timezone.utc).isoformat(), "runs": runs})
            print(
                f"  -> sent={res['ingestion']['sent_rps']}/s proc_out={res['throughput']['proc_outcomes_win_rps']}/s "
                f"biz_pred={res['throughput']['biz_pred_win_rps']}/s "
                f"hot_e2e_p95={res['latency_ms']['hot_e2e_accept_to_biz']['p95']}ms "
                f"bg_e2e_p95={res['latency_ms']['bg_e2e_accept_to_biz']['p95']}ms "
                f"http_p95={res['ingestion']['http_latency_ms']['p95']}ms "
                f"drained={res['throughput']['drained']} viol={res['invariants']['total_violations']}",
                flush=True,
            )


def run_sweeps(args) -> None:
    secrets = read_k3d_secrets(args.kubeconfig, args.namespace)
    proc_dsn = secrets["local-processing-owner"]["url"]
    biz_dsn = secrets["local-business-owner"]["url"]
    cid = secrets["local-oidc-sensor"]["client_id"]
    csec = secrets["local-oidc-sensor"]["client_secret"]
    out_file = ROOT / "docs/benchmarks/hybrid-ordering/k3d-hybrid/replica_sweeps.json"
    runs = []

    # Standardized Hybrid HGB WL=C sweeping individual service replicas
    # (category, kwargs)
    sweep_cases = [
        # 1. ml-service-shadow sweep (per_key=4 so workers don't bottleneck ML)
        ("ml-service-shadow", {"ml_shadow_replicas": 1, "worker_replicas": 6, "relay_replicas": 2, "biz_replicas": 4, "ingestion_replicas": 2, "per_key": 4, "max_total_inflight": 16, "biz_commit_batch_size": 16}),
        ("ml-service-shadow", {"ml_shadow_replicas": 2, "worker_replicas": 6, "relay_replicas": 2, "biz_replicas": 4, "ingestion_replicas": 2, "per_key": 4, "max_total_inflight": 16, "biz_commit_batch_size": 16}),
        ("ml-service-shadow", {"ml_shadow_replicas": 3, "worker_replicas": 6, "relay_replicas": 2, "biz_replicas": 4, "ingestion_replicas": 2, "per_key": 4, "max_total_inflight": 16, "biz_commit_batch_size": 16}),
        # 2. processing-worker sweep (with 2 ML shadow replicas, 4 biz, 2 relay)
        ("processing-worker", {"worker_replicas": 1, "ml_shadow_replicas": 2, "relay_replicas": 2, "biz_replicas": 4, "ingestion_replicas": 2, "per_key": 4, "max_total_inflight": 16, "biz_commit_batch_size": 16}),
        ("processing-worker", {"worker_replicas": 2, "ml_shadow_replicas": 2, "relay_replicas": 2, "biz_replicas": 4, "ingestion_replicas": 2, "per_key": 4, "max_total_inflight": 16, "biz_commit_batch_size": 16}),
        ("processing-worker", {"worker_replicas": 4, "ml_shadow_replicas": 2, "relay_replicas": 2, "biz_replicas": 4, "ingestion_replicas": 2, "per_key": 4, "max_total_inflight": 16, "biz_commit_batch_size": 16}),
        ("processing-worker", {"worker_replicas": 6, "ml_shadow_replicas": 2, "relay_replicas": 2, "biz_replicas": 4, "ingestion_replicas": 2, "per_key": 4, "max_total_inflight": 16, "biz_commit_batch_size": 16}),
        # 3. processing-outbox-relay sweep
        ("processing-outbox-relay", {"relay_replicas": 1, "worker_replicas": 6, "ml_shadow_replicas": 2, "biz_replicas": 4, "ingestion_replicas": 2, "per_key": 4, "max_total_inflight": 16, "biz_commit_batch_size": 16}),
        ("processing-outbox-relay", {"relay_replicas": 2, "worker_replicas": 6, "ml_shadow_replicas": 2, "biz_replicas": 4, "ingestion_replicas": 2, "per_key": 4, "max_total_inflight": 16, "biz_commit_batch_size": 16}),
        # 4. business-consumer sweep (batch=1 at 1 rep vs batch=16 at 1,2,4 reps)
        ("business-consumer", {"biz_replicas": 1, "biz_commit_batch_size": 1, "worker_replicas": 6, "ml_shadow_replicas": 2, "relay_replicas": 2, "ingestion_replicas": 2, "per_key": 4, "max_total_inflight": 16}),
        ("business-consumer", {"biz_replicas": 1, "biz_commit_batch_size": 16, "worker_replicas": 6, "ml_shadow_replicas": 2, "relay_replicas": 2, "ingestion_replicas": 2, "per_key": 4, "max_total_inflight": 16}),
        ("business-consumer", {"biz_replicas": 2, "biz_commit_batch_size": 16, "worker_replicas": 6, "ml_shadow_replicas": 2, "relay_replicas": 2, "ingestion_replicas": 2, "per_key": 4, "max_total_inflight": 16}),
        ("business-consumer", {"biz_replicas": 4, "biz_commit_batch_size": 16, "worker_replicas": 6, "ml_shadow_replicas": 2, "relay_replicas": 2, "ingestion_replicas": 2, "per_key": 4, "max_total_inflight": 16}),
        # 5. ingestion-api sweep
        ("ingestion-api", {"ingestion_replicas": 1, "worker_replicas": 6, "ml_shadow_replicas": 2, "relay_replicas": 2, "biz_replicas": 4, "per_key": 4, "max_total_inflight": 16, "biz_commit_batch_size": 16}),
        ("ingestion-api", {"ingestion_replicas": 2, "worker_replicas": 6, "ml_shadow_replicas": 2, "relay_replicas": 2, "biz_replicas": 4, "per_key": 4, "max_total_inflight": 16, "biz_commit_batch_size": 16}),
    ]

    for comp, kw in sweep_cases:
        print(f"\n[SWEEP] component={comp} params={kw}", flush=True)
        apply_cluster_profile(
            args.kubeconfig,
            args.namespace,
            hybrid_enabled=True,
            per_key=kw["per_key"],
            max_total_inflight=kw["max_total_inflight"],
            ml_base_url="http://ml-service-shadow:8001",
            biz_commit_batch_size=kw["biz_commit_batch_size"],
            worker_replicas=kw["worker_replicas"],
            relay_replicas=kw["relay_replicas"],
            biz_replicas=kw["biz_replicas"],
            ingestion_replicas=kw["ingestion_replicas"],
            ml_champion_replicas=1,
            ml_shadow_replicas=kw["ml_shadow_replicas"],
        )
        res = run_single_k3d_trial(
            kubeconfig=args.kubeconfig,
            namespace=args.namespace,
            proc_dsn=proc_dsn,
            biz_dsn=biz_dsn,
            client_id=cid,
            client_secret=csec,
            label=f"sweep-{comp}",
            mode="hybrid",
            ml_target="hgb-shadow",
            workload="C",
            run_idx=1,
            config_snapshot={"sweep_component": comp, **kw},
            warmup_seconds=args.warmup_seconds,
            measure_seconds=args.measure_seconds,
        )
        runs.append(res)
        save_json(out_file, {"generated_at": datetime.now(timezone.utc).isoformat(), "runs": runs})
        print(
            f"  -> sent={res['ingestion']['sent_rps']}/s proc={res['throughput']['proc_outcomes_win_rps']}/s "
            f"biz_pred={res['throughput']['biz_pred_win_rps']}/s "
            f"hot_p95={res['latency_ms']['hot_e2e_accept_to_biz']['p95']}ms "
            f"http_p95={res['ingestion']['http_latency_ms']['p95']}ms "
            f"lag={res['kafka_max_lag']} conns_proc={res['pg']['proc_conns_peak']} viol={res['invariants']['total_violations']}",
            flush=True,
        )


def run_pod_kills(args) -> None:
    secrets = read_k3d_secrets(args.kubeconfig, args.namespace)
    proc_dsn = secrets["local-processing-owner"]["url"]
    biz_dsn = secrets["local-business-owner"]["url"]
    cid = secrets["local-oidc-sensor"]["client_id"]
    csec = secrets["local-oidc-sensor"]["client_secret"]
    out_file = ROOT / "docs/benchmarks/hybrid-ordering/k3d-hybrid/pod_kill_tests.json"
    runs = []

    cfg = {
        "mode": "hybrid",
        "ml_target": "hgb-shadow",
        "per_key": 4,
        "max_total_inflight": 16,
        "biz_commit_batch_size": 16,
        "worker_replicas": 6,
        "relay_replicas": 2,
        "biz_replicas": 4,
        "ingestion_replicas": 2,
        "ml_shadow_replicas": 2,
    }
    apply_cluster_profile(
        args.kubeconfig,
        args.namespace,
        hybrid_enabled=True,
        per_key=4,
        max_total_inflight=16,
        ml_base_url="http://ml-service-shadow:8001",
        biz_commit_batch_size=16,
        worker_replicas=6,
        relay_replicas=2,
        biz_replicas=4,
        ingestion_replicas=2,
        ml_champion_replicas=1,
        ml_shadow_replicas=2,
    )

    for victim_kind in ("processing-worker", "business-consumer", "ml-service-shadow"):
        print(f"\n[CHAOS] Killing 1 {victim_kind} pod mid-Workload-C...", flush=True)
        res = run_single_k3d_trial(
            kubeconfig=args.kubeconfig,
            namespace=args.namespace,
            proc_dsn=proc_dsn,
            biz_dsn=biz_dsn,
            client_id=cid,
            client_secret=csec,
            label=f"chaos-{victim_kind}",
            mode="hybrid",
            ml_target="hgb-shadow",
            workload="C",
            run_idx=1,
            config_snapshot=cfg,
            warmup_seconds=args.warmup_seconds,
            measure_seconds=args.measure_seconds,
            drain_timeout=120,
            chaos_kind=victim_kind,
        )
        runs.append(res)
        save_json(out_file, {"generated_at": datetime.now(timezone.utc).isoformat(), "runs": runs})
        print(
            f"  -> victim={res['chaos']} drained={res['throughput']['drained']} drain_s={res['throughput']['drain_seconds']} "
            f"kinds={res['throughput']['outcome_kinds']} disps={res['throughput']['alert_dispositions']} viol={res['invariants']['total_violations']}",
            flush=True,
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kubeconfig", default=DEFAULT_KUBECONFIG)
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--task", choices=["smoke", "expA_base", "expA_hybrid", "expB_base", "expB_hybrid", "expB_hybrid_pk4", "sweeps", "pod_kill"], required=True)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmup-seconds", type=int, default=5)
    parser.add_argument("--measure-seconds", type=int, default=15)
    args = parser.parse_args()

    if args.task == "smoke":
        args.runs = 1
        args.warmup_seconds = 2
        args.measure_seconds = 5
        run_suite_exp(
            args,
            ml_target="logreg-smoke",
            ml_url="http://ml-service:8001",
            mode="hybrid",
            per_key=2,
            max_inflight=8,
            batch_size=16,
            shadow_reps=1,
            workloads=["A"],
            out_file=Path("/tmp/k3d_smoke.json"),
        )
    elif args.task == "expA_base":
        run_suite_exp(
            args,
            ml_target="logreg-champion",
            ml_url="http://ml-service:8001",
            mode="baseline",
            per_key=1,
            max_inflight=8,
            batch_size=1,
            shadow_reps=1,
            workloads=["A", "C"],
            out_file=ROOT / "docs/benchmarks/hybrid-ordering/k3d-baseline/expA_fast_ml.json",
        )
    elif args.task == "expA_hybrid":
        run_suite_exp(
            args,
            ml_target="logreg-champion",
            ml_url="http://ml-service:8001",
            mode="hybrid",
            per_key=2,
            max_inflight=8,
            batch_size=16,
            shadow_reps=1,
            workloads=["A", "C"],
            out_file=ROOT / "docs/benchmarks/hybrid-ordering/k3d-hybrid/expA_fast_ml.json",
        )
    elif args.task == "expB_base":
        run_suite_exp(
            args,
            ml_target="hgb-shadow",
            ml_url="http://ml-service-shadow:8001",
            mode="baseline",
            per_key=1,
            max_inflight=8,
            batch_size=1,
            shadow_reps=2,
            workloads=["A", "C"],
            out_file=ROOT / "docs/benchmarks/hybrid-ordering/k3d-baseline/expB_real_hgb.json",
        )
    elif args.task == "expB_hybrid":
        out = ROOT / "docs/benchmarks/hybrid-ordering/k3d-hybrid/expB_real_hgb.json"
        run_suite_exp(
            args,
            ml_target="hgb-shadow",
            ml_url="http://ml-service-shadow:8001",
            mode="hybrid",
            per_key=2,
            max_inflight=8,
            batch_size=16,
            shadow_reps=2,
            workloads=["A", "C"],
            out_file=out,
            append=False,
        )
        run_suite_exp(
            args,
            ml_target="hgb-shadow",
            ml_url="http://ml-service-shadow:8001",
            mode="hybrid",
            per_key=4,
            max_inflight=16,
            batch_size=16,
            shadow_reps=2,
            workloads=["C"],
            out_file=out,
            append=True,
        )
    elif args.task == "expB_hybrid_pk4":
        out = ROOT / "docs/benchmarks/hybrid-ordering/k3d-hybrid/expB_real_hgb.json"
        if out.is_file():
            doc = json.loads(out.read_text())
            doc["runs"] = [r for r in doc.get("runs", []) if r.get("config", {}).get("per_key") != 4]
            save_json(out, doc)
        run_suite_exp(
            args,
            ml_target="hgb-shadow",
            ml_url="http://ml-service-shadow:8001",
            mode="hybrid",
            per_key=4,
            max_inflight=16,
            batch_size=16,
            shadow_reps=2,
            workloads=["C"],
            out_file=out,
            append=True,
        )
    elif args.task == "sweeps":
        run_sweeps(args)
    elif args.task == "pod_kill":
        run_pod_kills(args)


if __name__ == "__main__":
    main()
