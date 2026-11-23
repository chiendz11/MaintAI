#!/usr/bin/env python3
"""Reproducible k3d Burst Benchmark Harness for Keyed FIFO vs Hybrid Processing.

Drives synchronized 50-machine 1s burst cycles via MaintAI's real SensorEngine
(SQLite WAL spool, individual HTTPS POST requests, OAuth client_credentials),
captures real Playwright Chromium browser SSE timestamps, reads host cgroup v2
CPU/throttling/memory directly without docker exec skew, scans Kafka offsets for
real arrival spread, scrapes worker/ML Prometheus /metrics, and verifies zero
ordering/durability/alert correctness violations.
"""

from __future__ import annotations

import argparse
import base64
import gc
import json
import os
import re
import shutil
import socket
import ssl
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[3]
CA_FILE = Path("/tmp/maintai-root-ca.crt")
if not os.environ.get("SSL_CERT_FILE") and CA_FILE.is_file():
    os.environ["SSL_CERT_FILE"] = str(CA_FILE)

# Map *.maintai.localhost -> 127.0.0.1 in-process so TLS SNI + OIDC work via Ingress without editing /etc/hosts.
_ORIG_GETADDRINFO = socket.getaddrinfo


def _local_maintai_getaddrinfo(host, port, *args, **kwargs):
    if isinstance(host, str) and host.endswith(".maintai.localhost"):
        return _ORIG_GETADDRINFO("127.0.0.1", port, *args, **kwargs)
    return _ORIG_GETADDRINFO(host, port, *args, **kwargs)


socket.getaddrinfo = _local_maintai_getaddrinfo

sys.path.insert(0, str(ROOT / "sensor/src"))
sys.path.insert(0, str(ROOT / "messaging/src"))

import psycopg
from confluent_kafka import Consumer as RawKafkaConsumer
from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec
from psycopg.rows import dict_row

from simulator.scheduling.engine import SensorEngine
from simulator.settings import RuntimeSettings

DEFAULT_KUBECONFIG = "/tmp/k3d-bench-kubeconfig.yaml"
DEFAULT_NAMESPACE = "maintai-local"
SSE_PROBE_SCRIPT = Path(__file__).resolve().with_name("sse_browser_probe.mjs")

PIPELINE_GROUPS = [
    "local-processing-v1",
    "local-business-prediction-v1",
    "local-business-live-v1",
    "local-business-alert-v1",
]
TOPICS = ["sensor.readings", "inference.results", "alert.signals"]

SCHEDULER_PRESETS = {
    "A0": {
        "scheduler": "keyed_fifo",
        "hybrid_enabled": False,
        "active_keys": 8,
        "per_key": 1,
        "max_total_inflight": 8,
    },
    "A1": {
        "scheduler": "keyed_fifo",
        "hybrid_enabled": False,
        "active_keys": 16,
        "per_key": 1,
        "max_total_inflight": 16,
    },
    "B1": {
        "scheduler": "hybrid",
        "hybrid_enabled": True,
        "active_keys": 16,
        "per_key": 4,
        "max_total_inflight": 16,
    },
    "A2": {
        "scheduler": "keyed_fifo",
        "hybrid_enabled": False,
        "active_keys": 16,
        "per_key": 1,
        "max_total_inflight": 16,
    },
    "B2": {
        "scheduler": "hybrid",
        "hybrid_enabled": True,
        "active_keys": 16,
        "per_key": 4,
        "max_total_inflight": 16,
    },
}


def parse_iso_dt(val) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if isinstance(val, str):
        s = val.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


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
        return {"count": 0, "mean": None, "p50": None, "p95": None, "p99": None, "max": None, "min": None}
    return {
        "count": len(values),
        "mean": round(statistics.fmean(values), 3),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": round(max(values), 3),
        "min": round(min(values), 3),
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


def rewrite_local_pg_dsn(cluster_dsn: str, local_pg_port: int) -> str:
    parsed = urllib.parse.urlsplit(cluster_dsn)
    userinfo = ""
    if parsed.username:
        userinfo = urllib.parse.quote(parsed.username, safe="")
        if parsed.password is not None:
            userinfo += f":{urllib.parse.quote(parsed.password, safe='')}"
        userinfo += "@"
    netloc = f"{userinfo}127.0.0.1:{local_pg_port}"
    return urllib.parse.urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


class PortForwardPool:
    def __init__(self, kubeconfig: str, namespace: str):
        self.kubeconfig = kubeconfig
        self.namespace = namespace
        self.procs: list[tuple[subprocess.Popen, object]] = []

    def open(self, target: str, remote_port: int, timeout: float = 15.0, *, local_port: int | None = None) -> int:
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
                time.sleep(0.08)
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


def read_k3d_secrets(kubeconfig: str, namespace: str) -> dict[str, dict[str, str]]:
    out = {}
    for sname in ("local-processing-owner", "local-business-owner", "local-oidc-sensor", "local-oidc-registry"):
        raw = json.loads(kube_cmd(kubeconfig, namespace, "get", "secret", sname, "-o", "json"))
        out[sname] = {k: base64.b64decode(v).decode() for k, v in raw["data"].items()}
    return out


def fetch_oidc_token(client_id: str, client_secret: str) -> str:
    ctx = ssl.create_default_context(cafile=str(CA_FILE)) if CA_FILE.is_file() else None
    body = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
    ).encode()
    req = urllib.request.Request(
        "https://auth.maintai.localhost/realms/maintai/protocol/openid-connect/token",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    resp = json.loads(urllib.request.urlopen(req, context=ctx, timeout=10).read().decode())
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
        mem_mi = (
            int(mem_s[:-2])
            if mem_s.endswith("Mi")
            else (int(mem_s[:-2]) * 1024 if mem_s.endswith("Gi") else int(mem_s[:-2]) // 1024 if mem_s.endswith("Ki") else 0)
        )
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


def get_server0_cgroup_root() -> Path | None:
    res = subprocess.run(
        ["docker", "inspect", "-f", "{{.Id}}", "k3d-maintai-local-server-0"],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0 or not res.stdout.strip():
        return None
    cid = res.stdout.strip()
    p = Path(f"/sys/fs/cgroup/system.slice/docker-{cid}.scope/kubepods")
    return p if p.is_dir() else None


def resolve_pod_container_cgroups(kubeconfig: str, namespace: str, labels: list[str]) -> dict[str, dict]:
    """Map pod_name -> host cgroup v2 directory for primary container (read with zero docker exec)."""
    cg_root = get_server0_cgroup_root()
    if cg_root is None:
        return {}
    out = {}
    raw = json.loads(kube_cmd(kubeconfig, namespace, "get", "pods", "-o", "json"))
    for item in raw.get("items", []):
        app_lbl = item.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/name")
        if app_lbl not in labels:
            continue
        if item.get("status", {}).get("phase") != "Running":
            continue
        pname = item["metadata"]["name"]
        uid = item["metadata"]["uid"]
        cs_list = item.get("status", {}).get("containerStatuses", [])
        main_cs = next((c for c in cs_list if c.get("name") == app_lbl), cs_list[0] if cs_list else None)
        if not main_cs or not main_cs.get("containerID"):
            continue
        cid = main_cs["containerID"].split("://", 1)[-1]
        for qos in ("burstable", "guaranteed", "besteffort"):
            cand = cg_root / qos / f"pod{uid}" / cid
            if not cand.is_dir():
                cand = cg_root / qos / f"pod{uid.replace('-', '_')}" / cid
            if (cand / "cpu.stat").is_file():
                out[pname] = {"app": app_lbl, "cg_dir": cand}
                break
    return out


def read_cgroup_snapshot(cg_map: dict[str, dict]) -> dict[str, dict]:
    ts = time.monotonic()
    snap = {"ts": ts, "pods": {}, "host_cpu": read_host_proc_stat()}
    for pname, meta in cg_map.items():
        cg = meta["cg_dir"]
        try:
            stat = {}
            for line in (cg / "cpu.stat").read_text().splitlines():
                k, v = line.split()
                stat[k] = int(v)
            mem_bytes = int((cg / "memory.current").read_text().strip()) if (cg / "memory.current").is_file() else 0
            cpu_max = (cg / "cpu.max").read_text().strip() if (cg / "cpu.max").is_file() else "unknown"
            snap["pods"][pname] = {
                "app": meta["app"],
                "cpu_stat": stat,
                "mem_bytes": mem_bytes,
                "cpu_max": cpu_max,
            }
        except OSError:
            pass
    return snap


def read_host_proc_stat() -> dict[str, int]:
    parts = Path("/proc/stat").read_text().splitlines()[0].split()[1:]
    vals = [int(x) for x in parts[:8]]
    idle = vals[3] + vals[4]
    total = sum(vals)
    return {"idle": idle, "total": total}


def diff_cgroup_snapshots(b: dict, a: dict) -> dict:
    dt = max(1e-6, a["ts"] - b["ts"])
    ht_tot = max(1, a["host_cpu"]["total"] - b["host_cpu"]["total"])
    ht_idle = max(0, a["host_cpu"]["idle"] - b["host_cpu"]["idle"])
    host_cpu_pct = round(100.0 * (1.0 - ht_idle / ht_tot), 2)
    by_pod = {}
    by_app: dict[str, dict] = defaultdict(lambda: {"pods": 0, "cpu_cores": 0.0, "nr_periods": 0, "nr_throttled": 0, "throttled_ms": 0.0})
    for pname, ap in a["pods"].items():
        if pname not in b["pods"]:
            continue
        bp = b["pods"][pname]
        du = max(0, ap["cpu_stat"].get("usage_usec", 0) - bp["cpu_stat"].get("usage_usec", 0))
        np_d = max(0, ap["cpu_stat"].get("nr_periods", 0) - bp["cpu_stat"].get("nr_periods", 0))
        nt_d = max(0, ap["cpu_stat"].get("nr_throttled", 0) - bp["cpu_stat"].get("nr_throttled", 0))
        tu_d = max(0, ap["cpu_stat"].get("throttled_usec", 0) - bp["cpu_stat"].get("throttled_usec", 0))
        cores = (du / 1e6) / dt
        thr_pct = (100.0 * nt_d / np_d) if np_d > 0 else 0.0
        by_pod[pname] = {
            "app": ap["app"],
            "cpu_max": ap["cpu_max"],
            "cpu_cores": round(cores, 4),
            "nr_periods": np_d,
            "nr_throttled": nt_d,
            "throttled_ratio_pct": round(thr_pct, 2),
            "throttled_ms": round(tu_d / 1000.0, 2),
            "mem_mib_end": round(ap["mem_bytes"] / (1024 * 1024), 2),
        }
        agg = by_app[ap["app"]]
        agg["pods"] += 1
        agg["cpu_cores"] = round(agg["cpu_cores"] + cores, 4)
        agg["nr_periods"] += np_d
        agg["nr_throttled"] += nt_d
        agg["throttled_ms"] = round(agg["throttled_ms"] + tu_d / 1000.0, 2)
    for app, agg in by_app.items():
        agg["throttled_ratio_pct"] = round(100.0 * agg["nr_throttled"] / agg["nr_periods"], 2) if agg["nr_periods"] > 0 else 0.0
    return {
        "window_seconds": round(dt, 3),
        "host_cpu_pct": host_cpu_pct,
        "by_app": dict(by_app),
        "by_pod": by_pod,
    }


def parse_prometheus_text(text: str) -> dict:
    samples = []
    line_re = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?|Nan|[+-]?Inf)$')
    lbl_re = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"])*)"')
    for raw in text.splitlines():
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            continue
        m = line_re.match(raw)
        if not m:
            continue
        name, lbls_str, val_s = m.group(1), m.group(2), m.group(3)
        lbls = {km.group(1): km.group(2) for km in lbl_re.finditer(lbls_str)} if lbls_str else {}
        try:
            val = float(val_s)
        except ValueError:
            continue
        samples.append((name, lbls, val))
    return {"samples": samples}


def histogram_quantile_from_buckets(buckets: dict[float, float], q: float) -> float | None:
    """Compute Prometheus linear bucket quantile (returns ms when input buckets are seconds -> multiply by 1000)."""
    total = buckets.get(float("inf"), 0.0)
    if total <= 0:
        return None
    target = q * total
    prev_b = 0.0
    prev_c = 0.0
    for b in sorted(k for k in buckets if k != float("inf")):
        c = buckets[b]
        if c >= target:
            span_c = c - prev_c
            if span_c <= 0:
                return round(b * 1000.0, 3)
            frac = (target - prev_c) / span_c
            return round((prev_b + frac * (b - prev_b)) * 1000.0, 3)
        prev_b = b
        prev_c = c
    return round(prev_b * 1000.0, 3)


def summarize_prom_deltas(before_map: dict[str, dict], after_map: dict[str, dict]) -> dict:
    """Aggregate worker & ML Prometheus deltas between T0 and T1."""
    hist_names = (
        "maintai_processing_queue_wait_seconds",
        "maintai_processing_ml_seconds",
        "maintai_processing_admission_seconds",
        "maintai_processing_persist_seconds",
        "maintai_ml_inference_duration_seconds",
    )
    buckets_agg = {h: defaultdict(float) for h in hist_names}
    sums_agg = defaultdict(float)
    cnts_agg = defaultdict(float)
    ml_pod_reqs: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    ml_pod_dur: dict[str, dict[str, float]] = {}

    for pod, after_doc in after_map.items():
        before_doc = before_map.get(pod, {"samples": []})
        b_idx = {(n, tuple(sorted(l.items()))): v for n, l, v in before_doc["samples"]}
        pod_ml_buckets = defaultdict(float)
        pod_ml_sum = 0.0
        pod_ml_cnt = 0.0
        for name, lbls, v_after in after_doc["samples"]:
            v_before = b_idx.get((name, tuple(sorted(lbls.items()))), 0.0)
            dv = max(0.0, v_after - v_before)
            for h in hist_names:
                if name == f"{h}_bucket" and "le" in lbls:
                    le = float("inf") if lbls["le"] == "+Inf" else float(lbls["le"])
                    buckets_agg[h][le] += dv
                    if pod.startswith("ml-service-shadow") and h == "maintai_ml_inference_duration_seconds":
                        pod_ml_buckets[le] += dv
                elif name == f"{h}_sum":
                    sums_agg[h] += dv
                    if pod.startswith("ml-service-shadow") and h == "maintai_ml_inference_duration_seconds":
                        pod_ml_sum += dv
                elif name == f"{h}_count":
                    cnts_agg[h] += dv
                    if pod.startswith("ml-service-shadow") and h == "maintai_ml_inference_duration_seconds":
                        pod_ml_cnt += dv
            if pod.startswith("ml-service-shadow") and name == "maintai_ml_inference_requests_total":
                ml_pod_reqs[pod][lbls.get("status", "UNKNOWN")] += dv
        if pod.startswith("ml-service-shadow"):
            ml_pod_dur[pod] = {
                "count": int(pod_ml_cnt),
                "mean_ms": round(1000.0 * pod_ml_sum / pod_ml_cnt, 3) if pod_ml_cnt > 0 else None,
                "p50_ms": histogram_quantile_from_buckets(pod_ml_buckets, 0.50),
                "p95_ms": histogram_quantile_from_buckets(pod_ml_buckets, 0.95),
                "p99_ms": histogram_quantile_from_buckets(pod_ml_buckets, 0.99),
            }

    hist_summary = {}
    for h in hist_names:
        cnt = cnts_agg[h]
        hist_summary[h] = {
            "count": int(cnt),
            "mean_ms": round(1000.0 * sums_agg[h] / cnt, 3) if cnt > 0 else None,
            "p50_ms": histogram_quantile_from_buckets(buckets_agg[h], 0.50),
            "p95_ms": histogram_quantile_from_buckets(buckets_agg[h], 0.95),
            "p99_ms": histogram_quantile_from_buckets(buckets_agg[h], 0.99),
        }

    return {
        "histograms_ms": hist_summary,
        "ml_pod_distribution": {
            pod: {**dict(ml_pod_reqs[pod]), **ml_pod_dur.get(pod, {})}
            for pod in sorted(set(ml_pod_reqs) | set(ml_pod_dur))
        },
    }


def apply_burst_profile(
    kubeconfig: str,
    namespace: str,
    *,
    preset_name: str,
    ml_shadow_replicas: int = 2,
    ml_shadow_image: str | None = None,
    ml_shadow_env: dict[str, str] | None = None,
    ml_shadow_args: list[str] | None = None,
    ml_shadow_cpu_limit: str | None = None,
    worker_replicas: int = 6,
    relay_replicas: int = 2,
    biz_replicas: int = 4,
    ingestion_replicas: int = 2,
    ml_champion_replicas: int = 2,
    commit_batch_size: int = 16,
    commit_max_wait_ms: int = 25,
) -> dict:
    p = SCHEDULER_PRESETS[preset_name]
    # Pin KEDA and HPAs so autoscalers NEVER change replicas mid-benchmark
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
            json.dumps({"spec": {"minReplicas": cnt, "maxReplicas": cnt}}),
        )

    kube_cmd(
        kubeconfig,
        namespace,
        "set",
        "env",
        "deploy/processing-worker",
        "-c",
        "processing-worker",
        f"PROCESSING_HYBRID_ORDERING_ENABLED={'true' if p['hybrid_enabled'] else 'false'}",
        f"PROCESSING_MAX_ACTIVE_KEYS={p['active_keys']}",
        f"PROCESSING_MAX_INFLIGHT_PER_KEY={p['per_key']}",
        f"PROCESSING_MAX_TOTAL_INFLIGHT={p['max_total_inflight']}",
        f"KAFKA_COMMIT_BATCH_SIZE={commit_batch_size}",
        f"KAFKA_COMMIT_MAX_WAIT_MS={commit_max_wait_ms}",
        "ML_BASE_URL=http://ml-service-shadow:8001",
        "PROCESSING_MODEL_IDENTITY_FIXTURE=/bench-fixtures/model-identity.json",
    )
    kube_cmd(
        kubeconfig,
        namespace,
        "set",
        "env",
        "deploy/business-consumer",
        "-c",
        "business-consumer",
        f"KAFKA_COMMIT_BATCH_SIZE={commit_batch_size}",
        f"KAFKA_COMMIT_MAX_WAIT_MS={commit_max_wait_ms}",
    )

    if ml_shadow_image:
        kube_cmd(kubeconfig, namespace, "set", "image", "deploy/ml-service-shadow", f"ml-service-shadow={ml_shadow_image}")
    if ml_shadow_env:
        env_args = [f"{k}={v}" for k, v in ml_shadow_env.items()]
        kube_cmd(kubeconfig, namespace, "set", "env", "deploy/ml-service-shadow", "-c", "ml-service-shadow", *env_args)
    if ml_shadow_args is not None:
        patch_c = {"args": ml_shadow_args} if ml_shadow_args else {"args": None}
        kube_cmd(
            kubeconfig,
            namespace,
            "patch",
            "deploy/ml-service-shadow",
            "--type=strategic",
            "-p",
            json.dumps({"spec": {"template": {"spec": {"containers": [{"name": "ml-service-shadow", **patch_c}]}}}}),
        )
    if ml_shadow_cpu_limit is not None:
        limits = {"memory": "1Gi"}
        if ml_shadow_cpu_limit:
            limits["cpu"] = ml_shadow_cpu_limit
        kube_cmd(
            kubeconfig,
            namespace,
            "patch",
            "deploy/ml-service-shadow",
            "--type=strategic",
            "-p",
            json.dumps(
                {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "name": "ml-service-shadow",
                                        "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": limits},
                                    }
                                ]
                            }
                        }
                    }
                }
            ),
        )

    for dep, reps in [
        ("sensor", 0),
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
        kube_cmd(kubeconfig, namespace, "rollout", "status", f"deploy/{dep}", "--timeout=240s")

    time.sleep(3.0)
    # Return verified live config snapshot
    shadow_dep = json.loads(kube_cmd(kubeconfig, namespace, "get", "deploy", "ml-service-shadow", "-o", "json"))
    worker_dep = json.loads(kube_cmd(kubeconfig, namespace, "get", "deploy", "processing-worker", "-o", "json"))
    sc = shadow_dep["spec"]["template"]["spec"]["containers"][0]
    wc = worker_dep["spec"]["template"]["spec"]["containers"][0]
    return {
        "preset": preset_name,
        **p,
        "commit_batch_size": commit_batch_size,
        "commit_max_wait_ms": commit_max_wait_ms,
        "worker_replicas": worker_replicas,
        "relay_replicas": relay_replicas,
        "biz_replicas": biz_replicas,
        "ingestion_replicas": ingestion_replicas,
        "ml_shadow_replicas": ml_shadow_replicas,
        "ml_shadow_image": sc["image"],
        "ml_shadow_resources": sc.get("resources"),
        "ml_shadow_args": sc.get("args"),
        "worker_image": wc["image"],
    }


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


def query_pg_stats(conn) -> dict:
    db_row = conn.execute(
        "SELECT xact_commit, xact_rollback, blks_read, blks_hit, tup_inserted, tup_updated, tup_fetched, deadlocks "
        "FROM pg_stat_database WHERE datname = current_database()"
    ).fetchone()
    return dict(db_row) if db_row else {}


def scan_kafka_sensor_timestamps(
    kafka_port: int, start_offsets: dict[int, int], target_epoch: str
) -> dict[str, dict]:
    """Read sensor.readings records from start_offsets..latest to get exact broker CreateTime & partition."""
    c = RawKafkaConsumer(
        {
            "bootstrap.servers": f"127.0.0.1:{kafka_port}",
            "group.id": f"burst-scan-{uuid4().hex[:8]}",
            "enable.auto.commit": False,
        }
    )
    out = {}
    try:
        hi = {p: c.get_watermark_offsets(TopicPartition("sensor.readings", p), timeout=5)[1] for p in range(12)}
        tps = [TopicPartition("sensor.readings", p, start_offsets[p]) for p in range(12) if start_offsets[p] < hi[p]]
        if not tps:
            return out
        c.assign(tps)
        remaining = {tp.partition: hi[tp.partition] for tp in tps}
        dl = time.monotonic() + 20.0
        while remaining and time.monotonic() < dl:
            msg = c.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                continue
            part = msg.partition()
            off = msg.offset()
            try:
                doc = json.loads(msg.value())
            except Exception:
                doc = None
            if isinstance(doc, dict) and doc.get("stream_epoch") == target_epoch:
                ts_type, ts_ms = msg.timestamp()
                out[doc["event_id"]] = {
                    "kafka_partition": part,
                    "kafka_offset": off,
                    "kafka_timestamp_ms": ts_ms if ts_ms and ts_ms > 0 else None,
                }
            if part in remaining and off + 1 >= remaining[part]:
                del remaining[part]
    finally:
        c.close()
    return out


def run_single_burst_trial(
    *,
    kubeconfig: str,
    namespace: str,
    proc_dsn: str,
    biz_dsn: str,
    sensor_cid: str,
    sensor_csec: str,
    reg_cid: str,
    reg_csec: str,
    preset_name: str,
    config_snapshot: dict,
    run_idx: int,
    warmup_seconds: int,
    measure_seconds: int,
    drain_timeout: int,
    machine_count: int = 50,
    chaos_kind: str | None = None,
) -> dict:
    run_id = uuid4().hex[:8]
    tmp_dir = Path(tempfile.mkdtemp(prefix=f"burst_{preset_name}_{run_id}_"))
    pf = PortForwardPool(kubeconfig, namespace)
    sse_proc = None
    sse_tok_stop = threading.Event()
    stop_mon = threading.Event()

    try:
        kc_port = pf.open("svc/keycloak", 8080)  # Verify KC service reachable even as traffic hits Ingress
        _ = kc_port
        pg_port = pf.open("svc/postgres", 5432)
        kafka_port = pf.open("pod/kafka-0", 19092, local_port=19092)

        # Open ephemeral pod port-forwards to scrape Prometheus /metrics from all worker + shadow pods
        metrics_forwards: dict[str, int] = {}
        for sel_lbl in ("processing-worker", "ml-service-shadow"):
            pod_names = kube_cmd(
                kubeconfig,
                namespace,
                "get",
                "pods",
                "-l",
                f"app.kubernetes.io/name={sel_lbl}",
                "--field-selector=status.phase=Running",
                "-o",
                "jsonpath={.items[*].metadata.name}",
            ).split()
            rport = 8081 if sel_lbl == "processing-worker" else 8001
            for pname in sorted(p for p in pod_names if p):
                metrics_forwards[pname] = pf.open(f"pod/{pname}", rport)

        def scrape_all_metrics() -> dict[str, dict]:
            res_map = {}
            for pname, lport in metrics_forwards.items():
                try:
                    raw = urllib.request.urlopen(f"http://127.0.0.1:{lport}/metrics", timeout=3).read().decode()
                    res_map[pname] = parse_prometheus_text(raw)
                except Exception:
                    pass
            return res_map

        local_proc_dsn = rewrite_local_pg_dsn(proc_dsn, pg_port)
        local_biz_dsn = rewrite_local_pg_dsn(biz_dsn, pg_port)
        admin = AdminClient({"bootstrap.servers": f"127.0.0.1:{kafka_port}"})

        # Capture start offsets on sensor.readings BEFORE run
        hi_futs = admin.list_offsets({TopicPartition("sensor.readings", p): OffsetSpec.latest() for p in range(12)})
        kafka_start_offsets = {p: hi_futs[TopicPartition("sensor.readings", p)].result(timeout=5).offset for p in range(12)}

        # Determine safe strictly-monotonic starting sequence_number per machine
        machines = [f"M-{i:03d}" for i in range(1, machine_count + 1)]
        with psycopg.connect(local_proc_dsn, row_factory=dict_row) as pc, psycopg.connect(local_biz_dsn, row_factory=dict_row) as bc:
            seq_start = {m: 0 for m in machines}
            for sql_txt, conn in [
                ("SELECT machine_id, MAX(sequence_number) AS s FROM sensor_events WHERE machine_id = ANY(%s) GROUP BY machine_id", pc),
                ("SELECT machine_id, MAX(last_sequence) AS s FROM maintai_processing_machine_cursor WHERE machine_id = ANY(%s) GROUP BY machine_id", pc),
                ("SELECT machine_id, MAX(last_sequence) AS s FROM processing_source_cursor WHERE machine_id = ANY(%s) GROUP BY machine_id", pc),
                ("SELECT machine_id, MAX(last_admitted_sequence) AS s FROM processing_admission_cursor WHERE machine_id = ANY(%s) GROUP BY machine_id", pc),
                ("SELECT machine_id, MAX(sequence_number) AS s FROM business_source_inbox WHERE machine_id = ANY(%s) GROUP BY machine_id", bc),
            ]:
                for r in conn.execute(sql_txt, (machines,)).fetchall():
                    if r["s"] is not None:
                        seq_start[r["machine_id"]] = max(seq_start[r["machine_id"]], int(r["s"]))

            proc_stats_before = query_pg_stats(pc)
            biz_stats_before = query_pg_stats(bc)
            proc_conflicts_before = pc.execute("SELECT count(*) AS n FROM processing_conflicts").fetchone()["n"]
            biz_conflicts_before = bc.execute("SELECT count(*) AS n FROM business_projection_conflicts").fetchone()["n"]
            proc_alert_outbox_before = pc.execute("SELECT count(*) AS n FROM maintai_kafka_outbox WHERE topic='alert.signals'").fetchone()["n"]
            biz_alert_inbox_before = bc.execute("SELECT count(*) AS n FROM maintai_kafka_inbox WHERE topic='alert.signals'").fetchone()["n"]

        pod_health_before = snapshot_pod_health(kubeconfig, namespace)
        cg_map = resolve_pod_container_cgroups(kubeconfig, namespace, ["processing-worker", "ml-service-shadow"])

        # Configure real SensorEngine(sampling_mode="burst")
        sensor_secret_file = tmp_dir / "sensor_secret"
        sensor_secret_file.write_text(sensor_csec, encoding="utf-8")
        sensor_secret_file.chmod(0o600)
        trace_path = tmp_dir / "sensor_burst_trace.jsonl"
        state_db = tmp_dir / "sensor_state.sqlite3"

        settings = RuntimeSettings(
            backend_base_url="https://ingest.maintai.localhost",
            dataset_path=ROOT / "sensor/data/ai4i2020.csv",
            scenario_path=ROOT / "sensor/scenarios",
            state_path=state_db,
            host="127.0.0.1",
            auth_mode="local",
            ingestion_auth="oauth",
            oauth_token_url="https://auth.maintai.localhost/realms/maintai/protocol/openid-connect/token",
            oauth_client_id=sensor_cid,
            oauth_secret_file=sensor_secret_file,
            oauth_scope="sensor:write",
            machine_count=machine_count,
            interval_ms=1000,
            send_workers=max(50, machine_count) if machine_count <= 64 else 64,
            scenario="normal",
            initial_state="RUNNING",
            loop_dataset=True,
            sampling_mode="burst",
            burst_trace_path=trace_path,
            timeout_seconds=10.0,
        )
        engine = SensorEngine(settings)
        epoch = engine.epoch
        with engine.store.transaction() as sdb:
            for idx, mid in enumerate(machines):
                # Spread dataset cursor evenly across 10k rows without creating any synthetic hot machine
                sdb.execute(
                    "UPDATE machines SET sequence_number=?, row_cursor=? WHERE machine_id=?",
                    (seq_start[mid], idx * 197, mid),
                )

        # Start Real Playwright Browser SSE Probe with rotating token file
        sse_tok_file = tmp_dir / "sse_token.txt"
        sse_tok_file.write_text(fetch_oidc_token(reg_cid, reg_csec), encoding="utf-8")
        sse_events_path = tmp_dir / "browser_sse_events.jsonl"
        sse_ready_file = tmp_dir / "sse_ready.json"
        sse_stats_file = tmp_dir / "sse_stats.json"

        def token_refresher():
            while not sse_tok_stop.wait(120):
                try:
                    sse_tok_file.write_text(fetch_oidc_token(reg_cid, reg_csec), encoding="utf-8")
                except Exception:
                    pass

        threading.Thread(target=token_refresher, daemon=True).start()

        sse_proc = subprocess.Popen(
            [
                "node",
                str(SSE_PROBE_SCRIPT),
                "--url",
                "https://api.maintai.localhost/api/v1/events/stream",
                "--token-file",
                str(sse_tok_file),
                "--out",
                str(sse_events_path),
                "--ready-file",
                str(sse_ready_file),
                "--stats-file",
                str(sse_stats_file),
                "--rotate-seconds",
                "180",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        sse_dl = time.monotonic() + 25.0
        while not sse_ready_file.is_file():
            if sse_proc.poll() is not None:
                raise RuntimeError(f"Playwright SSE probe failed: {sse_proc.stderr.read()}")
            if time.monotonic() >= sse_dl:
                raise RuntimeError("Timed out waiting for Playwright Chromium SSE stream ready")
            time.sleep(0.1)

        # Background monitor loop (Kafka lag, kubectl top, PG locks/conns, Prometheus gauges)
        lag_samples = []
        resource_samples = []
        pg_conn_samples = []
        pg_lock_waits = 0
        prom_gauge_samples = []
        chaos_events = []

        def monitor_loop():
            nonlocal pg_lock_waits
            pc_m = bc_m = None
            tick_i = 0
            try:
                while not stop_mon.is_set():
                    t0 = time.monotonic()
                    try:
                        lag_samples.append(sample_kafka_lag(admin))
                    except Exception:
                        pass
                    if tick_i % 2 == 0:
                        top = sample_kubectl_top_pods(kubeconfig, namespace)
                        if top:
                            resource_samples.append(top)
                    try:
                        if pc_m is None or pc_m.closed:
                            pc_m = psycopg.connect(local_proc_dsn, row_factory=dict_row, autocommit=True, connect_timeout=5)
                        if bc_m is None or bc_m.closed:
                            bc_m = psycopg.connect(local_biz_dsn, row_factory=dict_row, autocommit=True, connect_timeout=5)
                        p_act = pc_m.execute(
                            "SELECT count(*) AS total, count(*) FILTER (WHERE state='active') AS active, "
                            "count(*) FILTER (WHERE wait_event_type='Lock') AS lock_wait "
                            "FROM pg_stat_activity WHERE datname='processing_db'"
                        ).fetchone()
                        b_act = bc_m.execute(
                            "SELECT count(*) AS total, count(*) FILTER (WHERE state='active') AS active, "
                            "count(*) FILTER (WHERE wait_event_type='Lock') AS lock_wait "
                            "FROM pg_stat_activity WHERE datname='business_db'"
                        ).fetchone()
                        pg_lock_waits += int(p_act["lock_wait"]) + int(b_act["lock_wait"])
                        pg_conn_samples.append({"proc": dict(p_act), "biz": dict(b_act)})
                    except Exception:
                        for c in (pc_m, bc_m):
                            try:
                                if c is not None:
                                    c.close()
                            except Exception:
                                pass
                        pc_m = bc_m = None
                    if tick_i % 2 == 0:
                        # Lightweight gauge sample from processing workers
                        active_k = inflight_t = max_pk = paused_p = 0
                        q_states = defaultdict(float)
                        for pname, lport in metrics_forwards.items():
                            if not pname.startswith("processing-worker"):
                                continue
                            try:
                                raw = urllib.request.urlopen(f"http://127.0.0.1:{lport}/metrics", timeout=1.5).read().decode()
                                doc = parse_prometheus_text(raw)
                                for n, lbl, v in doc["samples"]:
                                    if n == "maintai_processing_active_keys":
                                        active_k += int(v)
                                    elif n == "maintai_processing_inflight_tasks":
                                        inflight_t += int(v)
                                    elif n == "maintai_processing_max_per_key_inflight":
                                        max_pk = max(max_pk, int(v))
                                    elif n == "maintai_processing_paused_partitions":
                                        paused_p += int(v)
                                    elif n == "maintai_processing_queue_records":
                                        q_states[lbl.get("state", "?")] += v
                            except Exception:
                                pass
                        prom_gauge_samples.append(
                            {
                                "active_keys_sum": active_k,
                                "inflight_tasks_sum": inflight_t,
                                "max_per_key_inflight": max_pk,
                                "paused_partitions_sum": paused_p,
                                "queue_states": dict(q_states),
                            }
                        )
                    tick_i += 1
                    dt = time.monotonic() - t0
                    stop_mon.wait(max(0.2, 1.0 - dt))
            finally:
                for c in (pc_m, bc_m):
                    try:
                        if c is not None:
                            c.close()
                    except Exception:
                        pass

        # Start SensorEngine burst generator!
        t_start_wall = time.monotonic()
        engine.start()
        mon_thread = threading.Thread(target=monitor_loop, daemon=True)
        mon_thread.start()

        # Warm-up window
        if warmup_seconds > 0:
            time.sleep(warmup_seconds)
        t_win_start_utc = datetime.now(timezone.utc)
        cg_before = read_cgroup_snapshot(cg_map)
        prom_t0 = scrape_all_metrics()

        # Measurement window (optionally inject mid-window pod-kill chaos if requested)
        if chaos_kind:
            half = measure_seconds / 2.0
            time.sleep(half)
            sel = f"app.kubernetes.io/name={chaos_kind}"
            cand_pods = kube_cmd(
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
            if cand_pods:
                victim = cand_pods[0]
                t_kill = time.monotonic()
                kube_cmd(kubeconfig, namespace, "delete", "pod", victim, "--wait=false")
                chaos_events.append({"kind": chaos_kind, "victim": victim, "killed_mono": t_kill})
            time.sleep(max(0.0, measure_seconds - half))
        else:
            time.sleep(measure_seconds)

        t_win_end_utc = datetime.now(timezone.utc)
        # Wait slightly or stop generation cleanly? Wait for SensorEngine to finish delivering any in-flight spool!
        engine.close()
        t_http_done = time.monotonic()

        # Drain full async pipeline (Processing -> Outbox -> Business)
        # Count accepted events generated in spool/trace
        trace_lines = [json.loads(line) for line in trace_path.read_text().splitlines() if line.strip()]
        accepted_traces = [r for r in trace_lines if r["outcome"] == "accepted"]
        # Deduplicate by event_id keeping last/accepted
        by_eid_trace = {r["event_id"]: r for r in accepted_traces}
        total_sent = len(by_eid_trace)

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
                biz_done = bc.execute(
                    "SELECT count(*) AS n FROM business_source_inbox WHERE stream_epoch=%s", (epoch,)
                ).fetchone()["n"]
                biz_alert_delta = bc.execute("SELECT count(*) AS n FROM maintai_kafka_inbox WHERE topic='alert.signals'").fetchone()["n"] - biz_alert_inbox_before
                proc_alert_delta = pc.execute("SELECT count(*) AS n FROM maintai_kafka_outbox WHERE topic='alert.signals'").fetchone()["n"] - proc_alert_outbox_before
                last_counts = {
                    "pr_done": pr_done,
                    "ob_pend": ob_pend,
                    "ledger_pend": ledger_pend,
                    "biz_done": biz_done,
                    "biz_alert_delta": biz_alert_delta,
                    "proc_alert_delta": proc_alert_delta,
                }
                if pr_done >= total_sent and ob_pend == 0 and ledger_pend == 0 and biz_done >= total_sent and biz_alert_delta >= proc_alert_delta:
                    drained = True
                    break
                time.sleep(0.2)
        drain_seconds = round(time.monotonic() - t_http_done, 3)

        # Allow NotificationHub 50ms coalesce window to flush the final cycle to Chromium SSE
        time.sleep(1.0)
        cg_after = read_cgroup_snapshot(cg_map)
        prom_t1 = scrape_all_metrics()

        recovery_seconds = None
        if chaos_events:
            kube_cmd(kubeconfig, namespace, "rollout", "status", f"deploy/{chaos_kind}", "--timeout=180s")
            recovery_seconds = round(time.monotonic() - chaos_events[0]["killed_mono"], 3)

        stop_mon.set()
        mon_thread.join(timeout=5)
        sse_tok_stop.set()
        if sse_proc and sse_proc.poll() is None:
            sse_proc.terminate()
            try:
                sse_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                sse_proc.kill()

        pod_health_after = snapshot_pod_health(kubeconfig, namespace)

        # Scan Kafka sensor.readings timestamps for this stream_epoch
        kafka_meta = scan_kafka_sensor_timestamps(kafka_port, kafka_start_offsets, epoch)

        # Parse Playwright Browser SSE output
        sse_by_eid = {}
        if sse_events_path.is_file():
            for line in sse_events_path.read_text().splitlines():
                if not line.strip():
                    continue
                ev = json.loads(line)
                eid = ev.get("source_event_id")
                if eid and ev.get("stream_epoch") == epoch and eid not in sse_by_eid:
                    sse_by_eid[eid] = ev
        sse_stats = json.loads(sse_stats_file.read_text()) if sse_stats_file.is_file() else {}

        # Query full per-event & invariant rows from processing_db and business_db
        with psycopg.connect(local_proc_dsn, row_factory=dict_row) as pc, psycopg.connect(local_biz_dsn, row_factory=dict_row) as bc:
            proc_stats_after = query_pg_stats(pc)
            biz_stats_after = query_pg_stats(bc)
            proc_conflicts_delta = pc.execute("SELECT count(*) AS n FROM processing_conflicts").fetchone()["n"] - proc_conflicts_before
            biz_conflicts_delta = bc.execute("SELECT count(*) AS n FROM business_projection_conflicts").fetchone()["n"] - biz_conflicts_before

            rows_proc = pc.execute(
                """
                SELECT se.event_id, se.machine_id, se.sequence_number, se.source_server_accepted_at,
                       pr.result_id, pr.kind, pr.completed_at,
                       p.prediction_id, p.predicted_at, p.inference_ms, p.label, p.failure_probability,
                       ob.message_id AS ob_msg_id, ob.created_at AS ob_created_at, ob.published_at AS ob_pub_at,
                       pal.admitted_at AS pal_admitted_at, pal.disposition AS pal_disp
                FROM sensor_events se
                JOIN processing_results pr ON pr.event_id = se.event_id
                LEFT JOIN predictions p ON p.event_id = se.event_id
                LEFT JOIN maintai_kafka_outbox ob ON ob.topic='inference.results' AND ob.message_key=se.machine_id AND ob.source_sequence_number=se.sequence_number
                LEFT JOIN processing_alert_ledger pal ON pal.stream_epoch=se.stream_epoch AND pal.machine_id=se.machine_id AND pal.sequence_number=se.sequence_number
                WHERE se.stream_epoch=%s
                """,
                (epoch,),
            ).fetchall()

            ob_msg_to_eid = {r["ob_msg_id"]: r["event_id"] for r in rows_proc if r["ob_msg_id"]}
            biz_pred_ts_by_eid = {}
            if ob_msg_to_eid:
                for br in bc.execute(
                    "SELECT message_id, processed_at FROM maintai_kafka_inbox WHERE topic='inference.results' AND message_id = ANY(%s)",
                    (list(ob_msg_to_eid.keys()),),
                ).fetchall():
                    biz_pred_ts_by_eid[ob_msg_to_eid[br["message_id"]]] = br["processed_at"]

            lvp_rows = bc.execute(
                "SELECT machine_id, sequence_number, stream_epoch, source_event_id FROM latest_valid_prediction WHERE machine_id = ANY(%s)",
                (machines,),
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
                "SELECT count(*) AS n FROM alerts WHERE latest_source_stream_epoch=%s", (epoch,)
            ).fetchone()["n"]

        # Correctness invariants
        lvp_map = {r["machine_id"]: int(r["sequence_number"]) for r in lvp_rows}
        exp_map = {r["machine_id"]: int(r["max_seq"]) for r in exp_rows}
        wrong_latest_seq = sum(1 for m, exp in exp_map.items() if lvp_map.get(m) != exp)
        kind_counts = Counter(r["kind"] for r in rows_proc)
        disp_counts = Counter(r["pal_disp"] for r in rows_proc if r["pal_disp"])
        bad_unavail = sum(
            1
            for r in rows_proc
            if r["kind"] == "INFERENCE_UNAVAILABLE" and r["pal_disp"] in ("NO_SIGNAL", "EMITTED", "RESOLVED_NO_SIGNAL")
        )
        missing_outcomes = max(0, total_sent - len(rows_proc))

        # Index processing rows by event_id
        proc_by_eid = {r["event_id"]: r for r in rows_proc}

        # Group by cycle_id & select COMPLETE 50-machine cycles strictly inside measurement window
        by_cycle: dict[int, list[dict]] = defaultdict(list)
        for eid, tr in by_eid_trace.items():
            cid = tr.get("cycle_id")
            if cid is not None:
                by_cycle[int(cid)].append(tr)

        measured_cycles = []
        for cid, items in sorted(by_cycle.items()):
            if len(items) < machine_count:
                continue
            sdt = parse_iso_dt(items[0]["sampled_at"])
            if sdt and t_win_start_utc <= sdt <= t_win_end_utc:
                measured_cycles.append(cid)

        # Fallback if warmup==0 / short smoke test: take all complete cycles >=2 if >1 exists else all
        if not measured_cycles:
            full_cids = [cid for cid, items in sorted(by_cycle.items()) if len(items) == machine_count]
            measured_cycles = full_cids[1:] if len(full_cids) > 1 else full_cids

        measured_cset = set(measured_cycles)

        # Compute per-event latencies (ms) and per-cycle metrics (ms)
        sample_to_edge_ms = []
        edge_to_ack_ms = []
        sample_to_ack_ms = []
        kafka_to_admit_ms = []
        ml_infer_ms = []
        ml_comp_to_proc_db_ms = []
        proc_db_to_outbox_ms = []
        outbox_to_biz_ms = []
        biz_to_sse_ms = []
        sample_to_biz_ms = []
        sample_to_sse_ms = []
        ack_to_proc_db_ms = []

        kafka_cycle_spread_ms = []
        ack_cycle_spread_ms = []
        first_pred_biz_ms = []
        last_pred_biz_ms = []
        spread_50_biz_ms = []
        first_pred_ui_ms = []
        last_pred_ui_ms = []
        spread_50_ui_ms = []
        sse_cycle_coverage = []

        kafka_partitions = Counter()

        for cid in measured_cycles:
            items = by_cycle[cid]
            k_times = []
            ack_times = []
            biz_deltas = []
            sse_deltas = []
            for tr in items:
                eid = tr["event_id"]
                s_dt = parse_iso_dt(tr["sampled_at"])
                ep_dt = parse_iso_dt(tr["edge_persisted_at"])
                sent_dt = parse_iso_dt(tr["sent_at"])
                ack_dt = parse_iso_dt(tr["ingestion_ack_at"])
                srv_acc_dt = parse_iso_dt(tr["server_accepted_at"])
                if s_dt and ep_dt:
                    sample_to_edge_ms.append(max(0.0, (ep_dt - s_dt).total_seconds() * 1000.0))
                if ep_dt and ack_dt:
                    edge_to_ack_ms.append(max(0.0, (ack_dt - ep_dt).total_seconds() * 1000.0))
                if s_dt and ack_dt:
                    sample_to_ack_ms.append(max(0.0, (ack_dt - s_dt).total_seconds() * 1000.0))
                if ack_dt:
                    ack_times.append(ack_dt)

                km = kafka_meta.get(eid)
                if km and km["kafka_timestamp_ms"] is not None:
                    kafka_partitions[km["kafka_partition"]] += 1
                    k_times.append(km["kafka_timestamp_ms"])
                elif srv_acc_dt:
                    k_times.append(srv_acc_dt.timestamp() * 1000.0)

                pr = proc_by_eid.get(eid)
                if not pr:
                    continue
                k_dt = (
                    datetime.fromtimestamp(km["kafka_timestamp_ms"] / 1000.0, tz=timezone.utc)
                    if km and km["kafka_timestamp_ms"] is not None
                    else pr["source_server_accepted_at"]
                )
                adm_dt = pr["pal_admitted_at"]
                ml_comp_dt = pr["predicted_at"]
                inf_ms = pr["inference_ms"]
                comp_dt = pr["completed_at"]
                pub_dt = pr["ob_pub_at"]
                biz_dt = biz_pred_ts_by_eid.get(eid)
                sse_ev = sse_by_eid.get(eid)
                sse_dt = parse_iso_dt(sse_ev["browser_sse_received_at"]) if sse_ev else None

                if k_dt and adm_dt:
                    kafka_to_admit_ms.append(max(0.0, (adm_dt - k_dt).total_seconds() * 1000.0))
                if inf_ms is not None:
                    ml_infer_ms.append(float(inf_ms))
                if ml_comp_dt and comp_dt:
                    ml_comp_to_proc_db_ms.append(max(0.0, (comp_dt - ml_comp_dt).total_seconds() * 1000.0))
                if srv_acc_dt and comp_dt:
                    ack_to_proc_db_ms.append(max(0.0, (comp_dt - srv_acc_dt).total_seconds() * 1000.0))
                if comp_dt and pub_dt:
                    proc_db_to_outbox_ms.append(max(0.0, (pub_dt - comp_dt).total_seconds() * 1000.0))
                if pub_dt and biz_dt:
                    outbox_to_biz_ms.append(max(0.0, (biz_dt - pub_dt).total_seconds() * 1000.0))
                if s_dt and biz_dt:
                    d_biz = max(0.0, (biz_dt - s_dt).total_seconds() * 1000.0)
                    sample_to_biz_ms.append(d_biz)
                    biz_deltas.append(d_biz)
                if biz_dt and sse_dt:
                    biz_to_sse_ms.append(max(0.0, (sse_dt - biz_dt).total_seconds() * 1000.0))
                if s_dt and sse_dt:
                    d_sse = max(0.0, (sse_dt - s_dt).total_seconds() * 1000.0)
                    sample_to_sse_ms.append(d_sse)
                    sse_deltas.append(d_sse)

            if k_times:
                kafka_cycle_spread_ms.append(max(k_times) - min(k_times))
            if ack_times:
                ack_cycle_spread_ms.append((max(ack_times) - min(ack_times)).total_seconds() * 1000.0)
            if len(biz_deltas) == machine_count:
                first_pred_biz_ms.append(min(biz_deltas))
                last_pred_biz_ms.append(max(biz_deltas))
                spread_50_biz_ms.append(max(biz_deltas) - min(biz_deltas))
            sse_cycle_coverage.append(len(sse_deltas))
            if sse_deltas:
                first_pred_ui_ms.append(min(sse_deltas))
                last_pred_ui_ms.append(max(sse_deltas))
                spread_50_ui_ms.append(max(sse_deltas) - min(sse_deltas))

        # Resource & lag aggregation
        cgroup_diff = diff_cgroup_snapshots(cg_before, cg_after)
        prom_diff = summarize_prom_deltas(prom_t0, prom_t1)

        res_summary = {}
        apps_seen = set()
        for rs in resource_samples:
            apps_seen.update(rs.keys())
        for app in sorted(apps_seen):
            cpus = [rs[app]["cpu_m"] for rs in resource_samples if app in rs]
            mems = [rs[app]["mem_mi"] for rs in resource_samples if app in rs]
            res_summary[app] = {
                "pods": max((rs[app]["pods"] for rs in resource_samples if app in rs), default=0),
                "cpu_m_mean": round(statistics.fmean(cpus), 1) if cpus else None,
                "cpu_m_peak": max(cpus) if cpus else None,
                "mem_mi_mean": round(statistics.fmean(mems), 1) if mems else None,
                "mem_mi_peak": max(mems) if mems else None,
            }

        max_lag = {
            g: max((s[g]["total_lag"] for s in lag_samples if g in s), default=0)
            for g in PIPELINE_GROUPS
        }

        def stat_delta(a: dict, b: dict) -> dict:
            return {k: (a.get(k, 0) - b.get(k, 0)) for k in b}

        restart_deltas = {}
        oom_deltas = 0
        for p, st in pod_health_after.items():
            prev = pod_health_before.get(p, {"restarts": 0, "oom": False})
            dr = st["restarts"] - prev["restarts"]
            if dr > 0 and not any(p.startswith(c["kind"]) for c in chaos_events):
                restart_deltas[p] = dr
            if st["oom"] and not prev["oom"]:
                oom_deltas += 1

        win_expected = len(measured_cycles) * machine_count
        sse_matched_win = len(sample_to_sse_ms)

        return {
            "preset": preset_name,
            "run_index": run_idx,
            "stream_epoch": epoch,
            "config": config_snapshot,
            "window": {
                "warmup_seconds": warmup_seconds,
                "measure_seconds": measure_seconds,
                "complete_cycles": len(measured_cycles),
                "expected_window_readings": win_expected,
                "total_sent": total_sent,
                "status_counts": dict(Counter(r["status_code"] for r in trace_lines)),
                "drained": drained,
                "drain_seconds": drain_seconds,
                "last_counts": last_counts,
                "sse_total_captured": len(sse_by_eid),
                "sse_window_matched": sse_matched_win,
                "sse_window_coverage_pct": round(100.0 * sse_matched_win / win_expected, 2) if win_expected else 0.0,
                "sse_probe_stats": sse_stats,
            },
            "throughput": {
                "sent_rps": round(win_expected / max(1, len(measured_cycles)), 2),
                "outcome_kinds": dict(kind_counts),
                "alert_dispositions": dict(disp_counts),
                "alerts_created_or_updated": alerts_in_epoch,
                "kafka_partitions_used": len(kafka_partitions),
            },
            "latency_ms": {
                "sample_to_edge_spool": summarize(sample_to_edge_ms),
                "edge_to_ingestion_ack": summarize(edge_to_ack_ms),
                "sample_to_ingestion_ack": summarize(sample_to_ack_ms),
                "kafka_arrival_spread_per_cycle": summarize(kafka_cycle_spread_ms),
                "ack_spread_per_cycle": summarize(ack_cycle_spread_ms),
                "kafka_to_hybrid_admit": summarize(kafka_to_admit_ms),
                "ml_inference_server": summarize(ml_infer_ms),
                "ml_completed_to_proc_db": summarize(ml_comp_to_proc_db_ms),
                "ack_to_proc_db": summarize(ack_to_proc_db_ms),
                "proc_db_to_outbox": summarize(proc_db_to_outbox_ms),
                "outbox_to_business_db": summarize(outbox_to_biz_ms),
                "business_db_to_browser_sse": summarize(biz_to_sse_ms),
                "sample_to_business_db": summarize(sample_to_biz_ms),
                "sample_to_browser_sse_ui": summarize(sample_to_sse_ms),
            },
            "cycle_metrics_ms": {
                "first_prediction_biz_db": summarize(first_pred_biz_ms),
                "last_prediction_biz_db": summarize(last_pred_biz_ms),
                "spread_50_machines_biz_db": summarize(spread_50_biz_ms),
                "first_prediction_browser_ui": summarize(first_pred_ui_ms),
                "cycle_completion_browser_ui": summarize(last_pred_ui_ms),
                "spread_50_machines_browser_ui": summarize(spread_50_ui_ms),
            },
            "prometheus_deltas": prom_diff,
            "processing_runtime_gauges": {
                "active_keys_peak": max((s["active_keys_sum"] for s in prom_gauge_samples), default=0),
                "inflight_tasks_peak": max((s["inflight_tasks_sum"] for s in prom_gauge_samples), default=0),
                "max_per_key_inflight_peak": max((s["max_per_key_inflight"] for s in prom_gauge_samples), default=0),
                "paused_partitions_peak": max((s["paused_partitions_sum"] for s in prom_gauge_samples), default=0),
                "queued_records_peak": max((s["queue_states"].get("QUEUED", 0) for s in prom_gauge_samples), default=0),
            },
            "kafka_max_lag": max_lag,
            "cgroup_v2": cgroup_diff,
            "kubectl_top": res_summary,
            "postgres": {
                "proc_stat_delta": stat_delta(proc_stats_after, proc_stats_before),
                "biz_stat_delta": stat_delta(biz_stats_after, biz_stats_before),
                "lock_wait_samples": pg_lock_waits,
                "proc_conns_peak": max((s["proc"]["total"] for s in pg_conn_samples), default=0),
                "proc_active_peak": max((s["proc"]["active"] for s in pg_conn_samples), default=0),
                "biz_conns_peak": max((s["biz"]["total"] for s in pg_conn_samples), default=0),
                "biz_active_peak": max((s["biz"]["active"] for s in pg_conn_samples), default=0),
            },
            "invariants": {
                "missing_outcomes": missing_outcomes,
                "wrong_latest_sequence": wrong_latest_seq,
                "stuck_ledger_rows": int(last_counts.get("ledger_pend", -1)),
                "pending_outbox_after_drain": int(last_counts.get("ob_pend", -1)),
                "duplicate_active_alerts": len(dup_active_alerts),
                "proc_conflicts_delta": int(proc_conflicts_delta),
                "biz_conflicts_delta": int(biz_conflicts_delta),
                "bad_unavailable_downgrade": bad_unavail,
                "unexpected_pod_restarts": restart_deltas,
                "oom_killed_delta": oom_deltas,
                "total_violations": (
                    missing_outcomes
                    + wrong_latest_seq
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
        stop_mon.set()
        sse_tok_stop.set()
        if sse_proc and sse_proc.poll() is None:
            sse_proc.terminate()
            try:
                sse_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                sse_proc.kill()
        gc.collect()
        pf.close_all()
        shutil.rmtree(tmp_dir, ignore_errors=True)


def aggregate_runs(runs: list[dict]) -> dict:
    def avg(paths: tuple[str, ...]):
        vals = []
        for r in runs:
            cur = r
            for k in paths:
                cur = cur.get(k) if isinstance(cur, dict) else None
            if isinstance(cur, (int, float)):
                vals.append(float(cur))
        return round(statistics.fmean(vals), 3) if vals else None

    return {
        "run_count": len(runs),
        "violations_sum": sum(r["invariants"]["total_violations"] for r in runs),
        "drained_all": all(r["window"]["drained"] for r in runs),
        "sample_to_ui_p50_ms": avg(("latency_ms", "sample_to_browser_sse_ui", "p50")),
        "sample_to_ui_p95_ms": avg(("latency_ms", "sample_to_browser_sse_ui", "p95")),
        "sample_to_ui_p99_ms": avg(("latency_ms", "sample_to_browser_sse_ui", "p99")),
        "sample_to_biz_p50_ms": avg(("latency_ms", "sample_to_business_db", "p50")),
        "sample_to_biz_p95_ms": avg(("latency_ms", "sample_to_business_db", "p95")),
        "sample_to_biz_p99_ms": avg(("latency_ms", "sample_to_business_db", "p99")),
        "first_pred_ui_p50_ms": avg(("cycle_metrics_ms", "first_prediction_browser_ui", "p50")),
        "cycle_completion_ui_p95_ms": avg(("cycle_metrics_ms", "cycle_completion_browser_ui", "p95")),
        "spread_50_ui_p95_ms": avg(("cycle_metrics_ms", "spread_50_machines_browser_ui", "p95")),
        "spread_50_biz_p95_ms": avg(("cycle_metrics_ms", "spread_50_machines_biz_db", "p95")),
        "queue_wait_p95_ms": avg(("prometheus_deltas", "histograms_ms", "maintai_processing_queue_wait_seconds", "p95_ms")),
        "ml_server_p95_ms": avg(("latency_ms", "ml_inference_server", "p95")),
        "ml_client_p95_ms": avg(("prometheus_deltas", "histograms_ms", "maintai_processing_ml_seconds", "p95_ms")),
    }


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--kubeconfig", default=DEFAULT_KUBECONFIG)
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--presets", nargs="+", required=True, help="e.g. A0 A1 B1 or A2 B2")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmup-seconds", type=int, default=30)
    parser.add_argument("--measure-seconds", type=int, default=300)
    parser.add_argument("--drain-timeout", type=int, default=120)
    parser.add_argument("--machines", type=int, default=50)
    parser.add_argument("--ml-replicas", type=int, default=2)
    parser.add_argument("--ml-image", default=None)
    parser.add_argument("--ml-env", nargs="*", default=[], help="KEY=VAL pairs for ml-service-shadow")
    parser.add_argument("--ml-args", default=None, help="JSON array of container args for ml-service-shadow")
    parser.add_argument("--ml-cpu-limit", default=None)
    parser.add_argument("--chaos", choices=["processing-worker", "ml-service-shadow"], default=None)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    secrets = read_k3d_secrets(args.kubeconfig, args.namespace)
    proc_dsn = secrets["local-processing-owner"]["url"]
    biz_dsn = secrets["local-business-owner"]["url"]
    sensor_cid = secrets["local-oidc-sensor"]["client_id"]
    sensor_csec = secrets["local-oidc-sensor"]["client_secret"]
    reg_cid = secrets["local-oidc-registry"]["client_id"]
    reg_csec = secrets["local-oidc-registry"]["client_secret"]

    ml_env_dict = {}
    for kv in args.ml_env:
        k, v = kv.split("=", 1)
        ml_env_dict[k] = v
    ml_args_list = json.loads(args.ml_args) if args.ml_args is not None else None

    out_path = Path(args.out)
    results = json.loads(out_path.read_text()) if out_path.is_file() else {"presets": {}}

    for preset in args.presets:
        print(f"\n=== Configuring cluster for preset={preset} (ml_replicas={args.ml_replicas}) ===", flush=True)
        cfg = apply_burst_profile(
            args.kubeconfig,
            args.namespace,
            preset_name=preset,
            ml_shadow_replicas=args.ml_replicas,
            ml_shadow_image=args.ml_image,
            ml_shadow_env=ml_env_dict or None,
            ml_shadow_args=ml_args_list,
            ml_shadow_cpu_limit=args.ml_cpu_limit,
        )
        preset_runs = []
        for r in range(1, args.runs + 1):
            print(f"[{preset}] Run {r}/{args.runs} (warmup={args.warmup_seconds}s, measure={args.measure_seconds}s)...", flush=True)
            res = run_single_burst_trial(
                kubeconfig=args.kubeconfig,
                namespace=args.namespace,
                proc_dsn=proc_dsn,
                biz_dsn=biz_dsn,
                sensor_cid=sensor_cid,
                sensor_csec=sensor_csec,
                reg_cid=reg_cid,
                reg_csec=reg_csec,
                preset_name=preset,
                config_snapshot=cfg,
                run_idx=r,
                warmup_seconds=args.warmup_seconds,
                measure_seconds=args.measure_seconds,
                drain_timeout=args.drain_timeout,
                machine_count=args.machines,
                chaos_kind=args.chaos,
            )
            preset_runs.append(res)
            results["presets"][preset] = {
                "config": cfg,
                "runs": preset_runs,
                "aggregate": aggregate_runs(preset_runs),
            }
            results["updated_at"] = datetime.now(timezone.utc).isoformat()
            save_json(out_path, results)
            ui_s = res["latency_ms"]["sample_to_browser_sse_ui"]
            cyc_s = res["cycle_metrics_ms"]
            print(
                f"  -> cycles={res['window']['complete_cycles']} drained={res['window']['drained']} ({res['window']['drain_seconds']}s) "
                f"sse_cov={res['window']['sse_window_coverage_pct']}% "
                f"ui_p50={ui_s['p50']}ms ui_p95={ui_s['p95']}ms "
                f"cyc_p95={cyc_s['cycle_completion_browser_ui']['p95']}ms "
                f"spread_p95={cyc_s['spread_50_machines_browser_ui']['p95']}ms "
                f"ml_p95={res['latency_ms']['ml_inference_server']['p95']}ms "
                f"viol={res['invariants']['total_violations']}",
                flush=True,
            )


if __name__ == "__main__":
    main()
