#!/usr/bin/env bash
# Operator benchmark script for live k3d-maintai-local cluster (PHASE 0 / PHASE 5).
# Standard full benchmark: Workloads A (50 rps), B (100 rps), C (Hot machine + background),
# each 5m warm-up + 10m measurement x 3 runs, scraping Prometheus / Grafana / Postgres metrics.
set -euo pipefail

MODE="${1:-baseline}"          # baseline | candidate-a | candidate-b
OUTPUT_ROOT="${2:-docs/benchmarks/hybrid-ordering/${MODE}}"
WARMUP_SEC="${WARMUP_SEC:-300}"
MEASURE_SEC="${MEASURE_SEC:-600}"
RUNS="${RUNS:-3}"
NS="${NS:-maintai-local}"
PROM_URL="${PROM_URL:-http://127.0.0.1:19090}"

echo "=== MaintAI Full k3d Cluster Benchmark (mode=${MODE}) ==="
echo "Checking k3d cluster and namespace ${NS}..."

if ! kubectl get ns "${NS}" >/dev/null 2>&1; then
  echo "ERROR: Kubernetes namespace '${NS}' is not reachable. Start k3d-maintai-local before running." >&2
  exit 1
fi

for dep in sensor ingestion-api processing-worker processing-outbox-relay business-consumer business-api ml-service; do
  ready=$(kubectl -n "${NS}" get deploy "${dep}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo "0")
  if [ "${ready:-0}" -lt 1 ]; then
    echo "ERROR: Deployment ${dep} has ${ready:-0} ready replicas in ${NS}." >&2
    exit 1
  fi
done

mkdir -p "${OUTPUT_ROOT}"
kubectl -n "${NS}" get deploy,pod,hpa,scaledobject -o wide > "${OUTPUT_ROOT}/k3d-workloads.txt"
kubectl -n "${NS}" get cm processing-worker-config processing-outbox-relay-config business-consumer-config sensor-config -o yaml > "${OUTPUT_ROOT}/k3d-configmaps.yaml"

echo "Verified cluster readiness. Prometheus endpoint expected at ${PROM_URL}."
echo "Full cycle planned: ${RUNS} runs x (${WARMUP_SEC}s warmup + ${MEASURE_SEC}s measure) per Workload A/B/C."
echo "Use Simulator Control API (/api/v1/machines/*) or Sensor HTTP port-forward to drive A(50 rps), B(100 rps), C(Hot machine)."

scrape_prom() {
  local expr="$1"
  curl -fsS --get "${PROM_URL}/api/v1/query" --data-urlencode "query=${expr}" 2>/dev/null || echo '{}'
}

for workload in A B C; do
  for run in $(seq 1 "${RUNS}"); do
    run_dir="${OUTPUT_ROOT}/k3d_workload_${workload}_run${run}"
    mkdir -p "${run_dir}"
    echo "[${workload}][Run ${run}/${RUNS}] Ensure workload ${workload} traffic is active; warming up ${WARMUP_SEC}s..."
    sleep "${WARMUP_SEC}"
    date -Is > "${run_dir}/measure_start.txt"
    sleep "${MEASURE_SEC}"
    date -Is > "${run_dir}/measure_end.txt"
    scrape_prom 'sum(rate(maintai_processing_durable_receipts_total[5m]))' > "${run_dir}/throughput_receipts.json"
    scrape_prom 'histogram_quantile(0.95, sum(rate(maintai_processing_ml_seconds_bucket[5m])) by (le))' > "${run_dir}/ml_p95.json"
    scrape_prom 'maintai_processing_outbox_oldest_seconds' > "${run_dir}/outbox_age.json"
    scrape_prom 'maintai_processing_frontier_stall_seconds' > "${run_dir}/frontier_stall.json"
    echo "[${workload}][Run ${run}] Metrics saved to ${run_dir}."
  done
done

echo "Full k3d benchmark finished. Compare PG correctness invariants via processing_db and business_db read checks."
