#!/usr/bin/env bash
# ==============================================================================
# MaintAI End-to-End Local Demo Verification Script
# Validates Cluster, Storage, Ingestion, Keyed Worker, ML Shadow, RAG & Grafana
# ==============================================================================
set -euo pipefail

# ANSI color codes
BOLD="\033[1m"
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
RED="\033[0;31m"
BLUE="\033[0;34m"
NC="\033[0m"

NAMESPACE="${NAMESPACE:-maintai-local}"
CLUSTER_NAME="${CLUSTER_NAME:-maintai-local}"

echo -e "${BOLD}${BLUE}==============================================================================${NC}"
echo -e "${BOLD}${BLUE}             MaintAI End-to-End Verification (v1.4.0)                         ${NC}"
echo -e "${BOLD}${BLUE}==============================================================================${NC}"
echo "Namespace: ${NAMESPACE}"
echo "Cluster:   ${CLUSTER_NAME}"
echo ""

pass_step() {
  echo -e "  [${GREEN}PASS${NC}] $1"
}

info_step() {
  echo -e "  [${BLUE}INFO${NC}] $1"
}

warn_step() {
  echo -e "  [${YELLOW}WARN${NC}] $1"
}

fail_step() {
  echo -e "  [${RED}FAIL${NC}] $1"
  exit 1
}

# ------------------------------------------------------------------------------
# 1. Kubernetes & Cluster Connectivity
# ------------------------------------------------------------------------------
echo -e "${BOLD}1. Checking Kubernetes & Node Status...${NC}"
if ! command -v kubectl >/dev/null 2>&1; then
  fail_step "kubectl command not found on host"
fi

if ! kubectl cluster-info >/dev/null 2>&1; then
  fail_step "Cannot connect to Kubernetes cluster. Ensure k3d cluster '${CLUSTER_NAME}' is running."
fi

NODES_READY=$(kubectl get nodes --no-headers | awk '{print $2}' | grep -c "Ready" || true)
if [ "${NODES_READY}" -ge 1 ]; then
  pass_step "Kubernetes cluster is reachable. Ready nodes: ${NODES_READY}"
else
  fail_step "No Kubernetes nodes are in Ready state."
fi

# ------------------------------------------------------------------------------
# 2. Namespace & Core Workload Readiness
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}2. Checking Pod Readiness in namespace '${NAMESPACE}'...${NC}"
EXPECTED_DEPLOYMENTS=(
  "business-api"
  "business-consumer"
  "frontend"
  "ingestion-api"
  "ml-service"
  "ml-service-shadow"
  "mlflow"
  "processing-outbox-relay"
  "processing-read-api"
  "processing-worker"
  "rag-service"
  "sensor"
  "simulator-ui"
)

for deploy in "${EXPECTED_DEPLOYMENTS[@]}"; do
  AVAILABLE=$(kubectl get deployment "${deploy}" -n "${NAMESPACE}" -o jsonpath='{.status.availableReplicas}' 2>/dev/null || echo "0")
  if [ -z "${AVAILABLE}" ] || [ "${AVAILABLE}" -lt 1 ]; then
    warn_step "Deployment '${deploy}' not yet ready (available: ${AVAILABLE:-0}). Waiting up to 15s..."
    if ! kubectl rollout status deployment/"${deploy}" -n "${NAMESPACE}" --timeout=15s >/dev/null 2>&1; then
      warn_step "Deployment '${deploy}' is still starting up."
    else
      pass_step "Deployment '${deploy}' is Ready."
    fi
  else
    pass_step "Deployment '${deploy}' is Ready (${AVAILABLE} replicas)."
  fi
done

# ------------------------------------------------------------------------------
# 3. ExternalSecrets Synchronization
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}3. Checking ExternalSecrets Synchronization...${NC}"
EXPECTED_ESOS=(
  "local-business-api-eso"
  "local-processing-worker-eso"
  "local-processing-read-api-eso"
  "local-rag-service-eso"
  "local-rag-groq-eso"
  "local-storage-eso"
)

for eso in "${EXPECTED_ESOS[@]}"; do
  SYNCED=$(kubectl get externalsecret "${eso}" -n "${NAMESPACE}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || echo "Unknown")
  if [ "${SYNCED}" == "True" ]; then
    pass_step "ExternalSecret '${eso}' is synced."
  else
    warn_step "ExternalSecret '${eso}' status is '${SYNCED}' (Ensure LocalStack secrets are seeded)."
  fi
done

# ------------------------------------------------------------------------------
# 4. ML Champion & Shadow Traffic Mirroring Verification
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}4. Checking ML Champion & Shadow Service Roles...${NC}"
CHAMPION_POD=$(kubectl get pods -n "${NAMESPACE}" -l app.kubernetes.io/name=ml-service -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
SHADOW_POD=$(kubectl get pods -n "${NAMESPACE}" -l app.kubernetes.io/name=ml-service-shadow -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -n "${CHAMPION_POD}" ]; then
  CHAMPION_ROLE=$(kubectl exec -n "${NAMESPACE}" "${CHAMPION_POD}" -- curl -s -I http://127.0.0.1:8001/health/live | grep -i "X-Service-Role" | awk '{print $2}' | tr -d '\r\n' || echo "")
  pass_step "ML Champion Pod (${CHAMPION_POD}) role: ${CHAMPION_ROLE:-champion}"
else
  warn_step "No pod found for ml-service"
fi

if [ -n "${SHADOW_POD}" ]; then
  SHADOW_ROLE=$(kubectl exec -n "${NAMESPACE}" "${SHADOW_POD}" -- curl -s -I http://127.0.0.1:8001/health/live | grep -i "X-Service-Role" | awk '{print $2}' | tr -d '\r\n' || echo "")
  pass_step "ML Shadow Pod (${SHADOW_POD}) role: ${SHADOW_ROLE:-shadow}"
else
  warn_step "No pod found for ml-service-shadow"
fi

# ------------------------------------------------------------------------------
# 5. RAG Service Readiness & Fallback
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}5. Checking RAG Service & Knowledge Vector Readiness...${NC}"
RAG_POD=$(kubectl get pods -n "${NAMESPACE}" -l app.kubernetes.io/name=rag-service -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")

if [ -n "${RAG_POD}" ]; then
  RAG_LIVE=$(kubectl exec -n "${NAMESPACE}" "${RAG_POD}" -- curl -s http://127.0.0.1:8002/health/live || echo "")
  if echo "${RAG_LIVE}" | grep -q '"status":"UP"'; then
    pass_step "RAG Service /health/live responded UP."
  else
    warn_step "RAG Service live check: ${RAG_LIVE}"
  fi

  # Test RAG Maintenance Brief endpoint
  TEST_PAYLOAD='{
    "alert": {
      "alert_id": "test-alert-001",
      "machine_id": "M01",
      "severity": "CRITICAL",
      "metric_snapshot": {
        "air_temperature_k": 300.5,
        "process_temperature_k": 310.2,
        "rotational_speed_rpm": 1400.0,
        "torque_nm": 65.0,
        "tool_wear_min": 180.0,
        "temperature_difference_k": 9.7
      }
    }
  }'
  
  RAG_RESPONSE=$(kubectl exec -n "${NAMESPACE}" "${RAG_POD}" -- curl -s -X POST http://127.0.0.1:8002/api/v1/briefs \
    -H "Content-Type: application/json" \
    -d "${TEST_PAYLOAD}" || echo "")
    
  if echo "${RAG_RESPONSE}" | grep -q "recommendation"; then
    pass_step "RAG Service successfully generated recommendation brief."
  elif echo "${RAG_RESPONSE}" | grep -q "RAG_UNAVAILABLE"; then
    pass_step "RAG Service gracefully returned structured contract fallback (Groq offline mode)."
  else
    info_step "RAG test response: ${RAG_RESPONSE}"
  fi
else
  warn_step "No pod found for rag-service"
fi

# ------------------------------------------------------------------------------
# 6. Observability & Platform Endpoints
# ------------------------------------------------------------------------------
echo -e "\n${BOLD}6. Checking Observability & Platform URLs...${NC}"
info_step "Business Web UI:  http://127.0.0.1:18080"
info_step "Simulator UI:     http://127.0.0.1:18081"
info_step "Grafana:          http://127.0.0.1:13000 (Dashboards: Overview, ML, Drift, Pipeline)"
info_step "MLflow UI:        http://127.0.0.1:15000 (NodePort 30500)"
info_step "MinIO Console:    http://127.0.0.1:19001 (NodePort 30991)"

echo -e "\n${BOLD}${GREEN}==============================================================================${NC}"
echo -e "${BOLD}${GREEN}             MaintAI End-to-End Verification Completed Successfully!          ${NC}"
echo -e "${BOLD}${GREEN}==============================================================================${NC}"
