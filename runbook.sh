#!/bin/bash
# =============================================================================
# Capitec Kafka Platform — Full Runbook
# Builds, deploys and seeds the entire platform from scratch.
# Usage:  bash runbook.sh [--skip-build] [--skip-seed]
# =============================================================================

set -euo pipefail

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

log_step()  { echo -e "\n${BOLD}${BLUE}▶ $1${NC}"; }
log_ok()    { echo -e "  ${GREEN}✔${NC}  $1"; }
log_warn()  { echo -e "  ${YELLOW}⚠${NC}  $1"; }
log_error() { echo -e "  ${RED}✘${NC}  $1" >&2; }
log_info()  { echo -e "  ${CYAN}→${NC}  $1"; }

die() { log_error "$1"; exit 1; }

SKIP_BUILD=false
SKIP_SEED=false
for arg in "$@"; do
  [[ "$arg" == "--skip-build" ]] && SKIP_BUILD=true
  [[ "$arg" == "--skip-seed"  ]] && SKIP_SEED=true
done

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║   Capitec Kafka Platform — Runbook           ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════╝${NC}"
[[ "$SKIP_BUILD" == true ]] && echo -e "  Mode: ${YELLOW}--skip-build${NC} (using existing Docker images)"
[[ "$SKIP_SEED"  == true ]] && echo -e "  Mode: ${YELLOW}--skip-seed${NC}  (skipping data seeding)"

# =============================================================================
# STEP 1 — Prerequisite checks
# =============================================================================
log_step "Step 1/7 — Checking prerequisites"

check_cmd() {
  if command -v "$1" &>/dev/null; then
    log_ok "$1 found ($(command -v "$1"))"
  else
    die "$1 is not installed or not in PATH. Please install it and retry."
  fi
}

check_cmd java
check_cmd mvn
check_cmd docker
check_cmd kubectl

# Java 17+ — check via Maven's JVM (handles Homebrew/sdkman installs)
JAVA_VER=$(mvn -version 2>&1 | grep -oE 'Java version: [0-9]+' | grep -oE '[0-9]+$' || echo "0")
if [[ "$JAVA_VER" -ge 17 ]]; then
  log_ok "Java version OK ($JAVA_VER)"
else
  die "Java 17+ required. Maven reports Java version: $JAVA_VER. Set JAVA_HOME to a JDK 17+ install."
fi

# kubectl cluster reachable
if ! kubectl cluster-info &>/dev/null; then
  die "kubectl cannot reach the cluster. Is Rancher Desktop / your k8s cluster running?"
fi
log_ok "Kubernetes cluster reachable"

# Docker daemon
if ! docker info &>/dev/null; then
  die "Docker daemon is not running. Start Docker / Rancher Desktop first."
fi
log_ok "Docker daemon running"

# =============================================================================
# STEP 2 — Deploy Kafka cluster
# =============================================================================
log_step "Step 2/7 — Deploying Kafka cluster"

kubectl apply -f k8s/kafka-statefulset.yaml &>/dev/null
log_info "Waiting for Kafka brokers to become ready (up to 3 minutes)…"

if kubectl rollout status statefulset/kafka --timeout=180s &>/dev/null; then
  log_ok "All Kafka brokers ready"
else
  # Check if already running
  READY=$(kubectl get pods -l app=kafka --no-headers 2>/dev/null | grep -c "Running" || true)
  [[ "$READY" -ge 1 ]] && log_warn "Rollout timed out but $READY broker(s) are Running — continuing" \
    || die "Kafka failed to start. Run: kubectl get pods && kubectl describe statefulset/kafka"
fi

# =============================================================================
# STEP 3 — Create Kafka topics
# =============================================================================
log_step "Step 3/7 — Creating Kafka topics"

KAFKA_POD=$(kubectl get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
[[ -z "$KAFKA_POD" ]] && die "No Kafka pods found. Check: kubectl get pods"

create_topic() {
  local topic="$1" partitions="${2:-3}" replication="${3:-3}"
  kubectl exec "$KAFKA_POD" -- /bin/bash -c \
    "kafka-topics --bootstrap-server localhost:9092 \
       --create --topic '$topic' \
       --partitions $partitions --replication-factor $replication \
       --if-not-exists 2>/dev/null" &>/dev/null \
    && log_ok "Topic: $topic" \
    || log_warn "Topic $topic may already exist — skipping"
}

create_topic customer-created  3 3
create_topic order-created     6 3
create_topic order-cancelled   3 3
create_topic payment-init      3 3
create_topic inventory         3 3

# Verify
TOPIC_COUNT=$(kubectl exec "$KAFKA_POD" -- /bin/bash -c \
  "kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null" \
  | grep -v "^__" | wc -l | tr -d ' ')
log_ok "$TOPIC_COUNT application topics present"

# =============================================================================
# STEP 4 — Build services
# =============================================================================
log_step "Step 4/7 — Building services"

build_service() {
  local dir="$1" image="$2"
  if [[ "$SKIP_BUILD" == true ]]; then
    # Check image exists
    if docker image inspect "$image:latest" &>/dev/null; then
      log_ok "$image — using existing image (--skip-build)"
    else
      log_warn "$image image not found locally — forcing build"
      (cd "$dir" && mvn clean package -q 2>&1 | tail -3) \
        || die "Maven build failed for $dir. Run: cd $dir && mvn clean package"
      docker build -t "$image:latest" "$dir" -q &>/dev/null \
        || die "Docker build failed for $image"
      log_ok "$image — built"
    fi
    return
  fi

  log_info "Building $image…"
  (cd "$dir" && mvn clean package -q 2>&1) \
    || die "Maven build failed for $dir. Run: cd $dir && mvn clean package to see errors."
  docker build -t "$image:latest" "$dir" -q &>/dev/null \
    || die "Docker build failed for $image. Run: docker build -t $image:latest $dir"
  log_ok "$image — built and tagged"
}

build_service kafka-order-service  kafka-order-service
build_service customer-portal      customer-portal
build_service inventory-service    inventory-service
build_service payment-processor    payment-processor
build_service kafka-producer       kafka-producer-ui

# =============================================================================
# STEP 5 — Deploy services to Kubernetes
# =============================================================================
log_step "Step 5/7 — Deploying services to Kubernetes"

deploy_service() {
  local manifest="$1" name="$2"
  kubectl apply -f "k8s/$manifest" &>/dev/null \
    || die "kubectl apply failed for $manifest"
  log_info "Waiting for $name..."
  kubectl rollout restart deployment/"$name" &>/dev/null || true
  if kubectl rollout status deployment/"$name" --timeout=90s &>/dev/null; then
    log_ok "$name — running"
  else
    READY=$(kubectl get deployment "$name" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
    [[ "$READY" -ge 1 ]] \
      && log_warn "$name rollout timed out but pod is ready — continuing" \
      || die "$name failed to start. Run: kubectl logs deployment/$name"
  fi
}

deploy_service kafka-order-service.yaml  kafka-order-service
deploy_service customer-portal.yaml      customer-portal
deploy_service inventory-service.yaml    inventory-service
deploy_service payment-processor.yaml    payment-processor
deploy_service kafka-producer-ui.yaml    kafka-producer-ui

# =============================================================================
# STEP 6 — Port-forward all services
# =============================================================================
log_step "Step 6/7 — Setting up port-forwards"

# Kill any stale port-forwards on our ports
for port in 8080 8081 8082 8083; do
  PIDS=$(lsof -ti tcp:$port 2>/dev/null || true)
  [[ -n "$PIDS" ]] && kill $PIDS 2>/dev/null && sleep 1 || true
done

wait_for_pod() {
  local label="$1"
  local pod
  for i in $(seq 1 20); do
    pod=$(kubectl get pod -l "$label" --field-selector=status.phase=Running \
          -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    [[ -n "$pod" ]] && echo "$pod" && return 0
    sleep 3
  done
  die "No running pod found for label: $label"
}

PROD_POD=$(wait_for_pod "app=kafka-producer-ui")
ORD_POD=$(wait_for_pod  "app=kafka-order-service")
POR_POD=$(wait_for_pod  "app=customer-portal")
INV_POD=$(wait_for_pod  "app=inventory-service")

kubectl port-forward "pod/$PROD_POD" 8080:8080 >/tmp/pf-producer.log  2>&1 &
kubectl port-forward "pod/$ORD_POD"  8081:8081 >/tmp/pf-dashboard.log 2>&1 &
kubectl port-forward "pod/$POR_POD"  8082:8082 >/tmp/pf-portal.log    2>&1 &
kubectl port-forward "pod/$INV_POD"  8083:8083 >/tmp/pf-inventory.log 2>&1 &

sleep 4

# Verify each port responds
check_port() {
  local port="$1" name="$2"
  if curl -s -o /dev/null -w "%{http_code}" "http://localhost:$port/" 2>/dev/null | grep -q "200"; then
    log_ok "$name → http://localhost:$port"
  else
    log_warn "$name port-forward on :$port not responding — check /tmp/pf-*.log"
  fi
}

check_port 8080 "Producer UI    "
check_port 8081 "Admin Dashboard"
check_port 8082 "Customer Portal"
check_port 8083 "Inventory API  (http://localhost:8083/api/inventory)"

# =============================================================================
# STEP 7 — Seed data
# =============================================================================
log_step "Step 7/7 — Seeding data"

if [[ "$SKIP_SEED" == true ]]; then
  log_warn "Skipping seed (--skip-seed)"
else
  log_info "Seeding 1000 customers…"
  bash kafka-producer/create-customers.sh 2>&1 | grep -E "Done|failed|WARN" || true
  log_ok "Customers seeded"

  log_info "Restarting customer-portal to sync sequence…"
  kubectl rollout restart deployment/customer-portal &>/dev/null
  kubectl rollout status deployment/customer-portal --timeout=60s &>/dev/null \
    && log_ok "Customer portal restarted" \
    || log_warn "Portal restart timed out — may still be starting"

  # Re-forward portal after restart
  kill $(lsof -ti tcp:8082 2>/dev/null) 2>/dev/null || true
  sleep 2
  POR_POD=$(wait_for_pod "app=customer-portal")
  kubectl port-forward "pod/$POR_POD" 8082:8082 >/tmp/pf-portal.log 2>&1 &
  sleep 3

  log_info "Seeding orders across all status stages…"
  bash kafka-producer/create-orders.sh 2>&1 | grep -E "Done|failed|WARN" || true
  log_ok "Orders seeded"

  log_info "Stocking 46 inventory SKUs at 100 units each…"
  bash kafka-producer/stock-inventory.sh 2>&1 | grep -E "Done|failed|WARN" || true
  log_ok "Inventory stocked"

  # Verify data
  sleep 5
  CUSTOMERS=$(curl -s "http://localhost:8081/api/customers?size=1" 2>/dev/null \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('total',0))" 2>/dev/null || echo "?")
  ORDERS=$(curl -s "http://localhost:8081/api/orders?size=1" 2>/dev/null \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('total',0))" 2>/dev/null || echo "?")
  INVENTORY=$(curl -s "http://localhost:8083/api/inventory?size=1" 2>/dev/null \
    | python3 -c "import sys,json;d=json.load(sys.stdin);print(d.get('total',0))" 2>/dev/null || echo "?")

  log_ok "DB: $CUSTOMERS customers · $ORDERS orders · $INVENTORY inventory SKUs"
fi

# =============================================================================
# Summary
# =============================================================================
echo ""
echo -e "${BOLD}${GREEN}╔══════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${GREEN}║   Platform is ready!                         ║${NC}"
echo -e "${BOLD}${GREEN}╚══════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${BOLD}Customer Order Portal${NC}   http://localhost:8082"
echo -e "  ${BOLD}Admin Dashboard${NC}         http://localhost:8081"
echo -e "  ${BOLD}Producer UI (dev)${NC}       http://localhost:8080"
echo -e "  ${BOLD}Inventory API${NC}           http://localhost:8083/api/inventory"
echo ""
echo -e "  ${BOLD}Test login:${NC}"
echo -e "    Cell:     0601000700"
echo -e "    Password: Capitec@01"
echo ""
echo -e "  ${BOLD}Re-seed anytime:${NC}"
echo -e "    bash kafka-producer/create-customers.sh"
echo -e "    bash kafka-producer/create-orders.sh"
echo -e "    bash kafka-producer/stock-inventory.sh"
echo ""
echo -e "  ${BOLD}Options:${NC}"
echo -e "    bash runbook.sh --skip-build   # skip Maven/Docker, use existing images"
echo -e "    bash runbook.sh --skip-seed    # skip data seeding"
echo ""
