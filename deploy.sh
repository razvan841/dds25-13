#!/usr/bin/env bash
# deploy.sh — Start minikube (if needed), build images, and deploy the full stack.
#
# Usage:
#   ./deploy.sh             # full deploy (start minikube + build + apply)
#   ./deploy.sh --no-build  # skip docker build (use existing images)
#   ./deploy.sh --down      # tear down: stop minikube and delete the dds25 namespace

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$REPO_ROOT/k8s"
NAMESPACE="dds25"
BUILD=true
GATEWAY_PORT=8000
PF_PID_FILE="$REPO_ROOT/.gateway-pf.pid"

# ── Helper: stop any running gateway port-forward ─────────────────────────────
stop_port_forward() {
  # Kill by saved PID
  if [[ -f "$PF_PID_FILE" ]]; then
    local pid
    pid=$(cat "$PF_PID_FILE")
    if kill -0 "$pid" 2>/dev/null; then
      echo "  Stopping existing gateway port-forward (PID $pid)..."
      kill "$pid" 2>/dev/null || true
    fi
    rm -f "$PF_PID_FILE"
  fi
  # Also sweep any stray port-forwards for this port/service
  pkill -f "kubectl port-forward.*svc/gateway" 2>/dev/null || true
  pkill -f "kubectl port-forward.*gateway.*${GATEWAY_PORT}" 2>/dev/null || true
}

# ── Argument parsing ──────────────────────────────────────────────────────────
for arg in "$@"; do
  case $arg in
    --no-build) BUILD=false ;;
    --down)
      stop_port_forward
      echo "==> Tearing down namespace $NAMESPACE..."
      kubectl delete namespace "$NAMESPACE" --ignore-not-found
      echo "Done. Run 'minikube stop' to also stop the cluster."
      exit 0
      ;;
  esac
done

# ── Helper: wait for a deployment to be ready ─────────────────────────────────
wait_deploy() {
  local name=$1
  echo "  Waiting for deployment/$name ..."
  kubectl rollout status deployment/"$name" -n "$NAMESPACE" --timeout=120s
}

# ── 1. Ensure minikube is running ─────────────────────────────────────────────
echo "==> Checking minikube..."
if ! minikube status --format='{{.Host}}' 2>/dev/null | grep -q "Running"; then
  echo "    minikube not running — starting it..."
  minikube start
else
  echo "    minikube already running."
fi

# ── 2. Point Docker at minikube's daemon ──────────────────────────────────────
echo "==> Pointing Docker at minikube's registry..."
eval "$(minikube docker-env)"

# ── 3. Build service images ───────────────────────────────────────────────────
if $BUILD; then
  echo "==> Building service images inside minikube..."
  docker build -t order:latest   -f "$REPO_ROOT/order/Dockerfile"   "$REPO_ROOT"
  docker build -t payment:latest -f "$REPO_ROOT/payment/Dockerfile" "$REPO_ROOT"
  docker build -t stock:latest   -f "$REPO_ROOT/stock/Dockerfile"   "$REPO_ROOT"
  echo "    Images built."
fi

# ── 4. Namespace ──────────────────────────────────────────────────────────────
echo "==> Creating namespace $NAMESPACE (if needed)..."
kubectl apply -f "$K8S_DIR/namespace.yaml"

# ── 5. Redis (9 instances) ────────────────────────────────────────────────────
echo "==> Deploying Redis instances..."
kubectl apply -f "$K8S_DIR/redis/"
echo "    Waiting for Redis pods to be ready..."
for svc in order-db-0 order-db-1 order-db-2 \
           payment-db-0 payment-db-1 payment-db-2 \
           stock-db-0   stock-db-1   stock-db-2; do
  wait_deploy "$svc"
done

# ── 6. Kafka ──────────────────────────────────────────────────────────────────
echo "==> Deploying Kafka..."
kubectl apply -f "$K8S_DIR/kafka/kafka-services.yaml"
kubectl apply -f "$K8S_DIR/kafka/kafka-statefulset.yaml"

echo "    Waiting for Kafka StatefulSet to be ready (this can take ~90 s)..."
kubectl rollout status statefulset/kafka -n "$NAMESPACE" --timeout=300s

# ── 7. Kafka topic initialisation ─────────────────────────────────────────────
echo "==> Creating Kafka topics..."
kubectl delete job kafka-init -n "$NAMESPACE" --ignore-not-found
kubectl apply -f "$K8S_DIR/kafka/kafka-init-job.yaml"

echo "    Waiting for kafka-init job to complete..."
kubectl wait job/kafka-init -n "$NAMESPACE" \
  --for=condition=complete --timeout=300s

# ── 8. Orchestration ConfigMap ────────────────────────────────────────────────
echo "==> Applying orchestration ConfigMap..."
kubectl apply -f "$K8S_DIR/orchestration-configmap.yaml"

# ── 9. Microservices ──────────────────────────────────────────────────────────
echo "==> Deploying order service shards..."
kubectl apply -f "$K8S_DIR/order/"
for svc in order-shard-0 order-shard-1 order-shard-2; do wait_deploy "$svc"; done

echo "==> Deploying payment service shards..."
kubectl apply -f "$K8S_DIR/payment/"
for svc in payment-shard-0 payment-shard-1 payment-shard-2; do wait_deploy "$svc"; done

echo "==> Deploying stock service shards..."
kubectl apply -f "$K8S_DIR/stock/"
for svc in stock-shard-0 stock-shard-1 stock-shard-2; do wait_deploy "$svc"; done

# ── 10. Gateway ───────────────────────────────────────────────────────────────
echo "==> Deploying OpenResty gateway..."
kubectl apply -f "$K8S_DIR/gateway/"
wait_deploy gateway

# ── 11. Gateway port-forward (fixed port, works on macOS and Linux) ───────────
echo "==> Starting gateway port-forward on localhost:${GATEWAY_PORT}..."
stop_port_forward
kubectl port-forward -n "$NAMESPACE" svc/gateway "${GATEWAY_PORT}:80" > /dev/null 2>&1 &
echo $! > "$PF_PID_FILE"

# Wait until the port is accepting connections
echo -n "    Waiting for port-forward to be ready"
for i in $(seq 1 20); do
  if curl -sf "http://localhost:${GATEWAY_PORT}/" > /dev/null 2>&1 || \
     curl -sf "http://localhost:${GATEWAY_PORT}/orders/health" > /dev/null 2>&1; then
    break
  fi
  sleep 1
  echo -n "."
done
echo ""

# ── 12. Summary ───────────────────────────────────────────────────────────────
echo ""
echo "================================================================"
echo " Deployment complete!"
echo " Gateway: http://localhost:${GATEWAY_PORT}"
echo "================================================================"
echo ""
echo " All pods:"
kubectl get pods -n "$NAMESPACE"
echo ""
cat <<INSTRUCTIONS
================================================================
 HOW TO TEST
================================================================

The gateway is live at http://localhost:${GATEWAY_PORT}
No tunnel or extra terminal needed.

Smoke tests:

  GATEWAY=http://localhost:${GATEWAY_PORT}

  # Health checks
  curl \$GATEWAY/orders/health
  curl \$GATEWAY/payment/health
  curl \$GATEWAY/stock/health

  # Seed data (100 users/items/orders per shard)
  for N in 0 1 2; do
    curl -X POST \$GATEWAY/payment/shard/\$N/batch_init/100/10000
    curl -X POST \$GATEWAY/stock/shard/\$N/batch_init/100/100/10
    curl -X POST \$GATEWAY/orders/shard/\$N/batch_init/100/100/100/10
  done

  # Full checkout flow
  USER=\$(curl -s -X POST \$GATEWAY/payment/create_user \\
    | python3 -c "import sys,json; print(json.load(sys.stdin)['user_id'])")
  ITEM=\$(curl -s -X POST \$GATEWAY/stock/item/create/5 \\
    | python3 -c "import sys,json; print(json.load(sys.stdin)['item_id'])")
  ORDER=\$(curl -s -X POST \$GATEWAY/orders/create/\$USER \\
    | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")
  curl -X POST \$GATEWAY/stock/add/\$ITEM/100
  curl -X POST \$GATEWAY/orders/addItem/\$ORDER/\$ITEM/1
  curl -X POST \$GATEWAY/payment/add_funds/\$USER/1000
  curl -X POST \$GATEWAY/orders/checkout/\$ORDER

Automated test suite (uses the same http://localhost:${GATEWAY_PORT}):

  cd test && python -m pytest test_microservices.py -v

Fault tolerance test:

  kubectl delete pod -n dds25 -l app=order-shard-0
  kubectl get pods -n dds25 -w

Tear down (also stops the port-forward):

  ./deploy.sh --down

INSTRUCTIONS
