#!/usr/bin/env bash
# deploy.sh — Build service images and deploy the full stack to minikube.
#
# Usage:
#   ./deploy.sh           # full deploy (build + apply)
#   ./deploy.sh --no-build  # skip docker build (use existing images)
#   ./deploy.sh --down    # tear down: delete the dds25 namespace
#
# Prerequisites:
#   minikube start --memory=8192 --cpus=4
#   kubectl, docker available in PATH

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="$REPO_ROOT/k8s"
NAMESPACE="dds25"
BUILD=true

# ── Argument parsing ──────────────────────────────────────────────────────────
for arg in "$@"; do
  case $arg in
    --no-build) BUILD=false ;;
    --down)
      echo "Tearing down namespace $NAMESPACE..."
      kubectl delete namespace "$NAMESPACE" --ignore-not-found
      echo "Done."
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

# ── 1. Point Docker at minikube's daemon ─────────────────────────────────────
echo "==> Pointing Docker at minikube's registry..."
eval "$(minikube docker-env)"

# ── 2. Build service images ───────────────────────────────────────────────────
if $BUILD; then
  echo "==> Building service images inside minikube..."
  docker build -t order:latest   -f "$REPO_ROOT/order/Dockerfile"   "$REPO_ROOT"
  docker build -t payment:latest -f "$REPO_ROOT/payment/Dockerfile" "$REPO_ROOT"
  docker build -t stock:latest   -f "$REPO_ROOT/stock/Dockerfile"   "$REPO_ROOT"
  echo "    Images built."
fi

# ── 3. Namespace ─────────────────────────────────────────────────────────────
echo "==> Creating namespace $NAMESPACE (if needed)..."
kubectl apply -f "$K8S_DIR/namespace.yaml"

# ── 4. Redis (9 instances) ────────────────────────────────────────────────────
echo "==> Deploying Redis instances..."
kubectl apply -f "$K8S_DIR/redis/"
echo "    Waiting for Redis pods to be ready..."
for svc in order-db-0 order-db-1 order-db-2 \
           payment-db-0 payment-db-1 payment-db-2 \
           stock-db-0   stock-db-1   stock-db-2; do
  wait_deploy "$svc"
done

# ── 5. Kafka ─────────────────────────────────────────────────────────────────
echo "==> Deploying Kafka..."
kubectl apply -f "$K8S_DIR/kafka/kafka-services.yaml"
kubectl apply -f "$K8S_DIR/kafka/kafka-statefulset.yaml"

echo "    Waiting for Kafka StatefulSet to be ready (this can take ~60 s)..."
kubectl rollout status statefulset/kafka -n "$NAMESPACE" --timeout=300s

# ── 6. Kafka topic initialisation ────────────────────────────────────────────
echo "==> Creating Kafka topics..."
# Delete a previous (possibly failed) job run to allow re-apply
kubectl delete job kafka-init -n "$NAMESPACE" --ignore-not-found
kubectl apply -f "$K8S_DIR/kafka/kafka-init-job.yaml"

echo "    Waiting for kafka-init job to complete..."
kubectl wait job/kafka-init -n "$NAMESPACE" \
  --for=condition=complete --timeout=300s

# ── 7. Microservices ─────────────────────────────────────────────────────────
echo "==> Deploying order service shards..."
kubectl apply -f "$K8S_DIR/order/"
for svc in order-shard-0 order-shard-1 order-shard-2; do wait_deploy "$svc"; done

echo "==> Deploying payment service shards..."
kubectl apply -f "$K8S_DIR/payment/"
for svc in payment-shard-0 payment-shard-1 payment-shard-2; do wait_deploy "$svc"; done

echo "==> Deploying stock service shards..."
kubectl apply -f "$K8S_DIR/stock/"
for svc in stock-shard-0 stock-shard-1 stock-shard-2; do wait_deploy "$svc"; done

# ── 8. Gateway ───────────────────────────────────────────────────────────────
echo "==> Deploying OpenResty gateway..."
kubectl apply -f "$K8S_DIR/gateway/"
wait_deploy gateway

# ── 9. Summary ───────────────────────────────────────────────────────────────
GATEWAY_URL="$(minikube service gateway -n "$NAMESPACE" --url 2>/dev/null || true)"

echo ""
echo "================================================================"
echo " Deployment complete!"
echo "================================================================"
echo ""
echo " Pods:"
kubectl get pods -n "$NAMESPACE" -o wide
echo ""
echo " Gateway URL:  ${GATEWAY_URL:-run 'minikube service gateway -n $NAMESPACE --url'}"
echo ""
echo " Quick smoke test:"
echo "   curl \${GATEWAY_URL}/orders/health"
echo ""
echo " Seed data (example — 100 users/items per shard):"
for n in 0 1 2; do
  echo "   curl -X POST \${GATEWAY_URL}/payment/shard/${n}/batch_init/100/10000"
  echo "   curl -X POST \${GATEWAY_URL}/stock/shard/${n}/batch_init/100/100/10"
  echo "   curl -X POST \${GATEWAY_URL}/orders/shard/${n}/batch_init/100/100/100/10"
done
echo ""
echo " Fault tolerance test:"
echo "   kubectl delete pod -n $NAMESPACE -l app=order-shard-0"
echo "   # K8s restarts it; other shards continue serving traffic"
