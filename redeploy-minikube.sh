#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="${NAMESPACE:-dds25}"
BUILD_UI=true
WAIT_FOR_ROLLOUT=true

usage() {
  cat <<'EOF'
Usage:
  ./redeploy-minikube.sh [--no-ui] [--no-wait]

What it does:
  1. Points Docker to Minikube's Docker daemon
  2. Rebuilds gateway/order/payment/stock images
  3. Optionally rebuilds the UI image
  4. Restarts the Kubernetes deployments that use those images
  5. Optionally waits for the rollouts to complete

Options:
  --no-ui    Skip rebuilding the ui image
  --no-wait  Restart deployments but do not wait for rollout completion

Environment:
  NAMESPACE  Kubernetes namespace to use (default: dds25)
EOF
}

for arg in "$@"; do
  case "$arg" in
    --no-ui) BUILD_UI=false ;;
    --no-wait) WAIT_FOR_ROLLOUT=false ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      usage >&2
      exit 1
      ;;
  esac
done

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
}

wait_deploy() {
  local name=$1
  echo "==> Waiting for deployment/$name"
  kubectl rollout status deployment/"$name" -n "$NAMESPACE" --timeout=180s
}

require_cmd minikube
require_cmd kubectl
require_cmd docker

echo "==> Checking minikube status"
if ! minikube status --format='{{.Host}} {{.Kubelet}} {{.APIServer}}' 2>/dev/null | grep -q 'Running Running Running'; then
  echo "Minikube is not fully running. Start it first with:" >&2
  echo "  minikube start --driver=docker" >&2
  exit 1
fi

echo "==> Pointing Docker at minikube"
eval "$(minikube docker-env)"

echo "==> Rebuilding backend images"
docker build -t order:latest -f "$REPO_ROOT/order/Dockerfile" "$REPO_ROOT"
docker build -t payment:latest -f "$REPO_ROOT/payment/Dockerfile" "$REPO_ROOT"
docker build -t stock:latest -f "$REPO_ROOT/stock/Dockerfile" "$REPO_ROOT"
docker build -t gateway:latest -f "$REPO_ROOT/gateway/Dockerfile" "$REPO_ROOT"

if $BUILD_UI; then
  echo "==> Rebuilding UI image"
  docker build -t dds-ui:latest "$REPO_ROOT/ui"
fi

echo "==> Restarting deployments"
deployments=(
  gateway-app
  order-shard-0
  order-shard-1
  order-shard-2
  payment-shard-0
  payment-shard-1
  payment-shard-2
  stock-shard-0
  stock-shard-1
  stock-shard-2
)

for deploy in "${deployments[@]}"; do
  kubectl rollout restart deployment/"$deploy" -n "$NAMESPACE"
done

if $WAIT_FOR_ROLLOUT; then
  for deploy in "${deployments[@]}"; do
    wait_deploy "$deploy"
  done
fi

echo ""
echo "==> Redeploy complete"
echo "Gateway URL:"
minikube service gateway -n "$NAMESPACE" --url
