# Running and Testing the Project

---

## Kubernetes / minikube (recommended)

### Prerequisites

- [minikube](https://minikube.sigs.k8s.io/) ≥ 1.32
- Docker Engine (used as minikube driver)
- `kubectl`

### Deploy

```bash
./deploy.sh
```

The script will:
1. Start minikube if it is not already running
2. Point Docker at minikube's internal registry
3. Build the `order`, `payment`, and `stock` images inside minikube
4. Apply all Kubernetes manifests in dependency order:
   - Namespace `dds25`
   - 9 Redis instances (one per shard per service)
   - 3-broker Kafka KRaft cluster
   - Kafka topic initialisation job (15 per-shard topics)
   - 9 service shards (order × 3, payment × 3, stock × 3)
   - OpenResty gateway (NodePort 30080)
5. Print a summary of all pods and step-by-step test instructions

Options:

| Flag | Effect |
|------|--------|
| *(none)* | Full deploy: build images + apply manifests |
| `--no-build` | Skip `docker build`; use existing images |
| `--down` | Delete the `dds25` namespace (stop all pods and services) |

> **First run** takes ~3–5 minutes (image builds + Kafka startup).
> `--no-build` reruns in ~2 minutes.

### Test

After `./deploy.sh` completes, open the gateway tunnel in a **new terminal** and keep it open:

```bash
minikube service gateway -n dds25
# Prints a URL like http://127.0.0.1:XXXXX — copy it
```

Then in another terminal:

```bash
GATEWAY=http://127.0.0.1:XXXXX   # paste URL from above

# ── Health checks ──────────────────────────────────────────────────────
curl $GATEWAY/orders/kafka_ping
curl $GATEWAY/stock/kafka_ping

# ── Seed data (100 users / items / orders per shard) ───────────────────
for N in 0 1 2; do
  curl -X POST $GATEWAY/payment/shard/$N/batch_init/100/10000
  curl -X POST $GATEWAY/stock/shard/$N/batch_init/100/100/10
  curl -X POST $GATEWAY/orders/shard/$N/batch_init/100/100/100/10
done

# ── Full checkout flow ──────────────────────────────────────────────────
USER=$(curl -s -X POST $GATEWAY/payment/create_user \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['user_id'])")

ITEM=$(curl -s -X POST $GATEWAY/stock/item/create/5 \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['item_id'])")

curl -X POST $GATEWAY/stock/add/$ITEM/100        # stock starts at 0

ORDER=$(curl -s -X POST $GATEWAY/orders/create/$USER \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")

curl -X POST $GATEWAY/orders/addItem/$ORDER/$ITEM/1
curl -X POST $GATEWAY/payment/add_funds/$USER/1000
curl -X POST $GATEWAY/orders/checkout/$ORDER
# → {"order_id":"...","status":"COMMITTED"}
```

### Fault tolerance test

```bash
# Kill a pod — Kubernetes restarts it within seconds
kubectl delete pod -n dds25 -l app=order-shard-0

# Watch the pod recover
kubectl get pods -n dds25 -w

# Traffic to the other two shards is unaffected during recovery
```

### Tear down

```bash
./deploy.sh --down          # removes the dds25 namespace
minikube stop               # optional: stop the VM
```

---

## docker-compose (single-node dev)

### Prerequisites

- Docker Engine ≥ 24 and Docker Compose v2 (`docker compose`)
- Python ≥ 3.11 with `requests` installed (for the test runner)

```bash
pip install requests
```

### Start the full stack

```bash
docker compose up --build -d
```

| Container | Role |
|-----------|------|
| `gateway` | nginx reverse proxy on `localhost:8000` |
| `order-service` | Flask + saga reaper/consumer worker |
| `order-db` | Redis for order/saga state |
| `payment-service` | Flask + Kafka command consumer |
| `payment-db` | Redis for payment/user state |
| `stock-service` | Flask + Kafka command consumer |
| `stock-db` | Redis for stock/reservation state |
| `kafka-1/2/3` | KRaft Kafka cluster (3 brokers) |
| `kafka-init` | One-shot topic creation job |

> Kafka brokers take ~60–90 s to become healthy on first run.

```bash
docker compose ps   # all services should show "Up"
```

### Smoke tests

All traffic goes through the nginx gateway on **port 8000**.

```bash
# Stock
ITEM=$(curl -s -X POST http://localhost:8000/stock/item/create/10 \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['item_id'])")
curl -s -X POST http://localhost:8000/stock/add/$ITEM/50
curl -s http://localhost:8000/stock/find/$ITEM
# → {"stock": 50, "price": 10}

# Payment
USER=$(curl -s -X POST http://localhost:8000/payment/create_user \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['user_id'])")
curl -s -X POST http://localhost:8000/payment/add_funds/$USER/100
curl -s http://localhost:8000/payment/find_user/$USER
# → {"user_id": "...", "credit": 100}

# Full checkout
ORDER=$(curl -s -X POST http://localhost:8000/orders/create/$USER \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")
curl -s -X POST http://localhost:8000/orders/addItem/$ORDER/$ITEM/2
curl -s -X POST http://localhost:8000/orders/checkout/$ORDER
# → {"order_id": "...", "status": "COMMITTED"}
```

### Run the automated test suite

```bash
cd test
python -m pytest test_microservices.py -v
```

| Test | What it covers |
|------|----------------|
| `test_stock` | CRUD for stock items via REST |
| `test_payment` | CRUD for users and payment endpoints |
| `test_order` | Order CRUD + checkout |
| `test_saga_checkout` | Full happy-path saga: checkout → COMMITTED, credit and stock deducted |
| `test_saga_checkout_insufficient_stock` | Compensation path: insufficient stock → FAILED, credit restored |

### Watch logs

```bash
docker compose logs -f order-service payment-service stock-service
```

| Log prefix | Source |
|------------|--------|
| `[order]` | order web process (gunicorn) |
| `[order-worker]` | order reaper/consumer thread |
| `[payment-worker]` | payment Kafka consumer thread |
| `[stock-worker]` | stock Kafka consumer thread |

### Stop

```bash
docker compose down       # stop containers
docker compose down -v    # also delete Kafka volumes (full reset)
```

---

## Troubleshooting

### Kubernetes

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Kafka pods in `CrashLoopBackOff` | Stale PVCs from a previous deploy | `./deploy.sh --down && ./deploy.sh` |
| `kafka-init` job never completes | Brokers not yet ready | Wait 2–3 min; check `kubectl logs job/kafka-init -n dds25` |
| Gateway returns 502 | Upstream service pod not ready | `kubectl get pods -n dds25`; wait for all `1/1 Running` |
| minikube CLI hangs | minikube container in bad state | `docker rm -f $(docker ps -q --filter name=minikube)`; `minikube delete --purge`; `minikube start` |
| Checkout returns `FAILED` | Cross-shard entity mismatch | Ensure you seed via `/shard/<N>/batch_init` or use the create endpoints so IDs are co-located |

### docker-compose

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Service exits on startup | Kafka not ready | Wait for brokers; re-run `docker compose up -d` |
| `checkout_status` stuck on `TRYING` | Consumer thread not started | Check `docker compose logs order-service` for `consumer thread started` |
| `FAILED` instead of `COMMITTED` | Deadline exceeded | Increase `CHECKOUT_DEADLINE_SECONDS` in the service environment |
