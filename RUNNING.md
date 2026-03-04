# Running and Testing the Project

## Prerequisites

- Docker Engine ≥ 24 and Docker Compose v2 (`docker compose` — note: no hyphen)
- Python ≥ 3.11 with `requests` installed (for local test runner only)

```bash
pip install requests
```

---

## 1. Start the full stack

From the **repo root**:

```bash
docker compose up --build -d
```

This starts:
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

> **Note:** Kafka brokers have a 45-second start-up health check. the services
> will not start until all three brokers report healthy. Allow ~60-90 s on
> first run.

Check that everything is up:

```bash
docker compose ps
```

All services should show `Up` (or `Exited (0)` for `kafka-init`).

---

## 2. Smoke-test individual services

All traffic is routed through the nginx gateway on **port 8000**.

### Stock service

```bash
# Create an item (price=10)
curl -s -X POST http://localhost:8000/stock/item/create/10
# → {"item_id": "<uuid>"}

ITEM_ID=<uuid from above>

# Check initial stock
curl -s http://localhost:8000/stock/find/$ITEM_ID
# → {"stock": 0, "price": 10}

# Add stock
curl -s -X POST http://localhost:8000/stock/add/$ITEM_ID/50
# → 200 OK

# Verify
curl -s http://localhost:8000/stock/find/$ITEM_ID
# → {"stock": 50, "price": 10}

# Subtract (valid)
curl -s -X POST http://localhost:8000/stock/subtract/$ITEM_ID/10
# → 200 OK

# Subtract too much (should fail)
curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8000/stock/subtract/$ITEM_ID/999
# → 400
```

### Payment service

```bash
# Create a user
curl -s -X POST http://localhost:8000/payment/create_user
# → {"user_id": "<uuid>"}

USER_ID=<uuid from above>

# Add funds
curl -s -X POST http://localhost:8000/payment/add_funds/$USER_ID/100
# → 200 OK

# Check balance
curl -s http://localhost:8000/payment/find_user/$USER_ID
# → {"user_id": "...", "credit": 100}
```

### Order service

```bash
# Create an order
curl -s -X POST http://localhost:8000/orders/create/$USER_ID
# → {"order_id": "<uuid>"}

ORDER_ID=<uuid from above>

# Add item to order (quantity 2)
curl -s -X POST http://localhost:8000/orders/addItem/$ORDER_ID/$ITEM_ID/2
# → 200 OK
```

### Kafka connectivity

```bash
# Verify Kafka producer is reachable from each service
curl -s http://localhost:8000/stock/kafka_ping
curl -s http://localhost:8000/payment/kafka_ping
# Both should return {"status":"sent","message_id":"...","transaction_id":"..."}
```

---

## 3. Run the saga checkout (end-to-end Kafka flow)

This exercises the full saga: order → stock reserve + payment reserve → commit → order marked paid.

```bash
# 1. Set up (reuse IDs from section 2, or re-create)
USER_ID=$(curl -s -X POST http://localhost:8000/payment/create_user | python3 -c "import sys,json; print(json.load(sys.stdin)['user_id'])")
curl -s -X POST http://localhost:8000/payment/add_funds/$USER_ID/100

ITEM_ID=$(curl -s -X POST http://localhost:8000/stock/item/create/10 | python3 -c "import sys,json; print(json.load(sys.stdin)['item_id'])")
curl -s -X POST http://localhost:8000/stock/add/$ITEM_ID/5

ORDER_ID=$(curl -s -X POST http://localhost:8000/orders/create/$USER_ID | python3 -c "import sys,json; print(json.load(sys.stdin)['order_id'])")
curl -s -X POST http://localhost:8000/orders/addItem/$ORDER_ID/$ITEM_ID/2

# 2. Trigger checkout — returns 200 with status TRYING
curl -s -X POST http://localhost:8000/orders/checkout/$ORDER_ID
# → {"order_id": "...", "status": "TRYING"}

# 3. Poll for saga completion (repeat until status != TRYING)
curl -s http://localhost:8000/orders/checkout_status/$ORDER_ID
# → {"order_id": "...", "status": "COMMITTED"}

# 4. Verify side effects
curl -s http://localhost:8000/stock/find/$ITEM_ID       # stock should be 3 (was 5, ordered 2)
curl -s http://localhost:8000/payment/find_user/$USER_ID # credit should be 80 (was 100, spent 20)
```

---

## 4. Run the automated test suite

From the **`test/`** directory:

```bash
cd test
python -m pytest test_microservices.py -v
```

Or with plain `unittest`:

```bash
cd test
python -m unittest test_microservices -v
```

### Test descriptions

| Test | What it covers |
|------|---------------|
| `test_stock` | CRUD for stock items via REST |
| `test_payment` | CRUD for users and REST payment endpoint |
| `test_order` | Order CRUD + REST-path checkout (no Kafka) |
| `test_saga_checkout` | **Full happy-path saga**: checkout → COMMITTED, credit and stock deducted |
| `test_saga_checkout_insufficient_stock` | **Compensation path**: insufficient stock → saga FAILED, credit fully restored |

> `test_saga_checkout` and `test_saga_checkout_insufficient_stock` poll
> `checkout_status` for up to 15 seconds waiting for the saga to terminate.
> If Kafka is not running these tests will raise `TimeoutError`.

---

## 5. Watch logs

```bash
# All services together
docker compose logs -f

# Only saga-relevant services
docker compose logs -f order-service payment-service stock-service

# Just one service
docker compose logs -f stock-service
```

Log prefixes help distinguish processes running inside the same container:

| Prefix | Source |
|--------|--------|
| `[order]` | order web process (gunicorn) |
| `[order-worker]` | order reaper/consumer thread |
| `[payment]` | payment web process |
| `[payment-worker]` | payment Kafka consumer thread |
| `[stock]` | stock web process |
| `[stock-worker]` | stock Kafka consumer thread |

---

## 6. Stop the stack

```bash
docker compose down
```

To also remove persistent Kafka volumes (full reset):

```bash
docker compose down -v
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `stock-service` exits on startup | Missing `KAFKA_BOOTSTRAP` env or brokers not ready | Check `docker compose ps`; wait for brokers to be healthy |
| `checkout_status` stuck on `TRYING` | `stock.worker` or `payment.worker` consumer not running | Check `docker compose logs stock-service payment-service` for `consumer thread started` message |
| `FAILED` instead of `COMMITTED` | Deadline exceeded before both reserves arrived | Increase `CHECKOUT_DEADLINE_SECONDS` (default 5 s) in order-service environment |
| `kafka_ping` returns 500 | Kafka producer cannot reach brokers | Verify `kafka-net` network; run `docker compose up -d kafka-1 kafka-2 kafka-3 kafka-init` first |
| Tests get 404 on `/orders/checkout_status` | Old order-service image without the saga worker | Rebuild with `docker compose build order-service` |
