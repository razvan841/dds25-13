# Architecture

Distributed e-commerce system built with Python/Flask and Redis. Three microservices — **order**, **stock**, and **payment** — each run 5 sharded instances behind an NGINX gateway on port 8000.

## System Topology

**36 containers total:**

| Category | Containers | Count |
|----------|-----------|-------|
| Gateway | `gateway` (NGINX) | 1 |
| App services | `{order,stock,payment}-service-{0..4}` | 15 |
| Business DBs | `{order,stock,payment}-db-{0..4}` (Redis 7.2) | 15 |
| Saga brokers | `saga-redis-{0..4}` (Redis 7.2) | 5 |

Each app service runs Gunicorn with 2 gevent workers (1000 connections/worker, 30s timeout) on port 5000 internally.

Business Redis instances have 512MB max memory; saga-redis instances have 256MB.

## Data Models

All models are serialized with MessagePack via `msgspec`.

- **OrderValue**: `paid: bool`, `items: list[tuple[str, int]]`, `user_id: str`, `total_cost: int`
- **StockValue**: `stock: int`, `price: int`
- **UserValue**: `credit: int`

## Sharding

Requests are routed to the correct shard via CRC32-based hashing, compatible with NGINX's `hash` directive (Cache::Memcached algorithm):

```python
def compute_shard(key: str, num_shards: int) -> int:
    crc = binascii.crc32(key.encode()) & 0xFFFFFFFF
    return ((crc >> 16) & 0x7FFF) % num_shards
```

NGINX extracts the resource ID from the URL (e.g., `/stock/find/<id>`) and hashes it to select an upstream server. The Python services use the same hash function for inter-service stream routing.

**Shard-affine UUID generation** (`common/streams.py`): When creating a resource, the service brute-forces UUIDs until one hashes to the current shard. This guarantees future requests for that resource route back to the same instance (~`num_shards` iterations on average).

**Batch init broadcast**: `batch_init` endpoints are mirrored to all shards via NGINX `mirror` directives — shard 0 is the primary target; shards 1–4 receive mirrored copies.

## REST API

### Order Service (`/orders/`)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/create/<user_id>` | Create order with shard-affine UUID |
| GET | `/find/<order_id>` | Fetch order |
| POST | `/addItem/<order_id>/<item_id>/<quantity>` | Add item (calls `/stock/find/` via gateway) |
| POST | `/checkout/<order_id>` | Checkout (saga or 2PC based on `CHECKOUT_MODE`) |
| POST | `/batch_init/<n>/<n_items>/<n_users>/<item_price>` | Bulk create orders |

### Stock Service (`/stock/`)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/item/create/<price>` | Create item with shard-affine UUID |
| GET | `/find/<item_id>` | Fetch item (stock + price) |
| POST | `/add/<item_id>/<amount>` | Add stock |
| POST | `/subtract/<item_id>/<amount>` | Subtract stock |
| POST | `/batch_init/<n>/<starting_stock>/<item_price>` | Bulk create items |

### Payment Service (`/payment/`)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/create_user` | Create user with shard-affine UUID |
| GET | `/find_user/<user_id>` | Fetch user credit |
| POST | `/add_funds/<user_id>/<amount>` | Add credit |
| POST | `/pay/<user_id>/<amount>` | Deduct credit |
| POST | `/batch_init/<n>/<starting_money>` | Bulk create users |

## Inter-Service Messaging

Services communicate asynchronously via Redis Streams on the sharded `saga-redis` instances.

**Stream names** (per shard):
- `stock-commands-{shard_id}` — commands to stock service
- `payment-commands-{shard_id}` — commands to payment service
- `saga-replies-{shard_id}` — replies back to order service

**Consumer groups**: `stock-workers`, `payment-workers`, `orchestrator-workers`

**Connection pool** (`common/streams.py`): `init_saga_pool()` creates one Redis connection per shard using the `SAGA_REDIS_HOST_TEMPLATE` (`saga-redis-{}`). All publish functions transparently route to the correct connection by extracting the shard index from the stream name suffix.

**Command message fields**: `saga_id`, `idempotency_key`, `command`, `payload`, `reply_stream`

**Reply message fields**: `saga_id`, `idempotency_key`, `command`, `status`, `reason`

The consumer loop uses `XREADGROUP` with 1s block timeout and processes 10 messages per call. Consumer names follow the format `{hostname}-{pid}`.

## Saga Protocol (`CHECKOUT_MODE=saga`)

Orchestrated saga with compensating transactions for checkout. The order service acts as the orchestrator.

### State Machine

```
STARTED → STOCK_SUBTRACTING → PAYMENT_PENDING → COMPLETED
                ↓                    ↓
          STOCK_COMPENSATING ← ← ← ←
                ↓
             FAILED
```

Stored as `saga:{order_id}` in order-db (msgpack-encoded `SagaState`).

### Flow

1. **STARTED → STOCK_SUBTRACTING**: Sends `stock_subtract` commands sequentially, one item at a time, to the correct stock shard's stream based on `compute_shard(item_id)`.
2. **STOCK_SUBTRACTING → PAYMENT_PENDING**: After all items subtracted, sends `payment_pay` to the payment shard for the order's `user_id`.
3. **PAYMENT_PENDING → COMPLETED**: Payment succeeds → marks order as paid.
4. **Failure at any step → STOCK_COMPENSATING**: Publishes `stock_add` for each previously subtracted item as compensation.
5. **STOCK_COMPENSATING → FAILED**: After all compensations acknowledged.

**Commands**: `stock_subtract`, `stock_add`, `payment_pay`, `payment_refund`

**Cross-shard example**: An order on shard 0 subtracting an item on shard 2 sends a `stock_subtract` to `stock-commands-2` with `reply_stream=saga-replies-0`. Stock shard 2 processes it and replies on `saga-replies-0`.

**Polling**: The checkout endpoint polls for completion at 50ms intervals with a 10s timeout.

## 2PC Protocol (`CHECKOUT_MODE=2pc`)

Two-Phase Locking + Two-Phase Commit for checkout. Provides stronger consistency than sagas at the cost of lock contention.

### State Machine

```
PREPARING → COMMITTING → COMMITTED
     ↓
  ABORTING → FAILED (or retry on lock_contention)
```

Stored as `tpc:{order_id}` in order-db (msgpack-encoded `TpcState`).

### Phase 1: Prepare

1. Items grouped by stock shard. One `stock_prepare` sent per involved shard (containing only that shard's items) + one `payment_prepare`.
2. `participant_count = distinct_stock_shards + 1` (payment shard).
3. Each stock participant acquires locks (`SET lock:{item_id} {txn_id} NX PX 30000`) and validates stock availability. Replies `VOTE-COMMIT` or `VOTE-ABORT`.
4. Payment participant validates credit. Replies `VOTE-COMMIT` or `VOTE-ABORT`.
5. Votes collected via atomic `INCR` counter on `tpc:{order_id}:vote_count`.

### Phase 2: Commit or Abort

- **All VOTE-COMMIT** → `COMMITTING`: Sends `stock_commit` / `payment_commit` to each participant. Stock commit decrements inventory (with `WATCH`-based optimistic locking) and releases locks.
- **Any VOTE-ABORT** → `ABORTING`: Sends `stock_abort` / `payment_abort` to release locks without data changes.
- Deterministic idempotency keys for crash-safe re-send: `tpc-{txn_id}-stock_commit-{shard_id}`, `tpc-{txn_id}-payment_commit`, etc.

### Lock Contention Retry

When abort reason is `lock_contention`:
- Retries with exponential backoff: `0.1 * 2^retry_count + random(0, 0.05)` seconds
- Max 5 retries, each with a new `txn_id` (new lock owner)
- Retries run in a daemon thread
- Non-retryable failures (`insufficient_stock`, `insufficient_credit`) go directly to `FAILED`

**Polling**: Same as saga — 50ms intervals, 10s timeout.

## Fault Tolerance

### Idempotency

Every command carries a UUID `idempotency_key`. The consumer checks `SET idempotency:{key} "processing" NX EX 3600` before processing. If the key already exists, the stored reply is re-sent instead of reprocessing. TTL: 1 hour.

### Dead Consumer Recovery

`consume_loop` calls `XAUTOCLAIM` at startup with `min_idle_time=5000ms` to reclaim messages pending for 5+ seconds from dead consumers. Null-fielded messages (deleted entries) are immediately ACKed and skipped.

### Startup Recovery

On each worker startup, guarded by a Redis lock (`recovery_lock`, TTL 30s):

**Saga recovery** (`recover_sagas`):
- `STARTED` → marked `FAILED`
- `STOCK_SUBTRACTING` / `PAYMENT_PENDING` → triggers compensation
- `STOCK_COMPENSATING` → left for XAUTOCLAIM

**2PC recovery** (`recover_tpcs`):
- `RETRY_PENDING` → marked `FAILED`
- `PREPARING` → sends abort
- `COMMITTING` → re-sends commits (deterministic idempotency keys)
- `ABORTING` → re-sends aborts

All containers use `restart: always`.

## Module Structure

```
common/
  streams.py          — Redis Streams helpers, connection pool, idempotency, locks, shard routing

order/
  app.py              — REST endpoints, DB setup, consumer thread, recovery on startup
  saga_orchestrator.py — saga state machine (start, advance, poll, recover)
  tpc_orchestrator.py  — 2PC state machine (start, handle votes, poll, recover)

stock/
  app.py              — REST endpoints, DB setup, consumer thread
  models.py           — StockValue struct
  saga_handler.py     — stock_subtract, stock_add handlers
  tpc_handler.py      — stock_prepare, stock_commit, stock_abort handlers

payment/
  app.py              — REST endpoints, DB setup, consumer thread
  models.py           — UserValue struct
  saga_handler.py     — payment_pay, payment_refund handlers
  tpc_handler.py      — payment_prepare, payment_commit, payment_abort handlers
```

Each `app.py` starts a background thread running `consume_loop` for its service's command stream.

## Deployment

### Docker Compose

```bash
docker-compose up --build      # start all 36 containers
docker-compose up --build -d   # background
docker-compose down            # tear down
```

### Kubernetes

```bash
bash deploy-charts-minikube.sh    # Minikube (Redis only)
bash deploy-charts-cluster.sh     # Cloud (Redis + NGINX Ingress)
kubectl apply -f k8s/             # App manifests
```

### Configuration

Environment files in `env/`:
- `checkout.env` — `CHECKOUT_MODE=saga` (or `2pc`)
- `saga_redis.env` — saga-redis connection + `SAGA_REDIS_HOST_TEMPLATE=saga-redis-{}`
- `{order,stock,payment}_redis.env` — per-service Redis connection (overridden per shard in docker-compose)
