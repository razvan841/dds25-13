# Architecture

Distributed e-commerce system built with Python/Flask and Redis. Four microservices — **order**, **stock**, **payment**, and **orchestrator** — each run 5 sharded instances behind an NGINX gateway on port 8000.

## System Topology

**46 containers total:**

| Category | Containers | Count |
|----------|-----------|-------|
| Gateway | `gateway` (NGINX) | 1 |
| App services | `{order,stock,payment,orchestrator}-service-{0..4}` | 20 |
| Business DBs | `{order,stock,payment,orchestrator}-db-{0..4}` (Redis 7.2) | 20 |
| Saga brokers | `saga-redis-{0..4}` (Redis 7.2) | 5 |

Each app service runs Gunicorn with 2 gevent workers (1000 connections/worker, 30s timeout) on port 5000 internally.

Business Redis instances have 512MB max memory; saga-redis instances have 256MB.

## Data Models

All models are serialized with MessagePack via `msgspec`.

- **OrderValue**: `paid: bool`, `items: list[tuple[str, int]]`, `user_id: str`, `total_cost: int`
- **StockValue**: `stock: int`, `price: int`
- **UserValue**: `credit: int`
- **TaskState** (orchestrator): `task_id`, `saga_id`, `command`, `resource_id`, `target`, `payload`, `status`, `result_status`, `result_reason`, `idempotency_key`, `retry_count`, `max_retries`, `created_at`, `updated_at`

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

### Orchestrator Service

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check (returns shard_id) |

The orchestrator has no external-facing REST API. It operates entirely via Redis Streams.

## Inter-Service Messaging

Services communicate asynchronously via Redis Streams on the sharded `saga-redis` instances.

### Streams (per shard)

| Stream | Producer | Consumer | Purpose |
|--------|----------|----------|---------|
| `orchestrator-commands-{shard_id}` | Order service | Orchestrator | Task submissions |
| `stock-commands-{shard_id}` | Orchestrator | Stock service | Stock operations |
| `payment-commands-{shard_id}` | Orchestrator | Payment service | Payment operations |
| `saga-replies-{shard_id}` | Stock/Payment | Orchestrator | Command results |
| `order-replies-{shard_id}` | Orchestrator | Order service | Forwarded results |

### Consumer Groups

| Group | Stream | Service |
|-------|--------|---------|
| `orchestrator-task-workers` | `orchestrator-commands-{shard_id}` | Orchestrator |
| `orchestrator-reply-workers` | `saga-replies-{shard_id}` | Orchestrator |
| `stock-workers` | `stock-commands-{shard_id}` | Stock |
| `payment-workers` | `payment-commands-{shard_id}` | Payment |
| `order-result-workers` | `order-replies-{shard_id}` | Order |

### Connection Pool

`init_saga_pool()` in `common/streams.py` creates one Redis connection per shard using `SAGA_REDIS_HOST_TEMPLATE` (`saga-redis-{}`). All publish functions transparently route to the correct connection by extracting the shard index from the stream name suffix.

### Message Formats

**Task submission** (order → orchestrator via `orchestrator-commands`):

| Field | Type | Description |
|-------|------|-------------|
| `task_id` | UUID | Unique task identifier (generated by `submit_task`) |
| `saga_id` | string | Order ID for correlation |
| `command` | string | e.g., `stock_subtract`, `payment_pay` |
| `resource_id` | string | Item ID or user ID (used to compute target shard) |
| `target` | string | `"stock"` or `"payment"` |
| `payload` | JSON string | Command-specific payload |
| `idempotency_key` | string | Optional caller-provided key (used by 2PC for deterministic keys) |

**Command** (orchestrator → stock/payment via `stock-commands`/`payment-commands`):

| Field | Type | Description |
|-------|------|-------------|
| `saga_id` | string | Order ID for correlation |
| `idempotency_key` | UUID | Unique per command for deduplication |
| `command` | string | Operation name |
| `payload` | JSON string | Command-specific payload |
| `reply_stream` | string | Where to send the reply (e.g., `saga-replies-0`) |

**Reply** (stock/payment → orchestrator via `saga-replies`):

| Field | Type | Description |
|-------|------|-------------|
| `saga_id` | string | Order ID for correlation |
| `idempotency_key` | string | Matches the command's key |
| `command` | string | Echoes the original command name |
| `status` | string | `"success"`, `"failure"`, `"VOTE-COMMIT"`, `"VOTE-ABORT"` |
| `reason` | string | Error reason on failure (e.g., `"insufficient_stock"`) |

**Result** (orchestrator → order via `order-replies`):

| Field | Type | Description |
|-------|------|-------------|
| `task_id` | string | Task ID from the original submission |
| `saga_id` | string | Order ID for correlation |
| `command` | string | Echoes the original command name |
| `status` | string | `"success"` or `"failure"` |
| `reason` | string | Error reason on failure |

The consumer loop uses `XREADGROUP` with 1s block timeout and processes 10 messages per call. Consumer names follow the format `{hostname}-{pid}`.

## Orchestrator Service

The orchestrator is a **task execution service** that sits between the order service and the stock/payment services. It guarantees reliable delivery of commands with persistence, retries, and crash recovery. Neither the saga nor 2PC state machine talks directly to stock/payment — all commands are routed through the orchestrator.

### Why It Exists

Without the orchestrator, the order service would publish commands directly to stock/payment streams and consume replies on `saga-replies`. This creates a reliability gap: if the order service crashes after publishing a command but before persisting that it did so, the reply arrives to a consumer that has no record of the pending operation. The orchestrator closes this gap by:

1. **Persisting every task** in its own Redis before publishing the command
2. **Tracking task state** through a lifecycle (`PENDING → SENT → COMPLETED/FAILED`)
3. **Retrying stale tasks** that never received a reply (30s timeout, max 3 retries)
4. **Re-publishing on crash recovery** so no command is ever silently lost

### Architecture

Each orchestrator shard runs two consumer threads and one background thread:

```
orchestrator-service-{shard_id}
├── Thread 1: consume_loop(orchestrator-commands-{shard_id})
│   └── Receives task submissions from order service
│   └── Persists TaskState to orchestrator-db
│   └── Publishes commands to stock/payment streams
│
├── Thread 2: consume_loop(saga-replies-{shard_id})
│   └── Receives replies from stock/payment services
│   └── Updates TaskState to COMPLETED
│   └── Forwards results to order-replies-{shard_id}
│
└── Thread 3: retry_loop (every 10s)
    └── Scans for SENT tasks older than 30s
    └── Re-publishes up to max_retries (3)
    └── Marks as FAILED after max retries exceeded
```

### Task Lifecycle

```
PENDING → SENT → COMPLETED
              ↓
            (30s timeout, retry up to 3x)
              ↓
           FAILED (max_retries_exceeded)
```

**State transitions in detail:**

1. **PENDING**: Task received from order service, persisted to `task:{task_id}` in orchestrator-db. A reverse index `idem:{idempotency_key} → task_id` is created for reply correlation.
2. **SENT**: Command published to the target service's stream. State is saved *before* publishing (persist-then-publish) for crash safety.
3. **COMPLETED**: Reply received from stock/payment. Result status and reason stored on the task. Result forwarded to `order-replies-{shard_id}`.
4. **FAILED**: Task exceeded max retries (3 attempts) without a reply. A synthetic failure result is forwarded to the order service.

### Redis Keys (orchestrator-db)

| Key Pattern | Value | Description |
|-------------|-------|-------------|
| `task:{task_id}` | msgpack-encoded `TaskState` | Full task state |
| `idem:{idempotency_key}` | `task_id` string (TTL 1h) | Reverse index for reply lookup |
| `saga_tasks:{saga_id}` | Redis SET of `task_id`s | All tasks belonging to a saga/tpc |
| `recovery_lock` | `"1"` (TTL 30s) | Guards startup recovery from concurrent workers |

### Reply Correlation

When a reply arrives on `saga-replies-{shard_id}`, the orchestrator looks up the task via `idem:{idempotency_key} → task_id`. This works because:
- Each command published by the orchestrator carries the task's `idempotency_key`
- Stock/payment services echo the `idempotency_key` back in their reply
- The reverse index maps it back to the originating task

If no task is found (e.g., during migration from a non-orchestrator setup), the reply is forwarded directly to the order service with an empty `task_id`.

### Crash Recovery

On startup (guarded by `recovery_lock` with 30s TTL), the orchestrator scans all `task:*` keys:

- **PENDING tasks**: Crashed before publishing → publish now
- **SENT tasks**: Crashed after publishing → re-publish (idempotent downstream due to `idempotency_key`)
- **COMPLETED/FAILED**: No action needed

This is safe because downstream services deduplicate via `SET idempotency:{key} NX`. A re-published command either gets processed (if the original was lost) or triggers a stored-reply re-send (if already processed).

### Retry Logic

A background thread runs every 10 seconds, scanning all `task:*` keys for SENT tasks whose `updated_at` is older than 30 seconds:

- If `retry_count < max_retries` (3): increment count, re-publish command, update `updated_at`
- If `retry_count >= max_retries`: mark FAILED, forward synthetic failure to order service with reason `"max_retries_exceeded"`

## Message Flow Overview

The complete path of a command from order service to stock/payment and back:

```
Order Service                Orchestrator              Stock/Payment
     │                            │                         │
     │ submit_task()              │                         │
     │──── XADD ────────────────►│                         │
     │  orchestrator-commands     │                         │
     │                            │ persist TaskState       │
     │                            │ (PENDING → SENT)        │
     │                            │                         │
     │                            │ publish_command()       │
     │                            │──── XADD ─────────────►│
     │                            │ stock/payment-commands   │
     │                            │                         │
     │                            │                         │ process command
     │                            │                         │ check idempotency
     │                            │       publish_reply()   │
     │                            │◄──── XADD ─────────────│
     │                            │    saga-replies          │
     │                            │                         │
     │                            │ lookup task by idem key │
     │                            │ mark COMPLETED          │
     │    forward result          │                         │
     │◄──── XADD ────────────────│                         │
     │    order-replies           │                         │
     │                            │                         │
     │ saga/tpc state machine     │                         │
     │ advances based on result   │                         │
```

## Saga Protocol (`CHECKOUT_MODE=saga`)

Orchestrated saga with compensating transactions for checkout. The order service manages the saga state machine and submits individual tasks to the orchestrator for reliable execution.

### State Machine

```
STARTED → STOCK_SUBTRACTING → PAYMENT_PENDING → COMPLETED
                ↓                    ↓
          STOCK_COMPENSATING ← ← ← ←
                ↓
             FAILED
```

Stored as `saga:{order_id}` in order-db (msgpack-encoded `SagaState`).

### SagaState Fields

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Current state machine state |
| `order_id` | string | Order identifier (also saga_id) |
| `user_id` | string | User to charge |
| `total_cost` | int | Amount to charge |
| `items` | list[tuple[str, int]] | (item_id, quantity) pairs to subtract |
| `subtracted_items` | list[tuple[str, int]] | Items successfully subtracted (for compensation) |
| `current_item_index` | int | Index into `items` for sequential processing |
| `compensations_expected` | int | Number of `stock_add` compensations to send |
| `compensations_received` | int | Atomic counter of compensation acks received |
| `error` | string | Failure reason |

### Saga Commands

| Command | Target | Payload | Reply Status |
|---------|--------|---------|-------------|
| `stock_subtract` | stock | `{"item_id": str, "quantity": int}` | `success` / `failure` |
| `stock_add` | stock | `{"item_id": str, "quantity": int}` | `success` |
| `payment_pay` | payment | `{"user_id": str, "amount": int}` | `success` / `failure` |
| `payment_refund` | payment | `{"user_id": str, "amount": int}` | `success` |

### Detailed Saga Flow

1. **Checkout request arrives** → `start_checkout()` creates `SagaState` with status `STARTED` and calls `_advance_saga()`.

2. **STARTED → STOCK_SUBTRACTING**: If items exist, sets `current_item_index = 0` and submits `stock_subtract` for `items[0]` via `submit_task()` to the orchestrator. If no items, skips directly to `PAYMENT_PENDING`.

3. **STOCK_SUBTRACTING — sequential item subtraction**: On each successful `stock_subtract` reply:
   - Appends the item to `subtracted_items`
   - Increments `current_item_index`
   - If more items remain: submits `stock_subtract` for the next item
   - If all items subtracted: transitions to `PAYMENT_PENDING`, submits `payment_pay`

4. **PAYMENT_PENDING → COMPLETED**: On successful `payment_pay` reply, marks saga as `COMPLETED` and calls `_mark_order_paid()` to set `order.paid = True`.

5. **Failure at any step → STOCK_COMPENSATING**: On any `failure` reply (from `stock_subtract` or `payment_pay`):
   - If `subtracted_items` is empty: transitions directly to `FAILED`
   - Otherwise: submits `stock_add` for each previously subtracted item and tracks completions via an atomic `INCR` counter on `saga:{order_id}:comp_count`

6. **STOCK_COMPENSATING → FAILED**: When all compensation acks received (`comp_count >= compensations_expected`), marks saga as `FAILED`.

### Cross-Shard Example

An order on shard 0 with items on shards 0 and 2:

```
Order-0                    Orchestrator-0           Stock-0    Stock-2
   │                            │                      │          │
   │ submit_task(stock_subtract │                      │          │
   │   item_A, target=stock)    │                      │          │
   │────────────────────────────►                      │          │
   │                            │ compute_shard(A) = 0 │          │
   │                            │────────────────────► │          │
   │                            │  stock-commands-0    │          │
   │                            │                      │ reply    │
   │                            │◄─────────────────────│          │
   │                            │  saga-replies-0      │          │
   │◄───────────────────────────│                      │          │
   │  order-replies-0           │                      │          │
   │                            │                      │          │
   │ submit_task(stock_subtract │                      │          │
   │   item_B, target=stock)    │                      │          │
   │────────────────────────────►                      │          │
   │                            │ compute_shard(B) = 2 │          │
   │                            │────────────────────────────────►│
   │                            │  stock-commands-2    │          │
   │                            │                      │  reply   │
   │                            │◄────────────────────────────────│
   │                            │  saga-replies-0      │          │
   │◄───────────────────────────│                      │          │
   │  order-replies-0           │                      │          │
```

Note: The orchestrator always publishes to the target service's shard based on `compute_shard(resource_id)`, but replies always come back to `saga-replies-{orchestrator_shard_id}` (the orchestrator's own shard). The orchestrator then forwards to `order-replies-{shard_id}`.

### Polling

The checkout endpoint calls `poll_result()` which checks `saga:{order_id}` status at 50ms intervals with a 10s timeout.

## 2PC Protocol (`CHECKOUT_MODE=2pc`)

Two-Phase Locking + Two-Phase Commit for checkout. Provides stronger consistency than sagas at the cost of lock contention. Like sagas, the order service manages the 2PC state machine and submits individual tasks to the orchestrator.

### State Machine

```
PREPARING → COMMITTING → COMMITTED
     ↓
  ABORTING → RETRY_PENDING (lock_contention) → new PREPARING
     ↓
   FAILED / ABORTED
```

Stored as `tpc:{order_id}` in order-db (msgpack-encoded `TpcState`).

### TpcState Fields

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Current state machine state |
| `order_id` | string | Order identifier |
| `txn_id` | UUID | Unique per attempt (serves as lock owner identity) |
| `user_id` | string | User to charge |
| `total_cost` | int | Amount to charge |
| `items` | list[tuple[str, int]] | (item_id, quantity) pairs |
| `stock_vote` | string | `""`, `"VOTE-COMMIT"`, `"VOTE-ABORT"` |
| `payment_vote` | string | Same as stock_vote |
| `retry_count` | int | Current retry attempt |
| `max_retries` | int | 5 |
| `abort_reason` | string | e.g., `"lock_contention"`, `"insufficient_stock"` |
| `error` | string | Final error message |
| `participant_count` | int | Total votes expected (distinct stock shards + 1 payment) |

### 2PC Commands

| Command | Target | Payload | Reply Status |
|---------|--------|---------|-------------|
| `stock_prepare` | stock | `{"items": [(id, qty), ...], "txn_id": str}` | `VOTE-COMMIT` / `VOTE-ABORT` |
| `stock_commit` | stock | `{"items": [(id, qty), ...], "txn_id": str}` | `success` |
| `stock_abort` | stock | `{"items": [(id, qty), ...], "txn_id": str}` | `success` |
| `payment_prepare` | payment | `{"user_id": str, "amount": int, "txn_id": str}` | `VOTE-COMMIT` / `VOTE-ABORT` |
| `payment_commit` | payment | `{"user_id": str, "amount": int, "txn_id": str}` | `success` |
| `payment_abort` | payment | `{"user_id": str, "txn_id": str}` | `success` |

### Phase 1: Prepare

1. Items are grouped by stock shard using `compute_shard(item_id)`. One `stock_prepare` is submitted per involved shard (containing only that shard's items) plus one `payment_prepare`.
2. `participant_count = distinct_stock_shards + 1` (payment shard).
3. Each stock participant acquires locks (`SET lock:{item_id} {txn_id} NX PX 30000`) and validates stock availability. Replies `VOTE-COMMIT` or `VOTE-ABORT`.
4. Payment participant validates credit. Replies `VOTE-COMMIT` or `VOTE-ABORT`.
5. Votes are collected via atomic `INCR` counter on `tpc:{order_id}:vote_count`. Any `VOTE-ABORT` records the abort reason. Once all votes are in, the decision is made.

### Phase 2: Commit or Abort

- **All VOTE-COMMIT** → `COMMITTING`: Submits `stock_commit` / `payment_commit` to each participant with **deterministic idempotency keys** (e.g., `tpc-{txn_id}-stock_commit-{shard_id}`, `tpc-{txn_id}-payment_commit`). Stock commit decrements inventory using `WATCH`-based optimistic locking and releases locks. Completions tracked via `tpc:{order_id}:commit_count`.
- **Any VOTE-ABORT** → `ABORTING`: Submits `stock_abort` / `payment_abort` to release locks without data changes. Uses deterministic idempotency keys (e.g., `tpc-{txn_id}-stock_abort-{shard_id}`).

### Lock Contention Retry

When abort reason is `lock_contention`:
- Transitions to `RETRY_PENDING`, spawns a daemon thread with exponential backoff: `0.1 * 2^retry_count + random(0, 0.05)` seconds
- After the delay, assigns a new `txn_id` (new lock owner) and restarts from `PREPARING`
- Max 5 retries; non-retryable failures (`insufficient_stock`, `insufficient_credit`) go directly to `FAILED`

### Deterministic Idempotency Keys (2PC)

2PC uses caller-provided idempotency keys passed to `submit_task()` instead of auto-generated UUIDs. This ensures crash-safe re-send: if the order service crashes during `COMMITTING` and recovers, it re-submits with the same deterministic keys. The orchestrator creates new tasks for these, but downstream services deduplicate via idempotency checks.

Key patterns:
- `tpc-{txn_id}-stock_commit-{shard_id}`
- `tpc-{txn_id}-stock_abort-{shard_id}`
- `tpc-{txn_id}-payment_commit`
- `tpc-{txn_id}-payment_abort`

### Polling

Same as saga — `poll_result()` checks `tpc:{order_id}` at 50ms intervals, 10s timeout.

## Order Service Reply Dispatching

The order service consumes `order-replies-{shard_id}` in a single consumer thread. It dispatches replies to the correct handler based on the `command` field:

- Commands in `TPC_COMMANDS` (`stock_prepare`, `stock_commit`, `stock_abort`, `payment_prepare`, `payment_commit`, `payment_abort`) → `tpc_orchestrator.handle_tpc_reply()`
- All other commands → `saga_orchestrator.handle_saga_reply()`

This allows both checkout modes to coexist on the same stream infrastructure.

## Fault Tolerance

### Idempotency

Every command carries a UUID `idempotency_key`. The consumer checks `SET idempotency:{key} "processing" NX EX 3600` before processing. If the key already exists, the stored reply is re-sent instead of reprocessing. TTL: 1 hour.

The idempotency lifecycle:
1. `check_idempotency()`: `SET NX` with value `"processing"` — returns True if new
2. Process the command
3. `publish_reply_with_idempotency()`: overwrites value with JSON-encoded reply data, then publishes reply
4. On duplicate delivery: `get_idempotency_state()` returns `"done"` + stored reply JSON → re-send stored reply

### Dead Consumer Recovery

`consume_loop` calls `XAUTOCLAIM` at startup with `min_idle_time=5000ms` to reclaim messages pending for 5+ seconds from dead consumers. Null-fielded messages (deleted entries) are immediately ACKed and skipped.

### Startup Recovery

On each worker startup, guarded by a Redis lock (`recovery_lock`, TTL 30s):

**Orchestrator recovery** (`recover_tasks` in `task_manager.py`):
- Scans all `task:*` keys
- `PENDING` → publishes command (crashed before publishing)
- `SENT` → re-publishes command (crashed after publishing, idempotent downstream)
- `COMPLETED` / `FAILED` → no action

**Saga recovery** (`recover_sagas` in `saga_orchestrator.py`):
- `STARTED` → marked `FAILED`
- `STOCK_SUBTRACTING` / `PAYMENT_PENDING` → triggers compensation
- `STOCK_COMPENSATING` → left for XAUTOCLAIM

**2PC recovery** (`recover_tpcs` in `tpc_orchestrator.py`):
- `RETRY_PENDING` → marked `FAILED`
- `PREPARING` → sends abort
- `COMMITTING` → re-sends commits (deterministic idempotency keys)
- `ABORTING` → re-sends aborts

All containers use `restart: always`.

## Module Structure

```
common/
  streams.py              — Redis Streams helpers, connection pool, idempotency, locks,
                            shard routing, submit_task()

orchestrator/
  app.py                  — Flask app, consumer threads (orchestrator-commands + saga-replies),
                            startup recovery, retry thread
  task_manager.py         — TaskState model, task submission/completion/retry logic,
                            crash recovery, reply forwarding

order/
  app.py                  — REST endpoints, DB setup, order-replies consumer thread,
                            reply dispatching (saga vs. 2PC), recovery on startup
  saga_orchestrator.py    — Saga state machine (start, advance, poll, recover)
  tpc_orchestrator.py     — 2PC state machine (start, handle votes, poll, recover)

stock/
  app.py                  — REST endpoints, DB setup, consumer thread
  models.py               — StockValue struct
  saga_handler.py         — stock_subtract, stock_add handlers
  tpc_handler.py          — stock_prepare, stock_commit, stock_abort handlers

payment/
  app.py                  — REST endpoints, DB setup, consumer thread
  models.py               — UserValue struct
  saga_handler.py         — payment_pay, payment_refund handlers
  tpc_handler.py          — payment_prepare, payment_commit, payment_abort handlers
```

Each `app.py` starts background threads running `consume_loop` for its service's streams.

## Deployment

### Docker Compose

```bash
docker-compose up --build      # start all 46 containers
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
