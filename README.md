# Distributed Data Systems Project Template

Basic project structure with Python's Flask and Redis. 

### Project structure

* `env`
    Folder containing the Redis env variables for the docker-compose deployment
    
* `helm-config` 
   Helm chart values for Redis and ingress-nginx
        
* `k8s`
    Folder containing the kubernetes deployments, apps and services for the ingress, order, payment and stock services.
    
* `order`
    Folder containing the order application logic and dockerfile. 
    
* `payment`
    Folder containing the payment application logic and dockerfile. 

* `stock`
    Folder containing the stock application logic and dockerfile. 

* `test`
    Folder containing some basic correctness tests for the entire system. (Feel free to enhance them)

### Deployment types:

#### docker-compose (local development)

After coding the REST endpoint logic run `docker-compose up --build` in the base folder to test if your logic is correct
(you can use the provided tests in the `\test` folder and change them as you wish). 

***Requirements:*** You need to have docker and docker-compose installed on your machine. 

K8s is also possible, but we do not require it as part of your submission. 


## Project Overview

Distributed microservices e-commerce system (Python/Flask) with three services: **order**, **stock**, and **payment**. Each service runs sharded instances, each with its own Redis instance. An NGINX gateway routes requests by hash-based sharding on resource IDs (`/orders/`, `/stock/`, `/payment/`).

## Build & Run

Four Docker Compose configurations are available for different cluster sizes:

| Config | Shards | Target CPUs | Use case |
|---|---|---|---|
| `docker-compose-small.yml` | 1 | 8 | Local development |
| `docker-compose.yml` | 5 | No limits | Default/testing |
| `docker-compose-medium.yml` | 5 | 48 | 32-core deployment |
| `docker-compose-large.yml` | 10 | 90 | 64+ core deployment |

```bash
# Build images (replace <file> with the compose file for your cluster size)
docker compose -f <file> build

# Start cluster
docker compose -f <file> up -d

# Clean rebuild (no cache)
docker compose -f <file> down --rmi all
docker builder prune -f
docker compose -f <file> build --no-cache
docker compose -f <file> up -d

# Tear down
docker compose -f <file> down
```

The gateway is exposed on **port 8000**. Each service runs Gunicorn with gevent workers on port 5000 internally.

### Performance tuning

The medium and large configs are tuned for checkout-heavy workloads based on profiling:

- **Order services get the most CPU** (5.0/4.0 CPUs, 5/4 workers) — they run saga/2PC orchestration, stream consumers, and BLPOP result waiting
- **Stock services** get moderate CPU (1.5 CPUs, 2 workers) — stream command processing
- **Payment services** get minimal CPU (1.0 CPUs, 1 worker) — lightest workload
- **BLPOP-based result notification** replaces busy-wait polling for checkout completion
- **4 consumer threads per worker** for parallel stream message processing
- **Gevent monkey patching** via `gunicorn_conf.py` for cooperative I/O
- **Nginx upstream keepalive** connections to avoid per-request TCP handshakes

Recommended host-level sysctl tuning for high throughput:

```bash
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"
sudo sysctl -w net.ipv4.tcp_tw_reuse=1
sudo sysctl -w net.core.somaxconn=65535
```

## Running Tests

Tests require services to be running first:

```bash
docker-compose up --build -d
cd test
python3 -m unittest test_microservices.py
```

Then also run the consistency tests:

```bash
cd ..
cd wdm-project-benchmark
cd consistency-test
python3 run_consistency_test.py
```

## Architecture

- **Asynchronous saga-based checkout** using Redis Streams for inter-service messaging via a shared `saga-redis` broker
- **Synchronous REST communication** for non-checkout operations (e.g., `addItem` calls `/stock/find/` via the NGINX gateway)
- **Data serialization**: MessagePack via `msgspec` for Redis storage
- **Service sharding** (3 shards per service): Each service runs instances 0/1/2, each with its own Redis DB. NGINX's `hash` directive routes requests by resource ID (CRC32-based, Cache::Memcached-compatible). `batch_init` endpoints are broadcast to all shards via NGINX `mirror`. Creates generate shard-affine UUIDs. Shard config: `SHARD_ID`, `SHARD_COUNT` env vars. Hash function in Python: `((crc32(key) >> 16) & 0x7fff) % num_shards`.
- **Shard-specific streams**: `stock-commands-{0,1,2}`, `payment-commands-{0,1,2}`, `saga-replies-{0,1,2}`. Orchestrators route commands to the correct shard's stream based on `compute_shard(resource_id)`. Reply routing uses `reply_stream` field in command messages.
- **Distributed transactions**: Order checkout uses an orchestrated saga pattern with compensating transactions. The order service acts as the orchestrator, sending commands to the correct stock/payment shard's stream based on resource ID hashing, and receiving replies on its own `saga-replies-{shard_id}`. On failure, compensating `stock_add` commands roll back previously subtracted items.
- **Saga state machine** stored in order-db as `saga:{order_id}`: `STARTED → STOCK_SUBTRACTING → PAYMENT_PENDING → COMPLETED`, with compensation path `STOCK_COMPENSATING → FAILED`. Completion is signalled via `LPUSH result:{order_id}` so the HTTP handler can `BLPOP` instead of polling.
- **Idempotency keys**: Each saga command carries a UUID idempotency key. Consumers use `SET NX EX 3600` on their business Redis to prevent duplicate processing.
- **No shared database**: Each service is fully isolated with its own Redis instance. The `saga-redis` instance is shared only for message passing (streams), not data storage.
- **Crash recovery**: All containers use `restart: always`. On restart, `consume_loop` runs `XAUTOCLAIM` to reclaim stale pending messages from dead consumers. Idempotency keys store reply data (`publish_reply_with_idempotency`), enabling duplicate messages to re-send stored replies instead of silently dropping them. The order service runs `recover_sagas`/`recover_tpcs` at startup (guarded by a Redis lock) to resolve intermediate-state transactions: sagas in STARTED are marked FAILED, STOCK_SUBTRACTING/PAYMENT_PENDING trigger compensations, TPC in PREPARING triggers abort, COMMITTING/ABORTING re-send with deterministic idempotency keys (`tpc-{txn_id}-{command}`).
- **Checkout mode** controlled by `CHECKOUT_MODE` env var in `env/checkout.env` (`saga` or `2pc`). Default: `saga`.
- **2PL+2PC checkout** (`CHECKOUT_MODE=2pc`): Two-Phase Locking + Two-Phase Commit. The coordinator groups items by stock shard and sends `stock_prepare` to each involved shard (with only that shard's items) plus `payment_prepare` to the correct payment shard. `participant_count` = stock shards involved + 1 payment. Each participant acquires locks and validates, replying VOTE-COMMIT or VOTE-ABORT. Commit/abort commands are sent to each involved shard with deterministic idempotency keys (`tpc-{txn_id}-stock_commit-{shard_id}`). Lock contention triggers retry with exponential backoff (max 5 retries, new txn_id per attempt). State machine stored as `tpc:{order_id}`: `PREPARING → COMMITTING → COMMITTED` or `PREPARING → ABORTING → FAILED/retry`.
- **Both checkout modes** coexist using the same Redis Streams infrastructure. The orchestrator reply handler dispatches to saga or 2PC based on the command name.

### Module Structure

Each protocol (saga, 2PC) is extracted into its own module per service:

- `common/streams.py` — protocol-agnostic Redis Streams helpers (publish, consume, locks, idempotency, shard routing)
- `order/saga_orchestrator.py` — saga state machine, start/poll
- `order/tpc_orchestrator.py` — 2PC state machine, start/poll
- `stock/saga_handler.py` — stock_subtract, stock_add command handlers
- `stock/tpc_handler.py` — stock_prepare, stock_commit, stock_abort handlers
- `payment/saga_handler.py` — payment_pay, payment_refund handlers
- `payment/tpc_handler.py` — payment_prepare, payment_commit, payment_abort handlers
- `stock/models.py`, `payment/models.py` — data model structs (avoids circular imports)

Each service's `app.py` contains only REST endpoints, DB setup, and a thin dispatcher that routes commands to the appropriate handler module.

### Service Data Models

- **Order** (`OrderValue`): `paid`, `items` (list of item_id/quantity tuples), `user_id`, `total_cost`
- **Stock** (`StockValue`): `stock`, `price`
- **Payment** (`UserValue`): `credit`

### Key Environment Variables

All services use: `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_DB`, `SHARD_ID`, `SHARD_COUNT`

## Dependencies

Each service uses: Flask 3.0.2, redis 5.0.3, gunicorn 21.2.0, msgspec 0.18.6, requests 2.31.0 (Python 3.12).

## Workflow Orchestration

### 1. Plan Mode Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately - don't keep pushing
- Use plan mode for verification steps, not just building
- Write detailed specs upfront to reduce ambiguity

### 2. Subagent Strategy
- Use subagents liberally to keep main context window clean
- Offload research, exploration, and parallel analysis to subagents
- For complex problems, throw more compute at it via subagents
- One task per subagent for focused executioni

### 3. Self-Improvement Loop
- After ANY correction from the user: update 'tasks/lessons.md' with the pattern
- Write rules for yourself that prevent the same mistake
- Ruthlessly iterate on these lessons until mistake rate drops
- Review lessons at session start for relevant projects

### 4. Verification Before Done
- Never mark a task complete without proving it works
- Diff behavior between main and your changes when relevant
- Ask yourself: "Would a staff engineer approve this?"
- Run tests, check logs, demonstrate correctness

### 5. Demand Elegance (Balanced)
- For non-trivial changes: pause and ask "is there a more elegant way?"
- If a fix feels hacky: "Knowing everything I know now, implement the elegant solution"
- Skip this for simple, obvious fixes - don't over-engineer
- Challenge your own work before presenting it

### 6. Autonomous Bug Fixing
- When given a bug report: just fix it. Don't ask for hand-holding
- Point at logs, errors, failing tests - then resolve them
- Zero context switching required from the user
- Go fix failing CI tests without being told how