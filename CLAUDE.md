# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Fault-tolerant distributed e-commerce backend with saga-based and 2PC-based checkout, shard-isolated databases, and Kafka event-driven coordination. Runs on Kubernetes with 3 shards per service.

## Commands

### Local Development (Docker Compose)
```bash
docker compose up --build -d        # Start full stack (gateway on :8000)
docker compose down                  # Tear down
docker compose logs -f <service>    # Follow logs for a service
```

### Kubernetes Deployment
```bash
./deploy.sh                          # Build images + apply all K8s manifests
./deploy.sh --no-build               # Apply manifests only (skip image builds)
./deploy.sh --down                   # Tear down namespace
```

### Tests
```bash
cd test && python -m pytest test_microservices.py -v         # All integration tests
cd test && python -m pytest test_microservices.py -v -k foo  # Single test by name
```

### Kafka Topics (manual setup)
```bash
NUM_SHARDS=3 TOPICS="payment.commands,stock.commands,payment.events,stock.events,order.events" \
  KAFKA_BOOTSTRAP=localhost:9092 bash init-topics.sh
```

## Architecture

### Services
Three Flask/gunicorn services — **order**, **payment**, **stock** — each with:
- A REST API (`app.py`) served by gunicorn on port 5000
- A background Kafka consumer/worker thread (`worker.py` or `reaper_worker.py`)
- An orchestrators module (`orchestrators.py`) implementing both saga and 2PC handlers

**Gateway**: OpenResty (NGINX + LuaJIT) on port 8000/30080 routes requests to the correct shard using Lua.

### Sharding
- **3 shards** per service, each with its own Redis instance (9 Redis total)
- Shard assignment: `compute_shard(id) = first_64_bits(UUID) % NUM_SHARDS`
- Co-location guarantee: user IDs and their orders hash to the same shard; item IDs and their stock hash to the same shard
- ID generation uses rejection sampling to create shard-local UUIDs (`generate_shard_uuid()` in `common_kafka/config.py`)
- **Critical**: The Lua shard formula in `gateway_nginx.conf` must match `common_kafka/config.py:compute_shard()`. Lua splits the UUID into two 32-bit halves to avoid IEEE-754 precision loss.

### Kafka Topics (per shard index N)
| Topic | Producer | Consumer |
|---|---|---|
| `payment.commands.N` | order-shard-N | payment-shard-N |
| `stock.commands.N` | order-shard-N | stock-shard-N |
| `payment.events.N` | payment-shard-N | order-shard-N |
| `stock.events.N` | stock-shard-N | order-shard-N |
| `order.events.N` | order-shard-N | order-shard-N |

### Coordination Modes
Switchable via `ORCHESTRATION_MODE` env var (default: `saga`):
- **Saga**: Eventual consistency with outbox pattern; compensation (cancel) on failure
- **2PC**: Two-phase locking + two-phase commit; order service runs prepare → commit/abort

### Message Envelope (`common_kafka/models.py`)
All Kafka messages use a typed envelope with: `type, version, message_id, transaction_id, correlation_id, causation_id, timestamp, payload, reply_topic`. Serialized with msgpack via msgspec.

### Outbox / State (`common_kafka/outbox.py`, `common_kafka/twopl.py`)
- Saga state stored in Redis using outbox pattern (pending commands + received events)
- 2PC lock state stored in Redis with prepare/commit/abort lifecycle keys
- Order service has a reaper worker that drains the outbox and handles timeouts (default 5s checkout deadline)

### Gateway Routing (`gateway_nginx.conf`)
- `/orders/create/<user_id>` → hash user_id → order shard
- `/orders/(checkout|addItem|find)/<order_id>` → hash order_id → order shard
- `/payment/.../<user_id>` → hash user_id → payment shard
- `/stock/.../<item_id>` → hash item_id → stock shard
- `/<service>/shard/<N>/...` → explicit shard N (used for batch seeding)
- Unkeyed endpoints → round-robin

### common_kafka Package
Shared Python package installed in all service containers via `pip install -e /app/common_kafka`. Contains: Kafka producer/consumer singletons, message models, shard config, codec, outbox/2PC state helpers.

## Key Environment Variables
| Variable | Description |
|---|---|
| `SHARD_INDEX` | Which shard this pod handles (0, 1, or 2) |
| `NUM_SHARDS` | Total shards (default 3) |
| `ORCHESTRATION_MODE` | `saga` or `twopc` |
| `KAFKA_BOOTSTRAP` | Kafka broker address |
| `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD` | Redis connection |
| `LOG_LEVEL` | Python logging level |
