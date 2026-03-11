# DDS25-13 — Fault-Tolerant Order Processing System

A distributed e-commerce backend implementing saga-based checkout with 2PL/2PC coordination. The system runs on Kubernetes with per-shard isolated databases for fault tolerance.

---

## Architecture

### Services

| Service | Role |
|---------|------|
| **Order** | Manages orders; orchestrates checkout via Kafka (saga/2PC) |
| **Payment** | Manages user accounts and credit; responds to payment commands |
| **Stock** | Manages item inventory; responds to stock reservation commands |
| **Gateway** | OpenResty (NGINX + LuaJIT) reverse proxy with shard-based routing |

### Sharding model

Each service runs as **3 independent shards** (pods), each with its own dedicated Redis instance. There is no shared state between shards.

```
Shard 0: order-shard-0 ↔ order-db-0    payment-shard-0 ↔ payment-db-0    stock-shard-0 ↔ stock-db-0
Shard 1: order-shard-1 ↔ order-db-1    payment-shard-1 ↔ payment-db-1    stock-shard-1 ↔ stock-db-1
Shard 2: order-shard-2 ↔ order-db-2    payment-shard-2 ↔ payment-db-2    stock-shard-2 ↔ stock-db-2
```

Entities are assigned to shards deterministically by hashing the first 64 bits of their UUID:

```python
# common_kafka/config.py
def compute_shard(entity_id: str) -> int:
    hex_clean = entity_id.replace("-", "")
    first_64_bits = int(hex_clean[:16], 16)
    return first_64_bits % NUM_SHARDS
```

The same formula is implemented in Lua in the gateway (split into two 32-bit halves to avoid IEEE-754 precision loss).

### ID generation

When a pod creates a new entity it uses rejection sampling to guarantee the UUID maps to its own shard, so subsequent requests are always routed back to the same pod:

```python
def generate_shard_uuid() -> str:
    while True:
        candidate = str(uuid.uuid4())
        if compute_shard(candidate) == SHARD_INDEX:
            return candidate  # averages ~3 attempts
```

### Kafka topics

Checkout is coordinated through per-shard Kafka topics so all communication for a given transaction stays within one shard:

| Topic | Publisher | Subscriber |
|-------|-----------|------------|
| `payment.commands.{N}` | order-shard-N | payment-shard-N |
| `stock.commands.{N}` | order-shard-N | stock-shard-N |
| `payment.events.{N}` | payment-shard-N | order-shard-N |
| `stock.events.{N}` | stock-shard-N | order-shard-N |
| `order.events.{N}` | order-shard-N | order-shard-N |

### Gateway routing

The OpenResty gateway routes each request to the correct shard based on the entity ID in the URL, with no external lookup required:

| URL pattern | Routing key | Target |
|-------------|-------------|--------|
| `POST /orders/create/<user_id>` | user_id | order-shard matching user (co-location) |
| `POST /orders/(checkout\|addItem\|find)/<order_id>` | order_id | order-shard owning the order |
| `POST /payment/(add_funds\|find_user\|pay)/<user_id>` | user_id | payment-shard owning the user |
| `GET/POST /stock/(find\|add\|subtract)/<item_id>` | item_id | stock-shard owning the item |
| `POST /*/shard/<N>/...` | literal N | exact shard (for batch seeding) |
| all other | — | round-robin across shards |

### Fault tolerance

If any pod crashes, Kubernetes restarts it automatically (typically within seconds). Other shards continue serving traffic uninterrupted. Data survives pod restarts because each Redis instance has a PersistentVolumeClaim.

---

## Project structure

```
.
├── common_kafka/       Shared Kafka config, shard helpers, msgpack models
├── order/              Order service (Flask + gunicorn + reaper_worker)
├── payment/            Payment service (Flask + gunicorn + worker)
├── stock/              Stock service (Flask + gunicorn + worker)
├── k8s/
│   ├── namespace.yaml
│   ├── redis/          9 Redis Deployments + PVCs + Services
│   ├── kafka/          3-broker KRaft StatefulSet + init Job
│   ├── order/          3 order shard Deployments + Services
│   ├── payment/        3 payment shard Deployments + Services
│   ├── stock/          3 stock shard Deployments + Services
│   └── gateway/        OpenResty Deployment + ConfigMap + NodePort Service
├── gateway_nginx.conf  OpenResty config (reference copy; embedded in ConfigMap)
├── deploy.sh           One-command deploy script for minikube
├── docker-compose.yml  Single-node dev setup (no sharding)
└── RUNNING.md          Step-by-step deployment and test guide
```

---

## Quick start (Kubernetes / minikube)

```bash
./deploy.sh
```

See [RUNNING.md](RUNNING.md) for full instructions including smoke tests and tear-down.

## Quick start (docker-compose — single node)

```bash
docker compose up --build -d
```

See [RUNNING.md](RUNNING.md#docker-compose-single-node-dev) for details.
