# Kafka-Backed Async Gateway

## What Changed

All external HTTP requests now flow through Kafka instead of being proxied directly to service pods via NGINX + Lua routing.

### Before
```
Client → NGINX (Lua shard routing) → HTTP → Service shard pod
```

### After
```
Client → NGINX (thin CORS proxy) → gateway-app (Flask)
  → Kafka: gateway.commands.<service>.<shard>
  → Service worker thread → Flask test_client() dispatch
  → Kafka: gateway.replies
  → gateway-app (reply matched by correlation_id)
  → HTTP response to client
```

---

## Files Changed

### `common_kafka/models.py`
Added `GatewayRequestPayload`, `GatewayReplyPayload`, `GATEWAY_REPLIES` constant, and `gateway_commands_topic(service, shard)` helper. These define the request-reply envelope schema.

### `common_kafka/gateway_consumer.py` *(new)*
A non-singleton `KafkaConsumer` that each service worker starts in a daemon thread. Subscribes to `gateway.commands.<service>.<SHARD_INDEX>`, decodes the request envelope, calls the Flask app via `test_client()` (local WSGI — no network hop), and publishes a reply to `gateway.replies`.

Uses `test_client()` instead of extracting handler logic because Flask handlers rely on `abort()`, `jsonify()`, and app context. `test_client()` provides all of that for free with zero overhead.

Uses its own `KafkaConsumer` instance (not the singleton in `consumer.py`) so it can run alongside the existing saga/2PC consumer in the same process without conflict.

### `stock/worker.py`, `payment/worker.py`, `order/reaper_worker.py`
Each gains a `_gateway_consumer_loop()` function and `_start_gateway_consumer_thread()` call at the end of `main()`. Same exponential-backoff retry pattern as the existing consumers. Runs regardless of `ORCHESTRATION_MODE`.

### `gateway/` *(new directory)*
- **`app.py`**: Flask application. On startup, launches a reply consumer daemon thread subscribed to `gateway.replies`. For each incoming HTTP request, resolves the target service and shard (same routing rules as the old Lua code), publishes a `GatewayRequest` envelope, registers a `threading.Event` keyed by `correlation_id`, and waits up to `GATEWAY_KAFKA_TIMEOUT_SECONDS` (default 10s) for the reply. Returns 504 on timeout.
- **`gunicorn_conf.py`**: gevent worker with `patch_all()`. This makes `threading.Event.wait()` greenlet-cooperative so a single worker process can handle hundreds of concurrent in-flight requests sharing the same `_pending` dict.
- **`Dockerfile`**: Python 3.12-slim, installs `common_kafka` (editable) + `requirements.txt`.
- **`requirements.txt`**: Flask, gunicorn, gevent, kafka-python, msgspec.

### `gateway_nginx.conf`
Replaced the entire Lua routing configuration with a minimal nginx config that only adds CORS headers and proxies everything to `gateway-app:8000`. Shard routing is now handled in Python.

### `docker-compose.yml`
Added `gateway-app` service (the new Python gateway). The existing `gateway` (NGINX) now depends on `gateway-app`. `NUM_SHARDS=1` in compose since docker-compose runs a single instance per service.

### `env/kafka.env`
Added `gateway.commands.order`, `gateway.commands.payment`, `gateway.commands.stock` to `TOPICS` (each gets a shard suffix). Added `NON_SHARDED_TOPICS=gateway.replies` (single topic, no shard suffix).

### `init-topics.sh`
Added a second loop after the existing sharded-topic loop to create non-sharded topics listed in `NON_SHARDED_TOPICS`.

### `k8s/kafka/kafka-init-job.yaml`
Updated `TOPICS` env var to include gateway command topics. Added `NON_SHARDED_TOPICS=gateway.replies`. Updated inline script to match `init-topics.sh` (added the non-sharded loop).

### `k8s/gateway/gateway-configmap.yaml`
Replaced the full Lua nginx config with the simplified thin-proxy config.

### `k8s/gateway/gateway-deployment.yaml`
Changed image from `openresty/openresty` to `nginx:1.25-alpine` (Lua no longer needed).

### `k8s/gateway/gateway-app-deployment.yaml` *(new)*
K8s Deployment for the Python gateway (`gateway:latest`), with Kafka env vars.

### `k8s/gateway/gateway-app-service.yaml` *(new)*
ClusterIP Service exposing `gateway-app` on port 8000 within the cluster.

### `deploy.sh`
Added `docker build -t gateway:latest` step. Added `wait_deploy gateway-app` before the NGINX gateway wait.

---

## Design Notes

- **No saga/2PC changes**: The internal Kafka topics and orchestration logic are completely untouched.
- **Correlation IDs**: Each HTTP request gets a unique `correlation_id` (UUID). The gateway holds a pending map; the reply consumer matches by `correlation_id` and unblocks the waiting greenlet.
- **Shard routing in Python**: `_resolve_route()` in `gateway/app.py` mirrors the old Lua logic using `common_kafka.config.compute_shard()` — the same function used everywhere else, guaranteeing consistency.
- **Timeout**: Default 10s. Returns HTTP 504 if no reply arrives.
- **Stale replies**: If a reply arrives after the request timed out (correlation_id already removed from `_pending`), it is silently discarded.
