"""
Kafka-backed async gateway
===========================
Receives HTTP requests, publishes them to per-shard Kafka topics, and waits
for replies on ``gateway.replies``.  NGINX sits in front as a thin CORS proxy.

Concurrency: gevent worker (gunicorn --worker-class gevent).  Monkey-patching
makes ``threading.Event.wait()`` greenlet-cooperative so hundreds of requests
can be in-flight simultaneously.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sys
import threading
import uuid
from datetime import datetime, timezone

from flask import Flask, Response, jsonify, request
from kafka import KafkaConsumer as _KafkaConsumer

from common_kafka.codec import decode_envelope, EnvelopeDecodeError
from common_kafka.config import compute_shard, NUM_SHARDS, load_kafka_settings
from common_kafka.models import (
    GATEWAY_REPLIES,
    gateway_commands_topic,
    make_envelope,
)
from common_kafka.producer import publish_envelope

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GATEWAY_KAFKA_TIMEOUT = int(os.environ.get("GATEWAY_KAFKA_TIMEOUT_SECONDS", "10"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [gateway-app] %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pending-request registry (shared across greenlets / threads)
# ---------------------------------------------------------------------------
# correlation_id → (Event, result_list)
# result_list will hold [status_code, body, content_type] when the reply arrives.
_pending: dict[str, tuple[threading.Event, list]] = {}

# ---------------------------------------------------------------------------
# Reply consumer — background daemon thread
# ---------------------------------------------------------------------------

def _reply_consumer_loop():
    """Consume from gateway.replies forever, matching replies to pending requests.
    Retries with backoff if Kafka is unavailable at startup or crashes mid-run."""
    import time
    retry_delay = 1
    while True:
        try:
            settings = load_kafka_settings()
            consumer = _KafkaConsumer(
                bootstrap_servers=settings.bootstrap_servers,
                client_id="gateway-reply-consumer",
                group_id=os.environ.get("KAFKA_GROUP_ID", "gateway-reply-consumer"),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            consumer.subscribe([GATEWAY_REPLIES])
            logger.info("[gateway-app] Reply consumer subscribed to %s", GATEWAY_REPLIES)
            retry_delay = 1  # reset on successful connect

            while True:
                records = consumer.poll(timeout_ms=500)
                if not records:
                    continue
                for tp, msgs in records.items():
                    for msg in msgs:
                        try:
                            env = decode_envelope(msg.value)
                        except EnvelopeDecodeError:
                            continue

                        cid = env.correlation_id
                        entry = _pending.get(cid)
                        if entry is None:
                            # Stale or duplicate reply — ignore
                            continue
                        event, result = entry
                        payload = env.payload
                        result.append(payload.get("status_code", 500))
                        result.append(payload.get("body", ""))
                        result.append(payload.get("content_type", "application/json"))
                        event.set()
        except Exception as exc:
            logger.warning("[gateway-app] Reply consumer error: %s — retrying in %ds", exc, retry_delay)
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 30)


# Start the reply consumer once on module import
_reply_thread = threading.Thread(target=_reply_consumer_loop, daemon=True)
_reply_thread.start()

# ---------------------------------------------------------------------------
# Flask application
# ---------------------------------------------------------------------------
app = Flask(__name__)

# Route patterns matching the current NGINX routing logic
# Stock routes
_STOCK_SHARD_RE = re.compile(r"^/stock/shard/(\d+)/(.*)")
_STOCK_KEYED_RE = re.compile(r"^/stock/(find|add|subtract)/([^/?]+)")
_STOCK_ANY_RE = re.compile(r"^/stock/")

# Payment routes
_PAYMENT_SHARD_RE = re.compile(r"^/payment/shard/(\d+)/(.*)")
_PAYMENT_KEYED_RE = re.compile(r"^/payment/(add_funds|find_user|subtract_funds|pay)/([^/?]+)")
_PAYMENT_ANY_RE = re.compile(r"^/payment/")

# Order routes
_ORDER_SHARD_RE = re.compile(r"^/orders/shard/(\d+)/(.*)")
_ORDER_CREATE_RE = re.compile(r"^/orders/create/([^/?]+)")
_ORDER_KEYED_RE = re.compile(r"^/orders/(checkout|find|addItem|checkout_status)/([^/?]+)")
_ORDER_ANY_RE = re.compile(r"^/orders/")

# Round-robin counter for unkeyed routes
_rr_counter = 0
_rr_lock = threading.Lock()


def _build_monitoring_overview() -> dict:
    orchestration_mode = os.environ.get("ORCHESTRATION_MODE", "saga")
    namespace = os.environ.get("K8S_NAMESPACE", "dds25")
    generated_at = datetime.now(timezone.utc).isoformat()

    services = ("order", "payment", "stock")
    service_instances = [
        _fetch_service_instance(service, shard)
        for service in services
        for shard in range(NUM_SHARDS)
    ]
    healthy_instances = sum(1 for svc in service_instances if svc.get("status") == "healthy")
    degraded_instances = len(service_instances) - healthy_instances

    databases = [
        {
            "name": f"{svc['service']}-db-{svc['shard']}",
            "service": svc["service"],
            "shard": svc["shard"],
            "role": "primary",
            "status": "healthy" if svc.get("db_ok") else "degraded",
            "reachable": bool(svc.get("db_ok")),
            "ping_ms": svc.get("db_ping_ms"),
            "key_count": svc.get("redis_keys"),
            "uptime_seconds": svc.get("uptime_seconds"),
            "pod": svc.get("pod"),
            "error": svc.get("error"),
        }
        for svc in service_instances
    ]
    reachable_databases = [db for db in databases if db["reachable"]]
    total_db_keys = sum(db["key_count"] or 0 for db in databases)

    saga_counts = {
        "TRYING": 0,
        "RESERVED": 0,
        "COMMITTED": 0,
        "FAILED": 0,
        "CANCELLED": 0,
    }
    saga_recent = []
    for svc in service_instances:
        saga = svc.get("saga")
        if not saga or not saga.get("enabled"):
            continue
        for status, count in saga.get("counts", {}).items():
            saga_counts[status] = saga_counts.get(status, 0) + count
        saga_recent.extend(saga.get("recent", []))
    saga_recent.sort(key=lambda item: item.get("age_seconds", 0), reverse=True)
    saga_recent = saga_recent[:8]

    two_pc_counts = {
        "PREPARING": 0,
        "PREPARED": 0,
        "COMMITTING": 0,
        "COMMITTED": 0,
        "ABORTING": 0,
        "ABORTED": 0,
        "FAILED": 0,
    }
    two_pc_recent = []
    prepared_locks = 0
    for svc in service_instances:
        twoplpc = svc.get("twoplpc")
        if not twoplpc or not twoplpc.get("enabled"):
            continue
        for status, count in twoplpc.get("counts", {}).items():
            two_pc_counts[status] = two_pc_counts.get(status, 0) + count
        prepared_locks += twoplpc.get("prepared_lock_count", 0)
        two_pc_recent.extend(twoplpc.get("recent", []))
    two_pc_recent.sort(key=lambda item: item.get("wait_ms", 0), reverse=True)
    two_pc_recent = two_pc_recent[:8]

    return {
        "generated_at": generated_at,
        "source": "gateway-mock-monitoring",
        "cluster": {
            "namespace": namespace,
            "num_shards": NUM_SHARDS,
            "mode": orchestration_mode,
        },
        "summary": {
            "total_instances": len(service_instances),
            "healthy_instances": healthy_instances,
            "degraded_instances": degraded_instances,
            "active_sagas": saga_counts.get("TRYING", 0) + saga_counts.get("RESERVED", 0) + saga_counts.get("FAILED", 0),
            "saga_failures": saga_counts.get("FAILED", 0),
            "active_2pc_transactions": (
                two_pc_counts.get("PREPARING", 0)
                + two_pc_counts.get("PREPARED", 0)
                + two_pc_counts.get("COMMITTING", 0)
                + two_pc_counts.get("ABORTING", 0)
            ),
            "prepared_locks": prepared_locks,
            "reachable_databases": len(reachable_databases),
            "total_databases": len(databases),
            "slowest_db_ms": max((db["ping_ms"] or 0) for db in databases),
            "total_db_keys": total_db_keys,
        },
        "service_instances": service_instances,
        "databases": databases,
        "sagas": {
            "status_breakdown": [
                {
                    "status": status,
                    "count": count,
                    "copy": {
                        "TRYING": "Fresh checkouts waiting on reservations.",
                        "RESERVED": "Reservations acquired; commit pressure can be monitored here.",
                        "COMMITTED": "Completed sagas, useful as throughput context.",
                        "FAILED": "Compensations or manual intervention candidates.",
                        "CANCELLED": "Cancelled flows after compensation completed.",
                    }[status],
                }
                for status, count in saga_counts.items()
            ],
            "recent": saga_recent,
        },
        "twoplpc": {
            "status_breakdown": [
                {
                    "status": status,
                    "count": count,
                    "copy": {
                        "PREPARING": "Coordinator still collecting prepare votes.",
                        "PREPARED": "Locks are held; long dwell time should alert operators.",
                        "COMMITTING": "Commit propagation currently in progress.",
                        "COMMITTED": "Transactions committed successfully across participants.",
                        "ABORTING": "Abort logic is actively releasing held locks.",
                        "ABORTED": "Timed-out or explicitly rolled-back transactions.",
                        "FAILED": "Transactions marked failed by recovery logic.",
                    }[status],
                }
                for status, count in two_pc_counts.items()
            ],
            "recent": two_pc_recent,
        },
    }


def _next_rr_shard() -> int:
    global _rr_counter
    with _rr_lock:
        shard = _rr_counter % NUM_SHARDS
        _rr_counter += 1
    return shard


def _resolve_route(path: str):
    """Return (service, shard, internal_path) or None if no match."""

    # --- Stock ---
    m = _STOCK_SHARD_RE.match(path)
    if m:
        return "stock", int(m.group(1)), "/" + m.group(2)

    m = _STOCK_KEYED_RE.match(path)
    if m:
        item_id = m.group(2)
        return "stock", compute_shard(item_id), path[len("/stock"):]

    if _STOCK_ANY_RE.match(path):
        return "stock", _next_rr_shard(), path[len("/stock"):]

    # --- Payment ---
    m = _PAYMENT_SHARD_RE.match(path)
    if m:
        return "payment", int(m.group(1)), "/" + m.group(2)

    m = _PAYMENT_KEYED_RE.match(path)
    if m:
        user_id = m.group(2)
        return "payment", compute_shard(user_id), path[len("/payment"):]

    if _PAYMENT_ANY_RE.match(path):
        return "payment", _next_rr_shard(), path[len("/payment"):]

    # --- Orders ---
    m = _ORDER_SHARD_RE.match(path)
    if m:
        return "order", int(m.group(1)), "/" + m.group(2)

    m = _ORDER_CREATE_RE.match(path)
    if m:
        user_id = m.group(1)
        return "order", compute_shard(user_id), path[len("/orders"):]

    m = _ORDER_KEYED_RE.match(path)
    if m:
        order_id = m.group(2)
        return "order", compute_shard(order_id), path[len("/orders"):]

    if _ORDER_ANY_RE.match(path):
        return "order", _next_rr_shard(), path[len("/orders"):]

    return None


def _dispatch_via_kafka(service: str, shard: int, internal_path: str, method: str, body: str = "") -> Response:
    """Publish a gateway command and wait for the reply."""
    correlation_id = str(uuid.uuid4())
    topic = gateway_commands_topic(service, shard)

    payload = {
        "method": method,
        "path": internal_path,
        "body": body,
    }
    env = make_envelope(
        "GatewayRequest",
        transaction_id=correlation_id,
        payload=payload,
        correlation_id=correlation_id,
    )

    # Register pending slot before publishing to avoid race
    event = threading.Event()
    result: list = []
    _pending[correlation_id] = (event, result)

    try:
        publish_envelope(topic, key=correlation_id, envelope=env)
        event.wait(timeout=GATEWAY_KAFKA_TIMEOUT)

        if not result:
            return Response('{"error": "Gateway timeout"}', status=504, content_type="application/json")

        status_code, body, content_type = result
        return Response(body, status=status_code, content_type=content_type)
    finally:
        _pending.pop(correlation_id, None)


def _fetch_service_instance(service: str, shard: int) -> dict:
    response = _dispatch_via_kafka(service, shard, "/monitoring/instance", "GET")
    if response.status_code != 200:
        return {
            "service": service,
            "shard": shard,
            "pod": f"{service}-shard-{shard}",
            "namespace": os.environ.get("K8S_NAMESPACE", "dds25"),
            "mode": os.environ.get("ORCHESTRATION_MODE", "saga"),
            "status": "unreachable",
            "db_ok": False,
            "db_ping_ms": None,
            "redis_keys": None,
            "uptime_seconds": None,
            "error": response.get_data(as_text=True),
        }
    try:
        return json.loads(response.get_data(as_text=True))
    except json.JSONDecodeError:
        return {
            "service": service,
            "shard": shard,
            "pod": f"{service}-shard-{shard}",
            "namespace": os.environ.get("K8S_NAMESPACE", "dds25"),
            "mode": os.environ.get("ORCHESTRATION_MODE", "saga"),
            "status": "unreachable",
            "db_ok": False,
            "db_ping_ms": None,
            "redis_keys": None,
            "uptime_seconds": None,
            "error": "Invalid JSON from service monitoring endpoint",
        }


@app.get("/monitoring/overview")
def monitoring_overview():
    """Return a mock dashboard snapshot shaped like a future live ops API."""
    return jsonify(_build_monitoring_overview())


@app.route("/", defaults={"path": ""}, methods=["GET", "POST"])
@app.route("/<path:path>", methods=["GET", "POST"])
def catch_all(path):
    full_path = "/" + path
    route = _resolve_route(full_path)
    if route is None:
        return Response('{"error": "Not found"}', status=404, content_type="application/json")
    service, shard, internal_path = route
    return _dispatch_via_kafka(service, shard, internal_path, request.method, request.get_data(as_text=True))
