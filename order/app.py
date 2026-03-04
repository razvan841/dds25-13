import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
import threading
import sys

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from order.orchestrators import select_orchestrator
from common_kafka.producer import publish_envelope
from common_kafka.models import make_envelope, ORDER_EVENTS, PAYMENT_COMMANDS

# Ensure we log to stdout even under gunicorn.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [order] %(message)s",
    stream=sys.stdout,
    force=True,
)

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"


def _get_bool_env(var_name: str, default: str = "false") -> bool:
    """Return True if the env var looks truthy; evaluate once at startup."""
    return os.environ.get(var_name, default).lower() in {"1", "true", "yes", "on"}


USE_2PL2PC = _get_bool_env("USE_2PL2PC", "false")
ORCHESTRATION_MODE = "2pl2pc"

GATEWAY_URL = os.environ['GATEWAY_URL']

# Dev toggle: if true, wipe Redis on startup (helps local testing).
DEV = True #os.environ.get("DEV", "true").lower() in {"1", "true", "yes", "on"}

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

if DEV:
    try:
        db.flushdb()
        app.logger.warning("[order] DEV=true -> Redis database flushed on startup")
    except redis.exceptions.RedisError:
        app.logger.exception("[order] Failed to flush Redis during DEV startup")

# How long to wait for saga completion (seconds) before timing out HTTP call.
CHECKOUT_DEADLINE_SECONDS = int(os.environ.get("CHECKOUT_DEADLINE_SECONDS", "5"))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)
app.logger.info("Order service initialized")
app.logger.info("[order] Coordination mode set to %s", ORCHESTRATION_MODE)
print("[order] Flask app loaded; background workers disabled in this process")


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


orchestrator = select_orchestrator(
    ORCHESTRATION_MODE,
    db=db,
    logger=app.logger,
    fetch_order_fn=get_order_from_db,
    checkout_deadline_seconds=CHECKOUT_DEADLINE_SECONDS,
)


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id} via mode {ORCHESTRATION_MODE}")
    order_entry: OrderValue = get_order_from_db(order_id)
    print(f"order_entry: {order_entry}")
    if order_entry.paid:
        abort(400, "Order already paid")

    # Aggregate quantities per item
    items_quantities = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    return orchestrator.checkout(order_id, order_entry, items_quantities)


@app.get('/checkout_status/<order_id>')
def checkout_status(order_id: str):
    return orchestrator.checkout_status(order_id)


def handle_event(envelope):
    """Route Kafka events through the selected orchestration strategy."""
    return orchestrator.handle_event(envelope)


# Background worker loops live in reaper_worker.py for isolation


@app.get("/kafka_ping")
def kafka_ping():
    """
    Lightweight health check: publishes a test envelope to Kafka and returns the message id.
    Useful to verify connectivity without mutating order state.
    """
    ping_id = str(uuid.uuid4())
    envelope = make_envelope(
        "OrderServicePing",
        transaction_id=ping_id,
        payload={"msg": "ping", "service": "order"},
    )
    try:
        publish_envelope(ORDER_EVENTS, key=ping_id, envelope=envelope)
    except Exception as exc:  # noqa: BLE001
        app.logger.exception("Kafka ping failed: %s", exc)
        abort(500, "Kafka publish failed")
    app.logger.info("Kafka ping sent: %s", ping_id)
    return jsonify({"status": "sent", "message_id": envelope.message_id, "transaction_id": ping_id})

@app.get("/kafka_ping_payment")
def kafka_ping_payment():
    ping_id = str(uuid.uuid4())
    envelope = make_envelope(
        "PaymentServicePing",
        transaction_id=ping_id,
        payload={"msg": "ping", "service": "order"},
    )
    publish_envelope(PAYMENT_COMMANDS, key=ping_id, envelope=envelope)
    return jsonify({"status": "sent", "message_id": envelope.message_id, "transaction_id": ping_id})


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    app.logger.propagate = True
    app.logger.info("[order] App loaded; background workers not started in web process")
    print("[order] App loaded under gunicorn; workers are isolated to reaper_worker", flush=True)
