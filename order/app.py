import logging
import os
import atexit
import random
import uuid
import threading
from collections import defaultdict

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from common.streams import (
    get_saga_redis, init_saga_pool, ensure_all_streams, consume_loop,
    ORCHESTRATOR_WORKERS, SHARD_ID, SHARD_COUNT,
    saga_replies_stream, generate_shard_affine_uuid, compute_shard,
)
import saga_orchestrator
import tpc_orchestrator


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


CHECKOUT_MODE = os.environ.get("CHECKOUT_MODE", "saga")


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        abort(400, f"Order: {order_id} not found!")
    return entry


def _get_order_raw(order_id: str) -> OrderValue | None:
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return None
    if not entry:
        return None
    return msgpack.decode(entry, type=OrderValue)


def _mark_order_paid(order_id: str):
    order = _get_order_raw(order_id)
    if order:
        order.paid = True
        try:
            db.set(order_id, msgpack.encode(order))
        except redis.exceptions.RedisError:
            pass


# --- Stream consumer setup ---

init_saga_pool()
saga_redis = get_saga_redis()
ensure_all_streams(saga_redis)

# Run startup recovery before starting consumer thread (single-worker guard)
if db.set("recovery_lock", "1", nx=True, ex=30):
    try:
        saga_orchestrator.recover_sagas(db, saga_redis)
        tpc_orchestrator.recover_tpcs(db, saga_redis)
    finally:
        db.delete("recovery_lock")

_saga_reply_handler = saga_orchestrator.create_reply_handler(db, saga_redis, _mark_order_paid)
_tpc_reply_handler = tpc_orchestrator.create_reply_handler(db, saga_redis, _mark_order_paid)


def handle_orchestrator_reply(message_id, fields):
    command = fields.get("command", "")
    if command in tpc_orchestrator.TPC_COMMANDS:
        _tpc_reply_handler(message_id, fields)
    else:
        _saga_reply_handler(message_id, fields)


CONSUMER_THREADS = int(os.environ.get("CONSUMER_THREADS", "4"))
for _i in range(CONSUMER_THREADS):
    threading.Thread(
        target=consume_loop,
        args=(saga_redis, saga_replies_stream(SHARD_ID), ORCHESTRATOR_WORKERS, handle_orchestrator_reply),
        daemon=True,
    ).start()


# --- REST endpoints ---

@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = generate_shard_affine_uuid(SHARD_ID, SHARD_COUNT)
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
                                  for i in range(n) if compute_shard(str(i), SHARD_COUNT) == SHARD_ID}
    if kv_pairs:
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


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id} (mode={CHECKOUT_MODE})")
    order_entry: OrderValue = get_order_from_db(order_id)

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    aggregated_items = list(items_quantities.items())

    if CHECKOUT_MODE == "2pc":
        tpc_orchestrator.start_checkout(db, saga_redis, order_id, order_entry.user_id,
                                        order_entry.total_cost, aggregated_items)
        success, error = tpc_orchestrator.poll_result(db, order_id)
    else:
        saga_orchestrator.start_checkout(db, saga_redis, order_id, order_entry.user_id,
                                         order_entry.total_cost, aggregated_items)
        success, error = saga_orchestrator.poll_result(db, order_id)

    if success:
        app.logger.debug("Checkout successful")
        return Response("Checkout successful", status=200)
    abort(400, error)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
