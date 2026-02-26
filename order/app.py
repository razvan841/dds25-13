import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from kafka_producer import publish_envelope
from kafka_consumer import start_consumer
from kafka_models import (
    make_envelope,
    PAYMENT_COMMANDS,
    STOCK_COMMANDS,
    ReserveFundsCommand,
    ReserveStockCommand,
)
from saga_store import (
    create_saga,
    get_saga,
    STATUS_TRYING,
    STATUS_FAILED,
)


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

# How long to wait for saga completion (seconds) before timing out HTTP call.
CHECKOUT_DEADLINE_SECONDS = int(os.environ.get("CHECKOUT_DEADLINE_SECONDS", "5"))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)
app.logger.info("Order service initialized")


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
    app.logger.debug(f"Checking out {order_id} via Kafka saga")
    order_entry: OrderValue = get_order_from_db(order_id)
    if order_entry.paid:
        abort(400, "Order already paid")

    # Aggregate quantities per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    # Initialize saga state with a deadline
    correlation_id = str(uuid.uuid4())
    deadline_ts = (datetime.now(timezone.utc) + timedelta(seconds=CHECKOUT_DEADLINE_SECONDS)).timestamp()
    create_saga(db, order_id, correlation_id, deadline_ts)

    # Build and publish reserve commands
    reserve_funds = ReserveFundsCommand(user_id=order_entry.user_id, amount=order_entry.total_cost)
    reserve_stock = ReserveStockCommand(items=list(items_quantities.items()))

    publish_envelope(
        PAYMENT_COMMANDS,
        key=order_id,
        envelope=make_envelope(
            "ReserveFundsCommand",
            saga_id=order_id,
            payload=reserve_funds.__dict__,
            correlation_id=correlation_id,
        ),
    )
    publish_envelope(
        STOCK_COMMANDS,
        key=order_id,
        envelope=make_envelope(
            "ReserveStockCommand",
            saga_id=order_id,
            payload={"items": reserve_stock.items},
            correlation_id=correlation_id,
        ),
    )

    # For now, return 202 Accepted while saga completes asynchronously.
    return jsonify({"order_id": order_id, "status": STATUS_TRYING})


@app.get('/checkout_status/<order_id>')
def checkout_status(order_id: str):
    saga = get_saga(db, order_id)
    if saga is None:
        abort(404, f"Saga for order {order_id} not found")
    return jsonify({"order_id": order_id, "status": saga.get("status", STATUS_FAILED)})


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
