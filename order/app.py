import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import threading
import time

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from kafka_producer import publish_envelope
from kafka_consumer import start_consumer
from kafka_codec import decode_envelope
from kafka_models import (
    make_envelope,
    PAYMENT_COMMANDS,
    STOCK_COMMANDS,
    ORDER_EVENTS,
    ReserveFundsCommand,
    ReserveStockCommand,
    CommitFundsCommand,
    CommitStockCommand,
    CancelFundsCommand,
    CancelStockCommand,
    FundsReservedEvent,
    StockReservedEvent,
    FundsReserveFailedEvent,
    StockReserveFailedEvent,
    FundsCommittedEvent,
    StockCommittedEvent,
    FundsCancelledEvent,
    StockCancelledEvent,
)
from saga_store import (
    create_saga,
    get_saga,
    set_reservation_ids,
    set_status,
    set_committed_flags,
    get_committed_flags,
    append_outbox,
    pop_any_outbox,
    STATUS_TRYING,
    STATUS_RESERVED,
    STATUS_COMMITTED,
    STATUS_CANCELLED,
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

    env_funds = make_envelope(
        "ReserveFundsCommand",
        saga_id=order_id,
        payload=reserve_funds.__dict__,
        correlation_id=correlation_id,
    )
    env_stock = make_envelope(
        "ReserveStockCommand",
        saga_id=order_id,
        payload={"items": reserve_stock.items},
        correlation_id=correlation_id,
    )
    append_outbox(db, order_id, PAYMENT_COMMANDS, env_funds)
    append_outbox(db, order_id, STOCK_COMMANDS, env_stock)

    # For now, return 202 Accepted while saga completes asynchronously.
    return jsonify({"order_id": order_id, "status": STATUS_TRYING})


@app.get('/checkout_status/<order_id>')
def checkout_status(order_id: str):
    saga = get_saga(db, order_id)
    if saga is None:
        abort(404, f"Saga for order {order_id} not found")
    return jsonify({"order_id": order_id, "status": saga.get("status", STATUS_FAILED)})


def _handle_event(envelope):
    """
    Lightweight event handler updating saga state and issuing follow-up commands.
    Uses match/case for readability; main costs are I/O, not branching.
    """
    order_id = envelope.saga_id
    msg_type = envelope.type

    # Idempotency: rely on Redis set
    from saga_store import is_processed, mark_processed

    if is_processed(db, order_id, envelope.message_id):
        return

    def publish_commit_if_ready():
        saga = get_saga(db, order_id) or {}
        pay_res = saga.get("payment_reservation_id", "")
        stock_res = saga.get("stock_reservation_id", "")
        if pay_res and stock_res:
            set_status(db, order_id, STATUS_RESERVED)
            publish_envelope(
                PAYMENT_COMMANDS,
                key=order_id,
                envelope=make_envelope(
                    "CommitFundsCommand",
                    saga_id=order_id,
                    payload=CommitFundsCommand(reservation_id=pay_res).__dict__,
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.message_id,
                ),
            )
            publish_envelope(
                STOCK_COMMANDS,
                key=order_id,
                envelope=make_envelope(
                    "CommitStockCommand",
                    saga_id=order_id,
                    payload=CommitStockCommand(reservation_id=stock_res).__dict__,
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.message_id,
                ),
            )

    match msg_type:
        case "FundsReservedEvent":
            payload = FundsReservedEvent(**envelope.payload)
            set_reservation_ids(db, order_id, payment_reservation_id=payload.reservation_id)
            publish_commit_if_ready()  # trigger commits once both reservations exist
        case "StockReservedEvent":
            payload = StockReservedEvent(**envelope.payload)
            set_reservation_ids(db, order_id, stock_reservation_id=payload.reservation_id)
            publish_commit_if_ready()
        case "FundsReserveFailedEvent":
            payload = FundsReserveFailedEvent(**envelope.payload)
            app.logger.warning("Funds reservation failed for %s: %s", order_id, payload.reason)
            set_status(db, order_id, STATUS_FAILED)
            saga = get_saga(db, order_id) or {}
            stock_res = saga.get("stock_reservation_id", "")
            if stock_res:
                append_outbox(
                    db,
                    order_id,
                    STOCK_COMMANDS,
                    make_envelope(
                        "CancelStockCommand",
                        saga_id=order_id,
                        payload=CancelStockCommand(reservation_id=stock_res).__dict__,
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.message_id,
                    ),
                )
        case "StockReserveFailedEvent":
            payload = StockReserveFailedEvent(**envelope.payload)
            app.logger.warning("Stock reservation failed for %s: %s", order_id, payload.reason)
            set_status(db, order_id, STATUS_FAILED)
            saga = get_saga(db, order_id) or {}
            pay_res = saga.get("payment_reservation_id", "")
            if pay_res:
                append_outbox(
                    db,
                    order_id,
                    PAYMENT_COMMANDS,
                    make_envelope(
                        "CancelFundsCommand",
                        saga_id=order_id,
                        payload=CancelFundsCommand(reservation_id=pay_res).__dict__,
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.message_id,
                    ),
                )
        case "FundsCommittedEvent":
            payload = FundsCommittedEvent(**envelope.payload)
            set_committed_flags(db, order_id, funds_committed=True)
            funds_committed, stock_committed = get_committed_flags(db, order_id)
            if funds_committed and stock_committed:
                set_status(db, order_id, STATUS_COMMITTED)
                # mark order as paid once both commits are in
                order_entry: OrderValue = get_order_from_db(order_id)
                order_entry.paid = True
                db.set(order_id, msgpack.encode(order_entry))
        case "StockCommittedEvent":
            payload = StockCommittedEvent(**envelope.payload)
            set_committed_flags(db, order_id, stock_committed=True)
            funds_committed, stock_committed = get_committed_flags(db, order_id)
            if funds_committed and stock_committed:
                set_status(db, order_id, STATUS_COMMITTED)
                order_entry: OrderValue = get_order_from_db(order_id)
                order_entry.paid = True
                db.set(order_id, msgpack.encode(order_entry))
        case "FundsCancelledEvent" | "StockCancelledEvent":
            # Compensation completed -> mark cancelled
            set_status(db, order_id, STATUS_CANCELLED)
        case _:
            app.logger.debug("Unhandled event type %s", msg_type)

    # Mark message as processed after successful handling
    mark_processed(db, order_id, envelope.message_id)


def _start_consumer_thread():
    t = threading.Thread(target=start_consumer, args=(_handle_event,), daemon=True)
    t.start()
    app.logger.info("Kafka consumer thread started")


def _outbox_publisher_loop():
    """
    Simple outbox drainer: pops envelopes from any saga outbox and publishes.
    """
    while True:
        item = pop_any_outbox(db)
        if not item:
            time.sleep(0.5)
            continue
        order_id, topic, payload = item
        try:
            env = decode_envelope(payload)
            publish_envelope(topic, key=order_id, envelope=env)
        except Exception as exc:  # noqa: BLE001
            app.logger.exception("Failed to publish outbox envelope for %s: %s", order_id, exc)
            # push back to avoid loss
            db.lpush(f"saga:{order_id}:outbox", payload)
            time.sleep(0.5)


# Kick off consumer thread at import time (per worker)
_start_consumer_thread()

# Kick off outbox publisher thread
threading.Thread(target=_outbox_publisher_loop, daemon=True).start()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
