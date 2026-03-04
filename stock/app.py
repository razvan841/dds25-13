import logging
import os
import atexit
import sys
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from common_kafka.models import make_envelope, STOCK_COMMANDS
from common_kafka.producer import publish_envelope
from stock.orchestrators import select_orchestrator


DB_ERROR_STR = "DB error"


def _get_bool_env(var_name: str, default: str = "false") -> bool:
    return os.environ.get(var_name, default).lower() in {"1", "true", "yes", "on"}

USE_2PL2PC = _get_bool_env("USE_2PL2PC", "false")
ORCHESTRATION_MODE = "2pl2pc"

app = Flask("stock-service")

DEV = True

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

if DEV:
    try:
        db.flushdb()
        app.logger.warning("[stock] DEV=true -> Redis database flushed on startup")
    except redis.exceptions.RedisError:
        app.logger.exception("[stock] Failed to flush Redis during DEV startup")


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    """Retrieve and deserialise a StockValue from Redis, aborting on error."""
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        abort(400, f"Item: {item_id} not found!")
    return entry


# ---------------------------------------------------------------------------
# Saga orchestrator (participant side)
# ---------------------------------------------------------------------------

orchestrator = select_orchestrator(
    ORCHESTRATION_MODE,
    db=db,
    logger=app.logger,
    fetch_item_fn=get_item_from_db,
)

app.logger.info("[stock] Coordination mode set to %s", ORCHESTRATION_MODE)


def handle_command(envelope) -> None:
    """
    Entry point for the Kafka consumer worker.

    Delegates the decoded :class:`~common_kafka.models.Envelope` to the
    active orchestrator so that stock commands (Reserve / Commit / Cancel)
    are handled consistently regardless of the chosen coordination mode.
    """
    return orchestrator.handle_command(envelope)


# ---------------------------------------------------------------------------
# Kafka health-check endpoint
# ---------------------------------------------------------------------------

@app.get("/kafka_ping")
def kafka_ping():
    """
    Publish a test envelope to ``stock.commands`` and return its ``message_id``.

    Useful for verifying that the Kafka producer is wired up correctly without
    triggering any saga logic (the consumer simply logs ``StockServicePing``
    messages and marks them processed).
    """
    ping_id = str(uuid.uuid4())
    envelope = make_envelope(
        "StockServicePing",
        saga_id=ping_id,
        payload={"msg": "ping", "service": "stock"},
    )
    try:
        publish_envelope(STOCK_COMMANDS, key=ping_id, envelope=envelope)
    except Exception as exc:  # noqa: BLE001
        app.logger.exception("Kafka ping failed: %s", exc)
        abort(500, "Kafka publish failed")
    app.logger.info("[stock] Kafka ping sent: %s", ping_id)
    return jsonify({"status": "sent", "message_id": envelope.message_id, "saga_id": ping_id})


# ---------------------------------------------------------------------------
# REST endpoints (item management)
# ---------------------------------------------------------------------------

@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
