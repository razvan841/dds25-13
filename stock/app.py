import logging
import os
import atexit
import uuid
import json
import threading

import redis

from msgspec import msgpack
from flask import Flask, jsonify, abort, Response

from common.streams import (
    get_saga_redis, ensure_all_streams, check_idempotency, get_idempotency_state,
    consume_loop, publish_reply,
    STOCK_WORKERS, SHARD_ID, SHARD_COUNT,
    stock_commands_stream, generate_shard_affine_uuid, compute_shard,
)
from models import StockValue
import saga_handler
import tpc_handler


DB_ERROR_STR = "DB error"

app = Flask("stock-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        abort(400, f"Item: {item_id} not found!")
    return entry


@app.post('/item/create/<price>')
def create_item(price: int):
    key = generate_shard_affine_uuid(SHARD_ID, SHARD_COUNT)
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
                                  for i in range(n) if compute_shard(str(i), SHARD_COUNT) == SHARD_ID}
    if kv_pairs:
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
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


# --- Stream consumer ---

SAGA_COMMANDS = {"stock_subtract", "stock_add"}
TPC_COMMANDS = {"stock_prepare", "stock_commit", "stock_abort"}

saga_redis = get_saga_redis()
ensure_all_streams(saga_redis)


def _handle_duplicate(idempotency_key, fields):
    """Handle a duplicate message: re-send stored reply or send failure for crashed processing."""
    saga_id = fields["saga_id"]
    command = fields["command"]
    reply_stream = fields.get("reply_stream")
    state, reply_json = get_idempotency_state(db, idempotency_key)
    if state == "done" and reply_json:
        reply = json.loads(reply_json)
        publish_reply(saga_redis, reply["saga_id"], idempotency_key,
                       reply["command"], reply["status"], reply.get("reason", ""),
                       reply_stream=reply_stream)
    elif state == "processing":
        if command in ("stock_commit", "stock_abort"):
            publish_reply(saga_redis, saga_id, idempotency_key, command, "ACK",
                           reply_stream=reply_stream)
        else:
            publish_reply(saga_redis, saga_id, idempotency_key, command,
                           "failure" if command in SAGA_COMMANDS else "VOTE-ABORT",
                           "crashed_during_processing", reply_stream=reply_stream)


def handle_stock_command(message_id, fields):
    idempotency_key = fields["idempotency_key"]
    command = fields["command"]

    if not check_idempotency(db, idempotency_key):
        logging.info(f"Duplicate command {idempotency_key}, replaying")
        _handle_duplicate(idempotency_key, fields)
        return

    if command in TPC_COMMANDS:
        tpc_handler.handle(db, saga_redis, fields)
    elif command in SAGA_COMMANDS:
        saga_handler.handle(db, saga_redis, fields)


consumer_thread = threading.Thread(
    target=consume_loop,
    args=(saga_redis, stock_commands_stream(SHARD_ID), STOCK_WORKERS, handle_stock_command),
    daemon=True,
)
consumer_thread.start()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
