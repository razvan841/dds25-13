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
    get_saga_redis, init_saga_pool, ensure_all_streams, check_idempotency, get_idempotency_state,
    consume_loop, publish_reply,
    PAYMENT_WORKERS, SHARD_ID, SHARD_COUNT,
    payment_commands_stream, generate_shard_affine_uuid, compute_shard,
)
from models import UserValue
import saga_handler
import tpc_handler


DB_ERROR_STR = "DB error"

app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        abort(400, f"User: {user_id} not found!")
    return entry


@app.post('/create_user')
def create_user():
    key = generate_shard_affine_uuid(SHARD_ID, SHARD_COUNT)
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n) if compute_shard(str(i), SHARD_COUNT) == SHARD_ID}
    if kv_pairs:
        try:
            db.mset(kv_pairs)
        except redis.exceptions.RedisError:
            return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


# --- Stream consumer ---

SAGA_COMMANDS = {"payment_pay", "payment_refund"}
TPC_COMMANDS = {"payment_prepare", "payment_commit", "payment_abort"}

init_saga_pool()
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
        if command in ("payment_commit", "payment_abort"):
            publish_reply(saga_redis, saga_id, idempotency_key, command, "ACK",
                           reply_stream=reply_stream)
        else:
            publish_reply(saga_redis, saga_id, idempotency_key, command,
                           "failure" if command in SAGA_COMMANDS else "VOTE-ABORT",
                           "crashed_during_processing", reply_stream=reply_stream)


def handle_payment_command(message_id, fields):
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


CONSUMER_THREADS = int(os.environ.get("CONSUMER_THREADS", "4"))
for _i in range(CONSUMER_THREADS):
    threading.Thread(
        target=consume_loop,
        args=(saga_redis, payment_commands_stream(SHARD_ID), PAYMENT_WORKERS, handle_payment_command),
        daemon=True,
    ).start()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
