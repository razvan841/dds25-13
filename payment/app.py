import logging
import os
import atexit
import uuid
import sys
import time

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from common_kafka.models import make_envelope, PAYMENT_COMMANDS, PAYMENT_EVENTS
from common_kafka.producer import publish_envelope
from common_kafka.config import generate_shard_uuid, SHARD_INDEX, NUM_SHARDS

from payment.orchestrators import select_orchestrator

DB_ERROR_STR = "DB error"


# Logging level controlled via env LOG_LEVEL (default INFO)
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [payment] %(message)s",
    stream=sys.stdout,
    force=True,
)


def _get_bool_env(var_name: str, default: str = "false") -> bool:
    return os.environ.get(var_name, default).lower() in {"1", "true", "yes", "on"}


ORCHESTRATION_MODE = os.environ.get("ORCHESTRATION_MODE", "saga")
DEV = os.environ.get("DEV", "false").lower() in {"1", "true", "yes", "on"}


app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

if DEV:
    _delay = 1
    while True:
        try:
            db.flushdb()
            app.logger.warning("[payment] DEV=true -> Redis database flushed on startup")
            break
        except redis.exceptions.RedisError as e:
            app.logger.warning("[payment] Redis not ready yet (%s), retrying in %ds...", e, _delay)
            time.sleep(_delay)
            _delay = min(_delay * 2, 30)


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


orchestrator = select_orchestrator(
    ORCHESTRATION_MODE,
    db=db,
    logger=app.logger,
    fetch_user_fn=get_user_from_db,
)


def _run_2pc_startup_recovery_once() -> None:
    if ORCHESTRATION_MODE != "2pl2pc":
        return
    if not hasattr(orchestrator, "recover_inflight_transactions"):
        return

    lock_key = "payment:2pc:startup-recovery-lock"
    acquired = db.set(lock_key, str(uuid.uuid4()), nx=True, ex=30)
    if not acquired:
        app.logger.info("[payment] Startup 2PC recovery already running in another process")
        return

    recovered = orchestrator.recover_inflight_transactions()
    if recovered:
        app.logger.warning("[payment] Startup 2PC recovery cleaned %s interrupted lock(s)", recovered)
    else:
        app.logger.info("[payment] Startup 2PC recovery found no interrupted locks")


_run_2pc_startup_recovery_once()

app.logger.info("[payment] Coordination mode set to %s", ORCHESTRATION_MODE)


def handle_command(envelope):
    """Entry point for Kafka consumer."""
    return orchestrator.handle_command(envelope)


@app.get("/kafka_ping")
def kafka_ping():
    """
    Health check: publishes a test envelope to Kafka and returns the message id.
    Mirrors order service behavior.
    """
    ping_id = str(uuid.uuid4())
    envelope = make_envelope(
        "PaymentServicePing",
        transaction_id=ping_id,
        payload={"msg": "ping", "service": "payment"},
    )
    try:
        publish_envelope(PAYMENT_COMMANDS, key=ping_id, envelope=envelope)
    except Exception as exc:  # noqa: BLE001
        app.logger.exception("Kafka ping failed: %s", exc)
        abort(500, "Kafka publish failed")
    app.logger.info("Kafka ping sent: %s", ping_id)
    return jsonify({"status": "sent", "message_id": envelope.message_id, "transaction_id": ping_id})


@app.post('/create_user')
def create_user():
    key = generate_shard_uuid()
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
    kv_pairs: dict[str, bytes] = {f"{i * NUM_SHARDS + SHARD_INDEX}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
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
    # update credit, serialize and update database
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
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)

@app.get("/kafka_ping_order")
def kafka_ping_order():
    ping_id = str(uuid.uuid4())
    envelope = make_envelope(
        "PaymentServicePing",
        transaction_id=ping_id,
        payload={"msg": "ping", "service": "payment"},
    )
    publish_envelope(PAYMENT_EVENTS, key=ping_id, envelope=envelope)
    return jsonify({"status": "sent", "message_id": envelope.message_id, "transaction_id": ping_id})
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
