import json
import logging
import os
import threading
import time

import redis

from common.streams import (
    get_saga_redis, init_saga_pool, ensure_all_streams, consume_loop,
    ORCHESTRATOR_WORKERS, SHARD_ID, SHARD_COUNT,
    saga_replies_stream, checkout_requests_stream, checkout_results_stream,
    CHECKOUT_REQUEST_WORKERS, saga_redis_for_shard,
)
import saga_orchestrator
import tpc_orchestrator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orchestrator")

# Orchestrator's own Redis for saga/TPC state
db = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB']),
)

CHECKOUT_MODE = os.environ.get("CHECKOUT_MODE", "saga")

# Initialize saga-redis pool (for publishing commands + consuming replies)
init_saga_pool()
saga_redis = get_saga_redis()
ensure_all_streams(saga_redis)


# --- Result publishing callback ---
def publish_checkout_result(order_id, success, error=""):
    """Publish checkout result to checkout-results-{SHARD_ID} stream."""
    stream = checkout_results_stream(SHARD_ID)
    conn = saga_redis_for_shard(SHARD_ID)
    conn.xadd(stream, {
        "order_id": order_id,
        "status": "success" if success else "failure",
        "error": error or "",
    })
    logger.info(f"Published result for {order_id}: {'success' if success else 'failure'}")


# --- Startup recovery ---
if db.set("recovery_lock", "1", nx=True, ex=30):
    try:
        saga_orchestrator.recover_sagas(db, saga_redis, on_complete_fn=publish_checkout_result)
        tpc_orchestrator.recover_tpcs(db, saga_redis, on_complete_fn=publish_checkout_result)
    finally:
        db.delete("recovery_lock")


# --- Reply handler (from stock/payment via saga-replies-{shard}) ---
_saga_reply_handler = saga_orchestrator.create_reply_handler(db, saga_redis, publish_checkout_result)
_tpc_reply_handler = tpc_orchestrator.create_reply_handler(db, saga_redis, publish_checkout_result)


def handle_orchestrator_reply(message_id, fields):
    command = fields.get("command", "")
    if command in tpc_orchestrator.TPC_COMMANDS:
        _tpc_reply_handler(message_id, fields)
    else:
        _saga_reply_handler(message_id, fields)


# --- Request handler (from order service via checkout-requests-{shard}) ---
def handle_checkout_request(message_id, fields):
    order_id = fields["order_id"]
    user_id = fields["user_id"]
    total_cost = int(fields["total_cost"])
    items = json.loads(fields["items"])
    mode = fields.get("checkout_mode", CHECKOUT_MODE)

    logger.info(f"Checkout request: {order_id} mode={mode}")

    if mode == "2pc":
        tpc_orchestrator.start_checkout(db, saga_redis, order_id, user_id, total_cost, items)
    else:
        saga_orchestrator.start_checkout(db, saga_redis, order_id, user_id, total_cost, items)


# --- Start consumer threads ---
reply_thread = threading.Thread(
    target=consume_loop,
    args=(saga_redis, saga_replies_stream(SHARD_ID), ORCHESTRATOR_WORKERS, handle_orchestrator_reply),
    daemon=True,
)
reply_thread.start()

request_thread = threading.Thread(
    target=consume_loop,
    args=(saga_redis, checkout_requests_stream(SHARD_ID), CHECKOUT_REQUEST_WORKERS, handle_checkout_request),
    daemon=True,
)
request_thread.start()

logger.info(f"Orchestrator shard {SHARD_ID} running (mode={CHECKOUT_MODE})")
while True:
    time.sleep(60)
