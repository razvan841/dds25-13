import logging
import os
import threading

import redis
from flask import Flask, jsonify

from common.streams import (
    init_saga_pool, get_saga_redis, ensure_all_streams, consume_loop,
    saga_replies_stream, orchestrator_commands_stream,
    ORCHESTRATOR_TASK_WORKERS, ORCHESTRATOR_REPLY_WORKERS,
    SHARD_ID,
)
import task_manager

app = Flask("orchestrator-service")

# Orchestrator's own Redis for task state storage
db: redis.Redis = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB']),
)

# Initialize saga-redis pool for publishing to stock/payment/order-replies streams
init_saga_pool()
saga_redis = get_saga_redis()

# Ensure all streams and consumer groups exist
ensure_all_streams(saga_redis)

# --- Startup recovery ---
if db.set("recovery_lock", "1", nx=True, ex=30):
    try:
        task_manager.recover_tasks(db)
    finally:
        db.delete("recovery_lock")

# --- Consumer thread: task submissions from order service ---

def handle_submission(message_id, fields):
    task_manager.handle_task_submission(db, message_id, fields)


submission_thread = threading.Thread(
    target=consume_loop,
    args=(saga_redis, orchestrator_commands_stream(SHARD_ID),
          ORCHESTRATOR_TASK_WORKERS, handle_submission),
    daemon=True,
)
submission_thread.start()

# --- Consumer thread: replies from stock/payment services ---

def handle_reply(message_id, fields):
    task_manager.handle_reply(db, message_id, fields)


reply_thread = threading.Thread(
    target=consume_loop,
    args=(saga_redis, saga_replies_stream(SHARD_ID),
          ORCHESTRATOR_REPLY_WORKERS, handle_reply),
    daemon=True,
)
reply_thread.start()

# --- Background retry thread ---
task_manager.start_retry_thread(db)


# --- REST endpoints ---

@app.get('/health')
def health():
    return jsonify({"status": "ok", "shard_id": SHARD_ID})


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
