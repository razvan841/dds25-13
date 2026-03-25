import json
import logging
import time
import threading
import uuid

import redis
from msgspec import msgpack, Struct

from common.streams import (
    compute_shard, publish_command,
    stock_commands_stream, payment_commands_stream, saga_replies_stream,
    order_replies_stream, saga_redis_for_shard,
    SHARD_ID, SHARD_COUNT,
)

logger = logging.getLogger(__name__)


class TaskState(Struct):
    task_id: str
    saga_id: str
    command: str
    resource_id: str
    target: str           # "stock" or "payment"
    payload: str          # JSON-encoded command payload
    status: str           # PENDING, SENT, COMPLETED, FAILED
    result_status: str
    result_reason: str
    idempotency_key: str
    retry_count: int
    max_retries: int
    created_at: float
    updated_at: float


def _get_task(db, task_id):
    entry = db.get(f"task:{task_id}")
    if not entry:
        return None
    return msgpack.decode(entry, type=TaskState)


def _save_task(db, task):
    task.updated_at = time.time()
    db.set(f"task:{task.task_id}", msgpack.encode(task))


def _publish_task_command(db, task):
    """Publish the actual command to the target service stream."""
    target_shard = compute_shard(task.resource_id, SHARD_COUNT)

    if task.target == "stock":
        stream = stock_commands_stream(target_shard)
    else:
        stream = payment_commands_stream(target_shard)

    reply_stream = saga_replies_stream(SHARD_ID)

    # Update status to SENT before publishing (persist-then-publish for crash safety)
    task.status = "SENT"
    _save_task(db, task)

    # Publish command to target service stream
    publish_command(
        None,  # ignored when saga pool is active
        stream,
        task.saga_id,
        task.idempotency_key,
        task.command,
        task.payload,
        reply_stream=reply_stream,
    )


def _forward_result_to_order(task):
    """Forward task result to order-replies stream."""
    stream = order_replies_stream(SHARD_ID)
    conn = saga_redis_for_shard(SHARD_ID)
    conn.xadd(stream, {
        "task_id": task.task_id,
        "saga_id": task.saga_id,
        "command": task.command,
        "status": task.result_status,
        "reason": task.result_reason,
    })


def handle_task_submission(db, message_id, fields):
    """Handle a task submission from the order service."""
    task_id = fields["task_id"]
    saga_id = fields["saga_id"]
    command = fields["command"]
    resource_id = fields["resource_id"]
    target = fields["target"]
    payload = fields["payload"]
    # Optional: caller-provided idempotency key (used by 2PC for deterministic keys)
    idempotency_key = fields.get("idempotency_key", "") or str(uuid.uuid4())

    now = time.time()
    task = TaskState(
        task_id=task_id,
        saga_id=saga_id,
        command=command,
        resource_id=resource_id,
        target=target,
        payload=payload,
        status="PENDING",
        result_status="",
        result_reason="",
        idempotency_key=idempotency_key,
        retry_count=0,
        max_retries=3,
        created_at=now,
        updated_at=now,
    )

    # Persist task and create reverse index for reply lookup
    _save_task(db, task)
    db.set(f"idem:{task.idempotency_key}", task.task_id, ex=3600)
    db.sadd(f"saga_tasks:{saga_id}", task.task_id)

    # Publish to target service stream
    _publish_task_command(db, task)


def handle_reply(db, message_id, fields):
    """Handle a reply from stock/payment, forward result to order."""
    idempotency_key = fields.get("idempotency_key", "")
    saga_id = fields["saga_id"]
    command = fields["command"]
    status = fields["status"]
    reason = fields.get("reason", "")

    # Look up task by idempotency key
    task_id = db.get(f"idem:{idempotency_key}")
    if task_id:
        if isinstance(task_id, bytes):
            task_id = task_id.decode()
        task = _get_task(db, task_id)
        if task:
            task.status = "COMPLETED"
            task.result_status = status
            task.result_reason = reason
            _save_task(db, task)
            _forward_result_to_order(task)
            return

    # No task found — could be a reply to a command not routed through us
    # (e.g., during migration). Forward directly using the fields.
    logger.warning(f"No task found for idempotency_key={idempotency_key}, "
                   f"forwarding reply directly: saga_id={saga_id} command={command}")
    stream = order_replies_stream(SHARD_ID)
    conn = saga_redis_for_shard(SHARD_ID)
    conn.xadd(stream, {
        "task_id": "",
        "saga_id": saga_id,
        "command": command,
        "status": status,
        "reason": reason,
    })


def recover_tasks(db):
    """Recover incomplete tasks on startup. Re-publish SENT tasks."""
    cursor = "0"
    recovered = 0
    while True:
        cursor, keys = db.scan(cursor=cursor, match="task:*", count=100)
        for key in keys:
            if isinstance(key, bytes):
                key = key.decode()
            if not key.startswith("task:"):
                continue
            task_id = key[5:]
            task = _get_task(db, task_id)
            if task is None:
                continue
            if task.status in ("COMPLETED", "FAILED"):
                continue
            if task.status == "PENDING":
                # Crashed before publishing — publish now
                logger.info(f"Recovery: publishing PENDING task {task.task_id}")
                _publish_task_command(db, task)
                recovered += 1
            elif task.status == "SENT":
                # Crashed after publishing — re-publish (idempotent downstream)
                logger.info(f"Recovery: re-publishing SENT task {task.task_id}")
                _publish_task_command(db, task)
                recovered += 1
        if cursor == 0 or cursor == b"0":
            break
    if recovered:
        logger.info(f"Recovery complete: re-published {recovered} tasks")


def _retry_stale_tasks(db):
    """Re-publish SENT tasks that have timed out (no reply within 30s)."""
    cursor = "0"
    cutoff = time.time() - 30
    while True:
        cursor, keys = db.scan(cursor=cursor, match="task:*", count=100)
        for key in keys:
            if isinstance(key, bytes):
                key = key.decode()
            if not key.startswith("task:"):
                continue
            task_id = key[5:]
            task = _get_task(db, task_id)
            if task is None or task.status != "SENT":
                continue
            if task.updated_at > cutoff:
                continue
            if task.retry_count >= task.max_retries:
                task.status = "FAILED"
                task.result_status = "failure"
                task.result_reason = "max_retries_exceeded"
                _save_task(db, task)
                _forward_result_to_order(task)
                logger.warning(f"Task {task.task_id} failed after {task.max_retries} retries")
                continue
            task.retry_count += 1
            _publish_task_command(db, task)
            logger.info(f"Retried task {task.task_id} (attempt {task.retry_count})")
        if cursor == 0 or cursor == b"0":
            break


def start_retry_thread(db):
    """Start background thread that retries stale SENT tasks."""
    def _retry_loop():
        while True:
            time.sleep(10)
            try:
                _retry_stale_tasks(db)
            except Exception:
                logger.exception("Error in retry loop")

    t = threading.Thread(target=_retry_loop, daemon=True)
    t.start()
    logger.info("Task retry thread started")
