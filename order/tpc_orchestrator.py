import json
import logging
import random
import time
import threading
import uuid
from collections import defaultdict

from msgspec import msgpack, Struct

from common.streams import (
    submit_task, compute_shard,
    SHARD_ID, SHARD_COUNT,
)

TPC_COMMANDS = {"stock_prepare", "stock_commit", "stock_abort",
                "payment_prepare", "payment_commit", "payment_abort"}


class TpcState(Struct):
    status: str           # PREPARING, COMMITTING, ABORTING, COMMITTED, ABORTED, FAILED
    order_id: str
    txn_id: str           # unique per attempt (lock owner)
    user_id: str
    total_cost: int
    items: list[tuple[str, int]]
    stock_vote: str       # "", "VOTE-COMMIT", "VOTE-ABORT"
    payment_vote: str
    retry_count: int
    max_retries: int      # 5
    abort_reason: str     # "lock_contention", "insufficient_stock", "insufficient_credit"
    error: str
    participant_count: int  # total votes expected (stock_shards_involved + 1 payment)


def _get_tpc_state(db, order_id):
    try:
        entry = db.get(f"tpc:{order_id}")
    except Exception:
        return None
    if not entry:
        return None
    return msgpack.decode(entry, type=TpcState)


def _save_tpc_state(db, tpc):
    db.set(f"tpc:{tpc.order_id}", msgpack.encode(tpc))


def _items_by_shard(items):
    """Group items by their stock shard."""
    by_shard = defaultdict(list)
    for item_id, qty in items:
        by_shard[compute_shard(item_id, SHARD_COUNT)].append((item_id, qty))
    return by_shard


def _start_tpc(db, saga_redis, tpc):
    items_by_shard = _items_by_shard(tpc.items)
    tpc.participant_count = len(items_by_shard) + 1  # stock shards + 1 payment
    tpc.status = "PREPARING"
    tpc.stock_vote = ""
    tpc.payment_vote = ""
    tpc.abort_reason = ""
    _save_tpc_state(db, tpc)
    db.set(f"tpc:{tpc.order_id}:vote_count", 0)
    db.delete(f"tpc:{tpc.order_id}:stock_vote")
    db.delete(f"tpc:{tpc.order_id}:payment_vote")
    db.delete(f"tpc:{tpc.order_id}:abort_reason")

    # Send stock_prepare to each involved stock shard (use first item as resource_id)
    for shard_id, shard_items in items_by_shard.items():
        submit_task(saga_redis, tpc.order_id, "stock_prepare",
                    shard_items[0][0], "stock",
                    json.dumps({"items": shard_items, "txn_id": tpc.txn_id}))

    # Send payment_prepare to correct payment shard
    submit_task(saga_redis, tpc.order_id, "payment_prepare",
                tpc.user_id, "payment",
                json.dumps({"user_id": tpc.user_id, "amount": tpc.total_cost, "txn_id": tpc.txn_id}))


def _send_commit(db, saga_redis, tpc):
    """Send commit commands with deterministic idempotency keys."""
    items_by_shard = _items_by_shard(tpc.items)
    tpc.status = "COMMITTING"
    _save_tpc_state(db, tpc)
    db.set(f"tpc:{tpc.order_id}:commit_count", 0)

    for shard_id, shard_items in items_by_shard.items():
        stock_key = f"tpc-{tpc.txn_id}-stock_commit-{shard_id}"
        submit_task(saga_redis, tpc.order_id, "stock_commit",
                    shard_items[0][0], "stock",
                    json.dumps({"items": shard_items, "txn_id": tpc.txn_id}),
                    idempotency_key=stock_key)

    payment_key = f"tpc-{tpc.txn_id}-payment_commit"
    submit_task(saga_redis, tpc.order_id, "payment_commit",
                tpc.user_id, "payment",
                json.dumps({"user_id": tpc.user_id, "amount": tpc.total_cost, "txn_id": tpc.txn_id}),
                idempotency_key=payment_key)


def _send_abort(db, saga_redis, tpc):
    """Send abort commands with deterministic idempotency keys."""
    items_by_shard = _items_by_shard(tpc.items)
    tpc.status = "ABORTING"
    _save_tpc_state(db, tpc)
    db.set(f"tpc:{tpc.order_id}:abort_count", 0)

    for shard_id, shard_items in items_by_shard.items():
        stock_key = f"tpc-{tpc.txn_id}-stock_abort-{shard_id}"
        submit_task(saga_redis, tpc.order_id, "stock_abort",
                    shard_items[0][0], "stock",
                    json.dumps({"items": shard_items, "txn_id": tpc.txn_id}),
                    idempotency_key=stock_key)

    payment_key = f"tpc-{tpc.txn_id}-payment_abort"
    submit_task(saga_redis, tpc.order_id, "payment_abort",
                tpc.user_id, "payment",
                json.dumps({"user_id": tpc.user_id, "txn_id": tpc.txn_id}),
                idempotency_key=payment_key)


def create_reply_handler(db, saga_redis, mark_order_paid_fn):
    def handle_tpc_reply(message_id, fields):
        saga_id = fields["saga_id"]
        command = fields["command"]
        status = fields["status"]
        reason = fields.get("reason", "")

        tpc = _get_tpc_state(db, saga_id)
        if tpc is None:
            logging.warning(f"TPC {saga_id} not found for reply {command}")
            return

        if tpc.status == "PREPARING":
            if command == "stock_prepare":
                if status == "VOTE-ABORT":
                    db.set(f"tpc:{tpc.order_id}:stock_vote", "VOTE-ABORT")
                    if reason:
                        db.set(f"tpc:{tpc.order_id}:abort_reason", reason)
                else:
                    db.setnx(f"tpc:{tpc.order_id}:stock_vote", "VOTE-COMMIT")
            elif command == "payment_prepare":
                db.set(f"tpc:{tpc.order_id}:payment_vote", status)
                if status == "VOTE-ABORT" and reason:
                    db.set(f"tpc:{tpc.order_id}:abort_reason", reason)

            vote_count = db.incr(f"tpc:{tpc.order_id}:vote_count")
            if vote_count < tpc.participant_count:
                return

            stock_vote = db.get(f"tpc:{tpc.order_id}:stock_vote")
            payment_vote = db.get(f"tpc:{tpc.order_id}:payment_vote")
            abort_reason = db.get(f"tpc:{tpc.order_id}:abort_reason") or b""
            if isinstance(stock_vote, bytes):
                stock_vote = stock_vote.decode()
            if isinstance(payment_vote, bytes):
                payment_vote = payment_vote.decode()
            if isinstance(abort_reason, bytes):
                abort_reason = abort_reason.decode()

            tpc.stock_vote = stock_vote
            tpc.payment_vote = payment_vote
            tpc.abort_reason = abort_reason
            _save_tpc_state(db, tpc)

            if stock_vote == "VOTE-COMMIT" and payment_vote == "VOTE-COMMIT":
                _send_commit(db, saga_redis, tpc)
            else:
                _send_abort(db, saga_redis, tpc)

        elif tpc.status == "COMMITTING":
            commit_count = db.incr(f"tpc:{tpc.order_id}:commit_count")
            if commit_count >= tpc.participant_count:
                tpc.status = "COMMITTED"
                _save_tpc_state(db, tpc)
                mark_order_paid_fn(tpc.order_id)

        elif tpc.status == "ABORTING":
            abort_count = db.incr(f"tpc:{tpc.order_id}:abort_count")
            if abort_count >= tpc.participant_count:
                reason = tpc.abort_reason
                if reason == "lock_contention" and tpc.retry_count < tpc.max_retries:
                    backoff = 0.1 * (2 ** tpc.retry_count) + random.uniform(0, 0.05)
                    next_retry = tpc.retry_count + 1
                    order_id = tpc.order_id
                    tpc.status = "RETRY_PENDING"
                    _save_tpc_state(db, tpc)

                    def _do_retry(delay=backoff, oid=order_id, rc=next_retry):
                        time.sleep(delay)
                        t = _get_tpc_state(db, oid)
                        if t is None or t.status != "RETRY_PENDING":
                            return
                        t.retry_count = rc
                        t.txn_id = str(uuid.uuid4())
                        t.abort_reason = ""
                        t.error = ""
                        _start_tpc(db, saga_redis, t)

                    threading.Thread(target=_do_retry, daemon=True).start()
                elif reason in ("insufficient_stock", "insufficient_credit"):
                    tpc.status = "FAILED"
                    tpc.error = reason
                    _save_tpc_state(db, tpc)
                else:
                    tpc.status = "FAILED" if tpc.retry_count >= tpc.max_retries else "ABORTED"
                    tpc.error = reason or "aborted"
                    _save_tpc_state(db, tpc)

    return handle_tpc_reply


def start_checkout(db, saga_redis, order_id, user_id, total_cost, items):
    tpc = TpcState(
        status="PREPARING",
        order_id=order_id,
        txn_id=str(uuid.uuid4()),
        user_id=user_id,
        total_cost=total_cost,
        items=items,
        stock_vote="",
        payment_vote="",
        retry_count=0,
        max_retries=5,
        abort_reason="",
        error="",
        participant_count=2,  # will be recalculated in _start_tpc
    )
    _start_tpc(db, saga_redis, tpc)


def poll_result(db, order_id, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(0.05)
        current = _get_tpc_state(db, order_id)
        if current is None:
            continue
        if current.status == "COMMITTED":
            return (True, None)
        if current.status in ("FAILED", "ABORTED"):
            return (False, current.error or "Checkout failed")
    return (False, "Checkout timeout")


def recover_tpcs(db, saga_redis):
    """Recover incomplete TPC transactions on startup."""
    cursor = "0"
    while True:
        cursor, keys = db.scan(cursor=cursor, match="tpc:*", count=100)
        for key in keys:
            if isinstance(key, bytes):
                key = key.decode()
            # Skip auxiliary keys (tpc:order_id:vote_count, etc.)
            parts = key.split(":")
            if len(parts) != 2:
                continue
            order_id = parts[1]
            tpc = _get_tpc_state(db, order_id)
            if tpc is None:
                continue
            if tpc.status in ("COMMITTED", "FAILED", "ABORTED"):
                continue
            if tpc.status == "RETRY_PENDING":
                logging.info(f"TPC recovery: failing RETRY_PENDING tpc:{order_id}")
                tpc.status = "FAILED"
                tpc.error = "retry_interrupted"
                _save_tpc_state(db, tpc)
                continue
            if tpc.status == "PREPARING":
                logging.info(f"TPC recovery: aborting PREPARING tpc:{order_id}")
                _send_abort(db, saga_redis, tpc)
            elif tpc.status == "COMMITTING":
                logging.info(f"TPC recovery: re-sending commits for tpc:{order_id}")
                _send_commit(db, saga_redis, tpc)
            elif tpc.status == "ABORTING":
                logging.info(f"TPC recovery: re-sending aborts for tpc:{order_id}")
                _send_abort(db, saga_redis, tpc)
        if cursor == 0 or cursor == b"0":
            break
