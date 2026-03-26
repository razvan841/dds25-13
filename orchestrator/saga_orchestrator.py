import json
import logging
import time
import uuid

from msgspec import msgpack, Struct

from common.streams import (
    publish_command, compute_shard,
    SHARD_ID, SHARD_COUNT,
    stock_commands_stream, payment_commands_stream, saga_replies_stream,
)


class SagaState(Struct):
    status: str
    order_id: str
    user_id: str
    total_cost: int
    items: list[tuple[str, int]]
    subtracted_items: list[tuple[str, int]]
    current_item_index: int
    compensations_expected: int
    compensations_received: int
    error: str


def _get_saga_state(db, order_id):
    try:
        entry = db.get(f"saga:{order_id}")
    except Exception:
        return None
    if not entry:
        return None
    return msgpack.decode(entry, type=SagaState)


def _save_saga_state(db, saga):
    db.set(f"saga:{saga.order_id}", msgpack.encode(saga))


def _publish_stock_command(saga_redis, saga_id, command, item_id, quantity):
    shard = compute_shard(item_id, SHARD_COUNT)
    key = str(uuid.uuid4())
    reply_stream = saga_replies_stream(SHARD_ID)
    publish_command(saga_redis, stock_commands_stream(shard), saga_id, key, command,
                    json.dumps({"item_id": item_id, "quantity": quantity}),
                    reply_stream=reply_stream)


def _publish_payment_command(saga_redis, saga_id, command, user_id, amount):
    shard = compute_shard(user_id, SHARD_COUNT)
    key = str(uuid.uuid4())
    reply_stream = saga_replies_stream(SHARD_ID)
    publish_command(saga_redis, payment_commands_stream(shard), saga_id, key, command,
                    json.dumps({"user_id": user_id, "amount": amount}),
                    reply_stream=reply_stream)


def _advance_saga(db, saga_redis, saga):
    if saga.status == "STARTED":
        if len(saga.items) == 0:
            saga.status = "PAYMENT_PENDING"
            _save_saga_state(db, saga)
            _publish_payment_command(saga_redis, saga.order_id, "payment_pay",
                                     saga.user_id, saga.total_cost)
        else:
            saga.status = "STOCK_SUBTRACTING"
            saga.current_item_index = 0
            _save_saga_state(db, saga)
            item_id, quantity = saga.items[0]
            _publish_stock_command(saga_redis, saga.order_id, "stock_subtract", item_id, quantity)


def _start_compensations(db, saga_redis, saga, error):
    """Start compensation phase. Returns True if immediately failed (no items to compensate)."""
    saga.error = error
    if len(saga.subtracted_items) == 0:
        saga.status = "FAILED"
        _save_saga_state(db, saga)
        return True
    saga.status = "STOCK_COMPENSATING"
    saga.compensations_expected = len(saga.subtracted_items)
    saga.compensations_received = 0
    _save_saga_state(db, saga)
    db.set(f"saga:{saga.order_id}:comp_count", 0)
    for item_id, quantity in saga.subtracted_items:
        _publish_stock_command(saga_redis, saga.order_id, "stock_add", item_id, quantity)
    return False


def create_reply_handler(db, saga_redis, on_complete_fn):
    def handle_saga_reply(message_id, fields):
        saga_id = fields["saga_id"]
        command = fields["command"]
        status = fields["status"]
        reason = fields.get("reason", "")

        saga = _get_saga_state(db, saga_id)
        if saga is None:
            logging.warning(f"Saga {saga_id} not found for reply {command}")
            return

        if saga.status == "STOCK_SUBTRACTING":
            if command == "stock_subtract":
                if status == "success":
                    item_id, quantity = saga.items[saga.current_item_index]
                    saga.subtracted_items.append((item_id, quantity))
                    saga.current_item_index += 1
                    if saga.current_item_index >= len(saga.items):
                        saga.status = "PAYMENT_PENDING"
                        _save_saga_state(db, saga)
                        _publish_payment_command(saga_redis, saga.order_id, "payment_pay",
                                                 saga.user_id, saga.total_cost)
                    else:
                        _save_saga_state(db, saga)
                        next_item_id, next_quantity = saga.items[saga.current_item_index]
                        _publish_stock_command(saga_redis, saga.order_id, "stock_subtract",
                                               next_item_id, next_quantity)
                else:
                    immediately_failed = _start_compensations(db, saga_redis, saga, reason)
                    if immediately_failed:
                        on_complete_fn(saga.order_id, False, saga.error)

        elif saga.status == "PAYMENT_PENDING":
            if command == "payment_pay":
                if status == "success":
                    saga.status = "COMPLETED"
                    _save_saga_state(db, saga)
                    on_complete_fn(saga.order_id, True)
                else:
                    immediately_failed = _start_compensations(db, saga_redis, saga, reason)
                    if immediately_failed:
                        on_complete_fn(saga.order_id, False, saga.error)

        elif saga.status == "STOCK_COMPENSATING":
            if command == "stock_add":
                count = db.incr(f"saga:{saga.order_id}:comp_count")
                if count >= saga.compensations_expected:
                    saga.compensations_received = saga.compensations_expected
                    saga.status = "FAILED"
                    _save_saga_state(db, saga)
                    on_complete_fn(saga.order_id, False, saga.error)

    return handle_saga_reply


def start_checkout(db, saga_redis, order_id, user_id, total_cost, items):
    saga = SagaState(
        status="STARTED",
        order_id=order_id,
        user_id=user_id,
        total_cost=total_cost,
        items=items,
        subtracted_items=[],
        current_item_index=0,
        compensations_expected=0,
        compensations_received=0,
        error="",
    )
    _save_saga_state(db, saga)
    _advance_saga(db, saga_redis, saga)


def poll_result(db, order_id, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(0.05)
        current = _get_saga_state(db, order_id)
        if current is None:
            continue
        if current.status == "COMPLETED":
            return (True, None)
        if current.status == "FAILED":
            return (False, current.error or "Checkout failed")
    return (False, "Checkout timeout")


def recover_sagas(db, saga_redis, on_complete_fn=None):
    """Recover incomplete sagas on startup."""
    cursor = "0"
    while True:
        cursor, keys = db.scan(cursor=cursor, match="saga:*", count=100)
        for key in keys:
            if isinstance(key, bytes):
                key = key.decode()
            # Skip auxiliary keys (saga:order_id:comp_count, etc.)
            parts = key.split(":")
            if len(parts) != 2:
                continue
            order_id = parts[1]
            saga = _get_saga_state(db, order_id)
            if saga is None:
                continue
            if saga.status == "COMPLETED":
                if on_complete_fn:
                    on_complete_fn(saga.order_id, True)
                continue
            if saga.status == "FAILED":
                if on_complete_fn:
                    on_complete_fn(saga.order_id, False, saga.error)
                continue
            if saga.status == "STARTED":
                logging.info(f"Saga recovery: marking STARTED saga:{order_id} as FAILED")
                saga.status = "FAILED"
                saga.error = "crashed_before_processing"
                _save_saga_state(db, saga)
                if on_complete_fn:
                    on_complete_fn(saga.order_id, False, saga.error)
            elif saga.status in ("STOCK_SUBTRACTING", "PAYMENT_PENDING"):
                logging.info(f"Saga recovery: compensating {saga.status} saga:{order_id}")
                _start_compensations(db, saga_redis, saga, "crashed_during_processing")
            elif saga.status == "STOCK_COMPENSATING":
                # Leave as-is — consume_loop recovery will re-deliver pending compensation replies
                logging.info(f"Saga recovery: leaving STOCK_COMPENSATING saga:{order_id} for consume_loop recovery")
        if cursor == 0 or cursor == b"0":
            break
