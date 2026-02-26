from __future__ import annotations

import time
from typing import Any, Optional

import redis

from kafka_models import Envelope
from kafka_codec import encode_envelope

# Saga status values
STATUS_TRYING = "TRYING"
STATUS_RESERVED = "RESERVED"   # both reservations obtained, awaiting commit
STATUS_COMMITTED = "COMMITTED"
STATUS_CANCELLED = "CANCELLED"
STATUS_FAILED = "FAILED"


def _saga_key(order_id: str) -> str:
    return f"saga:{order_id}"


def _processed_key(order_id: str) -> str:
    return f"{_saga_key(order_id)}:processed"


def _outbox_key(order_id: str) -> str:
    return f"{_saga_key(order_id)}:outbox"


def create_saga(
    db: redis.Redis,
    order_id: str,
    correlation_id: str,
    deadline_ts: float,
) -> None:
    """
    Initialize saga state for an order. Existing state is overwritten.
    """
    pipe = db.pipeline()
    pipe.hset(
        _saga_key(order_id),
        mapping={
            "status": STATUS_TRYING,
            "correlation_id": correlation_id,
            "deadline_ts": deadline_ts,
            "payment_reservation_id": "",
            "stock_reservation_id": "",
        },
    )
    pipe.delete(_processed_key(order_id))
    pipe.delete(_outbox_key(order_id))
    pipe.execute()


def get_saga(db: redis.Redis, order_id: str) -> dict[str, Any] | None:
    data = db.hgetall(_saga_key(order_id))
    if not data:
        return None
    # redis returns bytes; decode to str
    return {k.decode(): v.decode() for k, v in data.items()}


def set_status(db: redis.Redis, order_id: str, status: str) -> None:
    db.hset(_saga_key(order_id), "status", status)


def set_reservation_ids(
    db: redis.Redis,
    order_id: str,
    payment_reservation_id: Optional[str] = None,
    stock_reservation_id: Optional[str] = None,
) -> None:
    mapping = {}
    if payment_reservation_id is not None:
        mapping["payment_reservation_id"] = payment_reservation_id
    if stock_reservation_id is not None:
        mapping["stock_reservation_id"] = stock_reservation_id
    if mapping:
        db.hset(_saga_key(order_id), mapping=mapping)


def mark_processed(db: redis.Redis, order_id: str, message_id: str) -> None:
    db.sadd(_processed_key(order_id), message_id)


def is_processed(db: redis.Redis, order_id: str, message_id: str) -> bool:
    return bool(db.sismember(_processed_key(order_id), message_id))


def append_outbox(db: redis.Redis, order_id: str, envelope: Envelope) -> None:
    """
    Store an outbound envelope in the saga outbox (left-push for simple pop semantics).
    """
    payload = encode_envelope(envelope)
    db.lpush(_outbox_key(order_id), payload)


def pop_outbox(db: redis.Redis, order_id: str) -> Optional[bytes]:
    """
    Pop the latest outbound envelope payload (bytes) or None if empty.
    """
    return db.rpop(_outbox_key(order_id))


def is_deadline_exceeded(db: redis.Redis, order_id: str) -> bool:
    data = db.hget(_saga_key(order_id), "deadline_ts")
    if data is None:
        return False
    try:
        deadline = float(data.decode())
    except Exception:
        return False
    return time.time() > deadline
