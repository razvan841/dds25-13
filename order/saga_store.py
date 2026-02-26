from __future__ import annotations

import time
from typing import Any, Optional
import msgspec

import redis

from kafka_models import Envelope
from kafka_codec import encode_envelope

# Saga status values
STATUS_TRYING = "TRYING"
STATUS_RESERVED = "RESERVED"   # both reservations obtained, awaiting commit
STATUS_COMMITTED = "COMMITTED"
STATUS_CANCELLED = "CANCELLED"
STATUS_FAILED = "FAILED"
STATUS_UNKNOWN = "UNKNOWN"


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
            "funds_committed": "",
            "stock_committed": "",
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


def get_reservation_ids(db: redis.Redis, order_id: str) -> tuple[Optional[str], Optional[str]]:
    pay, stock = db.hmget(_saga_key(order_id), ["payment_reservation_id", "stock_reservation_id"])
    return (pay.decode() if pay else None, stock.decode() if stock else None)


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


def set_committed_flags(
    db: redis.Redis,
    order_id: str,
    funds_committed: Optional[bool] = None,
    stock_committed: Optional[bool] = None,
) -> None:
    mapping = {}
    if funds_committed is not None:
        mapping["funds_committed"] = "1" if funds_committed else ""
    if stock_committed is not None:
        mapping["stock_committed"] = "1" if stock_committed else ""
    if mapping:
        db.hset(_saga_key(order_id), mapping=mapping)


def get_committed_flags(db: redis.Redis, order_id: str) -> tuple[bool, bool]:
    data = db.hmget(_saga_key(order_id), ["funds_committed", "stock_committed"])
    funds, stock = data
    return (bool(funds and funds.decode()), bool(stock and stock.decode()))


def mark_processed(db: redis.Redis, order_id: str, message_id: str) -> None:
    db.sadd(_processed_key(order_id), message_id)


def is_processed(db: redis.Redis, order_id: str, message_id: str) -> bool:
    return bool(db.sismember(_processed_key(order_id), message_id))


def append_outbox(db: redis.Redis, order_id: str, topic: str, envelope: Envelope) -> None:
    """
    Store an outbound envelope + explicit topic in the saga outbox (left-push).
    """
    entry = msgspec.msgpack.encode(
        {
            "topic": topic,
            "payload": encode_envelope(envelope),
        }
    )
    db.lpush(_outbox_key(order_id), entry)


def pop_outbox(db: redis.Redis, order_id: str) -> Optional[tuple[str, bytes]]:
    """
    Pop the latest outbound entry for a saga.
    Returns (topic, payload) or None.
    """
    raw = db.rpop(_outbox_key(order_id))
    if not raw:
        return None
    data = msgspec.msgpack.decode(raw)
    return data["topic"], data["payload"]


def pop_any_outbox(db: redis.Redis) -> Optional[tuple[str, str, bytes]]:
    """
    Pop an entry from any saga outbox.
    Returns (order_id, topic, payload) or None if none exist.
    """
    for key in db.scan_iter(match="saga:*:outbox", count=50):
        raw = db.rpop(key)
        if raw:
            order_id = key.decode().split(":")[1]
            data = msgspec.msgpack.decode(raw)
            return order_id, data["topic"], data["payload"]
    return None


def is_deadline_exceeded(db: redis.Redis, order_id: str) -> bool:
    data = db.hget(_saga_key(order_id), "deadline_ts")
    if data is None:
        return False
    try:
        deadline = float(data.decode())
    except Exception:
        return False
    return time.time() > deadline


def iter_saga_ids(db: redis.Redis):
    """
    Yield saga order_ids by scanning saga hashes (excludes outbox keys).
    """
    for key in db.scan_iter(match="saga:*", count=100):
        name = key.decode()
        # skip outbox/processed sets which have extra suffixes
        if name.count(":") != 1:
            continue
        yield name.split(":")[1]
