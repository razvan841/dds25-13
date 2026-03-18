from __future__ import annotations

import time
from typing import Any, Optional

import msgspec
import redis

from ..models import Envelope
from ..codec import encode_envelope

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
    """Initialize saga state for an order. Existing state is overwritten."""
    pipe = db.pipeline()
    # Delete the whole hash first so stale per-attempt fields (stock_res_*,
    # stock_resolved_*, payment_resolved, etc.) from a previous attempt
    # don't bleed into this one.
    pipe.delete(_saga_key(order_id))
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
            "stock_shard": "-1",
            "stock_shards": "",
            "is_failing": "",
        },
    )
    pipe.delete(_processed_key(order_id))
    pipe.delete(_outbox_key(order_id))
    pipe.execute()


def set_stock_shard(db: redis.Redis, order_id: str, shard: int) -> None:
    """Store which stock shard owns the items for this order's checkout."""
    db.hset(_saga_key(order_id), "stock_shard", str(shard))


def get_stock_shard(db: redis.Redis, order_id: str) -> int:
    """Return the stock shard stored for this order (-1 if not set)."""
    data = db.hget(_saga_key(order_id), "stock_shard")
    if data is None:
        return -1
    try:
        return int(data.decode())
    except Exception:
        return -1


def set_failing_flag(db: redis.Redis, order_id: str) -> None:
    """Mark that a failure has been detected; compensation may still be in progress."""
    db.hset(_saga_key(order_id), "is_failing", "1")


def get_failing_flag(db: redis.Redis, order_id: str) -> bool:
    """Return True if a failure has been detected for this saga."""
    data = db.hget(_saga_key(order_id), "is_failing")
    return bool(data and data.decode() == "1")


def mark_payment_resolved(db: redis.Redis, order_id: str) -> None:
    """Mark payment as resolved (funds either failed to reserve OR were cancelled)."""
    db.hset(_saga_key(order_id), "payment_resolved", "1")


def is_payment_resolved(db: redis.Redis, order_id: str) -> bool:
    """Return True when the payment side requires no further compensation."""
    data = db.hget(_saga_key(order_id), "payment_resolved")
    return bool(data and data.decode() == "1")


def add_resolved_stock_shard(db: redis.Redis, order_id: str, shard: int) -> None:
    """Mark stock shard `shard` as fully resolved (reserve failed or cancel confirmed)."""
    db.hset(_saga_key(order_id), f"stock_resolved_{shard}", "1")


def all_stock_shards_resolved(db: redis.Redis, order_id: str) -> bool:
    """Return True when every stock shard that was commanded is fully resolved."""
    shards = get_stock_shards(db, order_id)
    if not shards:
        return True  # no stock shards involved
    data = db.hgetall(_saga_key(order_id))
    decoded = {k.decode(): v.decode() for k, v in data.items()}
    return all(decoded.get(f"stock_resolved_{s}", "") == "1" for s in shards)


def set_stock_shards(db: redis.Redis, order_id: str, shards: list) -> None:
    """Store the list of stock shard indices that were sent ReserveStockCommand."""
    db.hset(_saga_key(order_id), "stock_shards", ",".join(str(s) for s in shards))


def get_stock_shards(db: redis.Redis, order_id: str) -> list:
    """Return list of stock shard indices that were sent ReserveStockCommand."""
    data = db.hget(_saga_key(order_id), "stock_shards")
    if not data:
        return []
    val = data.decode()
    if not val:
        return []
    return [int(s) for s in val.split(",")]


def add_stock_reservation(db: redis.Redis, order_id: str, shard: int, res_id: str) -> None:
    """Record that stock shard `shard` returned reservation `res_id`."""
    db.hset(_saga_key(order_id), f"stock_res_{shard}", res_id)


def get_stock_reservations(db: redis.Redis, order_id: str) -> dict:
    """Return {shard_index: reservation_id} for all shards that have responded."""
    data = db.hgetall(_saga_key(order_id))
    result = {}
    for k, v in data.items():
        key = k.decode()
        if key.startswith("stock_res_"):
            shard = int(key[len("stock_res_"):])
            result[shard] = v.decode()
    return result


def all_stock_reserved(db: redis.Redis, order_id: str) -> bool:
    """Return True when every stock shard that was commanded has replied."""
    shards = get_stock_shards(db, order_id)
    if not shards:
        return False
    reservations = get_stock_reservations(db, order_id)
    return all(s in reservations and reservations[s] for s in shards)


def mark_stock_shard_committed(db: redis.Redis, order_id: str, shard: int) -> None:
    """Record that stock shard `shard` has committed."""
    db.hset(_saga_key(order_id), f"stock_com_{shard}", "1")


def all_stock_shards_committed(db: redis.Redis, order_id: str) -> bool:
    """Return True when every stock shard has sent StockCommittedEvent."""
    shards = get_stock_shards(db, order_id)
    if not shards:
        return False
    data = db.hgetall(_saga_key(order_id))
    decoded = {k.decode(): v.decode() for k, v in data.items()}
    return all(decoded.get(f"stock_com_{s}", "") == "1" for s in shards)


def get_saga(db: redis.Redis, order_id: str) -> dict[str, Any] | None:
    data = db.hgetall(_saga_key(order_id))
    if not data:
        return None
    return {k.decode(): v.decode() for k, v in data.items()}


def iter_saga_ids(db: redis.Redis):
    """Yield saga order_ids by scanning saga hashes (excludes outbox/processed keys)."""
    for key in db.scan_iter(match="saga:*", count=100):
        name = key.decode()
        if name.count(":") != 1:
            continue
        yield name.split(":")[1]


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
    """Store an outbound envelope + explicit topic in the saga outbox (left-push)."""
    entry = msgspec.msgpack.encode(
        {
            "topic": topic,
            "payload": encode_envelope(envelope),
        }
    )
    db.lpush(_outbox_key(order_id), entry)


def pop_outbox(db: redis.Redis, order_id: str) -> Optional[tuple[str, bytes]]:
    """Pop the latest outbound entry for a saga. Returns (topic, payload) or None."""
    raw = db.rpop(_outbox_key(order_id))
    if not raw:
        return None
    data = msgspec.msgpack.decode(raw)
    return data["topic"], data["payload"]


def pop_any_outbox(db: redis.Redis) -> Optional[tuple[str, str, bytes]]:
    """Pop an entry from any saga outbox. Returns (order_id, topic, payload) or None."""
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
