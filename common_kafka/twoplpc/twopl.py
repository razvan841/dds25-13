"""
Two-Phase Locking (2PL) + Two-Phase Commit (2PC) Database Operations
=====================================================================
Centralized database operations for 2PL/2PC coordination, analogous to
outbox.py for saga-based coordination.

This module provides helper functions for:
- Lock management (acquiring, releasing, checking locks)
- Prepared transaction state management
- Idempotency tracking for 2PC messages

Key Concepts:
- Lock: A resource-level lock (e.g., for a user's funds or stock item)
- Prepared Lock: A prepared transaction record holding lock metadata
- Transaction state follows: PREPARING -> PREPARED -> COMMITTED/ABORTED

Redis Key Layout:
-----------------
Coordinator (order service):
  2pc:{order_id}              hash   - transaction metadata (status, correlation_id, deadline_ts, etc.)
  2pc:{order_id}:processed    set    - processed message_ids for idempotency

Payment participant:
  payment:2pc:lock:{lock_id}      hash   - prepared lock record
  payment:2pc:userlock:{user_id}  string - resource-level lock

Stock participant:
  stock:2pc:lock:{lock_id}        hash   - prepared lock record
  stock:2pc:itemlock:{item_id}    string - resource-level lock

Common participant:
  {service}:{transaction_id}:processed   set    - processed message_ids
"""
from __future__ import annotations

import time
from typing import Any, Optional, List
from .lua_scripts import ACQUIRE_AND_PREPARE_PAYMENT_LUA, ACQUIRE_AND_PREPARE_STOCK_LUA

import msgspec
import redis
# Transaction status values for 2PL/2PC
STATUS_PREPARING = "PREPARING"
STATUS_PREPARED = "PREPARED"
STATUS_COMMITTING = "COMMITTING"
STATUS_COMMITTED = "COMMITTED"
STATUS_ABORTING = "ABORTING"
STATUS_ABORTED = "ABORTED"
STATUS_FAILED = "FAILED"

# Lock timeout in seconds (default 2 minutes)
DEFAULT_LOCK_TIMEOUT = 120


# ---------------------------------------------------------------------------
# Coordinator (Order Service) - Transaction Management
# ---------------------------------------------------------------------------

def _tx_key(order_id: str) -> str:
    """Redis key for the 2PC transaction hash."""
    return f"2pc:{order_id}"


def _tx_processed_key(order_id: str) -> str:
    """Redis key for the coordinator's idempotency set."""
    return f"2pc:{order_id}:processed"


def create_transaction(
    db: redis.Redis,
    order_id: str,
    correlation_id: str,
    deadline_ts: float,
) -> None:
    """Initialize a 2PC transaction for an order. Existing state is overwritten."""
    pipe = db.pipeline()
    pipe.hset(
        _tx_key(order_id),
        mapping={
            "status": STATUS_PREPARING,
            "correlation_id": correlation_id,
            "deadline_ts": deadline_ts,
            "payment_lock_id": "",
            "stock_lock_id": "",
            "funds_committed": "",
            "stock_committed": "",
            "stock_shard": "-1",
        },
    )
    pipe.delete(_tx_processed_key(order_id))
    pipe.execute()


def set_stock_shard(db: redis.Redis, order_id: str, shard: int) -> None:
    """Store which stock shard owns the items for this 2PC transaction."""
    db.hset(_tx_key(order_id), "stock_shard", str(shard))


def get_stock_shard(db: redis.Redis, order_id: str) -> int:
    """Return stock shard for this 2PC transaction (-1 if unset)."""
    data = db.hget(_tx_key(order_id), "stock_shard")
    if data is None:
        return -1
    try:
        return int(data.decode())
    except Exception:
        return -1


def get_transaction(db: redis.Redis, order_id: str) -> dict[str, Any] | None:
    """Load transaction state from Redis."""
    data = db.hgetall(_tx_key(order_id))
    if not data:
        return None
    return {k.decode(): v.decode() for k, v in data.items()}


def set_transaction_status(db: redis.Redis, order_id: str, status: str) -> None:
    """Update the transaction status."""
    db.hset(_tx_key(order_id), "status", status)


def set_lock_ids(
    db: redis.Redis,
    order_id: str,
    payment_lock_id: Optional[str] = None,
    stock_lock_id: Optional[str] = None,
) -> None:
    """Store lock identifiers received from participants."""
    mapping = {}
    if payment_lock_id is not None:
        mapping["payment_lock_id"] = payment_lock_id
    if stock_lock_id is not None:
        mapping["stock_lock_id"] = stock_lock_id
    if mapping:
        db.hset(_tx_key(order_id), mapping=mapping)


def get_lock_ids(db: redis.Redis, order_id: str) -> tuple[Optional[str], Optional[str]]:
    """Retrieve stored lock IDs for payment and stock."""
    pay, stock = db.hmget(_tx_key(order_id), ["payment_lock_id", "stock_lock_id"])
    return (pay.decode() if pay else None, stock.decode() if stock else None)


def set_committed_flags(
    db: redis.Redis,
    order_id: str,
    funds_committed: Optional[bool] = None,
    stock_committed: Optional[bool] = None,
) -> None:
    """Mark whether payment/stock have committed their prepared transactions."""
    mapping = {}
    if funds_committed is not None:
        mapping["funds_committed"] = "1" if funds_committed else ""
    if stock_committed is not None:
        mapping["stock_committed"] = "1" if stock_committed else ""
    if mapping:
        db.hset(_tx_key(order_id), mapping=mapping)


def get_committed_flags(db: redis.Redis, order_id: str) -> tuple[bool, bool]:
    """Check whether both participants have committed."""
    data = db.hmget(_tx_key(order_id), ["funds_committed", "stock_committed"])
    funds, stock = data
    return (bool(funds and funds.decode()), bool(stock and stock.decode()))


def mark_tx_processed(db: redis.Redis, order_id: str, message_id: str) -> None:
    """Mark a message as processed for idempotency (coordinator side)."""
    db.sadd(_tx_processed_key(order_id), message_id)


def is_tx_processed(db: redis.Redis, order_id: str, message_id: str) -> bool:
    """Check if a message was already processed (coordinator side)."""
    return bool(db.sismember(_tx_processed_key(order_id), message_id))


def is_tx_deadline_exceeded(db: redis.Redis, order_id: str) -> bool:
    """Check if the transaction has exceeded its deadline."""
    data = db.hget(_tx_key(order_id), "deadline_ts")
    if data is None:
        return False
    try:
        deadline = float(data.decode())
    except Exception:
        return False
    return time.time() > deadline


def iter_transaction_ids(db: redis.Redis):
    """Yield transaction order_ids by scanning 2PC hashes."""
    for key in db.scan_iter(match="2pc:*", count=100):
        name = key.decode()
        if name.count(":") != 1:
            continue
        yield name.split(":")[1]


# ---------------------------------------------------------------------------
# Participant - Idempotency Helpers
# ---------------------------------------------------------------------------

def _participant_processed_key(service: str, transaction_id: str) -> str:
    """Redis key for participant's idempotency set."""
    return f"{service}:{transaction_id}:processed"


def is_participant_processed(db: redis.Redis, service: str, transaction_id: str, message_id: str) -> bool:
    """Check if a message was already processed by a participant."""
    return bool(db.sismember(_participant_processed_key(service, transaction_id), message_id))


def mark_participant_processed(db: redis.Redis, service: str, transaction_id: str, message_id: str) -> None:
    """Mark a message as processed by a participant."""
    db.sadd(_participant_processed_key(service, transaction_id), message_id)


# ---------------------------------------------------------------------------
# Participant - Resource Lock Management (2PL)
# ---------------------------------------------------------------------------

def _resource_lock_key(service: str, resource_type: str, resource_id: str) -> str:
    """Redis key for a resource-level lock."""
    return f"{service}:2pc:{resource_type}lock:{resource_id}"


def acquire_resource_lock(
    db: redis.Redis,
    service: str,
    resource_type: str,
    resource_id: str,
    transaction_id: str,
    timeout_seconds: int = DEFAULT_LOCK_TIMEOUT,
) -> tuple[bool, Optional[str]]:
    """
    Attempt to acquire a lock on a resource.
    
    Returns:
        (success, owner_transaction_id) - If success is False, owner_transaction_id contains
        the transaction that currently holds the lock.
    """
    lock_key = _resource_lock_key(service, resource_type, resource_id)
    acquired = db.set(lock_key, transaction_id, nx=True, ex=timeout_seconds)
    if acquired:
        return True, None
    owner = db.get(lock_key)
    return False, owner.decode() if owner else "unknown"


def release_resource_lock(
    db: redis.Redis,
    service: str,
    resource_type: str,
    resource_id: str,
) -> None:
    """Release a resource-level lock."""
    lock_key = _resource_lock_key(service, resource_type, resource_id)
    db.delete(lock_key)


def acquire_and_prepare_payment(
    db: redis.Redis,
    transaction_id: str,
    lock_id: str,
    user_id: str,
    amount: int,
    timeout_seconds: int = DEFAULT_LOCK_TIMEOUT,
) -> tuple[bool, Optional[str]]:
    """
    Atomically acquire the user lock and write the prepared record.
    Returns (success, blocking_resource_id).
    """
    lock_key = _resource_lock_key("payment", "user", user_id)
    prep_key = _prepared_lock_key("payment", lock_id)

    script = db.register_script(_ACQUIRE_AND_PREPARE_PAYMENT_LUA)
    result = script(
        keys=[lock_key, prep_key],
        args=[transaction_id, str(timeout_seconds), transaction_id, user_id, str(amount)],
    )

    success = int(result[0]) == 1
    if success:
        return True, None
    blocked_key = result[1].decode() if isinstance(result[1], bytes) else result[1]
    return False, blocked_key.split(":")[-1]


def acquire_and_prepare_stock(
    db: redis.Redis,
    transaction_id: str,
    lock_id: str,
    items: list,
    timeout_seconds: int = DEFAULT_LOCK_TIMEOUT,
) -> tuple[bool, Optional[str]]:
    """
    Atomically acquire all item locks and write the prepared record.
    Returns (success, blocking_item_id).
    """
    sorted_items = sorted(items, key=lambda x: x[0])   # sort by item_id
    item_ids = [item_id for item_id, _ in sorted_items]
    lock_keys = [_resource_lock_key("stock", "item", iid) for iid in item_ids]
    prep_key = _prepared_lock_key("stock", lock_id)

    # pass items as msgpack bytes — Lua treats it as an opaque string
    items_bytes = msgspec.msgpack.encode(sorted_items)

    script = db.register_script(_ACQUIRE_AND_PREPARE_STOCK_LUA)
    result = script(
        keys=[*lock_keys, prep_key],
        args=[transaction_id, str(timeout_seconds), transaction_id, items_bytes],
    )

    success = int(result[0]) == 1
    if success:
        return True, None
    blocked_key = result[1].decode() if isinstance(result[1], bytes) else result[1]
    return False, blocked_key.split(":")[-1]


def release_multiple_resource_locks(
    db: redis.Redis,
    service: str,
    resource_type: str,
    resource_ids: List[str],
) -> None:
    """Release locks on multiple resources."""
    for resource_id in resource_ids:
        release_resource_lock(db, service, resource_type, resource_id)


# ---------------------------------------------------------------------------
# Participant - Prepared Lock Record Management
# ---------------------------------------------------------------------------

def _prepared_lock_key(service: str, lock_id: str) -> str:
    """Redis key for a prepared lock record."""
    return f"{service}:2pc:lock:{lock_id}"


def store_prepared_lock_payment(
    db: redis.Redis,
    lock_id: str,
    transaction_id: str,
    user_id: str,
    amount: int,
) -> None:
    """Store a prepared payment lock record."""
    db.hset(
        _prepared_lock_key("payment", lock_id),
        mapping={
            "transaction_id": transaction_id,
            "user_id": user_id,
            "amount": str(amount),
            "status": "prepared",
        },
    )


def get_prepared_lock_payment(db: redis.Redis, lock_id: str) -> dict | None:
    """Retrieve a prepared payment lock record."""
    data = db.hgetall(_prepared_lock_key("payment", lock_id))
    if not data:
        return None
    return {k.decode(): v.decode() for k, v in data.items()}


def delete_prepared_lock_payment(db: redis.Redis, lock_id: str) -> None:
    """Delete a prepared payment lock record."""
    db.delete(_prepared_lock_key("payment", lock_id))


def store_prepared_lock_stock(
    db: redis.Redis,
    lock_id: str,
    transaction_id: str,
    items: list,
) -> None:
    """Store a prepared stock lock record."""
    db.hset(
        _prepared_lock_key("stock", lock_id),
        mapping={
            "transaction_id": transaction_id,
            "items": msgspec.msgpack.encode(items),
            "status": "prepared",
        },
    )


def get_prepared_lock_stock(db: redis.Redis, lock_id: str) -> dict | None:
    """Retrieve a prepared stock lock record."""
    data = db.hgetall(_prepared_lock_key("stock", lock_id))
    if not data:
        return None
    raw_items = data.get(b"items")
    items = msgspec.msgpack.decode(raw_items) if raw_items else []
    return {
        "transaction_id": data.get(b"transaction_id", b"").decode(),
        "items": items,
        "status": data.get(b"status", b"").decode(),
    }


def delete_prepared_lock_stock(db: redis.Redis, lock_id: str) -> None:
    """Delete a prepared stock lock record."""
    db.delete(_prepared_lock_key("stock", lock_id))


# ---------------------------------------------------------------------------
# Helper to extract item IDs from items list
# ---------------------------------------------------------------------------

def extract_item_ids(items: list) -> List[str]:
    """Extract item IDs from a list of [item_id, qty] pairs."""
    return [item_id for item_id, _qty in items]
