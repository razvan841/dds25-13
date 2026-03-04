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
        },
    )
    pipe.delete(_tx_processed_key(order_id))
    pipe.execute()


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


def acquire_multiple_resource_locks(
    db: redis.Redis,
    service: str,
    resource_type: str,
    resource_ids: List[str],
    transaction_id: str,
    timeout_seconds: int = DEFAULT_LOCK_TIMEOUT,
) -> tuple[bool, Optional[str], List[str]]:
    """
    Acquire locks on multiple resources in sorted order (deadlock prevention).
    
    Returns:
        (success, failed_resource_id, acquired_resource_ids)
        - If success is False, releases all acquired locks and returns the
          resource_id that couldn't be locked.
    """
    sorted_ids = sorted(resource_ids)
    acquired_ids: List[str] = []
    
    for resource_id in sorted_ids:
        success, _ = acquire_resource_lock(
            db, service, resource_type, resource_id, transaction_id, timeout_seconds
        )
        if success:
            acquired_ids.append(resource_id)
        else:
            # Rollback acquired locks
            for acquired_id in acquired_ids:
                release_resource_lock(db, service, resource_type, acquired_id)
            return False, resource_id, []
    
    return True, None, acquired_ids


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
