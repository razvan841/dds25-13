"""
Stock-service saga orchestrator
================================
Handles the participant side of the checkout saga for the stock service.
The order orchestrator sends three command types on the ``stock.commands``
topic; this module processes them and publishes corresponding events on
``stock.events``.

Command/event flow (per checkout attempt):
  order → ReserveStockCommand → stock
  stock → StockReservedEvent  → order    (success)
  stock → StockReserveFailedEvent → order (failure)
  order → CommitStockCommand  → stock
  stock → StockCommittedEvent → order
  order → CancelStockCommand  → stock    (compensation)
  stock → StockCancelledEvent → order

All handlers are idempotent: duplicate messages (Kafka redeliveries) are
detected via a per-saga processed-message set stored in Redis and silently
skipped.

Redis key layout (stock service only):
  stock:<transaction_id>:processed       set   – message_ids already handled
  stock:reservation:<res_id>      hash  – {items: <msgpack bytes>, status: str}
"""
from __future__ import annotations

import uuid
from typing import Callable

from msgspec import msgpack, to_builtins
from werkzeug.exceptions import HTTPException

from common_kafka.models import (
    make_envelope,
    STOCK_EVENTS,
    ReserveStockCommand,
    CommitStockCommand,
    CancelStockCommand,
    StockReservedEvent,
    StockReserveFailedEvent,
    StockCommittedEvent,
    StockCancelledEvent,
    PrepareStockCommand,
    CommitPreparedStockCommand,
    AbortPreparedStockCommand,
    StockPreparedEvent,
    StockPrepareFailedEvent,
    StockCommitted2PCEvent,
    StockAborted2PCEvent,
)
from common_kafka.producer import publish_envelope
from common_kafka.twopl import (
    is_participant_processed,
    mark_participant_processed,
    acquire_multiple_resource_locks,
    release_multiple_resource_locks,
    store_prepared_lock_stock,
    get_prepared_lock_stock,
    delete_prepared_lock_stock,
    extract_item_ids,
)


# ---------------------------------------------------------------------------
# Redis key helpers
# ---------------------------------------------------------------------------

def _processed_key(transaction_id: str) -> str:
    """Redis key for the idempotency set of a transaction."""
    return f"stock:{transaction_id}:processed"


def _reservation_key(res_id: str) -> str:
    """Redis key for a stock reservation hash."""
    return f"stock:reservation:{res_id}"


# ---------------------------------------------------------------------------
# Saga orchestrator (participant side)
# ---------------------------------------------------------------------------

class SagaOrchestrator:
    """
    Participant-side saga handler for the stock service.

    Receives commands from the order orchestrator and performs local stock
    operations, publishing events back to ``stock.events``.

    Parameters
    ----------
    db:
        Redis connection (shared with the Flask app process).
    logger:
        Standard Python logger (typically ``app.logger``).
    fetch_item_fn:
        Callable that takes an ``item_id: str`` and returns a ``StockValue``
        object (same as the one defined in ``stock.app``).  It may call
        Flask's ``abort()`` for missing items; this module catches the
        resulting ``HTTPException``.
    """

    def __init__(self, db, logger, fetch_item_fn: Callable[[str], object]):
        self.db = db
        self.logger = logger
        self.fetch_item = fetch_item_fn

    # ------------------------------------------------------------------
    # Idempotency helpers
    # ------------------------------------------------------------------

    def _is_processed(self, transaction_id: str, message_id: str) -> bool:
        """Return True if this message has already been processed."""
        return bool(self.db.sismember(_processed_key(transaction_id), message_id))

    def _mark_processed(self, transaction_id: str, message_id: str) -> None:
        """Record that this message has been processed (idempotency guard)."""
        self.db.sadd(_processed_key(transaction_id), message_id)

    # ------------------------------------------------------------------
    # Reservation helpers
    # ------------------------------------------------------------------

    def _store_reservation(
        self, res_id: str, items: list[list], status: str
    ) -> None:
        """
        Persist a reservation record in Redis.

        ``items`` is a list of [item_id, quantity] pairs.  It is
        msgpack-encoded so that Redis stores it as an opaque byte string.
        """
        self.db.hset(
            _reservation_key(res_id),
            mapping={
                "items": msgpack.encode(items),
                "status": status,
            },
        )

    def _get_reservation(self, res_id: str) -> dict | None:
        """
        Load a reservation from Redis.

        Returns a dict ``{items: [[item_id, qty], ...], status: str}``
        or ``None`` if the reservation no longer exists.
        """
        data = self.db.hgetall(_reservation_key(res_id))
        if not data:
            return None
        raw_items = data.get(b"items")
        status = data.get(b"status", b"").decode()
        items = msgpack.decode(raw_items) if raw_items else []
        return {"items": items, "status": status}

    def _delete_reservation(self, res_id: str) -> None:
        """Remove a reservation from Redis (called on commit and cancel)."""
        self.db.delete(_reservation_key(res_id))

    # ------------------------------------------------------------------
    # Top-level command dispatcher
    # ------------------------------------------------------------------

    def handle_command(self, envelope) -> None:
        """
        Dispatch a Kafka command envelope to the appropriate handler.

        The method is the single entry point called by the background worker.
        Unknown message types are logged at DEBUG level and ignored so that
        future message types do not crash the consumer.

        Idempotency is enforced at the top level: if ``envelope.message_id``
        already appears in the processed set for the transaction, the entire message
        is skipped before any business logic runs.
        """
        transaction_id = envelope.transaction_id

        if self._is_processed(transaction_id, envelope.message_id):
            self.logger.debug(
                "Skipping already-processed message %s for transaction %s",
                envelope.message_id,
                transaction_id,
            )
            return

        match envelope.type:
            case "StockServicePing":
                self.logger.info(
                    "[stock] Received Kafka ping %s", envelope.message_id
                )
            case "ReserveStockCommand":
                self.logger.warning("Received ReserneStockCommand")
                self._handle_reserve(envelope)
            case "CommitStockCommand":
                self.logger.warning("Received CommitStockCommand")
                self._handle_commit(envelope)
            case "CancelStockCommand":
                self._handle_cancel(envelope)
            case _:
                self.logger.debug(
                    "Unhandled stock command type '%s' for transaction %s",
                    envelope.type,
                    transaction_id,
                )

        self._mark_processed(transaction_id, envelope.message_id)

    # ------------------------------------------------------------------
    # Command handlers
    # ------------------------------------------------------------------

    def _handle_reserve(self, envelope) -> None:
        """
        Attempt to reserve stock for every item in the command.

        Two-phase approach to keep the operation all-or-nothing:

        Phase 1 – validation
            Fetch every item from Redis and verify that it exists and has
            sufficient stock.  If any check fails, publish
            ``StockReserveFailedEvent`` immediately and return without
            mutating anything.

        Phase 2 – deduction
            Deduct the requested quantity from each item's stock in a single
            Redis pipeline (minimises round-trips), store the reservation,
            and publish ``StockReservedEvent``.

        The reservation record keeps a copy of the (item_id, quantity) pairs
        so that the cancel handler can restore stock without needing to know
        the original quantities.
        """
        payload = ReserveStockCommand(**envelope.payload)
        # items is a list of [item_id, qty] pairs (tuples become lists via msgpack/JSON)
        items: list = payload.items

        # Phase 1 – validate all items before touching state
        entries: dict[str, tuple] = {}
        for item_id, qty in items:
            try:
                item = self.fetch_item(item_id)
            except HTTPException as exc:
                reason = getattr(exc, "description", f"Item {item_id} not found")
                self._publish_reserve_failed(envelope, reason)
                return

            if item.stock < qty:
                self._publish_reserve_failed(
                    envelope,
                    f"Item {item_id} has insufficient stock "
                    f"(requested {qty}, available {item.stock})",
                )
                return

            entries[item_id] = (item, int(qty))

        # Phase 2 – deduct all items atomically via pipeline
        pipe = self.db.pipeline()
        for item_id, (item, qty) in entries.items():
            item.stock -= qty
            pipe.set(item_id, msgpack.encode(item))
        pipe.execute()

        # Persist reservation and notify the orchestrator
        res_id = str(uuid.uuid4())
        self._store_reservation(res_id, items, status="reserved")

        reply_topic = envelope.reply_topic or STOCK_EVENTS
        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "StockReservedEvent",
                transaction_id=envelope.transaction_id,
                payload=to_builtins(StockReservedEvent(reservation_id=res_id)),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )
        self.logger.info(
            "[stock] Reserved stock for transaction %s: reservation=%s items=%d",
            envelope.transaction_id,
            res_id,
            len(items),
        )

    def _handle_commit(self, envelope) -> None:
        """
        Commit a previously created stock reservation.

        Stock was already deducted during :meth:`_handle_reserve`, so the
        only work here is deleting the reservation record (which is no longer
        needed) and publishing ``StockCommittedEvent`` to unblock the order
        orchestrator.

        Idempotent: if the reservation was already deleted (e.g. duplicate
        commit delivery) the delete is a no-op and the event is still emitted.
        """
        payload = CommitStockCommand(**envelope.payload)
        if self._get_reservation(payload.reservation_id):
            self._delete_reservation(payload.reservation_id)

        reply_topic = envelope.reply_topic or STOCK_EVENTS
        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "StockCommittedEvent",
                transaction_id=envelope.transaction_id,
                payload=to_builtins(
                    StockCommittedEvent(reservation_id=payload.reservation_id)
                ),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )
        self.logger.info(
            "[stock] Committed reservation %s for transaction %s",
            payload.reservation_id,
            envelope.transaction_id,
        )

    def _handle_cancel(self, envelope) -> None:
        """
        Cancel a stock reservation and restore item quantities.

        Iterates over the stored (item_id, quantity) pairs and adds the
        reserved quantity back to each item's stock using a Redis pipeline.
        Items that no longer exist in the database are logged and skipped so
        that one missing item does not block the restoration of others.

        After restoring stock the reservation record is deleted and
        ``StockCancelledEvent`` is published to unblock the order
        orchestrator.

        Idempotent: if the reservation does not exist (already cancelled or
        never committed) the stock restore is skipped and the event is still
        emitted.
        """
        payload = CancelStockCommand(**envelope.payload)
        reservation = self._get_reservation(payload.reservation_id)

        if reservation:
            items: list = reservation["items"]
            pipe = self.db.pipeline()
            for item_id, qty in items:
                try:
                    item = self.fetch_item(item_id)
                    item.stock += int(qty)
                    pipe.set(item_id, msgpack.encode(item))
                except HTTPException:
                    self.logger.warning(
                        "[stock] Item %s not found while restoring for "
                        "reservation %s (transaction %s); skipping",
                        item_id,
                        payload.reservation_id,
                        envelope.transaction_id,
                    )
            pipe.execute()
            self._delete_reservation(payload.reservation_id)

        reply_topic = envelope.reply_topic or STOCK_EVENTS
        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "StockCancelledEvent",
                transaction_id=envelope.transaction_id,
                payload=to_builtins(
                    StockCancelledEvent(reservation_id=payload.reservation_id)
                ),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )
        self.logger.info(
            "[stock] Cancelled reservation %s for transaction %s",
            payload.reservation_id,
            envelope.transaction_id,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _publish_reserve_failed(self, envelope, reason: str) -> None:
        """Emit a StockReserveFailedEvent and log the reason."""
        self.logger.warning(
            "[stock] Stock reservation failed for transaction %s: %s",
            envelope.transaction_id,
            reason,
        )
        reply_topic = envelope.reply_topic or STOCK_EVENTS
        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "StockReserveFailedEvent",
                transaction_id=envelope.transaction_id,
                payload=to_builtins(StockReserveFailedEvent(reason=reason)),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )


# ---------------------------------------------------------------------------
# 2PL/2PC Implementation using common_kafka.twopl
# ---------------------------------------------------------------------------

class TwoPL2PCOrchestrator:
    """
    Stock participant implementing strict 2PL + 2PC semantics.
    Uses centralized twopl.py for database operations.
    """

    SERVICE = "stock"
    RESOURCE_TYPE = "item"

    def __init__(self, db, logger, fetch_item_fn: Callable[[str], object]):
        self.db = db
        self.logger = logger
        self.fetch_item = fetch_item_fn

    def _is_processed(self, transaction_id: str, message_id: str) -> bool:
        return is_participant_processed(self.db, self.SERVICE, transaction_id, message_id)

    def _mark_processed(self, transaction_id: str, message_id: str) -> None:
        mark_participant_processed(self.db, self.SERVICE, transaction_id, message_id)

    def handle_command(self, envelope) -> None:
        transaction_id = envelope.transaction_id
        if self._is_processed(transaction_id, envelope.message_id):
            return

        match envelope.type:
            case "PrepareStockCommand":
                self.logger.warning("2PC stock received PrepareStockCommand tx=%s", transaction_id)
                self._handle_prepare(envelope)
            case "CommitPreparedStockCommand":
                self.logger.warning("2PC stock received CommitPreparedStockCommand tx=%s", transaction_id)
                self._handle_commit_prepared(envelope)
            case "AbortPreparedStockCommand":
                self.logger.warning("2PC stock received AbortPreparedStockCommand tx=%s", transaction_id)
                self._handle_abort_prepared(envelope)
            case _:
                self.logger.debug("Unhandled 2PC stock command %s", envelope.type)

        self._mark_processed(transaction_id, envelope.message_id)

    def _handle_prepare(self, envelope) -> None:
        payload = PrepareStockCommand(**envelope.payload)
        reply_topic = envelope.reply_topic or STOCK_EVENTS
        items: list = payload.items
        item_ids = extract_item_ids(items)

        # Acquire locks on all items using twopl module (deadlock prevention via sorted order)
        success, failed_item_id, acquired_ids = acquire_multiple_resource_locks(
            self.db, self.SERVICE, self.RESOURCE_TYPE, item_ids, envelope.transaction_id
        )
        if not success:
            self.logger.warning("2PC stock lock acquisition failed for tx=%s item=%s", envelope.transaction_id, failed_item_id)
            self._publish_prepare_failed(envelope, f"Item {failed_item_id} is locked by another transaction")
            return

        # Validate stock availability
        entries: dict[str, tuple] = {}
        try:
            for item_id, qty in items:
                item = self.fetch_item(item_id)
                if item.stock < qty:
                    raise ValueError(
                        f"Item {item_id} has insufficient stock (requested {qty}, available {item.stock})"
                    )
                entries[item_id] = (item, int(qty))
        except HTTPException as exc:
            release_multiple_resource_locks(self.db, self.SERVICE, self.RESOURCE_TYPE, acquired_ids)
            reason = getattr(exc, "description", "Item lookup failed")
            self._publish_prepare_failed(envelope, reason)
            return
        except ValueError as exc:
            release_multiple_resource_locks(self.db, self.SERVICE, self.RESOURCE_TYPE, acquired_ids)
            self._publish_prepare_failed(envelope, str(exc))
            return

        # Store prepared lock record
        lock_id = str(uuid.uuid4())
        store_prepared_lock_stock(self.db, lock_id, envelope.transaction_id, items)
        self.logger.warning("2PC stock prepared tx=%s lock=%s items=%s", envelope.transaction_id, lock_id, len(items))

        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "StockPreparedEvent",
                transaction_id=envelope.transaction_id,
                payload=to_builtins(StockPreparedEvent(lock_id=lock_id)),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )

    def _handle_commit_prepared(self, envelope) -> None:
        payload = CommitPreparedStockCommand(**envelope.payload)
        reply_topic = envelope.reply_topic or STOCK_EVENTS
        prepared = get_prepared_lock_stock(self.db, payload.lock_id)
        if prepared:
            items: list = prepared["items"]
            pipe = self.db.pipeline()
            for item_id, qty in items:
                try:
                    item = self.fetch_item(item_id)
                    item.stock -= int(qty)
                    pipe.set(item_id, msgpack.encode(item))
                except HTTPException:
                    self.logger.warning("2PC commit: item %s lookup failed for lock %s", item_id, payload.lock_id)
            pipe.execute()
            # Release all item locks
            item_ids = extract_item_ids(items)
            release_multiple_resource_locks(self.db, self.SERVICE, self.RESOURCE_TYPE, item_ids)
            delete_prepared_lock_stock(self.db, payload.lock_id)
        self.logger.warning("2PC stock committed tx=%s lock=%s", envelope.transaction_id, payload.lock_id)

        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "StockCommitted2PCEvent",
                transaction_id=envelope.transaction_id,
                payload=to_builtins(StockCommitted2PCEvent(lock_id=payload.lock_id)),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )

    def _handle_abort_prepared(self, envelope) -> None:
        payload = AbortPreparedStockCommand(**envelope.payload)
        reply_topic = envelope.reply_topic or STOCK_EVENTS
        prepared = get_prepared_lock_stock(self.db, payload.lock_id)
        if prepared:
            items: list = prepared["items"]
            item_ids = extract_item_ids(items)
            release_multiple_resource_locks(self.db, self.SERVICE, self.RESOURCE_TYPE, item_ids)
            delete_prepared_lock_stock(self.db, payload.lock_id)
        self.logger.warning("2PC stock aborted tx=%s lock=%s", envelope.transaction_id, payload.lock_id)

        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "StockAborted2PCEvent",
                transaction_id=envelope.transaction_id,
                payload=to_builtins(StockAborted2PCEvent(lock_id=payload.lock_id)),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )

    def _publish_prepare_failed(self, envelope, reason: str) -> None:
        reply_topic = envelope.reply_topic or STOCK_EVENTS
        self.logger.warning("2PC stock prepare failed for tx=%s: %s", envelope.transaction_id, reason)
        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "StockPrepareFailedEvent",
                transaction_id=envelope.transaction_id,
                payload=to_builtins(StockPrepareFailedEvent(reason=reason)),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def select_orchestrator(mode: str, **kwargs):
    """
    Return the appropriate orchestrator for the given coordination mode.

    Parameters
    ----------
    mode:
        ``"saga"`` for the saga-based implementation (default);
        any other value falls back to the 2PL/2PC stub.
    **kwargs:
        Forwarded to the chosen orchestrator's ``__init__``.
    """
    if mode == "saga":
        return SagaOrchestrator(**kwargs)
    return TwoPL2PCOrchestrator(
        db=kwargs["db"],
        logger=kwargs["logger"],
        fetch_item_fn=kwargs["fetch_item_fn"],
    )
