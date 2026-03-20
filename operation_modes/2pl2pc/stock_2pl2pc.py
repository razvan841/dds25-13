from __future__ import annotations

import uuid
from typing import List

from msgspec import msgpack, to_builtins
import redis
from werkzeug.exceptions import HTTPException, abort
from common_kafka.models import (
    make_envelope,
    STOCK_EVENTS,
    PrepareStockCommand,
    CommitPreparedStockCommand,
    AbortPreparedStockCommand,
    StockPreparedEvent,
    StockPrepareFailedEvent,
    StockCommitted2PCEvent,
    StockAborted2PCEvent,
)
from common_kafka.producer import publish_envelope
from common_kafka.twoplpc.twopl import acquire_and_prepare_stock, delete_tx_lock_stock, get_prepared_lock_stock
from stock.app import StockValue

class TwoPL2PCOrchestrator:
    """
    Stock participant implementing strict 2PL + 2PC semantics.
    """

    SERVICE = "stock"
    RESOURCE_TYPE = "item"

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger


    def handle_command(self, envelope) -> None:
        transaction_id = envelope.transaction_id
        message_id = envelope.message_id

        if self.is_message_processed(transaction_id, message_id):
            self.logger.warning("2PC stock replay detected for tx=%s message_id=%s", transaction_id, message_id)
            return

        match envelope.type:
            case "PrepareStockCommand":
                self.logger.warning("2PC stock received PrepareStockCommand tx=%s", transaction_id)
                self.handle_prepare(envelope)
            case "CommitPreparedStockCommand":
                self.logger.warning("2PC stock received CommitPreparedStockCommand tx=%s", transaction_id)
                self.handle_commit_prepared(envelope)
            case "AbortPreparedStockCommand":
                self.logger.warning("2PC stock received AbortPreparedStockCommand tx=%s", transaction_id)
                self.handle_abort_prepared(envelope)
            case _:
                self.logger.debug("Unhandled 2PC stock command %s", envelope.type)

        self.mark_message_processed(transaction_id, envelope.message_id)

    def handle_prepare(self, envelope) -> None:
        payload = PrepareStockCommand(**envelope.payload)
        reply_topic = envelope.reply_topic or STOCK_EVENTS
        items: list = payload.items
        item_ids = self.extract_item_ids(items)

        # Atomically acquire locks on all items and write the prepared record.
        # The Lua script is idempotent: if this is a replay after crash,
        # it returns the existing lock_id instead of creating a new one.
        generated_lock_id = str(uuid.uuid4())
        success, actual_lock_id = acquire_and_prepare_stock(
            self.db, envelope.transaction_id, generated_lock_id, items
        )
        if not success:
            # actual_lock_id contains the blocking item_id on failure
            self.logger.warning("2PC stock lock acquisition failed for tx=%s item=%s", envelope.transaction_id, actual_lock_id)
            self._publish_prepare_failed(envelope, f"Item {actual_lock_id} is locked by another transaction")
            return

        # Validate stock availability (reads are safe — items are now locked).
        reason: str | None = None
        try:
            for item_id, qty in items:
                item = self.fetch_item(item_id)
                if item.stock < qty:
                    raise ValueError(
                        f"Item {item_id} has insufficient stock (requested {qty}, available {item.stock})"
                    )
        except HTTPException as exc:
            reason = getattr(exc, "description", "Item lookup failed")
        except ValueError as exc:
            reason = str(exc)
        if reason is not None:
            # Release all state created by acquire_and_prepare_* so replays can start cleanly.
            pipe = self.db.pipeline()
            for item_id in item_ids:
                pipe.delete(f"{self.SERVICE}:2pc:{self.RESOURCE_TYPE}lock:{item_id}")
            pipe.delete(f"{self.SERVICE}:2pc:lock:{actual_lock_id}")
            pipe.delete(f"{self.SERVICE}:2pc:tx:{envelope.transaction_id}")
            pipe.execute()
            self._publish_prepare_failed(envelope, reason)
            return
        self.logger.warning("2PC stock prepared tx=%s lock=%s items=%s", envelope.transaction_id, actual_lock_id, len(items))

        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "StockPreparedEvent",
                transaction_id=envelope.transaction_id,
                order_id=envelope.order_id,
                payload=to_builtins(StockPreparedEvent(lock_id=actual_lock_id))
            ),
        )

    def handle_commit_prepared(self, envelope) -> None:
        payload = CommitPreparedStockCommand(**envelope.payload)
        reply_topic = envelope.reply_topic or STOCK_EVENTS
        prepared = get_prepared_lock_stock(self.db, payload.lock_id)
        if prepared:
            items: list = prepared["items"]
            updated_items = []
            item_ids = self.extract_item_ids(items)
            for item_id, qty in items:
                try:
                    item = self.fetch_item(item_id)
                except HTTPException:
                    self.logger.warning("2PC commit: item %s lookup failed for lock %s", item_id, payload.lock_id)
                    return
                item.stock -= int(qty)
                updated_items.append((item_id, msgpack.encode(item)))

            pipe = self.db.pipeline()
            for item_id, encoded_item in updated_items:
                pipe.set(item_id, encoded_item)
            for item_id in item_ids:
                pipe.delete(f"{self.SERVICE}:2pc:{self.RESOURCE_TYPE}lock:{item_id}")
            pipe.delete(f"{self.SERVICE}:2pc:lock:{payload.lock_id}")
            pipe.delete(f"{self.SERVICE}:2pc:tx:{envelope.transaction_id}")
            pipe.execute()
        else:
            # A replay after a successful commit may legitimately arrive after cleanup.
            delete_tx_lock_stock(self.db, envelope.transaction_id)
        self.logger.warning("2PC stock committed tx=%s lock=%s", envelope.transaction_id, payload.lock_id)

        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "StockCommitted2PCEvent",
                transaction_id=envelope.transaction_id,
                order_id=envelope.order_id,
                payload=to_builtins(StockCommitted2PCEvent(lock_id=payload.lock_id))
            ),
        )

    def handle_abort_prepared(self, envelope) -> None:
        payload = AbortPreparedStockCommand(**envelope.payload)
        reply_topic = envelope.reply_topic or STOCK_EVENTS
        prepared = get_prepared_lock_stock(self.db, payload.lock_id)
        if prepared:
            items: list = prepared["items"]
            item_ids = self.extract_item_ids(items)
            pipe = self.db.pipeline()
            for item_id in item_ids:
                pipe.delete(f"{self.SERVICE}:2pc:{self.RESOURCE_TYPE}lock:{item_id}")
            pipe.delete(f"{self.SERVICE}:2pc:lock:{payload.lock_id}")
            pipe.delete(f"{self.SERVICE}:2pc:tx:{envelope.transaction_id}")
            pipe.execute()
        else:
            delete_tx_lock_stock(self.db, envelope.transaction_id)
        self.logger.warning("2PC stock aborted tx=%s lock=%s", envelope.transaction_id, payload.lock_id)

        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "StockAborted2PCEvent",
                transaction_id=envelope.transaction_id,
                order_id=envelope.order_id,
                payload=to_builtins(StockAborted2PCEvent(lock_id=payload.lock_id))
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
                order_id=envelope.order_id,
                payload=to_builtins(StockPrepareFailedEvent(reason=reason))
            ),
        )

# -----------------Redis helper functions for 2PC state management-----------------
    def is_message_processed(self, transaction_id: str, message_id: str) -> bool:
        if not transaction_id or not message_id:
            return False
        key = f"{self.SERVICE}:{transaction_id}:processed"
        return bool(self.db.sismember(key, message_id))

    def mark_message_processed(self, transaction_id: str, message_id: str) -> None:
        if not transaction_id or not message_id:
            return
        key = f"{self.SERVICE}:{transaction_id}:processed"
        self.db.sadd(key, message_id)

    def extract_item_ids(self, items: list) -> List[str]:
        """Extract item IDs from a list of [item_id, qty] pairs."""
        return [item_id for item_id, _qty in items]
    
    def fetch_item(self, item_id: str) -> StockValue:
        """Retrieve and deserialise a StockValue from Redis, aborting on error."""
        try:
            entry: bytes = self.db.get(item_id)
        except redis.exceptions.RedisError:
            return abort(400, "Database error during item lookup")
        entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
        if entry is None:
            abort(400, f"Item: {item_id} not found!")
        return entry
    
