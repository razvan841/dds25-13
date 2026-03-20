from flask import abort, jsonify
import uuid
import time
from datetime import datetime, timedelta, timezone

from msgspec import to_builtins
from pyparsing import Optional
import redis
from common_kafka.models import (
    make_envelope,
    PAYMENT_COMMANDS,
    STOCK_EVENTS,
    PrepareFundsCommand,
    PrepareStockCommand,
    CommitPreparedFundsCommand,
    CommitPreparedStockCommand,
    AbortPreparedFundsCommand,
    AbortPreparedStockCommand,
    FundsPreparedEvent,
    StockPreparedEvent,
    FundsPrepareFailedEvent,
    StockPrepareFailedEvent,
    FundsCommitted2PCEvent,
    StockCommitted2PCEvent,
    FundsAborted2PCEvent,
    StockAborted2PCEvent,
)

from common_kafka.twoplpc.twopl import is_tx_processed

class TwoPLTwoPCOrchestrator:
    """
    2pl2pc-based orchestration extracted so the app can swap strategies.
    """

    def __init__(
        self,
        db,
        logger,
        checkout_deadline_seconds: int,
    ):
        self.db = db
        self.logger = logger
        self.checkout_deadline_seconds = checkout_deadline_seconds

    # ---------- HTTP-layer operations ----------
    def checkout(self, order_id: str, order_entry, items_quantities: dict[str, int]):

        if self.order_processed(order_id):
            self.logger.info("Checkout request for already processed order %s", order_id)
            return jsonify({"message": "Order already processed"}), 200
        
        transaction_id = str(uuid.uuid4())
        deadline_ts = (
            datetime.now(timezone.utc) + timedelta(seconds=self.checkout_deadline_seconds)
        ).timestamp()

        self.create_transaction(self.db, order_id, transaction_id=transaction_id, deadline_ts=deadline_ts)

        prepare_funds = PrepareFundsCommand(user_id=order_entry.user_id, amount=order_entry.total_cost)
        prepare_stock = PrepareStockCommand(items=list(items_quantities.items()))

        publish_envelope(
            PAYMENT_COMMANDS,
            key=order_id,
            envelope=make_envelope(
                "PrepareFundsCommand",
                transaction_id=transaction_id,
                order_id=order_id,
                payload=to_builtins(prepare_funds),
            ),
        )
        publish_envelope(
            stock_commands_topic,
            key=order_id,
            envelope=make_envelope(
                "PrepareStockCommand",
                transaction_id=transaction_id,
                order_id=order_id,
                payload={"items": prepare_stock.items}
            ),
        )

        deadline = time.time() + self.checkout_deadline_seconds
        while time.time() < deadline:
            tx = self.get_transaction(self.db, order_id) or {}
            status = tx.get("status", STATUS_PREPARING)
            if status == STATUS_2PC_COMMITTED:
                return jsonify({"order_id": order_id, "status": status})
            if status in (STATUS_2PC_FAILED, STATUS_ABORTED):
                return jsonify({"order_id": order_id, "status": status}), 400
            time.sleep(0.01)

        return jsonify({"order_id": order_id, "status": STATUS_PREPARING}), 408

        #Steps:
        #1. Verify if order is new (idempotency)
            # 1.1 log the order received

            # 1.2 Generate idempotency id for stock and payment to distinguish between checkouts
            # 1.2 log idempotency id 

            # 1.3 Create a 2pl2pc and log it in db.

            # 1.4 Create prepare message for stock
            # 1.4 Log message for stock 
            # 1.4 Send message to stock

            # 1.5 Create prepare message for payment
            # 1.5 Log message for payment 
            # 1.5 Send message to payment            

        #2 else if order is already processed 
            # return
        
        #3 Maybe, maybe -> loop to check 2pl2pc status until 2pl2pc is completed/failed

    def checkout_status(self, order_id: str):
        tx = self.get_transaction(self.db, order_id)
        if tx is None:
            abort(404, f"2PC transaction for order {order_id} not found")
        return jsonify(
            {
                "order_id": order_id,
                "status": tx.get("status", STATUS_2PC_FAILED),
                "failure_reason": tx.get("failure_reason", ""),
            }
        )
    
    def handle_event(self, envelope):
        order_id = envelope.order_id
        msg_type = envelope.type

        if is_tx_processed(self.db, order_id, envelope.message_id):
            return

        # Reject stale events from previous checkout attempts on the same order
        tx_meta = self.get_transaction(self.db, order_id) or {}
        transaction_id = tx_meta.get("transaction_id", "")
        if transaction_id and envelope.transaction_id and envelope.transaction_id != transaction_id:
            self.logger.debug(
                "Ignoring stale event %s for order %s (transaction %s != current %s)",
                msg_type, order_id, envelope.transaction_id, transaction_id,
            )
            return

        match msg_type:
            case "FundsPreparedEvent":
                self.handle_funds_prepared_event(envelope, order_id, transaction_id)
            case "StockPreparedEvent":
                payload = StockPreparedEvent(**envelope.payload)
                shard_idx = payload.shard_index
                self.logger.warning("2PC stock prepared for %s (lock=%s, shard=%s)", order_id, payload.lock_id, shard_idx)
                add_2pc_stock_lock(self.db, order_id, shard_idx, payload.lock_id)
                tx = get_transaction(self.db, order_id) or {}
                if tx.get("status") in (STATUS_2PC_FAILED, STATUS_ABORTING, STATUS_ABORTED):
                    publish_envelope(
                        f"stock.commands.{shard_idx}",
                        key=order_id,
                        envelope=make_envelope(
                            "AbortPreparedStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(AbortPreparedStockCommand(lock_id=payload.lock_id)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                            reply_topic=STOCK_EVENTS,
                        ),
                    )
                else:
                    publish_commit_if_ready()
            case "FundsPrepareFailedEvent":
                payload = FundsPrepareFailedEvent(**envelope.payload)
                self.logger.warning("2PC funds prepare failed for %s: %s", order_id, payload.reason)
                set_transaction_status(self.db, order_id, STATUS_2PC_FAILED, failure_reason=payload.reason)
                publish_abort_for_current_locks(envelope.message_id)
            case "StockPrepareFailedEvent":
                payload = StockPrepareFailedEvent(**envelope.payload)
                self.logger.warning("2PC stock prepare failed for %s (shard=%s): %s", order_id, payload.shard_index, payload.reason)
                set_transaction_status(self.db, order_id, STATUS_2PC_FAILED, failure_reason=payload.reason)
                publish_abort_for_current_locks(envelope.message_id)
            case "FundsCommitted2PCEvent":
                _payload = FundsCommitted2PCEvent(**envelope.payload)
                self.logger.warning("2PC funds commit acknowledged for %s", order_id)
                set_2pc_committed_flags(self.db, order_id, funds_committed=True)
                funds_committed, _ = get_2pc_committed_flags(self.db, order_id)
                if funds_committed and all_2pc_stock_shards_committed(self.db, order_id):
                    set_transaction_status(self.db, order_id, STATUS_2PC_COMMITTED, clear_failure_reason=True)
                    self.logger.warning("2PC transaction committed for %s", order_id)
                    order_entry = self.fetch_order(order_id)
                    order_entry.paid = True
                    self.db.set(order_id, msgpack.encode(order_entry))
            case "StockCommitted2PCEvent":
                payload = StockCommitted2PCEvent(**envelope.payload)
                shard_idx = payload.shard_index
                self.logger.warning("2PC stock commit acknowledged for %s (shard=%s)", order_id, shard_idx)
                mark_2pc_stock_shard_committed(self.db, order_id, shard_idx)
                funds_committed, _ = get_2pc_committed_flags(self.db, order_id)
                if funds_committed and all_2pc_stock_shards_committed(self.db, order_id):
                    set_transaction_status(self.db, order_id, STATUS_2PC_COMMITTED, clear_failure_reason=True)
                    self.logger.warning("2PC transaction committed for %s", order_id)
                    order_entry = self.fetch_order(order_id)
                    order_entry.paid = True
                    self.db.set(order_id, msgpack.encode(order_entry))
            case "FundsAborted2PCEvent":
                _payload = FundsAborted2PCEvent(**envelope.payload)
                self.logger.warning("2PC funds abort acknowledged for %s", order_id)
                set_transaction_status(self.db, order_id, STATUS_ABORTED)
            case "StockAborted2PCEvent":
                payload = StockAborted2PCEvent(**envelope.payload)
                self.logger.warning("2PC stock abort acknowledged for %s (shard=%s)", order_id, payload.shard_index)
                set_transaction_status(self.db, order_id, STATUS_ABORTED)
            case _:
                self.logger.debug("Unhandled 2PC event type %s", msg_type)

        mark_tx_processed(self.db, order_id, envelope.message_id)

    def publish_commit_if_ready(self, order_id, transaction_id):
        tx = self.get_transaction(self.db, order_id) or {}
        if tx.get("status") != STATUS_PREPARING:
            return
        
        pay_lock, stock_lock = self.get_lock_ids(self.db, transaction_id)
        if not (pay_lock and stock_lock):
            self.logger.warning("2PC not ready to commit for %s: pay_lock=%s stock_lock=%s", order_id, pay_lock, stock_lock)
            return
        
        self.logger.warning("2PC prepared on all participants for %s; sending commit commands", order_id)
        set_transaction_status(self.db, order_id, STATUS_PREPARED, clear_failure_reason=True)
        publish_envelope(
            PAYMENT_COMMANDS,
            key=order_id,
            envelope=make_envelope(
                "CommitPreparedFundsCommand",
                transaction_id=transaction_id,
                payload=to_builtins(CommitPreparedFundsCommand(lock_id=pay_lock))
            ),
        )
        
        publish_envelope(
            STOCK_COMMANDS,
            key=order_id,
            envelope=make_envelope(
                "CommitPreparedStockCommand",
                transaction_id=transaction_id,
                payload=to_builtins(CommitPreparedStockCommand(lock_id=stock_lock))
            ),
        )

    def publish_abort_for_current_locks(self, causation_id: str):
        pay_lock, _ = get_lock_ids(self.db, order_id)
        if pay_lock:
            self.logger.warning("2PC abort dispatch for %s: payment lock=%s", order_id, pay_lock)
            publish_envelope(
                PAYMENT_COMMANDS,
                key=order_id,
                envelope=make_envelope(
                    "AbortPreparedFundsCommand",
                    transaction_id=order_id,
                    payload=to_builtins(AbortPreparedFundsCommand(lock_id=pay_lock)),
                    correlation_id=envelope.correlation_id,
                    causation_id=causation_id,
                ),
            )
        stock_locks = get_2pc_stock_locks(self.db, order_id)
        for shard, lock_id in stock_locks.items():
            self.logger.warning("2PC abort dispatch for %s: stock lock=%s shard=%s", order_id, lock_id, shard)
            publish_envelope(
                f"stock.commands.{shard}",
                key=order_id,
                envelope=make_envelope(
                    "AbortPreparedStockCommand",
                    transaction_id=order_id,
                    payload=to_builtins(AbortPreparedStockCommand(lock_id=lock_id)),
                    correlation_id=envelope.correlation_id,
                    causation_id=causation_id,
                    reply_topic=STOCK_EVENTS,
                ),
            )

    def handle_funds_prepared_event(self, envelope, order_id, transaction_id):
        payload = FundsPreparedEvent(**envelope.payload)
        self.logger.warning("2PC funds prepared for %s (lock=%s)", order_id, payload.lock_id)
        
        tx = self.get_transaction(self.db, order_id) or {}
        if tx.get("status") in (STATUS_2PC_FAILED, STATUS_ABORTING, STATUS_ABORTED):
            publish_envelope(
                PAYMENT_COMMANDS,
                key=order_id,
                envelope=make_envelope(
                    "AbortPreparedFundsCommand",
                    transaction_id=transaction_id,
                    payload=to_builtins(AbortPreparedFundsCommand(lock_id=payload.lock_id))
                ),
            )
        else:
            self.set_lock_ids(self.db, order_id, payment_lock_id=payload.lock_id)
            self.publish_commit_if_ready(order_id, transaction_id)


# --------------------------- Redis operations for 2PC state management ----------
    def order_processed(self, order_id: str) -> bool:
        return True
    
    def create_transaction(
        self,
        db: redis.Redis,
        order_id: str,
        transaction_id: str,
        deadline_ts: float,
    ) -> None:
        """Initialize a 2PC transaction for an order. Existing state is overwritten."""
        pipe = db.pipeline()
        pipe.hset(
            transaction_id,
            mapping={
                "status": STATUS_PREPARING,
                "order_id": order_id,
                "deadline_ts": deadline_ts,
                "payment_lock_id": "",
                "stock_lock_id": "",
                "funds_committed": "",
                "stock_committed": "",
                "failure_reason": "",
            },
        )
        pipe.execute()

    def get_transaction(self, db: redis.Redis, transaction_id: str) -> dict[str, Any] | None:
        """Load transaction state from Redis."""
        data = db.hgetall(transaction_id)
        if not data:
            return None
        return {k.decode(): v.decode() for k, v in data.items()}
    
    def set_lock_ids(
        self,
        db: redis.Redis,
        transaction_id: str,
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
            db.hset(transaction_id, mapping=mapping)

    def get_lock_ids(self, db: redis.Redis, transaction_id: str) -> tuple[Optional[str], Optional[str]]:
        """Retrieve stored lock IDs for payment and stock."""
        pay, stock = db.hmget(transaction_id, ["payment_lock_id", "stock_lock_id"])
        return (pay.decode() if pay else None, stock.decode() if stock else None)

    def set_transaction_status(
        db: redis.Redis,
        transaction_id: str,
        status: str,
        *,
        failure_reason: Optional[str] = None,
        clear_failure_reason: bool = False,
    ) -> None:
        """Update the transaction status, optionally storing or clearing a failure reason."""
        mapping = {"status": status}
        if failure_reason is not None:
            mapping["failure_reason"] = failure_reason
        elif clear_failure_reason:
            mapping["failure_reason"] = ""
        db.hset(transaction_id, mapping=mapping)