from typing import Any, Optional

from flask import abort, jsonify
import json
import uuid
import time
from datetime import datetime, timedelta, timezone

from msgspec import to_builtins
import redis
from common_kafka.producer import publish_envelope
from common_kafka.models import (
    make_envelope,
    PAYMENT_COMMANDS,
    STOCK_COMMANDS,
    PrepareFundsCommand,
    PrepareStockCommand,
    CommitPreparedFundsCommand,
    CommitPreparedStockCommand,
    AbortPreparedFundsCommand,
    AbortPreparedStockCommand,
    FundsPreparedEvent,
    StockPreparedEvent,
    FundsPrepareFailedEvent,
    StockPrepareFailedEvent
)

STATUS_PREPARING = "preparing"
STATUS_COMMITTING = "committing"
STATUS_ABORTING = "aborting"
STATUS_FAILED = "failed"
STATUS_ABORTED = "aborted"
STATUS_FINISHED = "finished"

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
        # Block duplicate checkout attempts while an order is in-flight or already successful.
        # Only failed/aborted orders are allowed to retry.
        if self.order_processed(order_id):
            self.logger.info("Checkout request for already processed order %s", order_id)
            return jsonify({"message": "Order is already processing or completed"}), 409
        
        transaction_id = str(uuid.uuid4())
        # Create the order record in the db. Order knowns the list of transaction_ids
        # If order already exists, transaction_id is added to the list of transaction_ids for that order
        self.create_order(order_id, transaction_id)
        deadline_ts = (
            datetime.now(timezone.utc) + timedelta(seconds=self.checkout_deadline_seconds)
        ).timestamp()

        self.create_transaction(order_id, transaction_id=transaction_id, deadline_ts=deadline_ts)

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
            STOCK_COMMANDS,
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
            tx = self.get_transaction(transaction_id) or {}
            status = tx.get("status", STATUS_PREPARING)
            if status == STATUS_FINISHED:
                return jsonify({"order_id": order_id, "status": status})
            if status in (STATUS_FAILED, STATUS_ABORTED):
                return jsonify({"order_id": order_id, "status": status}), 400
            time.sleep(0.01)

        return jsonify({"order_id": order_id, "status": STATUS_PREPARING}), 408

    def checkout_status(self, order_id: str):
        record = self.db.hgetall(f"2pl2pc:{order_id}")
        if not record:
            abort(404, f"2PC order record for order {order_id} not found")

        decoded = {k.decode(): v.decode() for k, v in record.items()}
        status_value = decoded.get("status", "processing").strip().lower()
        processed_raw = decoded.get("processed", "").strip().lower()
        if processed_raw:
            processed = processed_raw in {"1", "true", "yes", "finished", "failed", "aborted"}
        else:
            processed = status_value in {"finished", "failed", "aborted"}

        status_code_raw = decoded.get("status_code", "")
        try:
            status_code = int(status_code_raw) if status_code_raw else None
        except ValueError:
            status_code = None

        return jsonify(
            {
                "order_id": order_id,
                "processed": processed,
                "status_code": status_code,
                "fault_reason": decoded.get("fault_reason", ""),
            }
        )
    
    def handle_event(self, envelope):
        order_id = envelope.order_id
        msg_type = envelope.type
        payload = envelope.payload
        transaction_id = envelope.transaction_id
        reason = self._extract_reason(payload)

        if self.is_message_processed(transaction_id, envelope.message_id):
            return

        match msg_type:
            case "FundsPreparedEvent":
                self.handle_funds_prepared_event(envelope, order_id, transaction_id)
            case "StockPreparedEvent":
                self.handle_stock_prepared_event(envelope, order_id, transaction_id)
            case "FundsPrepareFailedEvent":
                self.handle_funds_failed_event(order_id, transaction_id, reason)
            case "StockPrepareFailedEvent":
                self.handle_stock_failed_event(order_id, transaction_id, reason)
            case "FundsCommitted2PCEvent":
                self.handle_funds_committed_event(order_id, transaction_id)
            case "StockCommitted2PCEvent":
                self.handle_stock_committed_event(order_id, transaction_id)
            case "FundsAborted2PCEvent":
                self.handle_funds_aborted_event(order_id, transaction_id, reason)
            case "StockAborted2PCEvent":
                self.handle_stock_aborted_event(order_id, transaction_id, reason)
            case _:
                self.logger.debug("Unhandled 2PC event type %s", msg_type)

        self.mark_message_processed(transaction_id, envelope.message_id)

    def publish_commit_if_ready(self, order_id, transaction_id):
        tx = self.get_transaction(transaction_id) or {}
        if tx.get("status") != STATUS_PREPARING:
            return
        
        pay_lock, stock_lock = self.get_lock_ids(transaction_id)
        if not (pay_lock and stock_lock):
            self.logger.warning("2PC not ready to commit for %s: pay_lock=%s stock_lock=%s", order_id, pay_lock, stock_lock)
            return
        
        self.logger.warning("2PC prepared on all participants for %s; sending commit commands", order_id)
        self.set_transaction_status(transaction_id, STATUS_COMMITTING, clear_failure_reason=True)
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

    def publish_abort_for_current_locks(self, order_id: str, transaction_id: str):
        pay_lock, stock_lock = self.get_lock_ids(transaction_id)
        if pay_lock:
            self.logger.warning("2PC abort dispatch for %s: payment lock=%s", transaction_id, pay_lock)
            publish_envelope(
                PAYMENT_COMMANDS,
                key=order_id,
                envelope=make_envelope(
                    "AbortPreparedFundsCommand",
                    transaction_id=transaction_id,
                    payload=to_builtins(AbortPreparedFundsCommand(lock_id=pay_lock))
                ),
            )
        if stock_lock:
            self.logger.warning("2PC abort dispatch for %s: stock lock=%s", order_id, stock_lock)
            publish_envelope(
                STOCK_COMMANDS,
                key=order_id,
                envelope=make_envelope(
                    "AbortPreparedStockCommand",
                    transaction_id=transaction_id,
                    payload=to_builtins(AbortPreparedStockCommand(lock_id=stock_lock))
                ),
            )

    def handle_funds_prepared_event(self, envelope, order_id, transaction_id):
        payload = FundsPreparedEvent(**envelope.payload)
        self.logger.info("2PC funds prepared for %s (lock=%s)", order_id, payload.lock_id)
        
        tx = self.get_transaction(transaction_id) or {}
        if tx.get("status") in (STATUS_FAILED, STATUS_ABORTING, STATUS_ABORTED):
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
            self.set_lock_ids(transaction_id, payment_lock_id=payload.lock_id)
            self.publish_commit_if_ready(order_id, transaction_id)

    def handle_stock_prepared_event(self, envelope, order_id, transaction_id):
        payload = StockPreparedEvent(**envelope.payload)
        self.logger.info("2PC stock prepared for %s (lock=%s)", order_id, payload.lock_id)

        tx = self.get_transaction(transaction_id) or {}
        if tx.get("status") in (STATUS_FAILED, STATUS_ABORTING, STATUS_ABORTED):
            publish_envelope(
                STOCK_COMMANDS,
                key=order_id,
                envelope=make_envelope(
                    "AbortPreparedStockCommand",
                    transaction_id=transaction_id,
                    payload=to_builtins(AbortPreparedStockCommand(lock_id=payload.lock_id))
                ),
            )
        else:
            self.set_lock_ids(transaction_id, stock_lock_id=payload.lock_id)
            self.publish_commit_if_ready(order_id, transaction_id)

    def handle_funds_failed_event(self, order_id: str, transaction_id: str, reason: str):
        payload = FundsPrepareFailedEvent(reason)
        self.logger.warning("2PC funds prepare failed for %s: %s", order_id, payload.reason)
        self.set_order_result(order_id, status="finished", status_code=400, fault_reason=payload.reason)
        self.set_transaction_status(transaction_id, STATUS_ABORTED, failure_reason=payload.reason)
        self.publish_abort_for_current_locks(order_id, transaction_id)

    def handle_stock_failed_event(self, order_id: str, transaction_id: str, reason: str):
        payload = StockPrepareFailedEvent(reason)
        self.logger.warning("2PC stock prepare failed for %s (shard=%s): %s", order_id, payload.shard_index, payload.reason)
        self.set_order_result(order_id, status="finished", status_code=400, fault_reason=payload.reason)
        self.set_transaction_status(transaction_id, STATUS_ABORTED, failure_reason=payload.reason)
        self.publish_abort_for_current_locks(order_id, transaction_id)

    def handle_funds_committed_event(self, order_id, transaction_id):
        self.logger.warning("2PC funds commit acknowledged for %s", transaction_id)
        self.set_funds_committed(transaction_id, committed=True)
        tx = self.get_transaction(transaction_id) or {}
        funds_committed = tx.get("funds_committed", "") == "1"
        stock_committed = tx.get("stock_committed", "") == "1"
        if funds_committed and stock_committed:
            self.handle_commits_ready(order_id, transaction_id)

    def handle_stock_committed_event(self, order_id, transaction_id):
        self.logger.warning("2PC stock commit acknowledged for %s", transaction_id)
        self.set_stock_committed(transaction_id, committed=True)
        tx = self.get_transaction(transaction_id) or {}
        funds_committed = tx.get("funds_committed", "") == "1"
        stock_committed = tx.get("stock_committed", "") == "1"
        if funds_committed and stock_committed:
            self.handle_commits_ready(order_id, transaction_id)

    # Received both commit acknowledgments, finalize the transaction
    def handle_commits_ready(self, order_id: str, transaction_id: str):
        self.set_transaction_status(transaction_id, STATUS_FINISHED, clear_failure_reason=True)
        self.logger.warning("2PC transaction finished for %s", order_id)
        self.set_order_result(order_id, status="finished", status_code=200)

    def handle_funds_aborted_event(self, order_id: str, transaction_id: str, reason: str):
        self.logger.warning("2PC funds abort acknowledged for %s", transaction_id)
        self.set_transaction_status(transaction_id, STATUS_ABORTED, failure_reason=reason)
        self.set_order_result(order_id, status="aborted", status_code=400, fault_reason=reason)

    def handle_stock_aborted_event(self, order_id: str, transaction_id: str, reason: str):
        self.logger.warning("2PC stock abort acknowledged for %s", transaction_id)
        self.set_transaction_status(transaction_id, STATUS_ABORTED, failure_reason=reason)
        self.set_order_result(order_id, status="aborted", status_code=400, fault_reason=reason)

    def _extract_reason(self, payload) -> str:
        if isinstance(payload, dict):
            return str(payload.get("reason", ""))
        return str(getattr(payload, "reason", ""))

# --------------------------- Redis operations for 2PC state management ----------
    def is_message_processed(self, transaction_id: str, message_id: str) -> bool:
        if not transaction_id or not message_id:
            return False
        return bool(self.db.sismember(f"2pl2pc:{transaction_id}:processed", message_id))

    def mark_message_processed(self, transaction_id: str, message_id: str) -> None:
        if not transaction_id or not message_id:
            return
        self.db.sadd(f"2pl2pc:{transaction_id}:processed", message_id)

    def set_funds_committed(self, transaction_id: str, committed: bool = True) -> None:
        self.db.hset(transaction_id, "funds_committed", "1" if committed else "")

    def set_stock_committed(self, transaction_id: str, committed: bool = True) -> None:
        self.db.hset(transaction_id, "stock_committed", "1" if committed else "")

    def set_order_result(self, order_id: str, status: str, status_code: int, fault_reason: str = "") -> None:
        processed = "true" if status in {"finished", "failed", "aborted"} else "false"
        self.db.hset(
            f"2pl2pc:{order_id}",
            mapping={
                "processed": processed,
                "status": status,
                "status_code": str(status_code),
                "fault_reason": fault_reason
            },
        )

    def order_processed(self, order_id: str) -> bool:
        data = self.db.hgetall(f"2pl2pc:{order_id}")
        if not data:
            return False

        decoded = {k.decode(): v.decode() for k, v in data.items()}
        status = decoded.get("status", "processing").strip().lower()
        if status in {"failed", "aborted"}:
            return False
        return True

    def create_order(self, order_id: str, transaction_id: str) -> None:
        key = f"2pl2pc:{order_id}"
        existing_data = self.db.hgetall(key)

        transaction_ids = []
        if existing_data:
            decoded = {k.decode(): v.decode() for k, v in existing_data.items()}
            raw_ids = decoded.get("transaction_ids", "[]")
            try:
                parsed_ids = json.loads(raw_ids)
                if isinstance(parsed_ids, list):
                    transaction_ids = [str(tx_id) for tx_id in parsed_ids]
            except json.JSONDecodeError:
                transaction_ids = []

        if transaction_id not in transaction_ids:
            transaction_ids.append(transaction_id)

        self.db.hset(
            key,
            mapping={
                "transaction_ids": json.dumps(transaction_ids),
                "processed": "false",
                "status": "processing",
                "status_code": "",
                "fault_reason": "",
            },
        )
    
    def create_transaction(
        self,
        order_id: str,
        transaction_id: str,
        deadline_ts: float,
    ) -> None:
        """Initialize a 2PC transaction for an order."""
        pipe = self.db.pipeline()
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

    def get_transaction(self, transaction_id: str) -> dict[str, Any] | None:
        """Load transaction state from Redis."""
        data = self.db.hgetall(transaction_id)
        if not data:
            return None
        return {k.decode(): v.decode() for k, v in data.items()}
    
    def set_lock_ids(
        self,
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
            self.db.hset(transaction_id, mapping=mapping)

    def get_lock_ids(self, transaction_id: str) -> tuple[Optional[str], Optional[str]]:
        """Retrieve stored lock IDs for payment and stock."""
        pay, stock = self.db.hmget(transaction_id, ["payment_lock_id", "stock_lock_id"])
        return (pay.decode() if pay else None, stock.decode() if stock else None)

    def set_transaction_status(
        self,
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
        self.db.hset(transaction_id, mapping=mapping)