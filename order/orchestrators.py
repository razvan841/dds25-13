import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable

from flask import abort, jsonify
from msgspec import msgpack, to_builtins

from common_kafka.producer import publish_envelope
from common_kafka.models import (
    make_envelope,
    PAYMENT_COMMANDS,
    STOCK_COMMANDS,
    ReserveFundsCommand,
    ReserveStockCommand,
    CommitFundsCommand,
    CommitStockCommand,
    CancelFundsCommand,
    CancelStockCommand,
    FundsReservedEvent,
    StockReservedEvent,
    FundsReserveFailedEvent,
    StockReserveFailedEvent,
    FundsCommittedEvent,
    StockCommittedEvent,
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
from common_kafka.outbox import (
    create_saga,
    get_saga,
    set_reservation_ids,
    set_status,
    set_committed_flags,
    get_committed_flags,
    append_outbox,
    is_processed,
    mark_processed,
    STATUS_TRYING,
    STATUS_RESERVED,
    STATUS_COMMITTED,
    STATUS_CANCELLED,
    STATUS_FAILED,
)
from common_kafka.twopl import (
    create_transaction,
    get_transaction,
    set_transaction_status,
    set_lock_ids,
    get_lock_ids,
    set_committed_flags as set_2pc_committed_flags,
    get_committed_flags as get_2pc_committed_flags,
    is_tx_processed,
    mark_tx_processed,
    STATUS_PREPARING,
    STATUS_PREPARED,
    STATUS_COMMITTED as STATUS_2PC_COMMITTED,
    STATUS_ABORTED,
    STATUS_FAILED as STATUS_2PC_FAILED,
)


class SagaOrchestrator:
    """
    Existing saga-based orchestration extracted so the app can swap strategies.
    """

    def __init__(
        self,
        db,
        logger,
        fetch_order_fn: Callable[[str], object],
        checkout_deadline_seconds: int,
    ):
        self.db = db
        self.logger = logger
        self.fetch_order = fetch_order_fn
        self.checkout_deadline_seconds = checkout_deadline_seconds

    # ---------- HTTP-layer operations ----------
    def checkout(self, order_id: str, order_entry, items_quantities: dict[str, int]):
        correlation_id = str(uuid.uuid4())
        deadline_ts = (
            datetime.now(timezone.utc) + timedelta(seconds=self.checkout_deadline_seconds)
        ).timestamp()

        create_saga(self.db, order_id, correlation_id, deadline_ts)

        # Build and publish reserve commands
        reserve_funds = ReserveFundsCommand(user_id=order_entry.user_id, amount=order_entry.total_cost)
        reserve_stock = ReserveStockCommand(items=list(items_quantities.items()))

        env_funds = make_envelope(
            "ReserveFundsCommand",
            transaction_id=order_id,
            payload=to_builtins(reserve_funds),
            correlation_id=correlation_id,
        )
        env_stock = make_envelope(
            "ReserveStockCommand",
            transaction_id=order_id,
            payload={"items": reserve_stock.items},
            correlation_id=correlation_id,
        )
        append_outbox(self.db, order_id, PAYMENT_COMMANDS, env_funds)
        append_outbox(self.db, order_id, STOCK_COMMANDS, env_stock)

        return jsonify({"order_id": order_id, "status": STATUS_TRYING})

    def checkout_status(self, order_id: str):
        saga = get_saga(self.db, order_id)
        if saga is None:
            abort(404, f"Saga for order {order_id} not found")
        return jsonify({"order_id": order_id, "status": saga.get("status", STATUS_FAILED)})

    # ---------- Kafka event handling ----------
    def handle_event(self, envelope):
        """
        Lightweight event handler updating saga state and issuing follow-up commands.
        """
        order_id = envelope.transaction_id
        msg_type = envelope.type

        if is_processed(self.db, order_id, envelope.message_id):
            return

        def publish_commit_if_ready():
            self.logger.warning(f"Commit ready to be published!")
            saga = get_saga(self.db, order_id) or {}
            pay_res = saga.get("payment_reservation_id", "")
            stock_res = saga.get("stock_reservation_id", "")
            if pay_res and stock_res:
                set_status(self.db, order_id, STATUS_RESERVED)
                publish_envelope(
                    PAYMENT_COMMANDS,
                    key=order_id,
                    envelope=make_envelope(
                        "CommitFundsCommand",
                        transaction_id=order_id,
                        payload=to_builtins(CommitFundsCommand(reservation_id=pay_res)),
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.message_id,
                    ),
                )
                publish_envelope(
                    STOCK_COMMANDS,
                    key=order_id,
                    envelope=make_envelope(
                        "CommitStockCommand",
                        transaction_id=order_id,
                        payload=to_builtins(CommitStockCommand(reservation_id=stock_res)),
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.message_id,
                    ),
                )

        match msg_type:
            case "OrderServicePing":
                self.logger.info("Received Kafka ping %s", envelope.message_id)
            case "FundsReservedEvent":
                payload = FundsReservedEvent(**envelope.payload)
                self.logger.warning(f"Received FundsReservedEvent")
                set_reservation_ids(self.db, order_id, payment_reservation_id=payload.reservation_id)
                publish_commit_if_ready()
            case "StockReservedEvent":
                payload = StockReservedEvent(**envelope.payload)
                self.logger.warning(f"Received StockReservedEvent")
                set_reservation_ids(self.db, order_id, stock_reservation_id=payload.reservation_id)
                publish_commit_if_ready()
            case "FundsReserveFailedEvent":
                payload = FundsReserveFailedEvent(**envelope.payload)
                self.logger.warning("Funds reservation failed for %s: %s", order_id, payload.reason)
                set_status(self.db, order_id, STATUS_FAILED)
                saga = get_saga(self.db, order_id) or {}
                stock_res = saga.get("stock_reservation_id", "")
                if stock_res:
                    append_outbox(
                        self.db,
                        order_id,
                        STOCK_COMMANDS,
                        make_envelope(
                            "CancelStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CancelStockCommand(reservation_id=stock_res)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                        ),
                    )
            case "StockReserveFailedEvent":
                payload = StockReserveFailedEvent(**envelope.payload)
                self.logger.warning("Stock reservation failed for %s: %s", order_id, payload.reason)
                set_status(self.db, order_id, STATUS_FAILED)
                saga = get_saga(self.db, order_id) or {}
                pay_res = saga.get("payment_reservation_id", "")
                if pay_res:
                    append_outbox(
                        self.db,
                        order_id,
                        PAYMENT_COMMANDS,
                        make_envelope(
                            "CancelFundsCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CancelFundsCommand(reservation_id=pay_res)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                        ),
                    )
            case "FundsCommittedEvent":
                self.logger.warning(f"Received FundsCommittedEvent")
                payload = FundsCommittedEvent(**envelope.payload)
                set_committed_flags(self.db, order_id, funds_committed=True)
                funds_committed, stock_committed = get_committed_flags(self.db, order_id)
                if funds_committed and stock_committed:
                    set_status(self.db, order_id, STATUS_COMMITTED)
                    order_entry = self.fetch_order(order_id)
                    order_entry.paid = True
                    self.db.set(order_id, msgpack.encode(order_entry))
            case "StockCommittedEvent":
                self.logger.warning(f"Received StockCommittedEvent")
                payload = StockCommittedEvent(**envelope.payload)
                set_committed_flags(self.db, order_id, stock_committed=True)
                funds_committed, stock_committed = get_committed_flags(self.db, order_id)
                if funds_committed and stock_committed:
                    set_status(self.db, order_id, STATUS_COMMITTED)
                    order_entry = self.fetch_order(order_id)
                    order_entry.paid = True
                    self.db.set(order_id, msgpack.encode(order_entry))
            case "FundsCancelledEvent" | "StockCancelledEvent":
                self.logger.warning(f"Received Stock/Funds cancelled events: {msg_type}")
                set_status(self.db, order_id, STATUS_CANCELLED)
                
                
            case "PaymentServicePing":
                self.logger.info("Received payment ping %s", envelope.message_id)

            case _:
                self.logger.debug("Unhandled event type %s", msg_type)

        mark_processed(self.db, order_id, envelope.message_id)


class TwoPL2PCOrchestrator:
    """
    2PC coordinator with strict 2PL semantics handled by participants.
    Uses centralized twopl.py for database operations.
    """

    def __init__(
        self,
        db,
        logger,
        fetch_order_fn: Callable[[str], object],
        checkout_deadline_seconds: int,
    ):
        self.db = db
        self.logger = logger
        self.fetch_order = fetch_order_fn
        self.checkout_deadline_seconds = checkout_deadline_seconds

    def checkout(self, order_id: str, order_entry, items_quantities: dict[str, int]):
        correlation_id = str(uuid.uuid4())
        deadline_ts = (
            datetime.now(timezone.utc) + timedelta(seconds=self.checkout_deadline_seconds)
        ).timestamp()

        # Initialize 2PC transaction state
        create_transaction(self.db, order_id, correlation_id, deadline_ts)

        prepare_funds = PrepareFundsCommand(user_id=order_entry.user_id, amount=order_entry.total_cost)
        prepare_stock = PrepareStockCommand(items=list(items_quantities.items()))

        publish_envelope(
            PAYMENT_COMMANDS,
            key=order_id,
            envelope=make_envelope(
                "PrepareFundsCommand",
                transaction_id=order_id,
                payload=to_builtins(prepare_funds),
                correlation_id=correlation_id,
            ),
        )
        publish_envelope(
            STOCK_COMMANDS,
            key=order_id,
            envelope=make_envelope(
                "PrepareStockCommand",
                transaction_id=order_id,
                payload={"items": prepare_stock.items},
                correlation_id=correlation_id,
            ),
        )

        # Block until transaction reaches a terminal state or deadline
        deadline = time.time() + self.checkout_deadline_seconds
        while time.time() < deadline:
            tx = get_transaction(self.db, order_id) or {}
            status = tx.get("status", STATUS_PREPARING)
            if status == STATUS_2PC_COMMITTED:
                return jsonify({"order_id": order_id, "status": status})
            if status in (STATUS_2PC_FAILED, STATUS_ABORTED):
                return jsonify({"order_id": order_id, "status": status}), 400
            time.sleep(0.01)

        return jsonify({"order_id": order_id, "status": STATUS_PREPARING}), 408

    def checkout_status(self, order_id: str):
        tx = get_transaction(self.db, order_id)
        if tx is None:
            abort(404, f"2PC transaction for order {order_id} not found")
        return jsonify({"order_id": order_id, "status": tx.get("status", STATUS_2PC_FAILED)})

    def handle_event(self, envelope):
        order_id = envelope.transaction_id
        msg_type = envelope.type

        if is_tx_processed(self.db, order_id, envelope.message_id):
            return

        # Reject stale events from previous checkout attempts on the same order
        tx_meta = get_transaction(self.db, order_id) or {}
        current_correlation = tx_meta.get("correlation_id", "")
        if current_correlation and envelope.correlation_id and envelope.correlation_id != current_correlation:
            self.logger.debug(
                "Ignoring stale event %s for order %s (correlation %s != current %s)",
                msg_type, order_id, envelope.correlation_id, current_correlation,
            )
            return

        def publish_commit_if_ready():
            tx = get_transaction(self.db, order_id) or {}
            if tx.get("status") != STATUS_PREPARING:
                return
            pay_lock, stock_lock = get_lock_ids(self.db, order_id)
            if pay_lock and stock_lock:
                set_transaction_status(self.db, order_id, STATUS_PREPARED)
                publish_envelope(
                    PAYMENT_COMMANDS,
                    key=order_id,
                    envelope=make_envelope(
                        "CommitPreparedFundsCommand",
                        transaction_id=order_id,
                        payload=to_builtins(CommitPreparedFundsCommand(lock_id=pay_lock)),
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.message_id,
                    ),
                )
                publish_envelope(
                    STOCK_COMMANDS,
                    key=order_id,
                    envelope=make_envelope(
                        "CommitPreparedStockCommand",
                        transaction_id=order_id,
                        payload=to_builtins(CommitPreparedStockCommand(lock_id=stock_lock)),
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.message_id,
                    ),
                )

        def publish_abort_for_current_locks(causation_id: str):
            pay_lock, stock_lock = get_lock_ids(self.db, order_id)
            if pay_lock:
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
            if stock_lock:
                publish_envelope(
                    STOCK_COMMANDS,
                    key=order_id,
                    envelope=make_envelope(
                        "AbortPreparedStockCommand",
                        transaction_id=order_id,
                        payload=to_builtins(AbortPreparedStockCommand(lock_id=stock_lock)),
                        correlation_id=envelope.correlation_id,
                        causation_id=causation_id,
                    ),
                )

        match msg_type:
            case "FundsPreparedEvent":
                payload = FundsPreparedEvent(**envelope.payload)
                set_lock_ids(self.db, order_id, payment_lock_id=payload.lock_id)
                tx = get_transaction(self.db, order_id) or {}
                if tx.get("status") in (STATUS_2PC_FAILED, STATUS_ABORTED):
                    publish_envelope(
                        PAYMENT_COMMANDS,
                        key=order_id,
                        envelope=make_envelope(
                            "AbortPreparedFundsCommand",
                            transaction_id=order_id,
                            payload=to_builtins(AbortPreparedFundsCommand(lock_id=payload.lock_id)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                        ),
                    )
                else:
                    publish_commit_if_ready()
            case "StockPreparedEvent":
                payload = StockPreparedEvent(**envelope.payload)
                set_lock_ids(self.db, order_id, stock_lock_id=payload.lock_id)
                tx = get_transaction(self.db, order_id) or {}
                if tx.get("status") in (STATUS_2PC_FAILED, STATUS_ABORTED):
                    publish_envelope(
                        STOCK_COMMANDS,
                        key=order_id,
                        envelope=make_envelope(
                            "AbortPreparedStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(AbortPreparedStockCommand(lock_id=payload.lock_id)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                        ),
                    )
                else:
                    publish_commit_if_ready()
            case "FundsPrepareFailedEvent":
                payload = FundsPrepareFailedEvent(**envelope.payload)
                self.logger.warning("2PC funds prepare failed for %s: %s", order_id, payload.reason)
                set_transaction_status(self.db, order_id, STATUS_2PC_FAILED)
                publish_abort_for_current_locks(envelope.message_id)
            case "StockPrepareFailedEvent":
                payload = StockPrepareFailedEvent(**envelope.payload)
                self.logger.warning("2PC stock prepare failed for %s: %s", order_id, payload.reason)
                set_transaction_status(self.db, order_id, STATUS_2PC_FAILED)
                publish_abort_for_current_locks(envelope.message_id)
            case "FundsCommitted2PCEvent":
                _payload = FundsCommitted2PCEvent(**envelope.payload)
                set_2pc_committed_flags(self.db, order_id, funds_committed=True)
                funds_committed, stock_committed = get_2pc_committed_flags(self.db, order_id)
                if funds_committed and stock_committed:
                    set_transaction_status(self.db, order_id, STATUS_2PC_COMMITTED)
                    order_entry = self.fetch_order(order_id)
                    order_entry.paid = True
                    self.db.set(order_id, msgpack.encode(order_entry))
            case "StockCommitted2PCEvent":
                _payload = StockCommitted2PCEvent(**envelope.payload)
                set_2pc_committed_flags(self.db, order_id, stock_committed=True)
                funds_committed, stock_committed = get_2pc_committed_flags(self.db, order_id)
                if funds_committed and stock_committed:
                    set_transaction_status(self.db, order_id, STATUS_2PC_COMMITTED)
                    order_entry = self.fetch_order(order_id)
                    order_entry.paid = True
                    self.db.set(order_id, msgpack.encode(order_entry))
            case "FundsAborted2PCEvent":
                _payload = FundsAborted2PCEvent(**envelope.payload)
                set_transaction_status(self.db, order_id, STATUS_ABORTED)
            case "StockAborted2PCEvent":
                _payload = StockAborted2PCEvent(**envelope.payload)
                set_transaction_status(self.db, order_id, STATUS_ABORTED)
            case _:
                self.logger.debug("Unhandled 2PC event type %s", msg_type)

        mark_tx_processed(self.db, order_id, envelope.message_id)




def select_orchestrator(mode: str, **kwargs):
    if mode == "saga":
        return SagaOrchestrator(**kwargs)
    return TwoPL2PCOrchestrator(
        db=kwargs["db"],
        logger=kwargs["logger"],
        fetch_order_fn=kwargs["fetch_order_fn"],
        checkout_deadline_seconds=kwargs["checkout_deadline_seconds"],
    )
