import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable

from flask import abort, jsonify
from msgspec import msgpack

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
            saga_id=order_id,
            payload=reserve_funds.__dict__,
            correlation_id=correlation_id,
        )
        env_stock = make_envelope(
            "ReserveStockCommand",
            saga_id=order_id,
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
        order_id = envelope.saga_id
        msg_type = envelope.type

        if is_processed(self.db, order_id, envelope.message_id):
            return

        def publish_commit_if_ready():
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
                        saga_id=order_id,
                        payload=CommitFundsCommand(reservation_id=pay_res).__dict__,
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.message_id,
                    ),
                )
                publish_envelope(
                    STOCK_COMMANDS,
                    key=order_id,
                    envelope=make_envelope(
                        "CommitStockCommand",
                        saga_id=order_id,
                        payload=CommitStockCommand(reservation_id=stock_res).__dict__,
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.message_id,
                    ),
                )

        match msg_type:
            case "OrderServicePing":
                self.logger.info("Received Kafka ping %s", envelope.message_id)
            case "FundsReservedEvent":
                payload = FundsReservedEvent(**envelope.payload)
                set_reservation_ids(self.db, order_id, payment_reservation_id=payload.reservation_id)
                publish_commit_if_ready()
            case "StockReservedEvent":
                payload = StockReservedEvent(**envelope.payload)
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
                            saga_id=order_id,
                            payload=CancelStockCommand(reservation_id=stock_res).__dict__,
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
                            saga_id=order_id,
                            payload=CancelFundsCommand(reservation_id=pay_res).__dict__,
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                        ),
                    )
            case "FundsCommittedEvent":
                payload = FundsCommittedEvent(**envelope.payload)
                set_committed_flags(self.db, order_id, funds_committed=True)
                funds_committed, stock_committed = get_committed_flags(self.db, order_id)
                if funds_committed and stock_committed:
                    set_status(self.db, order_id, STATUS_COMMITTED)
                    order_entry = self.fetch_order(order_id)
                    order_entry.paid = True
                    self.db.set(order_id, msgpack.encode(order_entry))
            case "StockCommittedEvent":
                payload = StockCommittedEvent(**envelope.payload)
                set_committed_flags(self.db, order_id, stock_committed=True)
                funds_committed, stock_committed = get_committed_flags(self.db, order_id)
                if funds_committed and stock_committed:
                    set_status(self.db, order_id, STATUS_COMMITTED)
                    order_entry = self.fetch_order(order_id)
                    order_entry.paid = True
                    self.db.set(order_id, msgpack.encode(order_entry))
            case "FundsCancelledEvent" | "StockCancelledEvent":
                set_status(self.db, order_id, STATUS_CANCELLED)
            case _:
                self.logger.debug("Unhandled event type %s", msg_type)

        mark_processed(self.db, order_id, envelope.message_id)


class TwoPL2PCOrchestrator:
    """
    Placeholder for the upcoming 2PL/2PC coordinator. Keeps API compatible.
    """

    def __init__(self, logger):
        self.logger = logger

    def checkout(self, order_id: str, order_entry, items_quantities: dict[str, int]):  # noqa: ARG002
        abort(501, "2PL/2PC checkout not implemented yet")

    def checkout_status(self, order_id: str):  # noqa: ARG002
        abort(501, "2PL/2PC status not implemented yet")

    def handle_event(self, envelope):  # noqa: ARG002
        self.logger.debug("2PL/2PC handler not implemented; ignoring event")


def select_orchestrator(mode: str, **kwargs):
    if mode == "saga":
        return SagaOrchestrator(**kwargs)
    return TwoPL2PCOrchestrator(logger=kwargs["logger"])
