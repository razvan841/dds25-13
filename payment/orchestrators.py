import uuid
from typing import Callable

from msgspec import msgpack
from werkzeug.exceptions import HTTPException

from common_kafka.models import (
    make_envelope,
    PAYMENT_EVENTS,
    ReserveFundsCommand,
    CommitFundsCommand,
    CancelFundsCommand,
    FundsReservedEvent,
    FundsReserveFailedEvent,
    FundsCommittedEvent,
    FundsCancelledEvent,
)
from common_kafka.producer import publish_envelope


def _processed_key(saga_id: str) -> str:
    return f"payment:{saga_id}:processed"


def _reservation_key(res_id: str) -> str:
    return f"payment:reservation:{res_id}"


class SagaOrchestrator:
    """
    Handles Reserve/Commit/Cancel commands for saga mode.
    """

    def __init__(self, db, logger, fetch_user_fn: Callable[[str], object]):
        self.db = db
        self.logger = logger
        self.fetch_user = fetch_user_fn

    # --- Idempotency helpers ---
    def _is_processed(self, saga_id: str, message_id: str) -> bool:
        return bool(self.db.sismember(_processed_key(saga_id), message_id))

    def _mark_processed(self, saga_id: str, message_id: str) -> None:
        self.db.sadd(_processed_key(saga_id), message_id)

    # --- Reservation helpers ---
    def _store_reservation(self, res_id: str, user_id: str, amount: int, status: str) -> None:
        self.db.hset(
            _reservation_key(res_id),
            mapping={"user_id": user_id, "amount": str(amount), "status": status},
        )

    def _get_reservation(self, res_id: str) -> dict | None:
        data = self.db.hgetall(_reservation_key(res_id))
        if not data:
            return None
        return {k.decode(): v.decode() for k, v in data.items()}

    def _delete_reservation(self, res_id: str) -> None:
        self.db.delete(_reservation_key(res_id))

    # --- Command handlers ---
    def handle_command(self, envelope):
        saga_id = envelope.saga_id
        if self._is_processed(saga_id, envelope.message_id):
            return

        match envelope.type:
            case "PaymentServicePing":
                print("Received kafka ping")
                self.logger.warning("Received Kafka ping %s", envelope.message_id)
            case "ReserveFundsCommand":
                self._handle_reserve(envelope)
            case "CommitFundsCommand":
                self._handle_commit(envelope)
            case "CancelFundsCommand":
                self._handle_cancel(envelope)
            case _:
                self.logger.debug("Unhandled payment command %s", envelope.type)

        self._mark_processed(saga_id, envelope.message_id)

    def _handle_reserve(self, envelope):
        payload = ReserveFundsCommand(**envelope.payload)
        try:
            user = self.fetch_user(payload.user_id)
        except HTTPException as exc:
            reason = getattr(exc, "description", "User lookup failed")
            publish_envelope(
                PAYMENT_EVENTS,
                key=envelope.saga_id,
                envelope=make_envelope(
                    "FundsReserveFailedEvent",
                    saga_id=envelope.saga_id,
                    payload=FundsReserveFailedEvent(reason=reason).__dict__,
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.message_id,
                ),
            )
            return
        if user.credit < payload.amount:
            publish_envelope(
                PAYMENT_EVENTS,
                key=envelope.saga_id,
                envelope=make_envelope(
                    "FundsReserveFailedEvent",
                    saga_id=envelope.saga_id,
                    payload=FundsReserveFailedEvent(reason="Insufficient funds").__dict__,
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.message_id,
                ),
            )
            return

        res_id = str(uuid.uuid4())
        user.credit -= payload.amount
        self.db.set(payload.user_id, msgpack.encode(user))
        self._store_reservation(res_id, payload.user_id, payload.amount, status="reserved")

        publish_envelope(
            PAYMENT_EVENTS,
            key=envelope.saga_id,
            envelope=make_envelope(
                "FundsReservedEvent",
                saga_id=envelope.saga_id,
                payload=FundsReservedEvent(reservation_id=res_id, amount=payload.amount).__dict__,
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )

    def _handle_commit(self, envelope):
        payload = CommitFundsCommand(**envelope.payload)
        reservation = self._get_reservation(payload.reservation_id)
        if reservation:
            self._delete_reservation(payload.reservation_id)

        publish_envelope(
            PAYMENT_EVENTS,
            key=envelope.saga_id,
            envelope=make_envelope(
                "FundsCommittedEvent",
                saga_id=envelope.saga_id,
                payload=FundsCommittedEvent(reservation_id=payload.reservation_id).__dict__,
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )

    def _handle_cancel(self, envelope):
        payload = CancelFundsCommand(**envelope.payload)
        reservation = self._get_reservation(payload.reservation_id)
        if reservation:
            try:
                amount = int(reservation.get("amount", "0"))
            except ValueError:
                amount = 0
            user_id = reservation.get("user_id")
            if user_id:
                try:
                    user = self.fetch_user(user_id)
                    user.credit += amount
                    self.db.set(user_id, msgpack.encode(user))
                except HTTPException:
                    self.logger.warning(
                        "CancelFundsCommand user lookup failed for reservation %s", payload.reservation_id
                    )
            self._delete_reservation(payload.reservation_id)

        publish_envelope(
            PAYMENT_EVENTS,
            key=envelope.saga_id,
            envelope=make_envelope(
                "FundsCancelledEvent",
                saga_id=envelope.saga_id,
                payload=FundsCancelledEvent(reservation_id=payload.reservation_id).__dict__,
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )


class TwoPL2PCOrchestrator:
    """
    Placeholder for future 2PL/2PC implementation.
    """

    def __init__(self, logger):
        self.logger = logger

    def handle_command(self, envelope):
        self.logger.warning(
            "2PL/2PC payment path not implemented; ignoring %s for saga_id=%s",
            envelope.type,
            envelope.saga_id,
        )


def select_orchestrator(mode: str, **kwargs):
    if mode == "saga":
        return SagaOrchestrator(**kwargs)
    return TwoPL2PCOrchestrator(logger=kwargs["logger"])
