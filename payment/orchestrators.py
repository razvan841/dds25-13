import uuid
from typing import Callable

from msgspec import msgpack, to_builtins
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
    PrepareFundsCommand,
    CommitPreparedFundsCommand,
    AbortPreparedFundsCommand,
    FundsPreparedEvent,
    FundsPrepareFailedEvent,
    FundsCommitted2PCEvent,
    FundsAborted2PCEvent,
)
from common_kafka.producer import publish_envelope
from common_kafka.twopl import (
    is_participant_processed,
    mark_participant_processed,
    acquire_resource_lock,
    release_resource_lock,
    store_prepared_lock_payment,
    get_prepared_lock_payment,
    delete_prepared_lock_payment,
)


def _processed_key(transaction_id: str) -> str:
    return f"payment:{transaction_id}:processed"


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
    def _is_processed(self, transaction_id: str, message_id: str) -> bool:
        return bool(self.db.sismember(_processed_key(transaction_id), message_id))

    def _mark_processed(self, transaction_id: str, message_id: str) -> None:
        self.db.sadd(_processed_key(transaction_id), message_id)

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
        transaction_id = envelope.transaction_id
        if self._is_processed(transaction_id, envelope.message_id):
            return

        match envelope.type:
            case "PaymentServicePing":
                print("Received kafka ping")
                self.logger.warning("Received Kafka ping %s", envelope.message_id)
            case "ReserveFundsCommand":
                self.logger.warning(f"Received ReserveFundsCommand")
                self._handle_reserve(envelope)
            case "CommitFundsCommand":
                self.logger.warning(f"Received CommitFundsCommand")
                self._handle_commit(envelope)
            case "CancelFundsCommand":
                self._handle_cancel(envelope)
            case _:
                self.logger.debug("Unhandled payment command %s", envelope.type)

        self._mark_processed(transaction_id, envelope.message_id)

    def _handle_reserve(self, envelope):
        payload = ReserveFundsCommand(**envelope.payload)
        try:
            user = self.fetch_user(payload.user_id)
        except HTTPException as exc:
            reason = getattr(exc, "description", "User lookup failed")
            publish_envelope(
                PAYMENT_EVENTS,
                key=envelope.transaction_id,
                envelope=make_envelope(
                    "FundsReserveFailedEvent",
                    transaction_id=envelope.transaction_id,
                    payload=to_builtins(FundsReserveFailedEvent(reason=reason)),
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.message_id,
                ),
            )
            return
        if user.credit < payload.amount:
            self.logger.warning(f"User doesnt have enough credit")
            publish_envelope(
                PAYMENT_EVENTS,
                key=envelope.transaction_id,
                envelope=make_envelope(
                    "FundsReserveFailedEvent",
                    transaction_id=envelope.transaction_id,
                    payload=to_builtins(FundsReserveFailedEvent(reason="Insufficient funds")),
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.message_id,
                ),
            )
            return

        res_id = str(uuid.uuid4())
        user.credit -= payload.amount
        self.db.set(payload.user_id, msgpack.encode(user))
        self._store_reservation(res_id, payload.user_id, payload.amount, status="reserved")
        self.logger.warning(f"FundsReservedEvent sent")

        publish_envelope(
            PAYMENT_EVENTS,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "FundsReservedEvent",
                transaction_id=envelope.transaction_id,
                payload=to_builtins(FundsReservedEvent(reservation_id=res_id, amount=payload.amount)),
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
            key=envelope.transaction_id,
            envelope=make_envelope(
                "FundsCommittedEvent",
                transaction_id=envelope.transaction_id,
                payload=to_builtins(FundsCommittedEvent(reservation_id=payload.reservation_id)),
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
            key=envelope.transaction_id,
            envelope=make_envelope(
                "FundsCancelledEvent",
                transaction_id=envelope.transaction_id,
                payload=to_builtins(FundsCancelledEvent(reservation_id=payload.reservation_id)),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )


class TwoPL2PCOrchestrator:
    """
    Payment participant implementing strict 2PL + 2PC semantics.
    Uses centralized twopl.py for database operations.
    """

    SERVICE = "payment"
    RESOURCE_TYPE = "user"

    def __init__(self, db, logger, fetch_user_fn: Callable[[str], object]):
        self.db = db
        self.logger = logger
        self.fetch_user = fetch_user_fn

    def _is_processed(self, transaction_id: str, message_id: str) -> bool:
        return is_participant_processed(self.db, self.SERVICE, transaction_id, message_id)

    def _mark_processed(self, transaction_id: str, message_id: str) -> None:
        mark_participant_processed(self.db, self.SERVICE, transaction_id, message_id)

    def handle_command(self, envelope):
        transaction_id = envelope.transaction_id
        if self._is_processed(transaction_id, envelope.message_id):
            return

        match envelope.type:
            case "PrepareFundsCommand":
                self._handle_prepare(envelope)
            case "CommitPreparedFundsCommand":
                self._handle_commit_prepared(envelope)
            case "AbortPreparedFundsCommand":
                self._handle_abort_prepared(envelope)
            case _:
                self.logger.debug("Unhandled 2PC payment command %s", envelope.type)

        self._mark_processed(transaction_id, envelope.message_id)

    def _handle_prepare(self, envelope):
        payload = PrepareFundsCommand(**envelope.payload)

        # Acquire resource lock using twopl module
        lock_acquired, owner_saga = acquire_resource_lock(
            self.db, self.SERVICE, self.RESOURCE_TYPE, payload.user_id, envelope.transaction_id
        )
        if not lock_acquired:
            publish_envelope(
                PAYMENT_EVENTS,
                key=envelope.transaction_id,
                envelope=make_envelope(
                    "FundsPrepareFailedEvent",
                    transaction_id=envelope.transaction_id,
                    payload=to_builtins(
                        FundsPrepareFailedEvent(reason=f"User funds lock is held by transaction {owner_saga}")
                    ),
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.message_id,
                ),
            )
            return

        try:
            user = self.fetch_user(payload.user_id)
            if user.credit < payload.amount:
                raise ValueError("Insufficient funds")

            lock_id = str(uuid.uuid4())
            store_prepared_lock_payment(self.db, lock_id, envelope.transaction_id, payload.user_id, payload.amount)

            publish_envelope(
                PAYMENT_EVENTS,
                key=envelope.transaction_id,
                envelope=make_envelope(
                    "FundsPreparedEvent",
                    transaction_id=envelope.transaction_id,
                    payload=to_builtins(FundsPreparedEvent(lock_id=lock_id, amount=payload.amount)),
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.message_id,
                ),
            )
        except HTTPException as exc:
            reason = getattr(exc, "description", "User lookup failed")
            release_resource_lock(self.db, self.SERVICE, self.RESOURCE_TYPE, payload.user_id)
            publish_envelope(
                PAYMENT_EVENTS,
                key=envelope.transaction_id,
                envelope=make_envelope(
                    "FundsPrepareFailedEvent",
                    transaction_id=envelope.transaction_id,
                    payload=to_builtins(FundsPrepareFailedEvent(reason=reason)),
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.message_id,
                ),
            )
        except ValueError as exc:
            release_resource_lock(self.db, self.SERVICE, self.RESOURCE_TYPE, payload.user_id)
            publish_envelope(
                PAYMENT_EVENTS,
                key=envelope.transaction_id,
                envelope=make_envelope(
                    "FundsPrepareFailedEvent",
                    transaction_id=envelope.transaction_id,
                    payload=to_builtins(FundsPrepareFailedEvent(reason=str(exc))),
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.message_id,
                ),
            )

    def _handle_commit_prepared(self, envelope):
        payload = CommitPreparedFundsCommand(**envelope.payload)
        prepared = get_prepared_lock_payment(self.db, payload.lock_id)
        if prepared:
            user_id = prepared.get("user_id", "")
            try:
                amount = int(prepared.get("amount", "0"))
            except ValueError:
                amount = 0
            if user_id:
                try:
                    user = self.fetch_user(user_id)
                    user.credit -= amount
                    self.db.set(user_id, msgpack.encode(user))
                except HTTPException:
                    self.logger.warning("2PC commit: user lookup failed for lock %s", payload.lock_id)
                release_resource_lock(self.db, self.SERVICE, self.RESOURCE_TYPE, user_id)
            delete_prepared_lock_payment(self.db, payload.lock_id)

        publish_envelope(
            PAYMENT_EVENTS,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "FundsCommitted2PCEvent",
                transaction_id=envelope.transaction_id,
                payload=to_builtins(FundsCommitted2PCEvent(lock_id=payload.lock_id)),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )

    def _handle_abort_prepared(self, envelope):
        payload = AbortPreparedFundsCommand(**envelope.payload)
        prepared = get_prepared_lock_payment(self.db, payload.lock_id)
        if prepared:
            user_id = prepared.get("user_id", "")
            if user_id:
                release_resource_lock(self.db, self.SERVICE, self.RESOURCE_TYPE, user_id)
            delete_prepared_lock_payment(self.db, payload.lock_id)

        publish_envelope(
            PAYMENT_EVENTS,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "FundsAborted2PCEvent",
                transaction_id=envelope.transaction_id,
                payload=to_builtins(FundsAborted2PCEvent(lock_id=payload.lock_id)),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )


def select_orchestrator(mode: str, **kwargs):
    if mode == "saga":
        return SagaOrchestrator(**kwargs)
    return TwoPL2PCOrchestrator(
        db=kwargs["db"],
        logger=kwargs["logger"],
        fetch_user_fn=kwargs["fetch_user_fn"],
    )
