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


def _processed_key(saga_id: str) -> str:
    return f"payment:{saga_id}:processed"


def _reservation_key(res_id: str) -> str:
    return f"payment:reservation:{res_id}"


def _prepared_lock_key(lock_id: str) -> str:
    return f"payment:2pc:lock:{lock_id}"


def _user_lock_key(user_id: str) -> str:
    return f"payment:2pc:userlock:{user_id}"


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
                self.logger.warning(f"Received ReserveFundsCommand")
                self._handle_reserve(envelope)
            case "CommitFundsCommand":
                self.logger.warning(f"Received CommitFundsCommand")
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
                key=envelope.saga_id,
                envelope=make_envelope(
                    "FundsReserveFailedEvent",
                    saga_id=envelope.saga_id,
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
            key=envelope.saga_id,
            envelope=make_envelope(
                "FundsReservedEvent",
                saga_id=envelope.saga_id,
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
            key=envelope.saga_id,
            envelope=make_envelope(
                "FundsCommittedEvent",
                saga_id=envelope.saga_id,
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
            key=envelope.saga_id,
            envelope=make_envelope(
                "FundsCancelledEvent",
                saga_id=envelope.saga_id,
                payload=to_builtins(FundsCancelledEvent(reservation_id=payload.reservation_id)),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )


class TwoPL2PCOrchestrator:
    """
    Payment participant implementing strict 2PL + 2PC semantics.
    """

    def __init__(self, db, logger, fetch_user_fn: Callable[[str], object]):
        self.db = db
        self.logger = logger
        self.fetch_user = fetch_user_fn

    def _is_processed(self, saga_id: str, message_id: str) -> bool:
        return bool(self.db.sismember(_processed_key(saga_id), message_id))

    def _mark_processed(self, saga_id: str, message_id: str) -> None:
        self.db.sadd(_processed_key(saga_id), message_id)

    def _store_prepared_lock(self, lock_id: str, saga_id: str, user_id: str, amount: int) -> None:
        self.db.hset(
            _prepared_lock_key(lock_id),
            mapping={"saga_id": saga_id, "user_id": user_id, "amount": str(amount), "status": "prepared"},
        )

    def _get_prepared_lock(self, lock_id: str) -> dict | None:
        data = self.db.hgetall(_prepared_lock_key(lock_id))
        if not data:
            return None
        return {k.decode(): v.decode() for k, v in data.items()}

    def _delete_prepared_lock(self, lock_id: str) -> None:
        self.db.delete(_prepared_lock_key(lock_id))

    def handle_command(self, envelope):
        saga_id = envelope.saga_id
        if self._is_processed(saga_id, envelope.message_id):
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

        self._mark_processed(saga_id, envelope.message_id)

    def _handle_prepare(self, envelope):
        payload = PrepareFundsCommand(**envelope.payload)
        lock_key = _user_lock_key(payload.user_id)

        lock_acquired = self.db.set(lock_key, envelope.saga_id, nx=True, ex=120)
        if not lock_acquired:
            owner = self.db.get(lock_key)
            owner_saga = owner.decode() if owner else "unknown"
            publish_envelope(
                PAYMENT_EVENTS,
                key=envelope.saga_id,
                envelope=make_envelope(
                    "FundsPrepareFailedEvent",
                    saga_id=envelope.saga_id,
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

            user.credit -= payload.amount
            self.db.set(payload.user_id, msgpack.encode(user))

            lock_id = str(uuid.uuid4())
            self._store_prepared_lock(lock_id, envelope.saga_id, payload.user_id, payload.amount)

            publish_envelope(
                PAYMENT_EVENTS,
                key=envelope.saga_id,
                envelope=make_envelope(
                    "FundsPreparedEvent",
                    saga_id=envelope.saga_id,
                    payload=to_builtins(FundsPreparedEvent(lock_id=lock_id, amount=payload.amount)),
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.message_id,
                ),
            )
        except HTTPException as exc:
            reason = getattr(exc, "description", "User lookup failed")
            self.db.delete(lock_key)
            publish_envelope(
                PAYMENT_EVENTS,
                key=envelope.saga_id,
                envelope=make_envelope(
                    "FundsPrepareFailedEvent",
                    saga_id=envelope.saga_id,
                    payload=to_builtins(FundsPrepareFailedEvent(reason=reason)),
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.message_id,
                ),
            )
        except ValueError as exc:
            self.db.delete(lock_key)
            publish_envelope(
                PAYMENT_EVENTS,
                key=envelope.saga_id,
                envelope=make_envelope(
                    "FundsPrepareFailedEvent",
                    saga_id=envelope.saga_id,
                    payload=to_builtins(FundsPrepareFailedEvent(reason=str(exc))),
                    correlation_id=envelope.correlation_id,
                    causation_id=envelope.message_id,
                ),
            )

    def _handle_commit_prepared(self, envelope):
        payload = CommitPreparedFundsCommand(**envelope.payload)
        prepared = self._get_prepared_lock(payload.lock_id)
        if prepared:
            user_id = prepared.get("user_id", "")
            self._delete_prepared_lock(payload.lock_id)
            if user_id:
                self.db.delete(_user_lock_key(user_id))

        publish_envelope(
            PAYMENT_EVENTS,
            key=envelope.saga_id,
            envelope=make_envelope(
                "FundsCommitted2PCEvent",
                saga_id=envelope.saga_id,
                payload=to_builtins(FundsCommitted2PCEvent(lock_id=payload.lock_id)),
                correlation_id=envelope.correlation_id,
                causation_id=envelope.message_id,
            ),
        )

    def _handle_abort_prepared(self, envelope):
        payload = AbortPreparedFundsCommand(**envelope.payload)
        prepared = self._get_prepared_lock(payload.lock_id)
        if prepared:
            user_id = prepared.get("user_id", "")
            try:
                amount = int(prepared.get("amount", "0"))
            except ValueError:
                amount = 0
            if user_id:
                try:
                    user = self.fetch_user(user_id)
                    user.credit += amount
                    self.db.set(user_id, msgpack.encode(user))
                except HTTPException:
                    self.logger.warning("2PC abort: user lookup failed for lock %s", payload.lock_id)
                self.db.delete(_user_lock_key(user_id))
            self._delete_prepared_lock(payload.lock_id)

        publish_envelope(
            PAYMENT_EVENTS,
            key=envelope.saga_id,
            envelope=make_envelope(
                "FundsAborted2PCEvent",
                saga_id=envelope.saga_id,
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
