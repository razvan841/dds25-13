from __future__ import annotations
import uuid
from msgspec import msgpack, to_builtins
import redis
from werkzeug.exceptions import HTTPException, abort
from common_kafka.models import (
    make_envelope,
    PAYMENT_EVENTS,
    PrepareFundsCommand,
    CommitPreparedFundsCommand,
    AbortPreparedFundsCommand,
    FundsPreparedEvent,
    FundsPrepareFailedEvent,
    FundsCommitted2PCEvent,
    FundsAborted2PCEvent,
)
from common_kafka.producer import publish_envelope
from common_kafka.twoplpc.twopl import acquire_and_prepare_payment, delete_tx_lock_payment, get_prepared_lock_payment
from payment.app import UserValue

class TwoPL2PCOrchestrator:
    """
    Payment participant implementing strict 2PL + 2PC semantics.
    Uses centralized twopl.py for database operations.
    """

    SERVICE = "payment"
    RESOURCE_TYPE = "user"
    LOCK_UNAVAILABLE_PREFIX = "LOCK_UNAVAILABLE:"
    INSUFFICIENT_FUNDS_PREFIX = "INSUFFICIENT_FUNDS:"
    USER_LOOKUP_FAILED_PREFIX = "USER_LOOKUP_FAILED:"

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger


    def handle_command(self, envelope):
        transaction_id = envelope.transaction_id
        if self.is_message_processed(transaction_id, envelope.message_id):
            return

        match envelope.type:
            case "PrepareFundsCommand":
                self.logger.warning("2PC payment received PrepareFundsCommand tx=%s", transaction_id)
                self.handle_prepare(envelope)
            case "CommitPreparedFundsCommand":
                self.logger.warning("2PC payment received CommitPreparedFundsCommand tx=%s", transaction_id)
                self.handle_commit_prepared(envelope)
            case "AbortPreparedFundsCommand":
                self.logger.warning("2PC payment received AbortPreparedFundsCommand tx=%s", transaction_id)
                self.handle_abort_prepared(envelope)
            case _:
                self.logger.debug("Unhandled 2PC payment command %s", envelope.type)

        self.mark_message_processed(transaction_id, envelope.message_id)

    def handle_prepare(self, envelope):
        payload = PrepareFundsCommand(**envelope.payload)
        reply_topic = envelope.reply_topic or PAYMENT_EVENTS

        # Step 1 — validate BEFORE acquiring any lock
        try:
            user = self.fetch_user(payload.user_id)
            if user.credit < payload.amount:
                raise ValueError(
                    f"{self.INSUFFICIENT_FUNDS_PREFIX}user_id={payload.user_id}; requested={payload.amount}; available={user.credit}"
                )
        except HTTPException as exc:
            lookup_reason = getattr(exc, "description", "User lookup failed")
            self.publish_prepare_failed(envelope, f"{self.USER_LOOKUP_FAILED_PREFIX}{lookup_reason}")
            return
        except ValueError as exc:
            self.publish_prepare_failed(envelope, str(exc))
            return

        # Step 2 — atomically acquire lock + write prepared record
        # The Lua script is idempotent: if this is a replay after crash,
        # it returns the existing lock_id instead of creating a new one.
        generated_lock_id = str(uuid.uuid4())
        success, actual_lock_id = acquire_and_prepare_payment(
            self.db,
            transaction_id=envelope.transaction_id,
            lock_id=generated_lock_id,
            user_id=payload.user_id,
            amount=payload.amount,
        )
        if not success:
            # actual_lock_id contains the blocking resource on failure
            self.publish_prepare_failed(
                envelope,
                f"{self.LOCK_UNAVAILABLE_PREFIX}user_id={payload.user_id}; User funds lock held by another transaction",
            )
            return

        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "FundsPreparedEvent",
                transaction_id=envelope.transaction_id,
                order_id=envelope.order_id,
                payload=to_builtins(FundsPreparedEvent(lock_id=actual_lock_id, amount=payload.amount))
            ),
        )

    def publish_prepare_failed(self, envelope, reason: str) -> None:
        reply_topic = envelope.reply_topic or PAYMENT_EVENTS
        self.logger.warning(
            "2PC payment prepare failed for tx=%s: %s", envelope.transaction_id, reason
        )
        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "FundsPrepareFailedEvent",
                transaction_id=envelope.transaction_id,
                order_id=envelope.order_id,
                payload=to_builtins(FundsPrepareFailedEvent(reason=reason))
            ),
        )

    def handle_commit_prepared(self, envelope):
        payload = CommitPreparedFundsCommand(**envelope.payload)
        reply_topic = envelope.reply_topic or PAYMENT_EVENTS
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
                except HTTPException:
                    self.logger.warning("2PC commit: user lookup failed for lock %s", payload.lock_id)
                    return
                user.credit -= amount
                updated_user = msgpack.encode(user)
                # Apply business update and lock cleanup atomically.
                pipe = self.db.pipeline()
                pipe.set(user_id, updated_user)
                pipe.delete(f"{self.SERVICE}:2pc:{self.RESOURCE_TYPE}lock:{user_id}")
            else:
                pipe = self.db.pipeline()
            pipe.delete(f"{self.SERVICE}:2pc:lock:{payload.lock_id}")
            pipe.delete(f"{self.SERVICE}:2pc:tx:{envelope.transaction_id}")
            pipe.execute()
        else:
            # A replay after a successful commit may legitimately arrive after cleanup.
            delete_tx_lock_payment(self.db, envelope.transaction_id)
        self.logger.warning("2PC payment committed tx=%s lock=%s", envelope.transaction_id, payload.lock_id)

        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "FundsCommitted2PCEvent",
                transaction_id=envelope.transaction_id,
                order_id=envelope.order_id,
                payload=to_builtins(FundsCommitted2PCEvent(lock_id=payload.lock_id))
            ),
        )

    def handle_abort_prepared(self, envelope):
        payload = AbortPreparedFundsCommand(**envelope.payload)
        reply_topic = envelope.reply_topic or PAYMENT_EVENTS
        prepared = get_prepared_lock_payment(self.db, payload.lock_id)
        if prepared:
            user_id = prepared.get("user_id", "")
            pipe = self.db.pipeline()
            if user_id:
                pipe.delete(f"{self.SERVICE}:2pc:{self.RESOURCE_TYPE}lock:{user_id}")
            pipe.delete(f"{self.SERVICE}:2pc:lock:{payload.lock_id}")
            pipe.delete(f"{self.SERVICE}:2pc:tx:{envelope.transaction_id}")
            pipe.execute()
        else:
            delete_tx_lock_payment(self.db, envelope.transaction_id)
        self.logger.warning("2PC payment aborted tx=%s lock=%s", envelope.transaction_id, payload.lock_id)

        publish_envelope(
            reply_topic,
            key=envelope.transaction_id,
            envelope=make_envelope(
                "FundsAborted2PCEvent",
                transaction_id=envelope.transaction_id,
                order_id=envelope.order_id,
                payload=to_builtins(FundsAborted2PCEvent(lock_id=payload.lock_id))
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
        
    def fetch_user(self, user_id: str) -> UserValue | None:
        try:
            # get serialized data
            entry: bytes = self.db.get(user_id)
        except redis.exceptions.RedisError:
            return abort(400, "Database error")
        # deserialize data if it exists else return null
        entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
        if entry is None:
            # if user does not exist in the database; abort
            abort(400, f"User: {user_id} not found!")
        return entry