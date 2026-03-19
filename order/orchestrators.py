import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable

from flask import abort, jsonify
from msgspec import msgpack, to_builtins

from common_kafka.config import compute_shard, SHARD_INDEX
from common_kafka.producer import publish_envelope
from common_kafka.models import (
    make_envelope,
    PAYMENT_COMMANDS,
    STOCK_COMMANDS,
    STOCK_EVENTS,
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
    FundsCancelledEvent,
    StockCancelledEvent,
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
from common_kafka.saga.outbox import (
    append_outbox,
    create_saga,
    get_saga,
    set_reservation_ids,
    set_status,
    set_committed_flags,
    get_committed_flags,
    is_processed,
    mark_processed,
    set_stock_shard,
    get_stock_shard,
    set_stock_shards,
    get_stock_shards,
    add_stock_reservation,
    get_stock_reservations,
    all_stock_reserved,
    mark_stock_shard_committed,
    all_stock_shards_committed,
    set_failing_flag,
    get_failing_flag,
    mark_payment_resolved,
    is_payment_resolved,
    add_resolved_stock_shard,
    all_stock_shards_resolved,
    STATUS_TRYING,
    STATUS_RESERVED,
    STATUS_COMMITTED,
    STATUS_CANCELLED,
    STATUS_FAILED,
)
from common_kafka.twoplpc.twopl import (
    create_transaction,
    get_transaction,
    iter_transaction_ids,
    set_transaction_status,
    set_lock_ids,
    get_lock_ids,
    set_stock_shard as set_2pc_stock_shard,
    get_stock_shard as get_2pc_stock_shard,
    set_committed_flags as set_2pc_committed_flags,
    get_committed_flags as get_2pc_committed_flags,
    set_2pc_stock_shards,
    get_2pc_stock_shards,
    add_2pc_stock_lock,
    get_2pc_stock_locks,
    all_2pc_stock_locks_received,
    mark_2pc_stock_shard_committed,
    all_2pc_stock_shards_committed,
    is_tx_processed,
    mark_tx_processed,
    STATUS_PREPARING,
    STATUS_PREPARED,
    STATUS_COMMITTING,
    STATUS_ABORTING,
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

        # Group items by their stock shard so each shard gets only its own items.
        items_by_shard: dict[int, list] = {}
        for item_id, qty in items_quantities.items():
            shard = compute_shard(item_id)
            items_by_shard.setdefault(shard, []).append((item_id, qty))

        create_saga(self.db, order_id, correlation_id, deadline_ts)
        set_stock_shards(self.db, order_id, list(items_by_shard.keys()))

        # Funds reserve command
        env_funds = make_envelope(
            "ReserveFundsCommand",
            transaction_id=order_id,
            payload=to_builtins(ReserveFundsCommand(user_id=order_entry.user_id, amount=order_entry.total_cost)),
            correlation_id=correlation_id,
        )
        append_outbox(self.db, order_id, PAYMENT_COMMANDS, env_funds)

        # One ReserveStockCommand per shard
        for shard, shard_items in items_by_shard.items():
            env_stock = make_envelope(
                "ReserveStockCommand",
                transaction_id=order_id,
                payload={"items": shard_items},
                correlation_id=correlation_id,
                reply_topic=STOCK_EVENTS,
            )
            append_outbox(self.db, order_id, f"stock.commands.{shard}", env_stock)

        # Block until the saga reaches a terminal state or the deadline is exceeded.
        terminal = (STATUS_COMMITTED, STATUS_CANCELLED, STATUS_FAILED)
        deadline_at = time.monotonic() + self.checkout_deadline_seconds
        saga_status = STATUS_TRYING
        while time.monotonic() < deadline_at:
            saga = get_saga(self.db, order_id) or {}
            saga_status = saga.get("status", STATUS_TRYING)
            if saga_status in terminal:
                break
            time.sleep(0.05)

        if saga_status == STATUS_COMMITTED:
            return jsonify({"order_id": order_id, "status": saga_status}), 200
        if saga_status in (STATUS_CANCELLED, STATUS_FAILED):
            return jsonify({"order_id": order_id, "status": saga_status}), 400
        return jsonify({"order_id": order_id, "status": STATUS_TRYING}), 202

    def checkout_status(self, order_id: str):
        saga = get_saga(self.db, order_id)
        if saga is None:
            abort(404, f"Saga for order {order_id} not found")
        return jsonify({"order_id": order_id, "status": saga.get("status", STATUS_FAILED)})

    # ---------- Internal helpers ----------
    def _try_finalise_cancellation(self, order_id: str) -> None:
        """Set STATUS_CANCELLED only once payment AND all stock shards are fully resolved."""
        if not is_payment_resolved(self.db, order_id):
            return
        if not all_stock_shards_resolved(self.db, order_id):
            return
        set_status(self.db, order_id, STATUS_CANCELLED)

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
            saga = get_saga(self.db, order_id) or {}
            pay_res = saga.get("payment_reservation_id", "")
            if not (pay_res and all_stock_reserved(self.db, order_id)):
                return
            set_status(self.db, order_id, STATUS_RESERVED)
            stock_reservations = get_stock_reservations(self.db, order_id)
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
            for shard, res_id in stock_reservations.items():
                publish_envelope(
                    f"stock.commands.{shard}",
                    key=order_id,
                    envelope=make_envelope(
                        "CommitStockCommand",
                        transaction_id=order_id,
                        payload=to_builtins(CommitStockCommand(reservation_id=res_id)),
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.message_id,
                        reply_topic=STOCK_EVENTS,
                    ),
                )

        match msg_type:
            case "OrderServicePing":
                self.logger.info("Received Kafka ping %s", envelope.message_id)
            case "FundsReservedEvent":
                payload = FundsReservedEvent(**envelope.payload)
                self.logger.warning(f"Received FundsReservedEvent")
                set_reservation_ids(self.db, order_id, payment_reservation_id=payload.reservation_id)
                saga = get_saga(self.db, order_id) or {}
                if saga.get("status") == STATUS_FAILED:
                    publish_envelope(
                        PAYMENT_COMMANDS,
                        key=order_id,
                        envelope=make_envelope(
                            "CancelFundsCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CancelFundsCommand(reservation_id=payload.reservation_id)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                        ),
                    )
                else:
                    publish_commit_if_ready()
            case "StockReservedEvent":
                payload = StockReservedEvent(**envelope.payload)
                self.logger.warning(f"Received StockReservedEvent from shard {payload.shard_index}")
                add_stock_reservation(self.db, order_id, payload.shard_index, payload.reservation_id)
                if get_failing_flag(self.db, order_id):
                    # Failure already detected; cancel this stock reservation immediately
                    append_outbox(
                        self.db, order_id, f"stock.commands.{payload.shard_index}",
                        make_envelope(
                            "CancelStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CancelStockCommand(reservation_id=payload.reservation_id)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                            reply_topic=STOCK_EVENTS,
                        ),
                    )
                else:
                    publish_commit_if_ready()
            case "FundsReserveFailedEvent":
                payload = FundsReserveFailedEvent(**envelope.payload)
                self.logger.warning("Funds reservation failed for %s: %s", order_id, payload.reason)
                set_failing_flag(self.db, order_id)
                mark_payment_resolved(self.db, order_id)
                # Cancel any stock shards that already responded
                stock_reservations = get_stock_reservations(self.db, order_id)
                for shard, res_id in stock_reservations.items():
                    append_outbox(
                        self.db, order_id, f"stock.commands.{shard}",
                        make_envelope(
                            "CancelStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CancelStockCommand(reservation_id=res_id)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                            reply_topic=STOCK_EVENTS,
                        ),
                    )
                # Try to finalise — only sets CANCELLED when all stock shards
                # are also resolved (failed or cancel-confirmed)
                self._try_finalise_cancellation(order_id)
            case "StockReserveFailedEvent":
                payload = StockReserveFailedEvent(**envelope.payload)
                self.logger.warning("Stock reservation failed for %s: %s", order_id, payload.reason)
                set_failing_flag(self.db, order_id)
                # Mark this shard as resolved (it failed, no cancel needed for it)
                if payload.shard_index >= 0:
                    add_resolved_stock_shard(self.db, order_id, payload.shard_index)
                saga = get_saga(self.db, order_id) or {}
                pay_res = saga.get("payment_reservation_id", "")
                if pay_res and pay_res != "FAILED":
                    publish_envelope(
                        PAYMENT_COMMANDS,
                        key=order_id,
                        envelope=make_envelope(
                            "CancelFundsCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CancelFundsCommand(reservation_id=pay_res)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                        ),
                    )
                # Cancel any other stock shards that already responded
                stock_reservations = get_stock_reservations(self.db, order_id)
                for shard, res_id in stock_reservations.items():
                    append_outbox(
                        self.db, order_id, f"stock.commands.{shard}",
                        make_envelope(
                            "CancelStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CancelStockCommand(reservation_id=res_id)),
                            correlation_id=envelope.correlation_id,
                            causation_id=envelope.message_id,
                            reply_topic=STOCK_EVENTS,
                        ),
                    )
                # Try to finalise — only sets CANCELLED when payment and all
                # remaining stock shards are also resolved
                self._try_finalise_cancellation(order_id)
            case "FundsCommittedEvent":
                self.logger.warning(f"Received FundsCommittedEvent")
                set_committed_flags(self.db, order_id, funds_committed=True)
                funds_committed, _ = get_committed_flags(self.db, order_id)
                if funds_committed and all_stock_shards_committed(self.db, order_id):
                    set_status(self.db, order_id, STATUS_COMMITTED)
                    order_entry = self.fetch_order(order_id)
                    order_entry.paid = True
                    self.db.set(order_id, msgpack.encode(order_entry))
            case "StockCommittedEvent":
                self.logger.warning(f"Received StockCommittedEvent")
                payload = StockCommittedEvent(**envelope.payload)
                # Find which shard this reservation belongs to
                stock_reservations = get_stock_reservations(self.db, order_id)
                committed_shard = next(
                    (s for s, r in stock_reservations.items() if r == payload.reservation_id), None
                )
                if committed_shard is not None:
                    mark_stock_shard_committed(self.db, order_id, committed_shard)
                funds_committed, _ = get_committed_flags(self.db, order_id)
                if funds_committed and all_stock_shards_committed(self.db, order_id):
                    set_status(self.db, order_id, STATUS_COMMITTED)
                    order_entry = self.fetch_order(order_id)
                    order_entry.paid = True
                    self.db.set(order_id, msgpack.encode(order_entry))
            case "FundsCancelledEvent":
                self.logger.warning("Received FundsCancelledEvent")
                mark_payment_resolved(self.db, order_id)
                self._try_finalise_cancellation(order_id)
            case "StockCancelledEvent":
                self.logger.warning("Received StockCancelledEvent")
                payload = StockCancelledEvent(**envelope.payload)
                # Find which shard this cancellation belongs to and mark it resolved
                stock_reservations = get_stock_reservations(self.db, order_id)
                cancelled_shard = next(
                    (s for s, r in stock_reservations.items() if r == payload.reservation_id), None
                )
                if cancelled_shard is not None:
                    add_resolved_stock_shard(self.db, order_id, cancelled_shard)
                self._try_finalise_cancellation(order_id)
                
                
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

    def recover_inflight_transactions(self) -> int:
        """
        Startup recovery hook for crash/restart scenarios.

        Any non-terminal 2PC transaction is treated as interrupted and moved to
        ABORTING. Abort commands are re-published for currently known locks.
        If no locks were ever acquired, the transaction is marked ABORTED
        immediately.
        """
        recovered = 0
        for order_id in iter_transaction_ids(self.db):
            tx = get_transaction(self.db, order_id) or {}
            status = tx.get("status", STATUS_PREPARING)
            if status in (STATUS_2PC_COMMITTED, STATUS_ABORTED, STATUS_2PC_FAILED):
                continue
            failure_reason = f"Recovered interrupted transaction from status {status}; aborting remaining prepared locks."

            correlation_id = tx.get("correlation_id") or str(uuid.uuid4())
            pay_lock, _ = get_lock_ids(self.db, order_id)
            stock_locks = get_2pc_stock_locks(self.db, order_id)

            set_transaction_status(self.db, order_id, STATUS_ABORTING, failure_reason=failure_reason)

            if pay_lock:
                publish_envelope(
                    PAYMENT_COMMANDS,
                    key=order_id,
                    envelope=make_envelope(
                        "AbortPreparedFundsCommand",
                        transaction_id=order_id,
                        payload=to_builtins(AbortPreparedFundsCommand(lock_id=pay_lock)),
                        correlation_id=correlation_id,
                        causation_id=f"startup-recovery:{uuid.uuid4()}",
                    ),
                )
            for shard, lock_id in stock_locks.items():
                publish_envelope(
                    f"stock.commands.{shard}",
                    key=order_id,
                    envelope=make_envelope(
                        "AbortPreparedStockCommand",
                        transaction_id=order_id,
                        payload=to_builtins(AbortPreparedStockCommand(lock_id=lock_id)),
                        correlation_id=correlation_id,
                        causation_id=f"startup-recovery:{uuid.uuid4()}",
                        reply_topic=STOCK_EVENTS,
                    ),
                )

            if not pay_lock and not stock_locks:
                set_transaction_status(self.db, order_id, STATUS_ABORTED, failure_reason=failure_reason)

            recovered += 1
            self.logger.warning(
                "Recovered interrupted 2PC transaction %s (prev_status=%s, pay_lock=%s, stock_locks=%s)",
                order_id,
                status,
                bool(pay_lock),
                len(stock_locks),
            )

        return recovered

    def checkout(self, order_id: str, order_entry, items_quantities: dict[str, int]):
        correlation_id = str(uuid.uuid4())
        deadline_ts = (
            datetime.now(timezone.utc) + timedelta(seconds=self.checkout_deadline_seconds)
        ).timestamp()

        # Group items by stock shard (same pattern as saga checkout)
        items_by_shard: dict[int, list] = {}
        for item_id, qty in items_quantities.items():
            shard = compute_shard(item_id)
            items_by_shard.setdefault(shard, []).append((item_id, qty))

        # Initialize 2PC transaction state
        create_transaction(self.db, order_id, correlation_id, deadline_ts)
        set_2pc_stock_shards(self.db, order_id, list(items_by_shard.keys()))

        # Payment prepare (single shard, co-located)
        publish_envelope(
            PAYMENT_COMMANDS,
            key=order_id,
            envelope=make_envelope(
                "PrepareFundsCommand",
                transaction_id=order_id,
                payload=to_builtins(PrepareFundsCommand(user_id=order_entry.user_id, amount=order_entry.total_cost)),
                correlation_id=correlation_id,
            ),
        )

        # Per-shard stock prepare
        for shard, shard_items in items_by_shard.items():
            publish_envelope(
                f"stock.commands.{shard}",
                key=order_id,
                envelope=make_envelope(
                    "PrepareStockCommand",
                    transaction_id=order_id,
                    payload={"items": shard_items},
                    correlation_id=correlation_id,
                    reply_topic=STOCK_EVENTS,
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
        return jsonify(
            {
                "order_id": order_id,
                "status": tx.get("status", STATUS_2PC_FAILED),
                "failure_reason": tx.get("failure_reason", ""),
            }
        )

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
            pay_lock, _ = get_lock_ids(self.db, order_id)
            if not (pay_lock and all_2pc_stock_locks_received(self.db, order_id)):
                return
            self.logger.warning("2PC prepared on all participants for %s; sending commit commands", order_id)
            set_transaction_status(self.db, order_id, STATUS_PREPARED, clear_failure_reason=True)
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
            stock_locks = get_2pc_stock_locks(self.db, order_id)
            for shard, lock_id in stock_locks.items():
                publish_envelope(
                    f"stock.commands.{shard}",
                    key=order_id,
                    envelope=make_envelope(
                        "CommitPreparedStockCommand",
                        transaction_id=order_id,
                        payload=to_builtins(CommitPreparedStockCommand(lock_id=lock_id)),
                        correlation_id=envelope.correlation_id,
                        causation_id=envelope.message_id,
                        reply_topic=STOCK_EVENTS,
                    ),
                )

        def publish_abort_for_current_locks(causation_id: str):
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

        match msg_type:
            case "FundsPreparedEvent":
                payload = FundsPreparedEvent(**envelope.payload)
                self.logger.warning("2PC funds prepared for %s (lock=%s)", order_id, payload.lock_id)
                set_lock_ids(self.db, order_id, payment_lock_id=payload.lock_id)
                tx = get_transaction(self.db, order_id) or {}
                if tx.get("status") in (STATUS_2PC_FAILED, STATUS_ABORTING, STATUS_ABORTED):
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

def select_orchestrator(mode: str, **kwargs):
    if mode == "saga":
        return SagaOrchestrator(**kwargs)
    return TwoPL2PCOrchestrator(
        db=kwargs["db"],
        logger=kwargs["logger"],
        fetch_order_fn=kwargs["fetch_order_fn"],
        checkout_deadline_seconds=kwargs["checkout_deadline_seconds"],
    )
