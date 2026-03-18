import time
import threading
import uuid
import sys

import logging
from kafka.errors import NoBrokersAvailable
from msgspec import to_builtins

from common_kafka.consumer import start_consumer
from common_kafka.codec import decode_envelope
from common_kafka.producer import publish_envelope
from common_kafka.models import (
    PAYMENT_COMMANDS,
    STOCK_COMMANDS,
    STOCK_EVENTS,
    CancelFundsCommand,
    CancelStockCommand,
    AbortPreparedFundsCommand,
    AbortPreparedStockCommand,
    CommitPreparedFundsCommand,
    CommitPreparedStockCommand,
    make_envelope,
)

from order.app import (
    app,
    db,
    handle_event,
    ORCHESTRATION_MODE,
)
from common_kafka.twoplpc.twopl import (
    iter_transaction_ids,
    get_transaction,
    get_lock_ids,
    get_stock_shard as get_2pc_stock_shard,
    set_transaction_status,
    is_tx_deadline_exceeded,
    STATUS_PREPARING,
    STATUS_PREPARED,
    STATUS_COMMITTED as STATUS_2PC_COMMITTED,
    STATUS_ABORTED,
    STATUS_FAILED as STATUS_2PC_FAILED,
)
from common_kafka.saga.outbox import (
    pop_any_outbox,
    iter_saga_ids,
    get_saga,
    get_reservation_ids,
    get_stock_shard,
    is_deadline_exceeded,
    append_outbox,
    set_status,
    STATUS_TRYING,
    STATUS_COMMITTED,
    STATUS_CANCELLED,
    STATUS_FAILED,
)

# Ensure logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [order-worker] %(message)s",
    stream=sys.stdout,
    force=True,
)


def _consumer_loop_with_retry():
    backoff = 1
    while True:
        try:
            start_consumer(handle_event)
        except NoBrokersAvailable:
            app.logger.warning("[order-worker] Kafka brokers unavailable, retrying in %s s", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as exc:  # noqa: BLE001
            app.logger.exception("[order-worker] Kafka consumer crashed: %s; retrying in %s s", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        else:
            backoff = 1


def _start_consumer_thread():
    t = threading.Thread(target=_consumer_loop_with_retry, daemon=True)
    t.start()
    app.logger.info("[order-worker] Kafka consumer thread started")
    print("[order-worker] Kafka consumer thread started")


def _outbox_publisher_loop():
    """
    Simple outbox drainer: pops envelopes from any saga outbox and publishes.
    """
    while True:
        item = pop_any_outbox(db)
        if not item:
            time.sleep(0.5)
            continue
        order_id, topic, payload = item
        try:
            env = decode_envelope(payload)
            publish_envelope(topic, key=order_id, envelope=env)
            app.logger.warning(f"The envelope: {env} was published!")
            
        except Exception as exc:  # noqa: BLE001
            app.logger.exception("Failed to publish outbox envelope for %s: %s", order_id, exc)
            # push back to avoid loss
            db.lpush(f"saga:{order_id}:outbox", payload)
            time.sleep(0.5)


def _saga_reaper_loop():
    """
    Periodically scans sagas and triggers compensation when deadlines expire.
    """
    while True:
        for order_id in iter_saga_ids(db):
            saga = get_saga(db, order_id)
            if not saga:
                continue
            status = saga.get("status", STATUS_TRYING)
            if status in (STATUS_COMMITTED, STATUS_CANCELLED):
                continue
            if not is_deadline_exceeded(db, order_id):
                continue

            pay_res, stock_res = get_reservation_ids(db, order_id)
            if pay_res and pay_res != "FAILED":
                append_outbox(
                    db,
                    order_id,
                    PAYMENT_COMMANDS,
                    make_envelope(
                        "CancelFundsCommand",
                        transaction_id=order_id,
                        payload=to_builtins(CancelFundsCommand(reservation_id=pay_res)),
                        correlation_id=saga.get("correlation_id", str(uuid.uuid4())),
                    ),
                )
            if stock_res and stock_res != "FAILED":
                stored_shard = get_stock_shard(db, order_id)
                stock_cancel_topic = f"stock.commands.{stored_shard}" if stored_shard >= 0 else STOCK_COMMANDS
                append_outbox(
                    db,
                    order_id,
                    stock_cancel_topic,
                    make_envelope(
                        "CancelStockCommand",
                        transaction_id=order_id,
                        payload=to_builtins(CancelStockCommand(reservation_id=stock_res)),
                        correlation_id=saga.get("correlation_id", str(uuid.uuid4())),
                        reply_topic=STOCK_EVENTS,
                    ),
                )
            set_status(db, order_id, STATUS_FAILED)
            app.logger.warning("[order-worker] Saga %s exceeded deadline; compensation triggered", order_id)
            print(f"[order-worker] Deadline exceeded for {order_id}, compensation enqueued")

        time.sleep(1.0)


def _2pc_reaper_loop():
    """
    Periodically scans 2PC transactions and recovers them when deadlines expire.

    PREPARING + deadline exceeded:
      - Abort any locks already acquired (pay_lock / stock_lock may be partial).
      - Mark transaction FAILED.

    PREPARED + deadline exceeded:
      - Coordinator already decided to commit before crashing; resume by
        re-sending commit commands.  Do NOT abort — that would violate the
        2PC decision.
    """
    while True:
        for order_id in iter_transaction_ids(db):
            tx = get_transaction(db, order_id)
            if not tx:
                continue

            status = tx.get("status")
            if status in (STATUS_2PC_COMMITTED, STATUS_ABORTED, STATUS_2PC_FAILED):
                continue

            if not is_tx_deadline_exceeded(db, order_id):
                continue

            correlation_id = tx.get("correlation_id", str(uuid.uuid4()))
            pay_lock, stock_lock = get_lock_ids(db, order_id)
            stored_shard = get_2pc_stock_shard(db, order_id)
            stock_topic = f"stock.commands.{stored_shard}" if stored_shard >= 0 else STOCK_COMMANDS

            if status == STATUS_PREPARING:
                # Abort any locks that were acquired before the crash.
                # If a lock_id is empty the participant never prepared, so no
                # abort command is needed for that participant.
                if pay_lock:
                    publish_envelope(
                        PAYMENT_COMMANDS,
                        key=order_id,
                        envelope=make_envelope(
                            "AbortPreparedFundsCommand",
                            transaction_id=order_id,
                            payload=to_builtins(AbortPreparedFundsCommand(lock_id=pay_lock)),
                            correlation_id=correlation_id,
                        ),
                    )
                if stock_lock:
                    publish_envelope(
                        stock_topic,
                        key=order_id,
                        envelope=make_envelope(
                            "AbortPreparedStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(AbortPreparedStockCommand(lock_id=stock_lock)),
                            correlation_id=correlation_id,
                            reply_topic=STOCK_EVENTS,
                        ),
                    )
                set_transaction_status(db, order_id, STATUS_2PC_FAILED)
                app.logger.warning(
                    "[order-worker] 2PC %s exceeded deadline in PREPARING; aborted (pay_lock=%s stock_lock=%s)",
                    order_id, pay_lock or "none", stock_lock or "none",
                )

            elif status == STATUS_PREPARED:
                # Both participants locked and ready; coordinator decided commit.
                # Re-send commit commands so participants can finalise.
                # Guard: skip if we already re-sent recently (within 10s).
                recovery_ts = tx.get("recovery_sent_at", "")
                if recovery_ts:
                    try:
                        if time.time() - float(recovery_ts) < 10.0:
                            continue
                    except (ValueError, TypeError):
                        pass

                if pay_lock and stock_lock:
                    publish_envelope(
                        PAYMENT_COMMANDS,
                        key=order_id,
                        envelope=make_envelope(
                            "CommitPreparedFundsCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CommitPreparedFundsCommand(lock_id=pay_lock)),
                            correlation_id=correlation_id,
                        ),
                    )
                    publish_envelope(
                        stock_topic,
                        key=order_id,
                        envelope=make_envelope(
                            "CommitPreparedStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CommitPreparedStockCommand(lock_id=stock_lock)),
                            correlation_id=correlation_id,
                            reply_topic=STOCK_EVENTS,
                        ),
                    )
                    db.hset(f"2pc:{order_id}", "recovery_sent_at", str(time.time()))
                    app.logger.warning(
                        "[order-worker] 2PC %s exceeded deadline in PREPARED; commit commands re-sent",
                        order_id,
                    )

        time.sleep(1.0)


def main():
    print(f"[order-worker] Starting worker process (mode={ORCHESTRATION_MODE})")
    if ORCHESTRATION_MODE in {"saga", "2pl2pc"}:
        _start_consumer_thread()
    if ORCHESTRATION_MODE == "saga":
        threading.Thread(target=_outbox_publisher_loop, daemon=True).start()
        threading.Thread(target=_saga_reaper_loop, daemon=True).start()
        app.logger.info("[order-worker] Background saga workers started (mode=%s)", ORCHESTRATION_MODE)
        print(f"[order-worker] Background saga workers started (mode={ORCHESTRATION_MODE})")
    elif ORCHESTRATION_MODE == "2pl2pc":
        threading.Thread(target=_2pc_reaper_loop, daemon=True).start()
        app.logger.info("[order-worker] Background 2PC reaper started (mode=%s)", ORCHESTRATION_MODE)
        print(f"[order-worker] Background 2PC reaper started (mode={ORCHESTRATION_MODE})")
    else:
        app.logger.info("[order-worker] No background workers started for mode=%s", ORCHESTRATION_MODE)
        print(f"[order-worker] No background workers started for mode={ORCHESTRATION_MODE}")
    # Keep the process alive; threads are daemonized.
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
