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
    CancelFundsCommand,
    CancelStockCommand,
    CommitPreparedFundsCommand,
    CommitPreparedStockCommand,
    AbortPreparedFundsCommand,
    AbortPreparedStockCommand,
    make_envelope,
)

from order.app import (
    app,
    db,
    handle_event,
    ORCHESTRATION_MODE,
)
from common_kafka.outbox import (
    pop_any_outbox,
    iter_saga_ids,
    get_saga,
    get_reservation_ids,
    is_deadline_exceeded,
    append_outbox,
    set_status,
    STATUS_TRYING,
    STATUS_COMMITTED,
    STATUS_CANCELLED,
    STATUS_FAILED,
)
from common_kafka.twopl import (
    iter_transaction_ids,
    get_transaction,
    is_tx_deadline_exceeded,
    set_transaction_status,
    get_lock_ids,
    STATUS_PREPARING,
    STATUS_PREPARED,
    STATUS_COMMITTING,
    STATUS_COMMITTED as STATUS_2PC_COMMITTED,
    STATUS_ABORTED,
    STATUS_FAILED as STATUS_2PC_FAILED,
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
            if pay_res:
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
            if stock_res:
                append_outbox(
                    db,
                    order_id,
                    STOCK_COMMANDS,
                    make_envelope(
                        "CancelStockCommand",
                        transaction_id=order_id,
                        payload=to_builtins(CancelStockCommand(reservation_id=stock_res)),
                        correlation_id=saga.get("correlation_id", str(uuid.uuid4())),
                    ),
                )
            set_status(db, order_id, STATUS_FAILED)
            app.logger.warning("[order-worker] Saga %s exceeded deadline; compensation triggered", order_id)
            print(f"[order-worker] Deadline exceeded for {order_id}, compensation enqueued")

        time.sleep(1.0)


def _2pc_reaper_loop():
    """
    Periodically scans 2PC transactions and recovers stuck ones.

    - PREPARING + deadline exceeded  → set FAILED, send aborts once
    - PREPARED / COMMITTING + deadline exceeded → re-send commit commands once
      (Kafka durably holds the message; the participant processes it on recovery.
       The single re-send covers the crash-between-state-update-and-publish case.)
    """
    while True:
        for order_id in iter_transaction_ids(db):
            tx = get_transaction(db, order_id)
            if not tx:
                continue
            status = tx.get("status", "")
            if status in (STATUS_2PC_COMMITTED, STATUS_ABORTED, STATUS_2PC_FAILED, STATUS_COMMITTING):
                continue
            if not is_tx_deadline_exceeded(db, order_id):
                continue

            correlation_id = tx.get("correlation_id", str(uuid.uuid4()))
            pay_lock = tx.get("payment_lock_id", "") or None
            stock_lock = tx.get("stock_lock_id", "") or None

            if status == STATUS_PREPARING:
                # Neither or only one participant responded — abort whatever was prepared.
                set_transaction_status(db, order_id, STATUS_2PC_FAILED)
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
                        STOCK_COMMANDS,
                        key=order_id,
                        envelope=make_envelope(
                            "AbortPreparedStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(AbortPreparedStockCommand(lock_id=stock_lock)),
                            correlation_id=correlation_id,
                        ),
                    )
                app.logger.warning(
                    "[order-worker] 2PC tx %s stuck in PREPARING; aborted (pay_lock=%s, stock_lock=%s)",
                    order_id, pay_lock, stock_lock,
                )

            elif status == STATUS_PREPARED:
                # Commit was decided but commands may have been lost on crash.
                # Re-send once, then mark COMMITTING so we don't repeat.
                # Kafka durably holds the message; the participant processes
                # it whenever it comes back online. No need to keep retrying.
                if pay_lock:
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
                if stock_lock:
                    publish_envelope(
                        STOCK_COMMANDS,
                        key=order_id,
                        envelope=make_envelope(
                            "CommitPreparedStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CommitPreparedStockCommand(lock_id=stock_lock)),
                            correlation_id=correlation_id,
                        ),
                    )
                # Transition PREPARED → COMMITTING so the reaper won't
                # re-send on the next scan.  If already COMMITTING this
                # is a no-op (idempotent).
                set_transaction_status(db, order_id, STATUS_COMMITTING)
                app.logger.warning(
                    "[order-worker] 2PC tx %s stuck in %s; re-sent commit commands once",
                    order_id, status,
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
        app.logger.info("[order-worker] Event consumer and 2PC reaper started (mode=%s)", ORCHESTRATION_MODE)
        print("[order-worker] Event consumer and 2PC reaper started")
    else:
        app.logger.info("[order-worker] No background workers started for mode=%s", ORCHESTRATION_MODE)
        print(f"[order-worker] No background workers started for mode={ORCHESTRATION_MODE}")
    # Keep the process alive; threads are daemonized.
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
