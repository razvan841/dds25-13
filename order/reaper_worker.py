import time
import threading
import uuid
import sys

import logging
from kafka.errors import NoBrokersAvailable
from msgspec import to_builtins

from common_kafka.consumer import start_consumer
from common_kafka.codec import decode_envelope
from common_kafka.gateway_consumer import start_gateway_consumer
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
    get_stock_reservations,
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
    get_2pc_stock_locks,
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
            time.sleep(0.05)
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
            if status in (STATUS_COMMITTED, STATUS_CANCELLED, STATUS_FAILED):
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
            for shard, res_id in get_stock_reservations(db, order_id).items():
                stock_cancel_topic = f"stock.commands.{shard}"
                append_outbox(
                    db,
                    order_id,
                    stock_cancel_topic,
                    make_envelope(
                        "CancelStockCommand",
                        transaction_id=order_id,
                        payload=to_builtins(CancelStockCommand(reservation_id=res_id)),
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
            pay_lock, _ = get_lock_ids(db, order_id)
            stored_shard = get_2pc_stock_shard(db, order_id)
            stock_locks = get_2pc_stock_locks(db, order_id)

            if status == STATUS_PREPARING:
                # Neither or only one participant responded — abort whatever was prepared.
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
                for shard, lock_id in stock_locks.items():
                    publish_envelope(
                        f"stock.commands.{shard}",
                        key=order_id,
                        envelope=make_envelope(
                            "AbortPreparedStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(AbortPreparedStockCommand(lock_id=lock_id)),
                            correlation_id=correlation_id,
                            reply_topic=STOCK_EVENTS,
                        ),
                    )
                app.logger.warning(
                    "[order-worker] 2PC tx %s stuck in PREPARING; aborted (pay_lock=%s, stock_locks=%s)",
                    order_id, pay_lock, len(stock_locks),
                )
                set_transaction_status(db, order_id, STATUS_2PC_FAILED)
            elif status == STATUS_PREPARED:
                # Commit was decided but commands may have been lost on crash.
                # Re-send once, then mark COMMITTING so we don't repeat.
                 recovery_ts = tx.get("recovery_sent_at", "")
                if recovery_ts:
                    try:
                        if time.time() - float(recovery_ts) < 10.0:
                            continue
                    except (ValueError, TypeError):
                        pass
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
                for shard, lock_id in stock_locks.items():
                    publish_envelope(
                        f"stock.commands.{shard}",
                        key=order_id,
                        envelope=make_envelope(
                            "CommitPreparedStockCommand",
                            transaction_id=order_id,
                            payload=to_builtins(CommitPreparedStockCommand(lock_id=lock_id)),
                            correlation_id=correlation_id,
                            reply_topic=STOCK_EVENTS,
                        ),
                    )
                db.hset(f"2pc:{order_id}", "recovery_sent_at", str(time.time()))
                set_transaction_status(db, order_id, STATUS_COMMITTING)
                app.logger.warning(
                    "[order-worker] 2PC tx %s stuck in %s; re-sent commit commands once",
                    order_id, status,
                )

        time.sleep(1.0)


def _gateway_consumer_loop():
    backoff = 1
    while True:
        try:
            start_gateway_consumer(app, "order")
        except NoBrokersAvailable:
            app.logger.warning("[order-gw] Kafka brokers unavailable, retrying in %s s", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as exc:  # noqa: BLE001
            app.logger.exception("[order-gw] Gateway consumer crashed: %s; retrying in %s s", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        else:
            backoff = 1


def _start_gateway_consumer_thread():
    t = threading.Thread(target=_gateway_consumer_loop, daemon=True)
    t.start()
    app.logger.info("[order-worker] Gateway consumer thread started")
    print("[order-worker] Gateway consumer thread started")


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

    _start_gateway_consumer_thread()

    # Keep the process alive; threads are daemonized.
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
