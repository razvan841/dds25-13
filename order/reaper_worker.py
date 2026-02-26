import time
import threading
import uuid

from kafka_consumer import start_consumer
from kafka_codec import decode_envelope
from kafka_producer import publish_envelope
from kafka_models import (
    PAYMENT_COMMANDS,
    STOCK_COMMANDS,
    CancelFundsCommand,
    CancelStockCommand,
    make_envelope,
)

from app import (
    app,
    db,
    _handle_event,
)
from saga_store import (
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


def _start_consumer_thread():
    t = threading.Thread(target=start_consumer, args=(_handle_event,), daemon=True)
    t.start()
    app.logger.info("Kafka consumer thread started (reaper worker)")


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
                        saga_id=order_id,
                        payload=CancelFundsCommand(reservation_id=pay_res).__dict__,
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
                        saga_id=order_id,
                        payload=CancelStockCommand(reservation_id=stock_res).__dict__,
                        correlation_id=saga.get("correlation_id", str(uuid.uuid4())),
                    ),
                )
            set_status(db, order_id, STATUS_FAILED)
            app.logger.warning("Saga %s exceeded deadline; compensation triggered", order_id)

        time.sleep(1.0)


def main():
    _start_consumer_thread()
    threading.Thread(target=_outbox_publisher_loop, daemon=True).start()
    threading.Thread(target=_saga_reaper_loop, daemon=True).start()
    app.logger.info("Background saga workers started (reaper worker)")
    # Keep the process alive; threads are daemonized.
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
