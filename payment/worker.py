import logging
import time
import threading
import sys
import os
from kafka.errors import NoBrokersAvailable

from common_kafka.consumer import start_consumer
from common_kafka.models import PAYMENT_COMMANDS

from payment.app import app, handle_command, ORCHESTRATION_MODE

# Ensure logging to stdout; honor LOG_LEVEL env if provided
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [payment-worker] %(message)s",
    stream=sys.stdout,
    force=True,
)


def _consumer_loop_with_retry():
    backoff = 1
    while True:
        try:
            start_consumer(handle_command, topics=[PAYMENT_COMMANDS])
        except NoBrokersAvailable:
            app.logger.warning("[payment-worker] Kafka brokers unavailable, retrying in %s s", backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as exc:  # noqa: BLE001
            app.logger.exception("[payment-worker] Kafka consumer crashed: %s; retrying in %s s", exc, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        else:
            backoff = 1


def _start_consumer_thread():
    t = threading.Thread(target=_consumer_loop_with_retry, daemon=True)
    t.start()
    app.logger.info("[payment-worker] Kafka consumer thread started")
    print("[payment-worker] Kafka consumer thread started")


def main():
    print(f"[payment-worker] Starting worker process (mode={ORCHESTRATION_MODE})")
    if ORCHESTRATION_MODE == "saga":
        _start_consumer_thread()
        app.logger.info("[payment-worker] Background consumer started (mode=%s)", ORCHESTRATION_MODE)
        print(f"[payment-worker] Background consumer started (mode={ORCHESTRATION_MODE})")
    else:
        app.logger.info("[payment-worker] No consumer started for mode=%s", ORCHESTRATION_MODE)
        print(f"[payment-worker] No consumer started for mode={ORCHESTRATION_MODE}")
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
