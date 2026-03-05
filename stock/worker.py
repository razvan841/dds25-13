"""
Stock-service Kafka worker
===========================
Background process that drives the consumer side of the stock saga.

Responsibilities
----------------
1. Subscribe to the ``stock.commands`` Kafka topic.
2. For each received message, decode the :class:`~common_kafka.models.Envelope`
   and delegate to :func:`stock.app.handle_command`.
3. Restart the consumer automatically with exponential back-off if the Kafka
   cluster is unreachable or the consumer thread crashes.

Usage
-----
Run this module as a separate process alongside the Gunicorn web server::

    python -m stock.worker

The ``docker-compose.yml`` and Kubernetes manifests start both processes
inside the same container via a shell command:

    python -m stock.worker &
    exec gunicorn ...

Environment variables
---------------------
``KAFKA_BOOTSTRAP``
    Comma-separated list of Kafka broker addresses, e.g.
    ``kafka-1:9092,kafka-2:9092,kafka-3:9092``.
``KAFKA_CLIENT_ID``
    Client identifier sent to Kafka (default: ``stock-service``).
``KAFKA_GROUP_ID``
    Consumer group identifier (default: ``stock-service``).
``LOG_LEVEL``
    Python log level string: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``
    (default: ``INFO``).
"""
import logging
import os
import sys
import threading
import time

from kafka.errors import NoBrokersAvailable

from common_kafka.consumer import start_consumer
from common_kafka.models import STOCK_COMMANDS
from stock.app import ORCHESTRATION_MODE, app, handle_command

# ---------------------------------------------------------------------------
# Logging
# Configured here (force=True) so that this dedicated worker process has
# consistent formatting even when imported via gunicorn in a combined start
# command.
# ---------------------------------------------------------------------------

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [stock-worker] %(message)s",
    stream=sys.stdout,
    force=True,
)


# ---------------------------------------------------------------------------
# Consumer loop
# ---------------------------------------------------------------------------

def _consumer_loop_with_retry() -> None:
    """
    Run :func:`~common_kafka.consumer.start_consumer` in an infinite loop.

    On ``NoBrokersAvailable`` (Kafka not yet ready) or any other exception
    the loop sleeps for *backoff* seconds before retrying.  The backoff
    doubles on each consecutive failure, capped at 30 seconds, and resets
    to 1 second after a successful session.
    """
    backoff = 1
    while True:
        try:
            start_consumer(handle_command, topics=[STOCK_COMMANDS])
        except NoBrokersAvailable:
            app.logger.warning(
                "[stock-worker] Kafka brokers unavailable, retrying in %s s",
                backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        except Exception as exc:  # noqa: BLE001
            app.logger.exception(
                "[stock-worker] Kafka consumer crashed: %s; retrying in %s s",
                exc,
                backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
        else:
            # Consumer exited cleanly (e.g. SIGTERM); reset back-off
            backoff = 1


def _start_consumer_thread() -> None:
    """Spawn the consumer loop in a daemon thread and log startup."""
    t = threading.Thread(target=_consumer_loop_with_retry, daemon=True)
    t.start()
    app.logger.info("[stock-worker] Kafka consumer thread started")
    print("[stock-worker] Kafka consumer thread started", flush=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Start the stock worker process.

    In ``saga`` mode a background consumer thread is started.
    In all other modes the worker exits with a log message so that the
    process supervisor (compose, K8s) does not restart it unnecessarily.
    The main thread then blocks indefinitely (sleeping 1 h at a time) to
    keep the daemon consumer thread alive.
    """
    print(
        f"[stock-worker] Starting worker process (mode={ORCHESTRATION_MODE})",
        flush=True,
    )

    if ORCHESTRATION_MODE == "saga":
        _start_consumer_thread()
        app.logger.info(
            "[stock-worker] Background consumer started (mode=%s)",
            ORCHESTRATION_MODE,
        )
        print(
            f"[stock-worker] Background consumer started (mode={ORCHESTRATION_MODE})",
            flush=True,
        )
    else:
        app.logger.info(
            "[stock-worker] No consumer started for mode=%s",
            ORCHESTRATION_MODE,
        )
        print(
            f"[stock-worker] No consumer started for mode={ORCHESTRATION_MODE}",
            flush=True,
        )

    # Keep the main thread alive so the daemon consumer thread keeps running.
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
