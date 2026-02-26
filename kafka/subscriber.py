from __future__ import annotations

import atexit
import logging
import signal
from typing import Callable, Optional

from kafka import KafkaConsumer

from kafka.codec import decode_envelope, EnvelopeDecodeError
from config import KafkaSettings, load_kafka_settings
from models import PAYMENT_EVENTS, STOCK_EVENTS, Envelope

logger = logging.getLogger(__name__)

MessageHandler = Callable[[Envelope], None]

_consumer: Optional[KafkaConsumer] = None
_settings: Optional[KafkaSettings] = None
_running = False


def _build_consumer(settings: KafkaSettings) -> KafkaConsumer:
    """
    Create a KafkaConsumer configured for manual offset commits.
    """
    topics = [PAYMENT_EVENTS, STOCK_EVENTS]
    config: dict = {
        "bootstrap_servers": settings.bootstrap_servers,
        "client_id": settings.client_id,
        "group_id": settings.group_id,
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,
        # We deliver raw bytes and decode ourselves.
    }

    # Optional security settings (left empty for PLAINTEXT)
    if settings.security_protocol:
        config["security_protocol"] = settings.security_protocol
    if settings.sasl_mechanism:
        config["sasl_mechanism"] = settings.sasl_mechanism
    if settings.sasl_username:
        config["sasl_plain_username"] = settings.sasl_username
    if settings.sasl_password:
        config["sasl_plain_password"] = settings.sasl_password

    consumer = KafkaConsumer(**config)
    consumer.subscribe(topics)
    logger.info("Kafka consumer subscribed to %s", topics)
    return consumer


def _get_consumer() -> KafkaConsumer:
    global _consumer, _settings
    if _consumer is None:
        _settings = load_kafka_settings()
        _consumer = _build_consumer(_settings)
    return _consumer


def _stop_running(*_args):
    global _running
    _running = False


def start_consumer(handler: MessageHandler):
    """
    Poll Kafka for payment/stock events and pass decoded envelopes to handler.
    Offsets are committed after handler completes without exception.
    """
    consumer = _get_consumer()
    global _running
    _running = True

    # Handle SIGINT/SIGTERM for graceful exit.
    signal.signal(signal.SIGINT, _stop_running)
    signal.signal(signal.SIGTERM, _stop_running)

    try:
        while _running:
            records = consumer.poll(timeout_ms=500)
            if not records:
                continue
            for tp, msgs in records.items():
                for msg in msgs:
                    try:
                        env = decode_envelope(msg.value)
                    except EnvelopeDecodeError as exc:
                        logger.warning("Skipping undecodable message on %s: %s", tp, exc)
                        consumer.commit()  # commit to avoid poison reprocessing
                        continue

                    try:
                        handler(env)
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("Handler failed for %s: %s", env.type, exc)
                        # Do not commit; allow retry
                        continue

                    consumer.commit()
    finally:
        close_consumer()


def close_consumer():
    global _consumer
    if _consumer is None:
        return
    try:
        _consumer.close()
        logger.info("Kafka consumer closed")
    finally:
        _consumer = None


atexit.register(close_consumer)
