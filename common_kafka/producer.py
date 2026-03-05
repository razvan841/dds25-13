from __future__ import annotations

import atexit
import logging
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .codec import encode_envelope
from .config import KafkaSettings, load_kafka_settings
from .models import Envelope

logger = logging.getLogger(__name__)

_producer: Optional[KafkaProducer] = None
_settings: Optional[KafkaSettings] = None


def _build_producer(settings: KafkaSettings) -> KafkaProducer:
    """Create a KafkaProducer with sensible defaults for reliability."""
    config: dict = {
        "bootstrap_servers": settings.bootstrap_servers,
        "client_id": settings.client_id,
        "acks": "all",
        "retries": 5,
        "linger_ms": 10,
        # We pass pre-encoded bytes; no value_serializer required.
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

    producer = KafkaProducer(**config)
    logger.info("Kafka producer created for %s", settings.bootstrap_servers)
    return producer


def _get_producer() -> KafkaProducer:
    """Lazily initialize a singleton producer using env-based settings."""
    global _producer, _settings
    if _producer is None:
        _settings = load_kafka_settings()
        _producer = _build_producer(_settings)
    return _producer


def publish_envelope(topic: str, key: str, envelope: Envelope) -> None:
    """Publish an Envelope to a Kafka topic using saga_id as the partition key."""
    producer = _get_producer()
    payload = encode_envelope(envelope)
    future = producer.send(topic, key=key.encode("utf-8"), value=payload)

    def _on_send_success(record_metadata):
        logger.debug(
            "Kafka sent %s partition=%s offset=%s", topic, record_metadata.partition, record_metadata.offset
        )

    def _on_send_error(excp: KafkaError):
        logger.error("Kafka send failed for topic %s: %s", topic, excp)

    future.add_callback(_on_send_success)
    future.add_errback(_on_send_error)


def close_producer():
    """Flush and close the global producer (called at process exit)."""
    global _producer
    if _producer is None:
        return
    try:
        _producer.flush()
        _producer.close()
        logger.info("Kafka producer closed")
    finally:
        _producer = None


atexit.register(close_producer)
