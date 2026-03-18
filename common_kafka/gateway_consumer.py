"""
Gateway command consumer
========================
Subscribes to ``gateway.commands.<service>.<SHARD_INDEX>`` and dispatches
each request to the Flask app via ``test_client()`` (local WSGI call).
Replies are published to ``gateway.replies``.

This module creates its own KafkaConsumer (not the singleton in consumer.py)
so it can run alongside the existing saga/2PC consumer in the same process.
"""
from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor

from kafka import KafkaConsumer as _KafkaConsumer
from kafka.errors import CommitFailedError

from .codec import decode_envelope, EnvelopeDecodeError
from .config import SHARD_INDEX, load_kafka_settings
from .models import (
    GATEWAY_REPLIES,
    gateway_commands_topic,
    make_envelope,
)
from .producer import publish_envelope

logger = logging.getLogger(__name__)

_GW_CONSUMER_THREADS = int(os.environ.get("GW_CONSUMER_THREADS", "64"))


def _handle_gateway_request(flask_app, env):
    """Process a single gateway request in a worker thread."""
    try:
        payload = env.payload
        method = payload.get("method", "GET").upper()
        path = payload.get("path", "/")
        body = payload.get("body", "")

        with flask_app.test_client() as client:
            if method == "POST":
                resp = client.post(path, data=body, content_type="application/json")
            else:
                resp = client.get(path)

        reply_payload = {
            "status_code": resp.status_code,
            "body": resp.get_data(as_text=True),
            "content_type": resp.content_type or "application/json",
        }
        reply_env = make_envelope(
            "GatewayReply",
            transaction_id=env.transaction_id,
            payload=reply_payload,
            correlation_id=env.correlation_id,
            causation_id=env.message_id,
        )
        publish_envelope(GATEWAY_REPLIES, key=env.correlation_id, envelope=reply_env)
    except Exception:
        logger.exception("[gateway-consumer] Error handling request %s", env.message_id)


def start_gateway_consumer(flask_app, service_name: str):
    """Blocking loop. Run in a daemon thread.

    For each incoming gateway command:
    1. Decode the Envelope (expects GatewayRequestPayload in ``payload``)
    2. Dispatch to a thread pool worker
    3. Commit offset immediately (at-most-once semantics, acceptable for gateway)
    """
    settings = load_kafka_settings()
    topic = gateway_commands_topic(service_name, SHARD_INDEX)
    group_id = f"gateway-{service_name}-{SHARD_INDEX}"

    consumer = _KafkaConsumer(
        bootstrap_servers=settings.bootstrap_servers,
        client_id=f"{settings.client_id}-gw",
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_interval_ms=600000,
        session_timeout_ms=30000,
        max_poll_records=50,
    )
    consumer.subscribe([topic])
    logger.info("[gateway-consumer] Subscribed to %s (group=%s, threads=%s)", topic, group_id, _GW_CONSUMER_THREADS)

    executor = ThreadPoolExecutor(max_workers=_GW_CONSUMER_THREADS)

    try:
        while True:
            records = consumer.poll(timeout_ms=500)
            if not records:
                continue
            for tp, msgs in records.items():
                for msg in msgs:
                    try:
                        env = decode_envelope(msg.value)
                    except EnvelopeDecodeError as exc:
                        logger.warning("[gateway-consumer] Skipping bad message: %s", exc)
                        continue

                    executor.submit(_handle_gateway_request, flask_app, env)

            try:
                consumer.commit()
            except CommitFailedError:
                logger.warning("[gateway-consumer] Commit failed (group rebalanced); continuing")
    finally:
        executor.shutdown(wait=False)
        consumer.close()
