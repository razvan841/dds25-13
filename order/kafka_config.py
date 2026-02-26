from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: list[str]
    client_id: str
    group_id: str
    security_protocol: Optional[str] = None
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None


def load_kafka_settings() -> KafkaSettings:
    """
    Load Kafka client settings from environment variables.

    Required:
      - KAFKA_BOOTSTRAP: comma-separated host:port list

    Optional:
      - KAFKA_CLIENT_ID (default: order-service)
      - KAFKA_GROUP_ID (default: order-orchestrator)
      - KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_MECHANISM,
        KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD
    """
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP")
    if not bootstrap:
        raise RuntimeError("KAFKA_BOOTSTRAP is required (comma-separated host:port list).")

    servers = [s.strip() for s in bootstrap.split(",") if s.strip()]
    if not servers:
        raise RuntimeError("KAFKA_BOOTSTRAP must contain at least one host:port entry.")

    return KafkaSettings(
        bootstrap_servers=servers,
        client_id=os.environ.get("KAFKA_CLIENT_ID", "order-service"),
        group_id=os.environ.get("KAFKA_GROUP_ID", "order-orchestrator"),
        security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL"),
        sasl_mechanism=os.environ.get("KAFKA_SASL_MECHANISM"),
        sasl_username=os.environ.get("KAFKA_SASL_USERNAME"),
        sasl_password=os.environ.get("KAFKA_SASL_PASSWORD"),
    )
