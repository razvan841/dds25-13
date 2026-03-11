from __future__ import annotations

import os
import uuid as _uuid
from dataclasses import dataclass
from typing import Optional

# ---------------------------------------------------------------------------
# Sharding helpers
# ---------------------------------------------------------------------------

SHARD_INDEX: int = int(os.environ.get("SHARD_INDEX", "0"))
NUM_SHARDS: int = int(os.environ.get("NUM_SHARDS", "1"))


def shard_topic(base: str) -> str:
    """Return the shard-specific Kafka topic name, e.g. 'payment.commands.0'."""
    return f"{base}.{SHARD_INDEX}"


def compute_shard(entity_id: str) -> int:
    """
    Deterministically compute which shard owns a UUID entity ID.
    Uses the first 64 bits of the UUID integer representation modulo NUM_SHARDS.
    Matches the identical computation in the OpenResty gateway Lua code.
    """
    hex_clean = entity_id.replace("-", "")
    first_64_bits = int(hex_clean[:16], 16)
    return first_64_bits % NUM_SHARDS


def generate_shard_uuid() -> str:
    """
    Generate a UUID that deterministically belongs to this pod's shard
    (i.e. compute_shard(result) == SHARD_INDEX).  Averages ~NUM_SHARDS
    attempts (microseconds) before finding a matching UUID.
    """
    while True:
        candidate = str(_uuid.uuid4())
        if compute_shard(candidate) == SHARD_INDEX:
            return candidate


# ---------------------------------------------------------------------------
# Kafka settings
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class KafkaSettings:
    # Aggregates client config for both producer and consumer.
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
