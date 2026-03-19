from __future__ import annotations
from datetime import datetime, timezone
import uuid

from msgspec import Struct

from .config import SHARD_INDEX, shard_topic

# Shard-specific topic names — each pod uses its own shard's topics.
# The base names are suffixed with the shard index (e.g. "payment.commands.0").
PAYMENT_COMMANDS = shard_topic("payment.commands")
STOCK_COMMANDS = shard_topic("stock.commands")
PAYMENT_EVENTS = shard_topic("payment.events")
STOCK_EVENTS = shard_topic("stock.events")
ORDER_EVENTS = shard_topic("order.events")  # optional high-level saga outcomes

# Consumer group is also shard-specific so groups across shards stay independent.
ORDER_CONSUMER_GROUP = f"order-orchestrator-{SHARD_INDEX}"


class Envelope(Struct):
    """Standard message envelope to support versioning, tracing, and idempotency."""

    type: str  # e.g., "ReserveFundsCommand"
    version: int = 1
    message_id: str = ""
    transaction_id: str = ""  # order_id
    correlation_id: str = ""  # per checkout attempt
    causation_id: str | None = None
    timestamp: str = ""  # ISO 8601 in UTC
    payload: dict = {}
    reply_topic: str = ""  # if set, participant replies here instead of its own events topic


def make_envelope(
    msg_type: str,
    transaction_id: str,
    payload: dict,
    *,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    version: int = 1,
    reply_topic: str = "",
) -> Envelope:
    """Build an envelope with generated IDs and timestamp."""
    now = datetime.now(timezone.utc).isoformat()
    return Envelope(
        type=msg_type,
        version=version,
        message_id=str(uuid.uuid4()),
        transaction_id=transaction_id,
        correlation_id=correlation_id or str(uuid.uuid4()),
        causation_id=causation_id,
        timestamp=now,
        payload=payload,
        reply_topic=reply_topic,
    )


# Command payloads SAGA -----------------------------------------------------------

class ReserveStockCommand(Struct):
    items: list[tuple[str, int]]


class ReserveFundsCommand(Struct):
    user_id: str
    amount: int

class CancelFundsCommand(Struct):
    reservation_id: str

class CommitFundsCommand(Struct):
    reservation_id: str

class CommitStockCommand(Struct):
    reservation_id: str

class CancelStockCommand(Struct):
    reservation_id: str



# 2PC
class PrepareFundsCommand(Struct):
    user_id: str
    amount: int

class CommitPreparedFundsCommand(Struct):
    lock_id: str


class AbortPreparedFundsCommand(Struct):
    lock_id: str


class PrepareStockCommand(Struct):
    items: list[tuple[str, int]]


class CommitPreparedStockCommand(Struct):
    lock_id: str

class AbortPreparedStockCommand(Struct):
    lock_id: str


# Event payloads -------------------------------------------------------------
class FundsReservedEvent(Struct):
    reservation_id: str
    amount: int


class FundsPreparedEvent(Struct):
    lock_id: str
    amount: int


class FundsReserveFailedEvent(Struct):
    reason: str


class FundsPrepareFailedEvent(Struct):
    reason: str


class FundsCommittedEvent(Struct):
    reservation_id: str


class FundsCommitted2PCEvent(Struct):
    lock_id: str


class FundsCancelledEvent(Struct):
    reservation_id: str


class FundsAborted2PCEvent(Struct):
    lock_id: str


class StockReservedEvent(Struct):
    reservation_id: str
    shard_index: int = 0


class StockPreparedEvent(Struct):
    lock_id: str
    shard_index: int = 0


class StockReserveFailedEvent(Struct):
    reason: str
    shard_index: int = -1


class StockPrepareFailedEvent(Struct):
    reason: str
    shard_index: int = -1


class StockCommittedEvent(Struct):
    reservation_id: str


class StockCommitted2PCEvent(Struct):
    lock_id: str
    shard_index: int = 0


class StockCancelledEvent(Struct):
    reservation_id: str


class StockAborted2PCEvent(Struct):
    lock_id: str
    shard_index: int = 0


class CheckoutSucceededEvent(Struct):
    order_id: str


class CheckoutFailedEvent(Struct):
    order_id: str
    reason: str


# Gateway request-reply payloads -----------------------------------------------

class GatewayRequestPayload(Struct):
    method: str          # "GET" or "POST"
    path: str            # e.g. "/find/abc-123"
    body: str = ""       # request body (unused for now)


class GatewayReplyPayload(Struct):
    status_code: int     # HTTP status code
    body: str            # JSON response body
    content_type: str = "application/json"


GATEWAY_REPLIES = "gateway.replies"


def gateway_commands_topic(service: str, shard: int) -> str:
    return f"gateway.commands.{service}.{shard}"
