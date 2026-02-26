from __future__ import annotations
from datetime import datetime, timezone
import uuid

from msgspec import Struct

# Topic and consumer group names
PAYMENT_COMMANDS = "payment.commands"
STOCK_COMMANDS = "stock.commands"
PAYMENT_EVENTS = "payment.events"
STOCK_EVENTS = "stock.events"
ORDER_EVENTS = "order.events"  # optional high-level saga outcomes

ORDER_CONSUMER_GROUP = "order-orchestrator"


class Envelope(Struct):
    """
    Standard message envelope to support versioning, tracing, and idempotency.
    """
    type: str  # e.g., "ReserveFundsCommand"
    version: int = 1
    message_id: str = ""
    saga_id: str = ""          # order_id
    correlation_id: str = ""   # per checkout attempt
    causation_id: str | None = None
    timestamp: str = ""        # ISO 8601 in UTC
    payload: dict = {}


def make_envelope(
    msg_type: str,
    saga_id: str,
    payload: dict,
    *,
    correlation_id: str | None = None,
    causation_id: str | None = None,
    version: int = 1,
) -> Envelope:
    """Build an envelope with generated IDs and timestamp."""
    now = datetime.now(timezone.utc).isoformat()
    return Envelope(
        type=msg_type,
        version=version,
        message_id=str(uuid.uuid4()),
        saga_id=saga_id,
        correlation_id=correlation_id or str(uuid.uuid4()),
        causation_id=causation_id,
        timestamp=now,
        payload=payload,
    )


# Command payloads -----------------------------------------------------------
class ReserveFundsCommand(Struct):
    user_id: str
    amount: int


class CommitFundsCommand(Struct):
    reservation_id: str


class CancelFundsCommand(Struct):
    reservation_id: str


class ReserveStockCommand(Struct):
    items: list[tuple[str, int]]


class CommitStockCommand(Struct):
    reservation_id: str


class CancelStockCommand(Struct):
    reservation_id: str


# Event payloads -------------------------------------------------------------
class FundsReservedEvent(Struct):
    reservation_id: str
    amount: int


class FundsReserveFailedEvent(Struct):
    reason: str


class FundsCommittedEvent(Struct):
    reservation_id: str


class FundsCancelledEvent(Struct):
    reservation_id: str


class StockReservedEvent(Struct):
    reservation_id: str


class StockReserveFailedEvent(Struct):
    reason: str


class StockCommittedEvent(Struct):
    reservation_id: str


class StockCancelledEvent(Struct):
    reservation_id: str


class CheckoutSucceededEvent(Struct):
    order_id: str


class CheckoutFailedEvent(Struct):
    order_id: str
    reason: str
