from __future__ import annotations

import msgspec

from .models import Envelope

# Use JSON to keep payloads human-readable and compatible with other clients.
# msgspec keeps it fast while maintaining typed decoding.
_encoder = msgspec.json.Encoder()
_decoder = msgspec.json.Decoder(type=Envelope)


class EnvelopeDecodeError(ValueError):
    """Raised when an incoming message cannot be decoded into an Envelope."""


def encode_envelope(envelope: Envelope) -> bytes:
    """Serialize an Envelope to bytes for Kafka payloads."""
    return _encoder.encode(envelope)


def decode_envelope(raw: bytes) -> Envelope:
    """Deserialize bytes into an Envelope, validating required fields."""
    try:
        env: Envelope = _decoder.decode(raw)
    except Exception as exc:  # msgspec raises MsgspecError subclasses
        raise EnvelopeDecodeError(f"Failed to decode envelope: {exc}") from exc

    if not env.type:
        raise EnvelopeDecodeError("Envelope.type is required")
    if not env.transaction_id:
        raise EnvelopeDecodeError("Envelope.transaction_id is required")
    if not env.message_id:
        raise EnvelopeDecodeError("Envelope.message_id is required")
    if not env.correlation_id:
        raise EnvelopeDecodeError("Envelope.correlation_id is required")
    return env
