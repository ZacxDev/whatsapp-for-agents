#!/usr/bin/env python3
"""Webhook authentication and idempotency keys — pure, no I/O.

Two jobs, both of which must be testable without a server:

1. **Authenticate** the POST. wuzapi signs the body with
   `HMAC-SHA256(global_hmac_key, raw_body)` and sends it in `x-hmac-signature`.
   Upstream's own README says "Always verify signatures before processing
   webhooks"; the verification happens BEFORE json parsing, on the raw bytes,
   because re-serialising a parsed dict changes key order and whitespace and
   would invalidate every signature.

2. **Derive a dedupe key.** wuzapi's delivery is at-least-once (webhook retries,
   plus a reconnect can replay). A key built from the event type plus the message
   id makes a retry a no-op. When there is no message id (HistorySync, receipts)
   we hash the body instead — weaker, but it still collapses an identical retry.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json

SIGNATURE_HEADER = "x-hmac-signature"
MIN_KEY_LENGTH = 32


class WebhookAuthError(Exception):
    """Raised when a delivery cannot be authenticated. Never swallowed."""


def verify_signature(secret: str | bytes, body: bytes, signature: str | None) -> bool:
    """Constant-time verification. Accepts hex or base64, since upstream does not
    document the encoding and a hard-coded assumption would silently reject
    every real delivery (or, worse, accept none and look 'secure')."""
    if not secret or not signature or not isinstance(body, (bytes, bytearray)):
        return False
    key = secret.encode("utf-8") if isinstance(secret, str) else secret
    expected = hmac.new(key, bytes(body), hashlib.sha256).digest()

    candidate = signature.strip()
    try:
        if len(candidate) == 64:
            return hmac.compare_digest(expected.hex(), candidate.lower())
        raw = base64.b64decode(candidate, validate=True)
        return hmac.compare_digest(expected, raw)
    except (ValueError, TypeError):
        return False


def event_type(payload: dict) -> str:
    """`{"event": {"Info": .., "Message": ..}, "type": "Message"}` and friends.

    Keeps every spelling upstream has used rather than assuming one:
    `type`, `Type`, `event.type`, or inferred from the envelope shape.
    """
    if not isinstance(payload, dict):
        return "unknown"
    for key in ("type", "Type", "eventType", "EventType"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    event = payload.get("event")
    if isinstance(event, dict):
        for key in ("type", "Type"):
            value = event.get(key)
            if isinstance(value, str) and value:
                return value
        if isinstance(event.get("Info"), dict) and isinstance(event.get("Message"), dict):
            return "Message"
        if "Conversations" in event or "conversations" in event:
            return "HistorySync"
    return "unknown"


def _info(payload: dict) -> dict:
    event = payload.get("event")
    if isinstance(event, dict):
        info = event.get("Info")
        if isinstance(info, dict):
            return info
        if isinstance(event.get("info"), dict):
            return event["info"]
    return {}


def dedupe_key(payload: dict, body: bytes | None = None) -> str:
    """Stable identity for one delivery.

    Message-shaped deliveries key on (chat, id) — the pair WhatsApp itself treats
    as the message identity. Everything else falls back to a content hash.
    """
    kind = event_type(payload)
    info = _info(payload)
    msg_id = info.get("ID") or info.get("id")
    chat = info.get("Chat") or info.get("chat") or ""

    if msg_id:
        return f"{kind}:{chat}:{msg_id}"

    # HistorySync / receipts: hash the body minus any embedded media blob, which
    # may legitimately differ between retries after re-encoding.
    material = json.dumps(
        {k: v for k, v in payload.items() if k != "base64"},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return f"{kind}:sha256:{hashlib.sha256(material).hexdigest()}"


def require_signature(secret: str | bytes, body: bytes, signature: str | None) -> None:
    if not secret:
        raise WebhookAuthError("no HMAC key configured; refusing every delivery")
    if len(secret if isinstance(secret, bytes) else secret.encode()) < MIN_KEY_LENGTH:
        raise WebhookAuthError(
            f"HMAC key shorter than {MIN_KEY_LENGTH} bytes; wuzapi requires at least that"
        )
    if not verify_signature(secret, body, signature):
        raise WebhookAuthError("signature mismatch")