#!/usr/bin/env python3
"""Webhook authentication and idempotency keys. Hermetic (stdlib only)."""
from __future__ import annotations

import base64
import hashlib
import hmac
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import _wa_webhook as W  # noqa: E402

SECRET = "0123456789abcdef0123456789abcdef"  # exactly the 32-char minimum


def sign(body: bytes, secret: str = SECRET, *, encoding: str = "hex") -> str:
    digest = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    return digest.hex() if encoding == "hex" else base64.b64encode(digest).decode()


class TestSignature(unittest.TestCase):
    body = b'{"event":{"Info":{"ID":"ABC"}}}'

    def test_accepts_hex(self):
        self.assertTrue(W.verify_signature(SECRET, self.body, sign(self.body)))

    def test_accepts_base64(self):
        sig = sign(self.body, encoding="base64")
        self.assertTrue(W.verify_signature(SECRET, self.body, sig))

    def test_rejects_wrong_body(self):
        self.assertFalse(W.verify_signature(SECRET, self.body + b" ", sign(self.body)))

    def test_rejects_wrong_key(self):
        self.assertFalse(W.verify_signature("f" * 32, self.body, sign(self.body)))

    def test_rejects_missing_and_empty(self):
        for bad in (None, "", "   "):
            self.assertFalse(W.verify_signature(SECRET, self.body, bad))

    def test_rejects_garbage_signature(self):
        self.assertFalse(W.verify_signature(SECRET, self.body, "!!!not-hex!!!"))
        self.assertFalse(W.verify_signature(SECRET, self.body, "a" * 63))

    def test_require_signature_raises_on_mismatch(self):
        with self.assertRaises(W.WebhookAuthError):
            W.require_signature(SECRET, self.body, "0" * 64)

    def test_require_signature_refuses_a_short_key(self):
        # wuzapi requires >= 32 chars; a shorter one is a misconfiguration that
        # must fail loudly rather than silently authenticate weaker traffic.
        with self.assertRaises(W.WebhookAuthError):
            W.require_signature("short", self.body, sign(self.body, "short"))

    def test_require_signature_refuses_when_unconfigured(self):
        with self.assertRaises(W.WebhookAuthError):
            W.require_signature("", self.body, sign(self.body))

    def test_require_signature_passes_the_happy_path(self):
        W.require_signature(SECRET, self.body, sign(self.body))  # must not raise


class TestEventType(unittest.TestCase):
    def test_explicit_type(self):
        self.assertEqual(W.event_type({"type": "Message"}), "Message")

    def test_capitalised_type(self):
        self.assertEqual(W.event_type({"Type": "HistorySync"}), "HistorySync")

    def test_nested_type(self):
        self.assertEqual(W.event_type({"event": {"type": "ReadReceipt"}}), "ReadReceipt")

    def test_inferred_from_envelope_shape(self):
        self.assertEqual(
            W.event_type({"event": {"Info": {"ID": "1"}, "Message": {}}}), "Message")

    def test_inferred_history_sync(self):
        self.assertEqual(W.event_type({"event": {"Conversations": []}}), "HistorySync")

    def test_unknown(self):
        self.assertEqual(W.event_type({}), "unknown")
        self.assertEqual(W.event_type({"foo": 1}), "unknown")


class TestDedupeKey(unittest.TestCase):
    def payload(self, msg_id="ABC", chat="5491155554444@s.whatsapp.net"):
        return {"type": "Message", "event": {"Info": {"ID": msg_id, "Chat": chat}}}

    def test_message_key_is_stable(self):
        self.assertEqual(W.dedupe_key(self.payload()), W.dedupe_key(self.payload()))

    def test_different_message_ids_differ(self):
        self.assertNotEqual(W.dedupe_key(self.payload("A")), W.dedupe_key(self.payload("B")))

    def test_same_id_different_chat_differs(self):
        # WhatsApp message ids are only unique WITHIN a chat, so chat is part of the key.
        a = W.dedupe_key(self.payload("A", "1@s.whatsapp.net"))
        b = W.dedupe_key(self.payload("A", "2@s.whatsapp.net"))
        self.assertNotEqual(a, b)

    def test_content_hash_fallback_is_stable(self):
        payload = {"type": "HistorySync", "event": {"Conversations": [{"ID": "x"}]}}
        self.assertEqual(W.dedupe_key(payload), W.dedupe_key(payload))

    def test_content_hash_ignores_media_blob(self):
        # Two retries of the same history payload may re-encode the blob; the key
        # must not change because of it, or every retry is a new "delivery".
        base = {"type": "HistorySync", "event": {"Conversations": [{"ID": "x"}]}}
        a = W.dedupe_key({**base, "base64": "AAAA"})
        b = W.dedupe_key({**base, "base64": "BBBB"})
        self.assertEqual(a, b)

    def test_key_namespaces_by_event_type(self):
        self.assertNotEqual(
            W.dedupe_key({"type": "Message", "event": {"Info": {"ID": "A"}}}),
            W.dedupe_key({"type": "HistorySync", "event": {"Info": {"ID": "A"}}}),
        )


if __name__ == "__main__":
    unittest.main()