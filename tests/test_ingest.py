#!/usr/bin/env python3
"""Ingest behaviour: replay, media policy, control messages, history safety.

Driven through a fake in-memory DB and a synchronous executor, so the whole file
runs without Postgres, MinIO or a phone. The fake records what it was asked to do
— which is the point: several of these assertions are about what must NOT happen
(no upload for a non-allowlisted group; no second row on a replay).

The DB's SQL itself (notably `_merge_contacts`, whose correctness is a sequence of
DELETEs/UPDATEs against real constraints) is NOT covered here; that needs a live
Postgres and lives in `test_db_identity.py`, which is skipped unless a DSN is given.
"""
from __future__ import annotations

import base64
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import psycopg2  # noqa: F401  (consumer imports _wa_db, which imports it)
except ImportError:  # pragma: no cover
    raise unittest.SkipTest("psycopg2 not installed; the ingest suite needs it")

import consumer as C  # noqa: E402

GROUP = "120363123456789012@g.us"
DM = "5491155554444@s.whatsapp.net"
OTHER_DM = "5491155555555@s.whatsapp.net"


class Inline:
    """Synchronous stand-in for ThreadPoolExecutor."""

    def submit(self, fn, *a, **kw):
        fn(*a, **kw)
        return None


class FakeMinio:
    """Records uploads; never touches a network."""

    def __init__(self, recorder):
        self.recorder = recorder
        self.entered = False

    def __call__(self):
        self.entered = True
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def put_attachment(self, *, chat, timestamp_ms, filename, data,
                       attachment_id, content_type=None, sidecar=None):
        self.recorder["uploads"].append({
            "chat": chat, "filename": filename, "size": len(data),
            "attachment_id": attachment_id, "content_type": content_type,
        })
        return f"{chat}/2026-11-09/{attachment_id}_{filename}"


class FakeDB:
    """Only the surface `Ingest` uses, plus recording of the calls that matter."""

    def __init__(self, allowlisted=()):
        self.deliveries: dict[str, int] = {}
        self._next = 1
        self.messages: list[dict] = []
        self.attachments: list[dict] = []
        self.reactions: list[dict] = []
        self.revokes: list[tuple] = []
        self.history: list[dict] = []
        self.chats: dict[str, int] = {}
        self.contacts: dict[tuple, int] = {}
        self.allowlisted = set(allowlisted)
        self.finished: list[dict] = []
        self.chat_labels: dict = {}
        self.identity_calls: list = []
        self.annotations: list = []
        self.known_addresses: set = set()

    # deliveries
    def record_delivery(self, kind, key, payload=None):
        if key in self.deliveries:
            return None
        self.deliveries[key] = self._next
        self._next += 1
        return self.deliveries[key]

    def finish_delivery(self, delivery_id, *, error=None):
        self.finished.append({"id": delivery_id, "error": error})

    # chats / contacts
    def upsert_chat(self, jid, chat_type, display_name=None, username=None):
        self.chat_labels[jid] = (display_name, username)
        return self.chats.setdefault(jid, len(self.chats) + 1)

    def touch_chat(self, chat_id, ts):
        return None

    def find_or_create_contact(self, pn, lid, push_name=None, username=None):
        if not pn and not lid:
            return None
        key = (pn, lid)
        self.identity_calls.append((pn, lid))
        if key not in self.contacts:
            self.contacts[key] = len(self.contacts) + 1
        return self.contacts[key]

    def annotate_contact(self, address, *, push_name=None, username=None):
        # Mirrors the real contract: label what exists, never create.
        self.annotations.append((address, push_name, username))
        return 1 if address in self.known_addresses else 0

    def upsert_group(self, jid, subject=None):
        return 1

    # messages
    def insert_message(self, **kw):
        for row in self.messages:
            if row["chat_id"] == kw["chat_id"] and row["whatsapp_id"] == kw["whatsapp_id"]:
                row.update(kw)
                return row["id"], False
        row = dict(kw, id=len(self.messages) + 1)
        self.messages.append(row)
        return row["id"], True

    def revoke_message(self, chat_id, target_id):
        self.revokes.append((chat_id, target_id))
        return 1

    def insert_attachment(self, **kw):
        self.attachments.append(dict(kw, minio_key=kw.get("minio_key")))
        return len(self.attachments)

    def finish_attachment(self, *, message_id, media_type, minio_key, error,
                          sha256, file_size):
        for row in self.attachments:
            if row["message_id"] == message_id and row["media_type"] == media_type:
                row.update({"minio_key": minio_key, "download_error": error,
                            "sha256": sha256, "file_size": file_size})

    def upsert_reaction(self, **kw):
        self.reactions.append(kw)

    def store_history_sync(self, payload, *, conversation_cnt, message_cnt,
                           parsed_messages, error):
        self.history.append({"payload": payload, "message_cnt": message_cnt,
                             "parsed": parsed_messages, "error": error})
        return len(self.history)

    def is_allowlisted(self, jid):
        return jid in self.allowlisted

    def update_health(self, instance, counters):
        return None


def message_payload(*, msg_id="M1", chat=DM, body="hello", sender=None,
                    alt=None, kind="conversation", is_from_me=False,
                    timestamp="2026-11-09T10:00:00Z", extra=None,
                    base64_blob=None, mime="image/jpeg", filename="a.jpg"):
    info = {"ID": msg_id, "Chat": chat, "Timestamp": timestamp,
            "IsFromMe": is_from_me, "PushName": "Someone"}
    if sender:
        info["Sender"] = sender
    if alt:
        info["SenderAlt"] = alt
    payload = {"type": "Message",
               "event": {"Info": info, "Message": {kind: {"caption": body} if kind != "conversation" else None}}}
    if kind == "conversation":
        payload["event"]["Message"] = {"conversation": body}
    if base64_blob is not None:
        payload["base64"] = base64_blob
        payload["mimeType"] = mime
        payload["fileName"] = filename
    if extra:
        payload.update(extra)
    return payload


class Base(unittest.TestCase):
    def setUp(self):
        self.db = FakeDB()
        self.rec = {"uploads": []}
        self.minio = FakeMinio(self.rec)
        self.ingest = C.Ingest(self.db, minio_factory=self.minio, executor=Inline())

    def ingest_one(self, payload):
        return self.ingest.handle(payload)


class TestReplay(Base):
    def test_message_stored_once(self):
        result = self.ingest_one(message_payload())
        self.assertTrue(result["stored"])
        self.assertEqual(len(self.db.messages), 1)

    def test_identical_replay_is_a_noop_and_still_succeeds(self):
        self.ingest_one(message_payload())
        second = self.ingest_one(message_payload())
        self.assertTrue(second["duplicate"])
        self.assertEqual(len(self.db.messages), 1)
        self.assertEqual(self.ingest.counters.snapshot()["duplicate"], 1)

    def test_redelivery_of_different_messages_both_store(self):
        self.ingest_one(message_payload(msg_id="A"))
        self.ingest_one(message_payload(msg_id="B"))
        self.assertEqual(len(self.db.messages), 2)

    def test_base64_blob_is_not_persisted_in_raw(self):
        self.ingest_one(message_payload(msg_id="M9", base64_blob="QUJD"))
        self.assertNotIn("base64", self.db.messages[0]["raw"])
        self.assertNotIn("base64", self.db.messages[0]["raw"]["event"])


class TestMalformed(Base):
    def test_missing_chat_is_counted_not_stored(self):
        payload = message_payload()
        del payload["event"]["Info"]["Chat"]
        result = self.ingest_one(payload)
        self.assertTrue(result["malformed"])
        self.assertEqual(len(self.db.messages), 0)
        self.assertEqual(self.ingest.counters.snapshot()["malformed"], 1)

    def test_missing_id_is_counted_not_stored(self):
        payload = message_payload()
        del payload["event"]["Info"]["ID"]
        result = self.ingest_one(payload)
        self.assertTrue(result["malformed"])
        self.assertEqual(len(self.db.messages), 0)


class TestMediaPolicy(Base):
    """The allowlist decision is made at the WRITE, and it is testable both ways."""

    def test_dm_media_is_uploaded(self):
        self.ingest_one(message_payload(kind="imageMessage", base64_blob="QUJD",
                                        filename="pic.jpg"))
        self.assertEqual(len(self.rec["uploads"]), 1)
        self.assertEqual(self.rec["uploads"][0]["chat"], DM)
        self.assertIsNotNone(self.db.attachments[0]["minio_key"])

    def test_group_media_is_skipped_when_not_allowlisted(self):
        self.ingest_one(message_payload(chat=GROUP, kind="imageMessage",
                                        base64_blob="QUJD"))
        self.assertEqual(self.rec["uploads"], [])
        self.assertFalse(self.minio.entered)
        self.assertIsNone(self.db.attachments[0]["minio_key"])
        self.assertIn("not in allowlist", self.db.attachments[0]["download_error"])

    def test_group_message_row_is_still_stored_when_media_is_skipped(self):
        # The text must survive even though the bytes are dropped.
        self.ingest_one(message_payload(chat=GROUP, kind="imageMessage",
                                        body="caption text", base64_blob="QUJD"))
        self.assertEqual(len(self.db.messages), 1)

    def test_group_media_is_uploaded_once_allowlisted(self):
        self.db.allowlisted.add(GROUP)
        self.ingest_one(message_payload(chat=GROUP, kind="imageMessage",
                                        base64_blob="QUJD"))
        self.assertEqual(len(self.rec["uploads"]), 1)
        self.assertIsNotNone(self.db.attachments[0]["minio_key"])

    def test_non_media_message_creates_no_attachment_row(self):
        self.ingest_one(message_payload())
        self.assertEqual(self.db.attachments, [])

    def test_media_upload_failure_leaves_the_row_and_records_the_error(self):
        class Boom(FakeMinio):
            def put_attachment(self, **kw):
                raise RuntimeError("minio down")

        self.ingest = C.Ingest(self.db, minio_factory=Boom(self.rec), executor=Inline())
        self.ingest_one(message_payload(kind="imageMessage", base64_blob="QUJD"))
        self.assertEqual(len(self.db.messages), 1)          # message stands
        self.assertIsNone(self.db.attachments[0]["minio_key"])
        self.assertIn("minio down", self.db.attachments[0]["download_error"])
        self.assertEqual(self.ingest.counters.snapshot()["media_failures"], 1)


class TestControlMessages(Base):
    def test_revoke_targets_the_quoted_message(self):
        payload = message_payload(kind="protocolMessage")
        payload["event"]["Message"] = {"protocolMessage": {"type": 0, "key": {"ID": "PARENT"}}}
        result = self.ingest_one(payload)
        self.assertEqual(result["revoked"], "PARENT")
        self.assertEqual(self.db.revokes, [(1, "PARENT")])
        self.assertEqual(len(self.db.messages), 0)

    def test_reaction_is_stored_against_the_target(self):
        payload = message_payload(kind="reactionMessage")
        payload["event"]["Message"] = {"reactionMessage": {
            "key": {"ID": "PARENT"}, "text": "\U0001f44d"}}
        self.ingest_one(payload)
        self.assertEqual(self.db.reactions[0]["target_id"], "PARENT")
        self.assertEqual(self.db.reactions[0]["emoji"], "\U0001f44d")

    def test_edit_updates_the_target_row_in_place(self):
        payload = message_payload(kind="protocolMessage", msg_id="EDIT1")
        payload["event"]["Message"] = {"protocolMessage": {
            "type": 14, "key": {"ID": "PARENT"},
            "editedMessage": {"conversation": "corrected"}}}
        self.ingest_one(payload)
        self.assertEqual(self.db.messages[0]["whatsapp_id"], "PARENT")
        self.assertEqual(self.db.messages[0]["body"], "corrected")
        self.assertIsNotNone(self.db.messages[0]["edited_at"])


class TestHistorySync(Base):
    def test_payload_is_stored_even_when_it_cannot_be_parsed(self):
        payload = {"type": "HistorySync", "event": {"Conversations": "not-a-list"}}
        self.ingest_one(payload)
        self.assertEqual(len(self.db.history), 1)
        self.assertEqual(self.db.history[0]["payload"]["type"], "HistorySync")

    def test_real_shape_is_ingested(self):
        """The shape a live capture actually delivered: event.Data.conversations.

        Each conversation wraps its messages as `{msgOrderID, message: WMI}`.
        """
        payload = {"type": "HistorySync", "event": {"Data": {
            "syncType": 3,
            "pushnames": [{"ID": DM, "pushname": "Alice"}],
            "conversations": [{
                "ID": DM, "name": "Alice",
                "messages": [
                    {"msgOrderID": 1, "message": {
                        "key": {"ID": "H1", "fromMe": False, "remoteJID": DM},
                        "message": {"conversation": "old message"},
                        "messageTimestamp": 1794247443}},
                    {"msgOrderID": 2, "message": {
                        "key": {"ID": "H2", "fromMe": False, "remoteJID": DM},
                        "message": {"conversation": "another"},
                        "messageTimestamp": 1794247500}},
                ]}]}}}
        result = self.ingest_one(payload)
        self.assertEqual(result["messages"], 2)
        self.assertEqual(result["parsed"], 2)
        self.assertEqual(len(self.db.messages), 2)
        self.assertEqual(self.db.messages[0]["body"], "old message")
        self.assertEqual(self.db.messages[0]["message_ts"].year, 2026)

    def test_group_history_names_the_participant(self):
        group = "120363123456789012@g.us"
        payload = {"type": "HistorySync", "event": {"Data": {
            "syncType": 0,
            "conversations": [{"ID": group, "name": "Family", "messages": [
                {"msgOrderID": 1, "message": {
                    "key": {"ID": "G1", "fromMe": False, "remoteJID": group,
                            "participant": "19990000005@lid"},
                    "message": {"conversation": "hello all"},
                    "messageTimestamp": 1794247443}},
            ]}]}}}
        self.ingest_one(payload)
        self.assertEqual(self.db.messages[0]["body"], "hello all")
        self.assertIn("19990000005@lid", self.db.messages[0]["raw"]["event"]["Info"]["Sender"])

    def test_a_push_name_payload_stores_no_messages_and_is_not_an_error(self):
        payload = {"type": "HistorySync", "event": {"Data": {
            "syncType": 4,
            "pushnames": [{"ID": DM, "pushname": "Alice"}]}}}
        result = self.ingest_one(payload)
        self.assertEqual(result["messages"], 0)
        self.assertEqual(result["parsed"], 0)
        self.assertIsNone(result["error"])

    def test_real_conversation_fields_name_the_chat(self):
        """`displayName`/`username`, not `name`/`subject` — the field the first
        parser read did not exist, which left all 119 live chats unnamed."""
        payload = {"type": "HistorySync", "event": {"Data": {"syncType": 3,
            "conversations": [{"ID": "19990000004@lid",
                               "displayName": "lorenauchoffen",
                               "username": "lorenauchoffen", "messages": []}]}}}
        self.ingest_one(payload)
        self.assertEqual(self.db.chat_labels["19990000004@lid"],
                         ("lorenauchoffen", "lorenauchoffen"))

    def test_pn_lid_mapping_is_applied_before_messages(self):
        """The map is WhatsApp stating two addresses are one human."""
        payload = {"type": "HistorySync", "event": {"Data": {
            "syncType": 3,
            "phoneNumberToLidMappings": [
                {"pnJID": "19990000007@s.whatsapp.net", "lidJID": "19990000006@lid"}],
            "conversations": []}}}
        result = self.ingest_one(payload)
        self.assertEqual(result["messages"], 0)
        self.assertIn(("19990000007", "19990000006"), self.db.identity_calls)

    def test_push_names_label_existing_contacts_and_never_create(self):
        payload = {"type": "HistorySync", "event": {"Data": {"syncType": 4,
            "pushnames": [{"ID": "19990000007@s.whatsapp.net", "pushname": "Bob"}]}}}
        self.ingest_one(payload)
        self.assertIn(("19990000007", "Bob", None), self.db.annotations)
        # No contact row was minted for an address we have no messages from.
        self.assertEqual(self.db.contacts, {})

    def test_history_sync_dedupe_key_is_content_based(self):
        payload = {"type": "HistorySync", "event": {"Data": {"conversations": []}}}
        self.ingest_one(payload)
        second = self.ingest_one(payload)
        self.assertTrue(second["duplicate"])


class TestOtherEvents(Base):
    def test_receipts_are_ignored_but_recorded_as_delivered(self):
        result = self.ingest_one({"type": "ReadReceipt", "event": {"Info": {"ID": "R1"}}})
        self.assertTrue(result["ignored"])
        self.assertEqual(self.ingest.counters.snapshot()["ignored"], 1)

    def test_unknown_event_is_counted_not_crashing(self):
        result = self.ingest_one({"type": "SomethingNew", "event": {}})
        self.assertTrue(result["unknown"])
        self.assertEqual(self.ingest.counters.snapshot()["unknown_event"], 1)

    def test_error_is_recorded_on_the_delivery_and_re_raised(self):
        class Boom(FakeDB):
            def upsert_chat(self, jid, chat_type, display_name=None):
                raise RuntimeError("db exploded")

        db = Boom()
        ingest = C.Ingest(db, minio_factory=self.minio, executor=Inline())
        with self.assertRaises(RuntimeError):
            ingest.handle(message_payload())
        self.assertEqual(len(db.finished), 1)
        self.assertIn("db exploded", db.finished[0]["error"])
        # The delivery row EXISTS, so a retry is a duplicate rather than a second
        # attempt to store a message the DB already half-processed.
        self.assertTrue(ingest.handle(message_payload())["duplicate"])


if __name__ == "__main__":
    unittest.main()