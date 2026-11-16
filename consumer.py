#!/usr/bin/env python3
"""WhatsApp consumer — wuzapi webhook → Postgres (+ MinIO media).

Runs as a long-lived in-cluster Deployment. `asternic/wuzapi` speaks the WhatsApp
multi-device protocol and POSTs HMAC-signed events here; this process turns each
one into durable rows and archives allowlisted media.

🔴 NO SEND PATH AND NO SEND CREDENTIAL IN THIS PROCESS. There is no function in
this file that posts to wuzapi's `/chat/send*` endpoints, and this pod is not
given a user token with send rights. Sending lives in `sender.py` — a separate
image, a separate Deployment, the only holder of a send token — behind a
draft → approve → send gate whose approval is bound to the digest of the exact
payload the operator was shown. So a parser bug here cannot reach a send
endpoint: an unapproved send has no code route at all.

WHAT IT MUST SURVIVE
--------------------
* **A replay.** wuzapi retries webhooks and a reconnect can redeliver. Every
  delivery is recorded in `webhook_deliveries` first; a seen key is a no-op that
  still answers 200, so the sender stops retrying.
* **A bad signature.** Verified on the RAW bytes before parsing. A failure is a
  401 and no write.
* **A failed media upload.** The message row and the attachment row are committed
  BEFORE the upload is attempted; `minio_key IS NULL` is the honest marker and
  never rolls back a message.
* **A payload shape we do not model.** The raw payload is stored. Especially for
  HistorySync: the pairing history is a ONE-SHOT delivery that WhatsApp acks and
  deletes server-side, so it is persisted verbatim before any parsing is
  attempted. A parser bug must cost a re-parse, not the history.
* **An idle account.** `last_event_at` is diagnosis, never liveness. An account
  can legitimately send nothing for hours; the probe reads a heartbeat FILE that
  depends on nothing external.

Third-party calls are injectable (`WaDB`, the MinIO factory, the HMAC secret)
which is what keeps the suite hermetic.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _wa_parse  # noqa: E402
import _wa_webhook  # noqa: E402
from _wa_db import SCHEMA, WaDB, ensure_schema  # noqa: E402
from _wa_webhook import (  # noqa: E402
    SIGNATURE_HEADER,
    WebhookAuthError,
    dedupe_key,
    event_type,
    require_signature,
)

HMAC_ENV = "WHATSAPP_HMAC_KEY"
HEARTBEAT_ENV = "WHATSAPP_HEARTBEAT_PATH"
HEARTBEAT_INTERVAL_ENV = "WHATSAPP_HEARTBEAT_INTERVAL"
HEARTBEAT_MAX_AGE_ENV = "WHATSAPP_HEARTBEAT_MAX_AGE"
INSTANCE_ENV = "WHATSAPP_INSTANCE"
MEDIA_CONCURRENCY_ENV = "WHATSAPP_MEDIA_CONCURRENCY"

DEFAULT_HEARTBEAT = "/tmp/whatsapp-consumer-heartbeat.json"
DEFAULT_INTERVAL = 30
DEFAULT_INSTANCE = "whatsapp-consumer"

MEDIA_KINDS = {"image", "video", "audio", "voice", "document", "sticker"}

# WhatsApp's disappearing-message timers, in seconds. Stored, NEVER enforced —
# the row keeps `expires_at` and nothing sweeps it. Stated here so nobody assumes
# the archive respects the timer.
_KIND_OF_MEDIA = {
    "image": "image/jpeg",
    "video": "video/mp4",
    "audio": "audio/ogg",
    "voice": "audio/ogg",
    "sticker": "image/webp",
    "document": "application/octet-stream",
}


class Counters:
    """Thread-safe counters, surfaced through the heartbeat and the health row."""

    KEYS = (
        "stored", "updated", "ignored", "duplicate", "unauthenticated",
        "malformed", "unknown_event", "reactions", "revokes", "edits",
        "media_uploaded", "media_skipped_policy", "media_failures",
        "history_syncs", "db_failures", "handled_errors",
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values = {k: 0 for k in self.KEYS}

    def inc(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self._values[key] = self._values.get(key, 0) + amount

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._values)


class Ingest:
    """Payload → durable rows. The one place the mapping is defined."""

    def __init__(self, db: WaDB, *, counters: Counters | None = None,
                 minio_factory=None, media_workers: int = 2,
                 executor=None, logger=print) -> None:
        self.db = db
        self.counters = counters or Counters()
        self.minio_factory = minio_factory
        self.log = logger
        # `executor` is the seam the suite drives: a synchronous stand-in makes
        # media uploads deterministic instead of a race against a thread pool.
        self._pool = executor if executor is not None else ThreadPoolExecutor(
            max_workers=max(1, media_workers), thread_name_prefix="wa-media")

    # -- delivery orchestration --------------------------------------------
    def handle(self, payload: dict, body: bytes | None = None) -> dict:
        kind = event_type(payload)
        key = dedupe_key(payload, body)
        stripped = _wa_parse.strip_blobs(payload)

        delivery_id = self.db.record_delivery(kind, key, stripped)
        if delivery_id is None:
            self.counters.inc("duplicate")
            return {"duplicate": True, "event": kind}

        try:
            if kind == "Message":
                result = self._handle_message(payload)
            elif kind == "HistorySync":
                result = self._handle_history(payload)
            elif kind in ("ReadReceipt", "ChatPresence"):
                self.counters.inc("ignored")
                result = {"ignored": True, "event": kind}
            else:
                self.counters.inc("unknown_event")
                result = {"unknown": True, "event": kind}
            self.db.finish_delivery(delivery_id)
            return result
        except Exception as exc:  # noqa: BLE001 - recorded, then re-raised to the caller
            self.counters.inc("handled_errors")
            with_path = f"{type(exc).__name__}: {exc}"
            try:
                self.db.finish_delivery(delivery_id, error=with_path)
            except Exception:  # noqa: BLE001
                self.counters.inc("db_failures")
            raise

    # -- message ------------------------------------------------------------
    def _handle_message(self, payload: dict) -> dict:
        event = payload.get("event") if isinstance(payload.get("event"), dict) else {}
        info = event.get("Info") if isinstance(event.get("Info"), dict) else {}
        message = event.get("Message") if isinstance(event.get("Message"), dict) else {}

        chat_jid = _wa_parse.normalize_jid(info.get("Chat") or info.get("chat"))
        msg_id = info.get("ID") or info.get("id")
        if not chat_jid or not msg_id:
            self.counters.inc("malformed")
            return {"malformed": True, "reason": "missing Chat or ID"}

        chat_type = _wa_parse.chat_type(chat_jid)
        push_name = info.get("PushName") if isinstance(info.get("PushName"), str) else None
        display = push_name if chat_type == "dm" else None
        chat_id = self.db.upsert_chat(chat_jid, chat_type, display)

        if chat_type == "group":
            self.db.upsert_group(chat_jid, None)

        identity = _wa_parse.identity_from_info(info)
        sender_id = self.db.find_or_create_contact(
            identity.get("pn"), identity.get("lid"), push_name,
        )

        ts = _wa_parse.parse_timestamp(info.get("Timestamp")) or datetime.now(timezone.utc)
        from_me = bool(info.get("IsFromMe"))
        raw = _wa_parse.strip_blobs({"event": event, "s3": payload.get("s3")})

        # -- control messages ------------------------------------------------
        protocol = _wa_parse.protocol_info(message)
        if protocol and protocol.get("is_revoke") and protocol.get("target_id"):
            affected = self.db.revoke_message(chat_id, protocol["target_id"])
            self.counters.inc("revokes")
            return {"revoked": protocol["target_id"], "rows": affected}

        if protocol and protocol.get("is_edit") and protocol.get("target_id"):
            self.counters.inc("edits")
            edited = message.get("protocolMessage", {}).get("editedMessage") or {}
            if isinstance(edited, dict) and edited:
                classified = _wa_parse.classify(edited)
                _, was_new = self.db.insert_message(
                    chat_id=chat_id, whatsapp_id=protocol["target_id"],
                    sender_contact_id=sender_id, from_me=from_me,
                    kind=classified["kind"], body=classified["body"],
                    caption=classified["caption"], quoted_whatsapp_id=None,
                    message_ts=ts, is_view_once=False, expires_at=None,
                    raw=raw, edited_at=ts,
                )
                self.counters.inc("updated" if not was_new else "stored")
                return {"edit": protocol["target_id"], "new_row": was_new}
            return {"edit": protocol["target_id"], "reparsed": False}

        reaction = _wa_parse.reaction_info(message)
        if reaction and reaction.get("target_id"):
            self.db.upsert_reaction(
                chat_id=chat_id, target_id=reaction["target_id"],
                sender_contact_id=sender_id, emoji=reaction.get("emoji") or "",
                reaction_ts=ts, removed=bool(reaction.get("removed")),
            )
            self.counters.inc("reactions")
            return {"reaction": reaction["target_id"], "emoji": reaction.get("emoji")}

        # -- regular message -------------------------------------------------
        classified = _wa_parse.classify(message)
        row_id, was_new = self.db.insert_message(
            chat_id=chat_id, whatsapp_id=msg_id, sender_contact_id=sender_id,
            from_me=from_me, kind=classified["kind"], body=classified["body"],
            caption=classified["caption"],
            quoted_whatsapp_id=_wa_parse.quoted_id(message),
            message_ts=ts,
            is_view_once=bool(classified.get("wrapper", "").startswith("viewOnce")),
            expires_at=None,
            raw=raw,
        )
        self.db.touch_chat(chat_id, ts)
        self.counters.inc("stored" if was_new else "updated")

        self._maybe_archive_media(payload, row_id, chat_jid, chat_type,
                                  classified["kind"], msg_id, ts)
        return {"stored": was_new, "id": row_id, "kind": classified["kind"]}

    def _maybe_archive_media(self, payload: dict, message_id: int, chat_jid: str,
                             chat_type: str, kind: str, msg_id: str, ts) -> None:
        blob = payload.get("base64")
        if kind not in MEDIA_KINDS or not isinstance(blob, str) or not blob:
            return

        # 🔴 THE ALLOWLIST IS ENFORCED HERE, at the write, not by wuzapi — its S3
        # path uploads everything and writes public-read objects. One policy, one place.
        if chat_type == "group" and not self.db.is_allowlisted(chat_jid):
            self.db.insert_attachment(
                message_id=message_id, media_type=kind,
                mimetype=payload.get("mimeType"), filename=payload.get("fileName"),
                file_size=None, sha256=None, minio_key=None,
                download_error="skipped: group not in allowlist",
            )
            self.counters.inc("media_skipped_policy")
            return

        self.db.insert_attachment(
            message_id=message_id, media_type=kind, mimetype=payload.get("mimeType"),
            filename=payload.get("fileName"), file_size=None, sha256=None,
            minio_key=None, download_error=None,
        )

        if self.minio_factory is None:
            self.db.finish_attachment(
                message_id=message_id, media_type=kind, minio_key=None,
                error="no MinIO client configured", sha256=None, file_size=None,
            )
            return

        self._pool.submit(
            self._upload_media, message_id, media_type=kind, chat_jid=chat_jid,
            msg_id=msg_id, ts=ts, blob=blob,
            filename=payload.get("fileName"), mimetype=payload.get("mimeType"),
        )

    def _upload_media(self, message_id: int, *, media_type: str, chat_jid: str,
                      msg_id: str, ts, blob: str, filename: str | None,
                      mimetype: str | None) -> None:
        try:
            data = base64.b64decode(blob)
            name = filename or _wa_parse.media_filename({"ID": msg_id}, media_type)
            with self.minio_factory() as mc:
                key = mc.put_attachment(
                    chat=chat_jid, timestamp_ms=int(ts.timestamp() * 1000),
                    filename=name, data=data, attachment_id=msg_id,
                    content_type=mimetype or _KIND_OF_MEDIA.get(media_type, "application/octet-stream"),
                    sidecar={
                        "chat": chat_jid, "message_id": msg_id,
                        "media_type": media_type, "mimetype": mimetype,
                        "sha256": hashlib.sha256(data).hexdigest(),
                        "size": len(data),
                        "source": "wuzapi-webhook",
                    },
                )
            self.db.finish_attachment(
                message_id=message_id, media_type=media_type, minio_key=key,
                error=None, sha256=hashlib.sha256(data).hexdigest(), file_size=len(data),
            )
            self.counters.inc("media_uploaded")
        except Exception as exc:  # noqa: BLE001 - the message row already stands
            self.counters.inc("media_failures")
            self.log(f"media upload failed for {msg_id} ({media_type}): "
                     f"{type(exc).__name__}: {exc}")
            try:
                self.db.finish_attachment(
                    message_id=message_id, media_type=media_type, minio_key=None,
                    error=f"{type(exc).__name__}: {exc}", sha256=None, file_size=None,
                )
            except Exception:  # noqa: BLE001
                self.counters.inc("db_failures")

    # -- history ------------------------------------------------------------
    def _ingest_history_conversations(self, payload: dict, *,
                                      account_jid: str | None = None) -> tuple[int, int, str | None]:
        """Ingest the conversations inside a HistorySync payload.

        Returns (parsed, message_count, error). Kept separate from
        `_handle_history` so the RE-PARSE path can replay a payload already in the
        store without writing a second `history_syncs` row for it.
        """
        conversations = _wa_parse.history_conversations(payload)
        names = _wa_parse.pushnames(payload)
        pairs = _wa_parse.history_pn_lid_pairs(payload)
        parsed = 0
        message_cnt = 0
        merges = 0
        error = None

        # 🔴 APPLY THE IDENTITY MAP FIRST, BEFORE THE MESSAGES.
        # `Data.phoneNumberToLidMappings` is WhatsApp stating that a PN and a LID
        # are one human. Doing it up front means every sender in this payload
        # resolves to the merged row immediately, instead of the messages landing on
        # two rows and being repointed afterwards.
        for pn, lid in pairs:
            self.db.find_or_create_contact(
                pn=pn.rsplit("@", 1)[0], lid=lid.rsplit("@", 1)[0])

        # Label existing contacts from the push-name list (never create — see
        # annotate_contact). Both encodings are tried because a name may be listed
        # against either address.
        for jid, name in names.items():
            bare = jid.rsplit("@", 1)[0]
            self.db.annotate_contact(bare, push_name=name)

        for conv in conversations:
            chat_jid = _wa_parse.normalize_jid(
                conv.get("ID") or conv.get("id") or conv.get("JID")
            )
            # `displayName`/`username`, not `name`/`subject` — the payload carries
            # names for most conversations and the first parser read neither.
            conv_name, conv_username = _wa_parse.chat_label(conv)
            if not conv_name and chat_jid:
                conv_name = names.get(chat_jid)
            entries = conv.get("messages") or conv.get("Messages") or []
            if isinstance(entries, dict):
                entries = [entries]
            message_cnt += len(entries)

            if chat_jid:
                ctype = _wa_parse.chat_type(chat_jid)
                self.db.upsert_chat(chat_jid, ctype, conv_name, conv_username)
                if ctype == "group":
                    self.db.upsert_group(chat_jid, conv_name)

            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                # Two shapes in the wild: the wrapper `{msgOrderID, message: WMI}`
                # seen in real history payloads, and a bare WebMessageInfo.
                wmi = entry.get("message") if isinstance(entry.get("message"), dict) else entry
                try:
                    synthetic = _wa_parse.from_web_message(
                        wmi, fallback_chat=chat_jid,
                        push_name=names.get(chat_jid) if chat_jid else None,
                        account_jid=account_jid,
                    )
                except Exception as exc:  # noqa: BLE001 - one bad entry, not the payload
                    error = f"wrap {type(exc).__name__}: {exc}"
                    continue
                try:
                    self._handle_message(synthetic)
                    parsed += 1
                except Exception as exc:  # noqa: BLE001 - keep going, record it
                    error = f"{type(exc).__name__}: {exc}"
        if pairs:
            merges = len(pairs)
        return parsed, message_cnt, error, merges

    def _handle_history(self, payload: dict, *, account_jid: str | None = None) -> dict:
        """Store FIRST, parse second.

        WhatsApp acks and deletes the history payload server-side as soon as it is
        delivered, and there is no re-fetch. So the verbatim payload is committed
        before anything is interpreted; a parser failure is recorded and the raw
        data survives for a later re-parse — which is exactly what happened: the
        first parser read the wrong level (`event.conversations` instead of
        `event.Data.conversations`) and reported 0 messages for payloads that in
        fact carried 3,755. Nothing was lost, because this stored the raw JSON.
        """
        parsed, message_cnt, error, _merges = self._ingest_history_conversations(
            payload, account_jid=account_jid)
        conversations = _wa_parse.history_conversations(payload)

        self.db.store_history_sync(
            _wa_parse.strip_blobs(payload),
            conversation_cnt=len(conversations),
            message_cnt=message_cnt,
            parsed_messages=parsed,
            error=error,
        )
        self.counters.inc("history_syncs")
        if error:
            self.log(f"history sync partially parsed: {error}")
        return {"history_syncs": 1, "conversations": len(conversations),
                "messages": message_cnt, "parsed": parsed, "error": error}


# --------------------------------------------------------------------------- #
# Heartbeat: a FILE, not a row.
#
# The liveness input must not depend on Postgres. An idle account legitimately
# produces no events for hours, so "no recent messages" can never be a liveness
# test — only "this thread stopped ticking" can.
# --------------------------------------------------------------------------- #
def _write_heartbeat(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, default=str), encoding="utf-8")
    tmp.replace(path)


def _heartbeat_loop(path: Path, interval: float, instance: str,
                    counters: Counters, stop: threading.Event) -> None:
    while not stop.wait(interval):
        try:
            _write_heartbeat(path, {
                "instance": instance,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "pid": os.getpid(),
                "counters": counters.snapshot(),
            })
        except OSError as exc:  # never take the process down over a probe file
            print(f"heartbeat write failed: {exc}", file=sys.stderr)


class _Handler(BaseHTTPRequestHandler):
    server_version = "whatsapp-consumer/1"
    protocol_version = "HTTP/1.1"

    # Injected by `serve()` through the server instance.
    ingest: Ingest
    secret: str
    heartbeat_path: Path
    max_age: float
    counters: Counters
    log_rejection = staticmethod(lambda *a, **k: None)

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        # Per-request logging scales with WhatsApp traffic; keep only errors.
        pass

    def _send(self, code: int, payload: dict) -> None:
        blob = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(blob)))
        self.end_headers()
        self.wfile.write(blob)

    def do_GET(self) -> None:  # noqa: N802
        if self.path.split("?", 1)[0] != "/healthz":
            self._send(404, {"error": "not found"})
            return
        try:
            beat = json.loads(self.heartbeat_path.read_text(encoding="utf-8"))
            stamp = datetime.fromisoformat(beat["updated_at"].replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - stamp).total_seconds()
        except Exception:  # noqa: BLE001
            self._send(503, {"healthy": False, "reason": "no heartbeat"})
            return
        healthy = age <= self.max_age
        self._send(200 if healthy else 503,
                   {"healthy": healthy, "age_seconds": round(age, 1)})

    def do_POST(self) -> None:  # noqa: N802
        if self.path.split("?", 1)[0] not in ("/hook", "/"):
            self._send(404, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0 or length > 64 * 1024 * 1024:
            self._send(413, {"error": "bad content-length"})
            return
        body = self.rfile.read(length)

        try:
            require_signature(self.secret, body, self.headers.get(SIGNATURE_HEADER))
        except WebhookAuthError as exc:
            self.counters.inc("unauthenticated")
            # 🔴 A REJECTED DELIVERY MUST LEAVE A TRACE. Without this, a 401 is
            # invisible: wuzapi retries three times, drops the payload to an error
            # queue, and the only local evidence is a counter that says nothing
            # about WHAT was refused. That is exactly how a suspected "receipts are
            # unsigned" bug had to be diagnosed from circumstantial evidence.
            #
            # The EVENT TYPE only — never the body. A WhatsApp webhook carries
            # private message text, and pod logs are not the place for it; knowing
            # which event type failed is what identifies the fault.
            kind = "non-json"
            try:
                parsed = json.loads(body)
                kind = event_type(parsed)
            except ValueError:
                pass
            self.log_rejection(kind, len(body), self.headers.get(SIGNATURE_HEADER), body)
            self._send(401, {"error": str(exc)})
            return

        try:
            payload = json.loads(body)
        except ValueError:
            self.counters.inc("malformed")
            self._send(400, {"error": "invalid json"})
            return

        try:
            result = self.ingest.handle(payload, body)
        except Exception as exc:  # noqa: BLE001
            # 500 so wuzapi RETRIES: the delivery row exists with an error and a
            # replay will be treated as a duplicate, so a retry after a transient
            # DB fault must not be swallowed as success.
            traceback.print_exc()
            self._send(500, {"error": f"{type(exc).__name__}: {exc}"})
            return

        self._send(200, {"ok": True, **result})


def _log_rejection(kind: str, length: int, signature: str | None, body: bytes) -> None:
    """Record a refused delivery. Event type and shape only — never the body."""
    print(
        f"webhook REJECTED (bad HMAC): event_type={kind} bytes={length} "
        f"signature={'present' if signature else 'ABSENT'} "
        f"sha256={hashlib.sha256(body).hexdigest()[:16]}",
        flush=True,
    )


def serve(*, host: str, port: int, secret: str, db: WaDB,
          minio_factory, heartbeat_path: Path, interval: float,
          max_age: float, instance: str, media_workers: int) -> None:
    counters = Counters()
    ingest = Ingest(db, counters=counters, minio_factory=minio_factory,
                    media_workers=media_workers)

    handler = type("BoundHandler", (_Handler,), {
        "ingest": ingest, "secret": secret,
        "heartbeat_path": heartbeat_path, "max_age": max_age,
        "counters": counters,
        "log_rejection": staticmethod(_log_rejection),
    })

    stop = threading.Event()
    _write_heartbeat(heartbeat_path, {
        "instance": instance, "updated_at": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(), "counters": counters.snapshot(),
    })
    threading.Thread(
        target=_heartbeat_loop,
        args=(heartbeat_path, interval, instance, counters, stop),
        daemon=True, name="wa-heartbeat",
    ).start()

    httpd = ThreadingHTTPServer((host, port), handler)
    httpd.daemon_threads = True
    print(f"whatsapp-consumer listening on {host}:{port} "
          f"(heartbeat {heartbeat_path})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        httpd.server_close()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _minio_factory():
    from _minio import MinioWhatsApp
    return MinioWhatsApp


def _db() -> WaDB:
    return WaDB()


def _print_rows(rows: list[dict]) -> None:
    for row in rows:
        parts = []
        for key in ("message_ts", "chat_type", "display_name", "jid", "sender",
                    "kind", "body", "caption", "whatsapp_id", "filename", "minio_key"):
            if row.get(key) not in (None, ""):
                parts.append(f"{key}={row[key]}")
        print("  ".join(parts))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="WhatsApp consumer / operator CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_serve = sub.add_parser("serve", help="run the webhook server (the Deployment)")
    p_serve.add_argument("--host", default="0.0.0.0")
    p_serve.add_argument("--port", type=int, default=int(os.environ.get("WHATSAPP_PORT", "8080")))
    p_serve.add_argument("--media-workers", type=int,
                         default=int(os.environ.get(MEDIA_CONCURRENCY_ENV, "2")))

    sub.add_parser("init-schema", help="apply the whatsapp schema (idempotent)")

    p_health = sub.add_parser("health", help="heartbeat file, or --from-db for the row")
    p_health.add_argument("--from-db", action="store_true")
    p_health.add_argument("--json", action="store_true")

    p_conv = sub.add_parser("conversations",
                            help="list chats, newest first (a DM is named after the counterpart)")
    p_conv.add_argument("--limit", type=int, default=20)

    p_search = sub.add_parser("search", help="full-text search over message bodies")
    p_search.add_argument("query")
    p_search.add_argument("--limit", type=int, default=20)
    p_search.add_argument("--allowlisted-only", action="store_true")

    p_show = sub.add_parser("show", help="recent messages in one chat")
    p_show.add_argument("jid")
    p_show.add_argument("--limit", type=int, default=30)

    p_stats = sub.add_parser("stats", help="row counts and newest message")
    p_stats.add_argument("--json", action="store_true")

    p_reparse = sub.add_parser(
        "reparse-history",
        help="re-ingest every STORED HistorySync payload (safe to re-run)",
    )
    p_reparse.add_argument(
        "--account",
        help="the account's own JID, used to attribute fromMe history rows. "
             "Without it those rows keep a NULL sender rather than naming the recipient as the author.",
    )

    p_allow = sub.add_parser("allowlist", help="groups whose MEDIA is archived")
    allow_sub = p_allow.add_subparsers(dest="allow_cmd", required=True)
    allow_sub.add_parser("list")
    a_add = allow_sub.add_parser("add")
    a_add.add_argument("jid")
    a_add.add_argument("--note")
    a_add.add_argument("--by", default=os.environ.get("USER", "operator"))
    a_rm = allow_sub.add_parser("remove")
    a_rm.add_argument("jid")

    args = parser.parse_args(argv)
    db = _db()

    if args.cmd == "serve":
        secret = os.environ.get(HMAC_ENV, "")
        if not secret:
            print(f"{HMAC_ENV} is required — refusing to accept unauthenticated "
                  "webhooks", file=sys.stderr)
            return 2
        ensure_schema()
        heartbeat = Path(os.environ.get(HEARTBEAT_ENV, DEFAULT_HEARTBEAT))
        heartbeat.parent.mkdir(parents=True, exist_ok=True)
        interval = float(os.environ.get(HEARTBEAT_INTERVAL_ENV, DEFAULT_INTERVAL))
        serve(
            host=args.host, port=args.port, secret=secret, db=db,
            minio_factory=_minio_factory(),
            heartbeat_path=heartbeat, interval=interval,
            max_age=float(os.environ.get(HEARTBEAT_MAX_AGE_ENV, interval * 4)),
            instance=os.environ.get(INSTANCE_ENV, DEFAULT_INSTANCE),
            media_workers=args.media_workers,
        )
        return 0

    if args.cmd == "init-schema":
        ensure_schema()
        print(f"schema `{SCHEMA}` applied")
        return 0

    if args.cmd == "health":
        if args.from_db:
            import psycopg2.extras  # noqa: F401  (kept for RealDictCursor parity)
            from _wa_db import cursor
            with cursor(db.dsn) as cur:
                cur.execute(f"SELECT * FROM {SCHEMA}.consumer_health")
                rows = [dict(r) for r in cur.fetchall()]
            print(json.dumps(rows, default=str, indent=2) if args.json else rows)
            return 0
        path = Path(os.environ.get(HEARTBEAT_ENV, DEFAULT_HEARTBEAT))
        interval = float(os.environ.get(HEARTBEAT_INTERVAL_ENV, DEFAULT_INTERVAL))
        max_age = float(os.environ.get(HEARTBEAT_MAX_AGE_ENV, interval * 4))
        try:
            beat = json.loads(path.read_text(encoding="utf-8"))
            stamp = datetime.fromisoformat(beat["updated_at"].replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - stamp).total_seconds()
        except Exception as exc:  # noqa: BLE001
            print(f"UNHEALTHY: cannot read {path}: {exc}")
            return 1
        healthy = age <= max_age
        if args.json:
            print(json.dumps({"healthy": healthy, "age_seconds": round(age, 1), **beat},
                             default=str))
        else:
            print(f"{'healthy' if healthy else 'STALE'} (last beat {age:.0f}s ago, "
                  f"max {max_age:.0f}s) counters={beat.get('counters')}")
        return 0 if healthy else 1

    if args.cmd == "conversations":
        _print_rows(db.conversations(args.limit))
        return 0

    if args.cmd == "search":
        _print_rows(db.search(args.query, args.limit,
                              allowlisted_only=args.allowlisted_only))
        return 0

    if args.cmd == "show":
        _print_rows(db.show(args.jid, args.limit))
        return 0

    if args.cmd == "reparse-history":
        # 🔴 REPLAY, DON'T RE-FETCH. WhatsApp deletes a history payload server-side
        # once delivered, so the ONLY copy of what arrived is the one in
        # `history_syncs`. This walks it and re-runs the ingest path.
        #
        # Idempotent: message rows collide on (chat_id, whatsapp_id) and update in
        # place, so a second run reports `updated` rather than duplicating.
        # Deliberately does NOT write new history_syncs rows — that table is the
        # record of what ARRIVED, and a re-parse is not an arrival.
        ingest = Ingest(db, minio_factory=_minio_factory(), media_workers=1)
        rows = db.history_syncs()
        if not rows:
            print("no stored history payloads to re-parse")
            return 0
        total_parsed = total_messages = total_pairs = 0
        for row in rows:
            parsed, messages, error, pairs = ingest._ingest_history_conversations(
                row["payload"], account_jid=args.account)
            total_parsed += parsed
            total_messages += messages
            total_pairs += pairs
            note = f" error={error}" if error else ""
            print(f"  payload {row['id']}: messages={messages} parsed={parsed} "
                  f"identity_pairs={pairs}{note}")
        print(f"re-parsed {len(rows)} payload(s): {total_parsed}/{total_messages} messages, "
              f"{total_pairs} PN<->LID pair(s) applied")
        for key, value in db.stats().items():
            print(f"  {key}: {value}")
        return 0

    if args.cmd == "stats":
        stats = db.stats()
        print(json.dumps(stats, default=str, indent=2) if args.json else stats)
        return 0

    if args.cmd == "allowlist":
        if args.allow_cmd == "list":
            for row in db.allowlist():
                print(f"{row['group_jid']}  {row.get('note') or ''}")
            return 0
        if args.allow_cmd == "add":
            db.allowlist_add(args.jid, args.note, args.by)
            print(f"allowlisted {args.jid} (media from the NEXT message onward)")
            return 0
        if args.allow_cmd == "remove":
            removed = db.allowlist_remove(args.jid)
            print(f"removed {removed} row(s) for {args.jid}")
            return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())