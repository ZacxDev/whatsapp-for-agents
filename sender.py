#!/usr/bin/env python3
"""WhatsApp SENDER — the only component in this repo that can send as the operator.

A SEPARATE Deployment from `whatsapp-consumer`, with its own wuzapi user token.
That split is deliberate and is the whole architectural point:

  * the CONSUMER parses arbitrary, internet-influenced payloads, and is built
    with NO send credential and no send function, so a parser bug cannot reach a
    send endpoint;
  * the SENDER holds the token and is a tiny, auditable surface — draft,
    approve, send, reconcile. It has no webhook, no parser and no MinIO.

The gate itself lives in `_wa_outbound` (pure, testable). This module is the
I/O around it: the database, the operator commands, and the single HTTP POST.

🔴 REPLY-ONLY BY DESIGN. Nothing here composes a message; it only transmits a
draft a human made and approved. Refusing to initiate to unsaved contacts is a
POLICY that lives in the skill, not a mechanism here — the mechanism is that a
draft cannot exist without someone running `draft`, and cannot transmit without
someone running `approve`.

WHAT IT MUST SURVIVE
--------------------
* **An agent approving its own draft.** `approve` requires
  `WHATSAPP_APPROVAL_TOKEN` in the environment. That is a speed bump, not proof
  of humanity — it stops an agent that has the DB but not the secret.
* **A draft edited after approval.** `_wa_outbound._mint_send_authorization()`
  refuses a row that no longer hashes to its recorded digest.
* **A double send.** The capability is single-use (`spend_authorization`).
* **A crash mid-send.** The row is `sending` — INERT — until `reconcile` records
  the outcome, because "POSTed and died" and "never POSTed" are indistinguishable
  from here.
* **A long body.** Refused at DRAFT time (see `_wa_outbound.enforce_length_policy`).
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _wa_outbound as ob  # noqa: E402
from _wa_db import SCHEMA, connect, cursor, ensure_schema  # noqa: E402

WUZAPI_URL_ENV = "WUZAPI_URL"
WUZAPI_TOKEN_ENV = "WUZAPI_USER_TOKEN"
SEND_TEXT_PATH = "/chat/send/text"
SEND_DOCUMENT_PATH = "/chat/send/document"
DEFAULT_TIMEOUT = 30


class SendError(Exception):
    """A transmit failed. Never swallowed."""


class AmbiguousSendError(SendError):
    """The POST was ATTEMPTED and its outcome is UNKNOWN.

    🔴 This is a different fact from "the send failed", and it must be recorded
    differently. A failure before the request leaves the process means nothing was
    sent, so the draft is genuinely `failed`. A failure during or after it means
    the message may have been delivered, and recording that as `failed` invites a
    retry that would send it twice. Only `reconcile` can settle it.
    """


def _record_error(db, draft_id: int, state: str, exc: Exception) -> None:
    with connect(db.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"UPDATE {SCHEMA}.outbound_drafts SET state = %s, error = %s "
            f"WHERE id = %s",
            (state, f"{type(exc).__name__}: {exc}", draft_id))
        conn.commit()


# --------------------------------------------------------------------------- #
# The ONE network call. Everything else in this file reads or writes Postgres.
# --------------------------------------------------------------------------- #
def _post(path: str, payload: dict, *, url: str, token: str, timeout: int = DEFAULT_TIMEOUT) -> dict:
    """POST a JSON body to wuzapi with the user token. The only outbound call."""
    if not token:
        raise SendError(f"{WUZAPI_TOKEN_ENV} is not set — refusing to send")
    req = urllib.request.Request(
        url.rstrip("/") + path,
        data=json.dumps(payload).encode("utf-8"),
        # 🔴 `token`, NOT `Authorization`. wuzapi's auth middleware is
        # `r.Header.Get("token")` (falling back to a `token` query parameter) —
        # an `Authorization` header is simply ignored and every send answers
        # 401 `{"error":"unauthorized"}`, which reads like a bad credential
        # rather than a wrong header name.
        headers={"Content-Type": "application/json", "token": token},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")[:400]
        raise SendError(f"wuzapi {path} -> HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise SendError(f"wuzapi {path} unreachable: {exc.reason}") from exc
    try:
        return json.loads(raw) if raw.strip() else {}
    except ValueError:
        return {"raw": raw}


# --------------------------------------------------------------------------- #
# Drafts
# --------------------------------------------------------------------------- #
def draft(db, to_jid: str, body: str, attachment_files: list[str] | None = None) -> int:
    """Compose and STORE a draft. Transmits nothing.

    🔴 The length policy is enforced HERE, not at send time. A refusal at send
    time would come after the operator already approved the text; the correction
    has to happen while it is still a draft.
    """
    attachments = []
    for spec in attachment_files or []:
        path = Path(spec)
        if not path.is_file():
            raise SendError(f"attachment not found: {path}")
        attachments.append({
            "filename": path.name,
            "content_type": _guess_content_type(path),
            "bytes": path.read_bytes(),
        })
    validated = ob.validated_attachments(attachments)
    ob.enforce_length_policy(body, validated)

    with connect(db.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {SCHEMA}.outbound_drafts (to_jid, body) "
            f"VALUES (%s, %s) RETURNING id", (to_jid, body))
        draft_id = cur.fetchone()[0]
        for a in validated:
            cur.execute(
                f"INSERT INTO {SCHEMA}.outbound_attachments "
                f"(draft_id, filename, content_type, sha256, bytes) "
                f"VALUES (%s, %s, %s, %s, %s)",
                (draft_id, a["filename"], a["content_type"], a["sha256"], a["bytes"]))
        conn.commit()
    return draft_id


def load_draft(db, draft_id: int) -> dict:
    """Read a draft row WITH its attachments, shaped for `_wa_outbound`."""
    with connect(db.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT id, to_jid, body, state, approval_ref, approval_digest, "
            f"       approved_at, whatsapp_message_id, error "
            f"FROM {SCHEMA}.outbound_drafts WHERE id = %s", (draft_id,))
        row = cur.fetchone()
        if not row:
            raise SendError(f"no draft {draft_id}")
        cols = ("id", "to_jid", "body", "state", "approval_ref",
                "approval_digest", "approved_at", "whatsapp_message_id", "error")
        d = dict(zip(cols, row))
        cur.execute(
            f"SELECT filename, content_type, sha256, octet_length(bytes) "
            f"FROM {SCHEMA}.outbound_attachments WHERE draft_id = %s "
            f"ORDER BY filename", (draft_id,))
        d["attachments"] = tuple(cur.fetchall())
    return d


def load_attachment_bytes(db, draft_id: int) -> list[dict]:
    with connect(db.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT filename, content_type, bytes FROM {SCHEMA}.outbound_attachments "
            f"WHERE draft_id = %s ORDER BY filename", (draft_id,))
        return [{"filename": r[0], "content_type": r[1], "bytes": bytes(r[2])}
                for r in cur.fetchall()]


# --------------------------------------------------------------------------- #
# Approve / unapprove — operator commands
# --------------------------------------------------------------------------- #
def approve(db, draft_id: int, ref: str) -> str:
    """Record the operator's approval of a draft's EXACT payload.

    Requires `WHATSAPP_APPROVAL_TOKEN`; that is a speed bump against an agent
    approving its own draft, not proof a human did it.
    """
    if not os.environ.get(ob.APPROVAL_TOKEN_ENV):
        raise SendError(
            f"{ob.APPROVAL_TOKEN_ENV} is not set — `approve` is an operator "
            f"command (it records a human decision). Refusing to approve "
            f"draft {draft_id} from an environment that cannot prove it."
        )
    d = load_draft(db, draft_id)
    if d["state"] != ob.STATE_PENDING:
        raise SendError(f"draft {draft_id} is {d['state']!r}; only 'pending' can be approved")
    digest = ob.draft_payload_digest(d)
    with connect(db.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"UPDATE {SCHEMA}.outbound_drafts SET state = %s, approval_ref = %s, "
            f"approval_digest = %s, approved_at = now() "
            f"WHERE id = %s AND state = %s",
            (ob.STATE_APPROVED, ref, digest, draft_id, ob.STATE_PENDING))
        if cur.rowcount != 1:
            raise SendError(f"draft {draft_id} changed under the approval — retry")
        conn.commit()
    return digest


def unapprove(db, draft_id: int) -> None:
    """Withdraw an approval: approved -> pending, so it can be re-approved."""
    if not os.environ.get(ob.APPROVAL_TOKEN_ENV):
        raise SendError(f"{ob.APPROVAL_TOKEN_ENV} is not set — operator command")
    with connect(db.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"UPDATE {SCHEMA}.outbound_drafts SET state = %s, approval_digest = NULL, "
            f"approval_ref = NULL, approved_at = NULL "
            f"WHERE id = %s AND state = %s",
            (ob.STATE_PENDING, draft_id, ob.STATE_APPROVED))
        if cur.rowcount != 1:
            raise SendError(f"draft {draft_id} is not 'approved'")
        conn.commit()


# --------------------------------------------------------------------------- #
# Send — mint, claim, spend, POST
# --------------------------------------------------------------------------- #
def transmit_approved(db, auth, *, to_jid, body, attachments, url=None, token=None) -> dict:
    """POST an approved draft. The ONLY call in the repo that sends a message."""
    url = url or os.environ.get(WUZAPI_URL_ENV, "http://whatsapp-api:8080")
    token = token or os.environ.get(WUZAPI_TOKEN_ENV, "")
    # 🔴 Spend BEFORE the network call. An approved draft transmits exactly once.
    ob.spend_authorization(auth, to_jid=to_jid, body=body, attachments=attachments)
    try:
        if attachments:
            # One document per draft, sent with the body as its caption. The body
            # is a cover note (enforced at draft time), so this is the short-note +
            # attachment shape the operator asked for.
            first = attachments[0]
            payload = {
                "Phone": to_jid,
                "Document": "data:%s;base64,%s" % (
                    first["content_type"], base64.b64encode(first["bytes"]).decode("ascii")),
                "FileName": first["filename"],
                "Caption": body,
            }
            return _post(SEND_DOCUMENT_PATH, payload, url=url, token=token)
        return _post(SEND_TEXT_PATH, {"Phone": to_jid, "Body": body}, url=url, token=token)
    except Exception as exc:  # noqa: BLE001
        # 🔴 TRANSLATED, NOT SWALLOWED. From here on the request has been
        # ATTEMPTED, so "nothing was sent" is no longer something we know. The
        # caller must leave the row inert rather than record it as failed.
        raise AmbiguousSendError(f"{type(exc).__name__}: {exc}") from exc


def _archive_outbound(db, *, draft_id: int, draft: dict, message_id,
                      sent_at, attachments) -> int | None:
    """Write what we just sent into `whatsapp.messages`. NEVER raises.

    🔴 THE SEND HAS ALREADY HAPPENED BY THE TIME THIS RUNS. A failure here must
    not be reported as a failed send — the message is out, and recording
    `failed` invites a retry that would send it twice. So it is logged loudly
    and the draft row keeps `whatsapp_message_id`, which is what a backfill
    would key on; that is why the warning names the draft.
    """
    if not message_id:
        print(f"archive: draft {draft_id} SENT but wuzapi returned no message "
              f"id — an archive row cannot be keyed without one; backfill from "
              f"the draft row", file=sys.stderr)
        return None
    try:
        row = ob.outbound_archive_row(
            draft_id=draft_id, to_jid=draft["to_jid"], body=draft["body"],
            whatsapp_id=str(message_id), sent_at=sent_at,
            attachments=attachments)
        chat_id = db.upsert_chat(row["jid"], row["chat_type"])
        fields = {k: v for k, v in row.items() if k not in ("jid", "chat_type")}
        row_id, was_new = db.insert_message(chat_id=chat_id, **fields)
        for a in ob.outbound_attachment_rows(row["kind"], attachments):
            db.insert_attachment(message_id=row_id, **a)
        db.touch_chat(chat_id, sent_at)
        if not was_new:
            # The upsert matched: wuzapi DOES echo this send (it does not today),
            # and the row it created was updated in place rather than duplicated.
            print(f"archive: draft {draft_id} matched an EXISTING "
                  f"whatsapp.messages row (id={row_id}) — updated in place, no "
                  f"duplicate", file=sys.stderr)
        return row_id
    except Exception as exc:  # noqa: BLE001 — the send stands regardless
        print(f"archive: draft {draft_id} SENT (whatsapp_message_id={message_id}) "
              f"but the archive row FAILED: {type(exc).__name__}: {exc}. The "
              f"message is out; backfill whatsapp.messages from the draft.",
              file=sys.stderr)
        return None


def send(db, draft_id: int, *, url=None, token=None) -> dict:
    """Transmit an APPROVED draft. Refuses anything else."""
    first = load_draft(db, draft_id)
    auth = ob._mint_send_authorization(first)          # refuses unless approved+unchanged

    # 🔴 THE MINT-READ -> POST-READ GAP, CLOSED BY THE CLAIM. `sending` is INERT:
    # a crash after the POST leaves the row here and `reconcile` owns it, so a
    # retry cannot silently double-send.
    with connect(db.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"UPDATE {SCHEMA}.outbound_drafts SET state = %s "
            f"WHERE id = %s AND state = %s",
            (ob.STATE_SENDING, draft_id, ob.STATE_APPROVED))
        if cur.rowcount != 1:
            raise SendError(
                f"draft {draft_id} could not be claimed for sending — it is no "
                f"longer 'approved' (another sender won the race, or it was "
                f"reconciled). Nothing was sent.")
        conn.commit()

    second = load_draft(db, draft_id)
    attachments = load_attachment_bytes(db, draft_id)
    try:
        result = transmit_approved(
            db, auth, to_jid=second["to_jid"], body=second["body"],
            attachments=attachments, url=url, token=token)
    except AmbiguousSendError as exc:
        # The POST was attempted. Leave the row INERT (`sending`) so a retry
        # cannot send it twice; only `reconcile` can settle it.
        _record_error(db, draft_id, ob.STATE_SENDING, exc)
        raise SendError(
            f"draft {draft_id} was NOT recorded as failed: the POST was attempted "
            f"and its outcome is unknown, so the row is left `sending` (inert). "
            f"Run `reconcile {draft_id} --outcome sent|failed` once you know. "
            f"Cause: {exc}"
        ) from exc
    except Exception as exc:  # noqa: BLE001 — pre-network, definitively unsent
        _record_error(db, draft_id, ob.STATE_FAILED, exc)
        raise
    msg_id = (result.get("data") or {}).get("Id") or result.get("id") or result.get("Id")
    sent_at = datetime.now(timezone.utc)
    with connect(db.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"UPDATE {SCHEMA}.outbound_drafts SET state = %s, whatsapp_message_id = %s, "
            f"server_ts = %s WHERE id = %s",
            (ob.STATE_SENT, msg_id, sent_at, draft_id))
        conn.commit()
    # 🔴 AFTER the draft is durable, never before. The draft row is the record
    # that the message EXISTS; the archive row is a second copy of the same
    # fact, and it must never be able to make a sent message look unsent.
    archived = _archive_outbound(db, draft_id=draft_id, draft=second,
                                 message_id=msg_id, sent_at=sent_at,
                                 attachments=attachments)
    return {"draft_id": draft_id, "whatsapp_message_id": msg_id,
            "archived_message_row": archived, "response": result}


def reconcile(db, draft_id: int, outcome: str, message_id: str | None = None) -> None:
    """Record what happened to a draft stranded in `sending`.

    The two outcomes are `sent` and `failed`. Only the operator can know which,
    because the POST may have landed and the process died before recording it.
    """
    if outcome not in (ob.STATE_SENT, ob.STATE_FAILED, ob.STATE_RECONCILED):
        raise SendError(f"unknown reconcile outcome {outcome!r}")
    d = load_draft(db, draft_id)
    if d["state"] != ob.STATE_SENDING:
        raise SendError(f"draft {draft_id} is {d['state']!r}; only 'sending' can be reconciled")
    with connect(db.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"UPDATE {SCHEMA}.outbound_drafts SET state = %s, whatsapp_message_id = %s, "
            f"error = %s WHERE id = %s AND state = %s",
            (outcome, message_id, None if outcome == ob.STATE_SENT else "reconciled by operator",
             draft_id, ob.STATE_SENDING))
        conn.commit()


def _guess_content_type(path: Path) -> str:
    suffix = path.suffix.lower()
    return {
        ".md": "text/markdown", ".txt": "text/plain", ".csv": "text/csv",
        ".json": "application/json", ".pdf": "application/pdf",
        ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    }.get(suffix, "application/octet-stream")


#: 🔴 ONE DEFAULT, TWO CALL SITES. The `/healthz` handler and `serve()` both read
#: this env var, and they once spelled their fallbacks separately — with different
#: values. The result: with the env var unset, `serve()` wrote
#: `/tmp/whatsapp-sender-ok` while the probe looked for `/tmp/ok`, so the pod
#: reported UNHEALTHY forever and never became Ready. The Deployment set the
#: variable, which is exactly why it went unnoticed. Pin the default in one place.
HEARTBEAT_ENV = "WHATSAPP_SENDER_HEARTBEAT"
DEFAULT_HEARTBEAT = "/tmp/whatsapp-sender-ok"


# --------------------------------------------------------------------------- #
# The pod's server — a health endpoint, and NOTHING else. No send route.
# --------------------------------------------------------------------------- #
class _Handler(BaseHTTPRequestHandler):
    server_version = "whatsapp-sender/1"

    def log_message(self, fmt, *args):  # noqa: A003
        pass

    def do_GET(self):  # noqa: N802
        if self.path.split("?", 1)[0] != "/healthz":
            self._send(404, {"error": "not found"})
            return
        ok = os.path.exists(os.environ.get(HEARTBEAT_ENV, DEFAULT_HEARTBEAT))
        self._send(200 if ok else 503, {"healthy": ok})

    def _send(self, code, payload):
        blob = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(blob)))
        self.end_headers()
        self.wfile.write(blob)


def serve(port: int = 8081) -> None:
    """Run the pod. Exposes /healthz only — the operator acts via `kubectl exec`."""
    heartbeat = os.environ.get(HEARTBEAT_ENV, DEFAULT_HEARTBEAT)
    Path(heartbeat).write_text("ok", encoding="utf-8")
    ThreadingHTTPServer(("0.0.0.0", port), _Handler).serve_forever()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="sender.py", description=__doc__.splitlines()[0])
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("draft", help="compose an outbound draft (transmits NOTHING)")
    d.add_argument("to_jid")
    d.add_argument("--body", required=True)
    d.add_argument("--attach", action="append", default=[], metavar="FILE")

    ls = sub.add_parser("ls", help="list drafts")
    ls.add_argument("--state", default=None)

    a = sub.add_parser("approve", help="record the operator's approval for a draft")
    a.add_argument("draft_id", type=int)
    a.add_argument("--ref", required=True,
                   help="an external reference the approval is recorded under")

    ua = sub.add_parser("unapprove", help="withdraw an approval (approved -> pending)")
    ua.add_argument("draft_id", type=int)

    s = sub.add_parser("send", help="transmit an APPROVED draft (gated)")
    s.add_argument("draft_id", type=int)

    rc = sub.add_parser("reconcile", help="record the outcome of a draft stranded in 'sending'")
    rc.add_argument("draft_id", type=int)
    rc.add_argument("--outcome", required=True, choices=[ob.STATE_SENT, ob.STATE_FAILED, ob.STATE_RECONCILED])
    rc.add_argument("--message-id", default=None)

    sv = sub.add_parser("serve", help="run the pod (health endpoint only)")
    sv.add_argument("--port", type=int, default=8081)

    args = p.parse_args(argv)
    if args.cmd == "serve":
        serve(args.port)
        return 0

    ensure_schema()
    from _wa_db import WaDB
    db = WaDB()

    if args.cmd == "draft":
        draft_id = draft(db, args.to_jid, args.body, args.attach)
        row = load_draft(db, draft_id)
        print(f"draft {draft_id} -> {row['to_jid']}  state={row['state']}")
        print(f"  body ({len(row['body'])} chars): {row['body']}")
        for fn, ct, sha, size in row["attachments"]:
            print(f"  attach: {fn} ({ct}, {size} B) sha256={sha[:16]}…")
        print(f"  digest={ob.draft_payload_digest(row)}")
        print("  NOTHING SENT. Approve it, then send it.")
        return 0
    if args.cmd == "ls":
        with cursor(db.dsn) as cur:
            q = (f"SELECT id, state, to_jid, left(body, 50) AS body_preview, created_at "
                 f"FROM {SCHEMA}.outbound_drafts")
            params = ()
            if args.state:
                q += " WHERE state = %s"
                params = (args.state,)
            cur.execute(q + " ORDER BY id DESC LIMIT 50", params)
            # `_wa_db.cursor()` uses RealDictCursor (unlike the plain
            # `conn.cursor()` the write paths use), so these rows are MAPPINGS and
            # `r[0]` is a KeyError, not the first column.
            for r in cur.fetchall():
                print(f"{r['id']:>5}  {r['state']:<11} {r['to_jid']:<26} {r['body_preview']!r}")
        return 0
    if args.cmd == "approve":
        digest = approve(db, args.draft_id, args.ref)
        print(f"draft {args.draft_id} APPROVED under {args.ref}; digest={digest[:16]}…")
        return 0
    if args.cmd == "unapprove":
        unapprove(db, args.draft_id)
        print(f"draft {args.draft_id} -> pending")
        return 0
    if args.cmd == "send":
        out = send(db, args.draft_id)
        print(f"draft {args.draft_id} SENT; whatsapp_message_id={out['whatsapp_message_id']!r}")
        if out["archived_message_row"] is None:
            print("  ⚠ NOT archived into whatsapp.messages — see the archive: "
                  "line above; the message IS sent, backfill from this draft.")
        else:
            print(f"  archived: whatsapp.messages row {out['archived_message_row']}")
        return 0
    if args.cmd == "reconcile":
        reconcile(db, args.draft_id, args.outcome, args.message_id)
        print(f"draft {args.draft_id} reconciled -> {args.outcome}")
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
