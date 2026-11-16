#!/usr/bin/env python3
"""Outbound WhatsApp gate — compose and transmit are SEPARATE calls.

The two halves are different invocations, minutes or hours apart, with a durable
row between them, and the approval is bound to the EXACT payload that was shown
to the operator.

Why this is a separate module from `consumer.py`, and why the sender is a
separate Deployment with its own token
----------------------------------------------------------------------------
The ingester parses arbitrary, attacker-influenced payloads off the internet. It
was built with **no send credential** on purpose, so a parser bug or a poisoned
payload can never reach a send endpoint. That property is preserved here by
putting every outbound byte in `sender.py` (its own Deployment, its own token)
and keeping this module free of I/O: nothing in it opens a socket, and the only
function that POSTs lives in `sender.py`.

⚠ THE ROLE SPLIT IS THE OTHER HALF OF THIS, and it is easy to skip. If the
consumer's DB role can read wuzapi's session store, then "the ingester has no
send credential" is true at the POD level and false at the DATABASE level.
`deploy/` puts wuzapi in its OWN database and gives the ingester role no access
to it. Keep that split.

The gate, in one paragraph
--------------------------
`draft()` stores a `pending` row and transmits NOTHING. `approve()` is an
operator-only command that records a digest of exactly what was displayed.
`send()` mints a one-shot `SendAuthorization` — refused unless the row is
`approved` AND still hashes to the recorded digest — then spends it immediately
before the network call. A row edited between approve and send therefore cannot
be transmitted under the old approval, which is the whole point.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping

# --------------------------------------------------------------------------- #
# States — these mirror the CHECK constraint on whatsapp.outbound_drafts.
# --------------------------------------------------------------------------- #
STATE_PENDING = "pending"
STATE_APPROVED = "approved"
STATE_SENDING = "sending"
STATE_SENT = "sent"
STATE_FAILED = "failed"
STATE_RECONCILED = "reconciled"

#: A draft found in `sending` is inert until reconciled — the process may have
#: POSTed and died before recording the result, so we cannot know it is unsent.
INERT_STATES = (STATE_SENDING,)

#: The operator-only token `approve()` requires. A speed bump against an agent
#: approving its own draft, NOT proof of humanity.
APPROVAL_TOKEN_ENV = "WHATSAPP_APPROVAL_TOKEN"

# --------------------------------------------------------------------------- #
# Length policy — THE POINT OF THIS MODULE'S SECOND JOB.
#
# WhatsApp is a short-to-medium medium. A long, well-structured wall of text is
# not "thorough" here, it is unreadable, and it is the failure mode an agent
# falls into by default. So the ceiling is enforced at DRAFT time: a draft whose
# body exceeds MAX_BODY_CHARS must carry the long content as an ATTACHMENT and
# keep the body as a short cover note.
#
# 🔴 ENFORCED AT DRAFT, NOT AT SEND. A refusal at send time would be a refusal
# after the operator already approved the text; the correction has to happen
# while it is still a draft, and the length is part of what the digest covers.
# --------------------------------------------------------------------------- #
#: A normal reply's ceiling, in characters. CALIBRATED, NOT GUESSED — measured
#: over the operator's own 815 sent messages with prose: median 17 chars, p90 72,
#: p95 114, p99 257, and only 4/815 (0.5%) over 300. So 300 is his p99.5: a body
#: above it is not a shorter version of how he writes, it is a different medium.
MAX_BODY_CHARS = 300
#: When the body is a cover note for an attachment it must be much shorter —
#: his p95 is 114 chars, so 150 keeps a cover note inside his normal range.
MAX_COVER_NOTE_CHARS = 150
#: Per-draft attachment cap: bounds a Postgres row and one non-streamed body.
MAX_DRAFT_ATTACHMENT_BYTES = 8 * 1024 * 1024


class SendGateError(Exception):
    """Raised when a draft cannot be transmitted. Never swallowed."""


class LengthPolicyError(SendGateError):
    """A draft body is too long for WhatsApp and carries no attachment."""


class AttachmentError(SendGateError):
    """An attachment row is missing or unreadable."""


class SendAuthorization:
    """A one-shot capability to transmit one specific payload.

    🔴 Cannot be constructed directly. `_mint_send_authorization()` mints it for
    an `approved` draft that still hashes to its approval digest, and
    `spend_authorization()` makes it single-use. An un-approved draft therefore
    has no way to obtain the object `transmit_approved()` requires.
    """

    def __init__(self, *_a, **_kw):  # pragma: no cover - exercised via the gate
        raise SendGateError(
            "SendAuthorization cannot be constructed directly — it is minted "
            "only by _wa_outbound._mint_send_authorization() for a draft whose "
            "state is 'approved' and whose payload still matches its approval "
            "digest (D3 approval gate)"
        )

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<SendAuthorization draft={self.draft_id} nonce=…>"


_ISSUED_NONCES: set[str] = set()


# --------------------------------------------------------------------------- #
# Attachments — the fingerprint is what the approval binds
# --------------------------------------------------------------------------- #
def attachment_sha256(data) -> str:
    if not isinstance(data, (bytes, bytearray)):
        raise AttachmentError(f"attachment bytes must be bytes, got {type(data).__name__}")
    return hashlib.sha256(bytes(data)).hexdigest()


def canonical_attachments(attachments) -> tuple:
    """Normalise to a sorted tuple of `(filename, content_type, sha256, size)`.

    🔴 THE FILENAME IS PART OF THE DIGEST. It is what the recipient sees, and
    renaming it changes the act being approved, so it cannot be swapped after
    approval. Order is normalised so a round-trip through SQL (which has no
    order guarantee without an ORDER BY) cannot change the digest.
    """
    rows = []
    for item in attachments or ():
        data = None
        if isinstance(item, Mapping):
            # 🔴 BOTH SHAPES MUST FINGERPRINT IDENTICALLY. The approve side reads
            # SQL rows (filename, content_type, sha256, size); the send side loads
            # the same rows WITH their bytes as mappings. Slicing a mapping in the
            # tuple branch raised `KeyError: slice(None, 4, None)` on the first
            # live transmit — the mint succeeded and the spend refused, so nothing
            # was sent. A unit test could not see it: nothing had ever passed a
            # bytes-carrying mapping through this function.
            filename = item.get("filename")
            content_type = item.get("content_type")
            digest = item.get("sha256")
            size = item.get("size")
            data = item.get("bytes")
        else:
            try:
                filename, content_type, digest, size = item[:4]
            except (TypeError, ValueError) as exc:
                raise AttachmentError(f"malformed attachment row: {item!r}") from exc
        if not filename:
            raise AttachmentError("attachment row has an empty filename")
        if data is not None:
            # Hash from the BYTES rather than trusting a supplied digest, so a
            # bytes-carrying row yields the same sha256/size it was stored with.
            digest = attachment_sha256(data)
            size = len(data)
        if not digest:
            # FAILS CLOSED: a deleted/truncated/NULLed row is indistinguishable
            # from "the writer we are guarding against removed the file".
            raise AttachmentError(f"attachment {filename!r} has no readable sha256")
        rows.append((str(filename), str(content_type or "application/octet-stream"),
                     str(digest), int(size)))
    return tuple(sorted(rows))


def validated_attachments(attachments) -> list:
    """Validate real attachment rows (with bytes) before a draft is stored."""
    rows, total, seen = [], 0, set()
    for item in attachments or ():
        filename = item.get("filename")
        data = item.get("bytes")
        if not filename:
            raise AttachmentError("every attachment needs a filename")
        if filename in seen:
            raise AttachmentError(f"duplicate attachment filename {filename!r}")
        seen.add(filename)
        digest = attachment_sha256(data)
        total += len(data)
        rows.append({
            "filename": filename,
            "content_type": item.get("content_type") or "application/octet-stream",
            "sha256": digest,
            "bytes": data,
            "size": len(data),
        })
    if total > MAX_DRAFT_ATTACHMENT_BYTES:
        raise AttachmentError(
            f"attachments total {total} bytes, over the {MAX_DRAFT_ATTACHMENT_BYTES}"
            "-byte per-draft cap (_wa_outbound.MAX_DRAFT_ATTACHMENT_BYTES)"
        )
    return rows


# --------------------------------------------------------------------------- #
# The digest — one definition, used by BOTH the approve and the send side
# --------------------------------------------------------------------------- #
def payload_digest(*, to_jid, body, attachments=()) -> str:
    """Stable fingerprint of the things that make up a sent WhatsApp message.

    Canonical JSON with sorted keys, so the digest depends on the VALUES and not
    on dict ordering or on whether `attachments` arrived as `None`, `()` or a
    list. Not a security primitive — it defends against the realistic case (a
    second writer, a buggy job, an agent editing a draft it did not approve),
    where the row changes and the digest does not.

    🔴 The `attachments` key is omitted when there are none, so
    a text-only draft's digest is stable whether or not the caller passed `()`.
    """
    document = {"recipient": to_jid, "body": body}
    fingerprint = canonical_attachments(attachments)
    if fingerprint:
        document["attachments"] = [list(a) for a in fingerprint]
    payload = json.dumps(document, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def draft_payload_digest(draft) -> str:
    """The approval digest OF A DRAFT ROW — one definition for both sides."""
    d = draft or {}
    return payload_digest(to_jid=d.get("to_jid"), body=d.get("body"),
                          attachments=d.get("attachments"))


# --------------------------------------------------------------------------- #
# The length policy
# --------------------------------------------------------------------------- #
def enforce_length_policy(body: str, attachments=()) -> None:
    """Refuse a draft that is too long for the medium.

    Two rules, both checked at DRAFT time:

    1. No attachment: the body must fit MAX_BODY_CHARS.
    2. With an attachment: the body is a COVER NOTE and must fit
       MAX_COVER_NOTE_CHARS — the detail belongs in the file, not above it.

    The error names the remedy, because a bare "too long" gets worked around by
    raising the constant.
    """
    text = body or ""
    has_attachment = bool(list(attachments or ()))
    ceiling = MAX_COVER_NOTE_CHARS if has_attachment else MAX_BODY_CHARS
    if len(text) <= ceiling:
        return
    raise LengthPolicyError(
        f"body is {len(text)} characters, over the {ceiling}-character ceiling "
        f"for a {'cover note' if has_attachment else 'WhatsApp message'}. "
        f"WhatsApp is a short-to-medium medium and a wall of text does not get "
        f"read. Shorten the body"
        + (", or move the detail into the attachment and leave a one-line note."
           if has_attachment else
           " — or, if the length is the point, put the detail in an ATTACHMENT "
           "and keep the body as a short cover note.")
        + f" (MAX_BODY_CHARS={MAX_BODY_CHARS}, "
          f"MAX_COVER_NOTE_CHARS={MAX_COVER_NOTE_CHARS})"
    )


# --------------------------------------------------------------------------- #
# The archive row for something WE sent
# --------------------------------------------------------------------------- #
def archive_chat_type(jid: str | None) -> str:
    """`dm` / `group` / … for an OUTBOUND jid.

    🔴 DELIBERATELY DUPLICATED FROM `_wa_parse.chat_type`, and pinned by a test.
    `_wa_parse` is an INGEST module and the sender image must not carry one
    (`tests/test_image_split.py` asserts that in both directions), so this module
    — which the sender DOES ship — needs its own copy.
    `test_outbound.test_the_two_chat_type_rules_agree_over_a_jid_table` compares
    the two over a table of JIDs, so the copy cannot drift silently.
    """
    jid = jid or ""
    if jid.endswith("@g.us"):
        return "group"
    if jid.endswith("@broadcast"):
        return "broadcast"
    if jid.endswith("@newsletter"):
        return "newsletter"
    if jid.endswith("@s.whatsapp.net") or jid.endswith("@lid"):
        return "dm"
    return "unknown"


def outbound_archive_row(*, draft_id: int, to_jid: str, body: str,
                         whatsapp_id: str, sent_at, attachments=()) -> dict:
    """The `whatsapp.messages` fields a successful send must leave behind.

    🔴 WHY THIS EXISTS: wuzapi does NOT echo messages sent through its own API
    back as webhooks. Measured on the first live send — `webhook_deliveries`
    gained no row and wuzapi logged no `Calling … webhook` at all. So without
    this, every reply an agent sends is MISSING from the archive, and the more
    the send path is used the less complete the archive becomes, exactly where
    it is most interesting.

    `kind` / `body` / `caption` mirror `_wa_parse.classify`, which is the shape
    the archive already holds: a text send carries the body, and an attachment
    send carries it in BOTH `body` and `caption` — because `classify` does that
    for an inbound media message, and because `body` is what the generated
    `search` tsvector is built from. A caption kept out of `body` would make our
    own sent documents unsearchable.

    Written through `_wa_db.insert_message`, which upserts on
    `(chat_id, whatsapp_id)`. So if wuzapi ever DOES start echoing, the echo
    updates this row instead of creating a duplicate.
    """
    has_attachment = bool(list(attachments or ()))
    return {
        "jid": to_jid,
        "chat_type": archive_chat_type(to_jid),
        "whatsapp_id": whatsapp_id,
        # None, not a contact row: the author of a `from_me` message is the
        # LINKED ACCOUNT itself. Inventing a contact for the operator would put
        # them into the PN/LID identity-merge machinery, which exists to stop
        # OTHER PEOPLE becoming two contacts.
        "sender_contact_id": None,
        "from_me": True,
        "kind": "document" if has_attachment else "text",
        "body": body,
        "caption": body if has_attachment else None,
        "quoted_whatsapp_id": None,
        "message_ts": sent_at,
        "is_view_once": False,
        "expires_at": None,
        # Provenance only. The message content is in `body`/`caption`; echoing
        # it into `raw` as well would double-store every outbound body.
        "raw": {"source": "sender.py", "draft_id": draft_id},
    }


def outbound_attachment_rows(kind: str, attachments) -> list[dict]:
    """Archive metadata for an outbound attachment — WITHOUT its bytes.

    The sender holds no MinIO credential, deliberately: the pod that can post is
    not the pod that can write the object store. Its bytes are therefore not
    archived, but the row is recorded anyway so the archive says "this message
    carried a 468 B document, and here is why the bytes are not here" rather
    than falling silent — the same shape the consumer uses for a policy skip.
    """
    rows = []
    for a in attachments or ():
        payload = a.get("bytes") or b""
        rows.append({
            "media_type": kind,
            "mimetype": a.get("content_type"),
            "filename": a.get("filename"),
            "file_size": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest() if payload else None,
            "minio_key": None,
            "download_error": "skipped: outbound — the sender holds no MinIO "
                              "credential, so an attachment we sent is not "
                              "archived (metadata only)",
        })
    return rows


# --------------------------------------------------------------------------- #
# The gate itself
# --------------------------------------------------------------------------- #
def _mint_send_authorization(draft: dict) -> SendAuthorization:
    """Mint a one-shot send capability for an APPROVED draft row.

    This is the whole gate. Raises `SendGateError` — naming the offending state
    — for any draft that is not `approved`, so an un-approved draft has no way
    to obtain the object `transmit_approved()` requires.
    """
    state = (draft or {}).get("state")
    if state != STATE_APPROVED:
        raise SendGateError(
            f"draft {(draft or {}).get('id')!r} has state={state!r}; only "
            f"{STATE_APPROVED!r} drafts may be transmitted (D3 approval gate)"
        )
    # 🔴 THE APPROVE -> MUTATE -> SEND WINDOW, CLOSED HERE. `approve` and `send`
    # are separate invocations; in between, the draft is an ordinary row that
    # anything holding the connection can rewrite. `approve_draft()` recorded a
    # digest of exactly what it displayed, and this refuses to mint for a row
    # that no longer hashes to it. FAILS CLOSED: a missing digest is a refusal,
    # not a pass, because "no digest" and "digest cleared by the writer we are
    # guarding against" are the same observation.
    recorded = (draft or {}).get("approval_digest")
    try:
        current = draft_payload_digest(draft)
    except AttachmentError as exc:
        raise SendGateError(
            f"draft {draft.get('id')!r} carries an unreadable attachment, so "
            f"what was approved cannot be recomputed: {exc}. Nothing was sent."
        ) from exc
    if not recorded:
        raise SendGateError(
            f"draft {draft.get('id')!r} is approved but carries NO approval "
            f"digest, so there is nothing to check the payload against. Either "
            f"it was approved before this binding existed, or the digest was "
            f"cleared — indistinguishable from here (D3 approval gate)"
        )
    if recorded != current:
        raise SendGateError(
            f"draft {draft.get('id')!r} CHANGED after it was approved: the row "
            f"now hashes to {current} but the approval recorded {recorded}. "
            f"Nothing was sent. Re-read the draft and re-approve it "
            f"(D3 approval gate — approve/mutate/send binding)"
        )
    auth = object.__new__(SendAuthorization)
    object.__setattr__(auth, "draft_id", draft["id"])
    object.__setattr__(auth, "to_jid", draft.get("to_jid"))
    object.__setattr__(auth, "body", draft.get("body"))
    # The attachment FINGERPRINT, not the bytes — `spend_authorization()`
    # compares fingerprints, and a second multi-megabyte copy buys nothing.
    object.__setattr__(auth, "attachments", canonical_attachments(draft.get("attachments")))
    nonce = uuid.uuid4().hex
    object.__setattr__(auth, "_nonce", nonce)
    _ISSUED_NONCES.add(nonce)
    return auth


def spend_authorization(auth: object, *, to_jid, body, attachments) -> None:
    """Consume a capability FOR A SPECIFIC PAYLOAD, or raise `SendGateError`.

    Called by `sender.transmit_approved()` immediately before it touches the
    network. Rejects (a) anything that is not a real `SendAuthorization`,
    (b) a capability that has already been spent — which is what makes an
    approved draft transmit EXACTLY ONCE — and (c) a payload that is not the one
    the capability was minted for.
    """
    if not isinstance(auth, SendAuthorization):
        raise SendGateError(
            "transmit refused: a SendAuthorization minted from an approved draft "
            "is required (D3 approval gate)"
        )
    nonce = getattr(auth, "_nonce", None)
    if nonce is None or nonce not in _ISSUED_NONCES:
        raise SendGateError(
            f"transmit refused: this SendAuthorization for draft "
            f"{getattr(auth, 'draft_id', None)!r} has already been spent. An "
            f"approved draft transmits exactly once (D3 approval gate)"
        )
    if to_jid != getattr(auth, "to_jid", None):
        raise SendGateError(
            f"transmit refused: recipient changed after the capability was "
            f"minted ({getattr(auth, 'to_jid', None)!r} -> {to_jid!r})"
        )
    if body != getattr(auth, "body", None):
        raise SendGateError(
            "transmit refused: body changed between mint and POST — the payload "
            "is not the one the capability was minted for (D3 approval gate)"
        )
    if canonical_attachments(attachments) != getattr(auth, "attachments", ()):
        raise SendGateError(
            "transmit refused: attachments changed between mint and POST — the "
            "payload is not the one the capability was minted for (D3 approval gate)"
        )
    _ISSUED_NONCES.discard(nonce)
