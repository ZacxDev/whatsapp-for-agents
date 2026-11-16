#!/usr/bin/env python3
"""Pure parsing helpers for the WhatsApp webhook payloads.

Deliberately dependency-free and free of I/O: every function here takes a dict
and returns a dict/str/tuple. That is what lets the whole suite run hermetically
and what makes the LID/PN identity rules (the highest-risk correctness area in
this module) testable without Postgres, MinIO or a phone.

THE PAYLOAD COMES FROM `asternic/wuzapi` (whatsmeow). Shapes were read from its
API.md/examples, NOT invented — but the wire format is jsontag-serialised Go
structs, so EVERY accessor here is defensive: a missing field degrades to
`unknown`/`None`, it never raises. An exception inside a webhook handler is a
message we never store, and WhatsApp history is not re-fetchable.

Identity model (read this before touching `identity_from_info`):

    WhatsApp addresses a person by EITHER a phone number (`<digits>@s.whatsapp.net`,
    "PN") OR an opaque LID (`<digits>@lid`). Since the 2025 LID migration the same
    human arrives as both, in either order, and LID -> PN is NOT always resolvable.
    whatsmeow exposes the second form as `SenderAlt` when it knows both.

    Consequence: keying contacts on a single JID string produces two rows for one
    person, which breaks dedupe of echoed messages and splits conversation
    history. `identity_from_info` therefore returns BOTH addresses, and the DB
    layer merges them onto one contact row when it sees a dual event.
"""
from __future__ import annotations

from datetime import datetime, timezone

WA_SERVER = "s.whatsapp.net"
LID_SERVER = "lid"
GROUP_SERVER = "g.us"
BROADCAST_SERVER = "broadcast"
NEWSLETTER_SERVER = "newsletter"

# Wrapper message kinds that CONTAIN the real message one level down.
_UNWRAP_KEYS = (
    "ephemeralMessage",
    "viewOnceMessage",
    "viewOnceMessageV2",
    "viewOnceMessageV2Extension",
    "documentWithCaptionMessage",
    "editedMessage",
)

# kind -> the key holding the human-readable text, if any. Every entry is a
# MESSAGE OBJECT (a dict); `conversation` is a bare string and is handled
# separately in `classify`, because a dict-only loop silently skips it —
# which is exactly the bug this comment exists to stop being reintroduced.
_TEXT_KEYS = (
    ("extendedTextMessage", "text"),
    ("imageMessage", "caption"),
    ("videoMessage", "caption"),
    ("documentMessage", "caption"),
    ("audioMessage", None),
    ("pttMessage", None),
    ("stickerMessage", None),
    ("locationMessage", None),
    ("liveLocationMessage", None),
    ("contactMessage", None),
    ("contactsArrayMessage", None),
    ("pollCreationMessage", None),
    ("pollCreationMessageV2", None),
    ("pollCreationMessageV3", None),
    ("groupInviteMessage", None),
    ("buttonsMessage", None),
    ("templateMessage", None),
    ("listMessage", None),
    ("interactiveMessage", None),
)

# kind names that are NOT the same as their protobuf key.
_KIND_ALIAS = {
    "pttMessage": "voice",
    "conversation": "text",
    "extendedTextMessage": "text",
    "imageMessage": "image",
    "videoMessage": "video",
    "audioMessage": "audio",
    "documentMessage": "document",
    "stickerMessage": "sticker",
    "locationMessage": "location",
    "liveLocationMessage": "live_location",
    "contactMessage": "contact",
    "contactsArrayMessage": "contact",
    "pollCreationMessage": "poll",
    "pollCreationMessageV2": "poll",
    "pollCreationMessageV3": "poll",
    "groupInviteMessage": "group_invite",
    "buttonsMessage": "interactive",
    "templateMessage": "interactive",
    "listMessage": "interactive",
    "interactiveMessage": "interactive",
    "reactionMessage": "reaction",
    "protocolMessage": "protocol",
    "senderKeyDistributionMessage": "protocol",
}


def normalize_jid(value: str | None) -> str | None:
    """Strip a DEVICE suffix, keep the server.

    whatsmeow reports e.g. `5491155554444.0:52@s.whatsapp.net`. The `:52` is a
    device id and the `.0` an agent id; neither identifies a person. Group JIDs
    (`1203...-123...@g.us`) contain `-` but never `.`, so the `.` rule is gated on
    the PN server to avoid mangling them.
    """
    if not value or not isinstance(value, str):
        return None
    local, sep, server = value.partition("@")
    if not sep:
        return value.strip() or None
    local = local.split(":", 1)[0]
    if server == WA_SERVER and "." in local:
        local = local.split(".", 1)[0]
    return f"{local}@{server}"


def jid_kind(jid: str | None) -> str:
    if not jid:
        return "unknown"
    _, _, server = jid.partition("@")
    if server == WA_SERVER:
        return "pn"
    if server == LID_SERVER:
        return "lid"
    if server == GROUP_SERVER:
        return "group"
    if server == BROADCAST_SERVER:
        return "broadcast"
    if server == NEWSLETTER_SERVER:
        return "newsletter"
    return "unknown"


def chat_type(jid: str | None) -> str:
    kind = jid_kind(jid)
    return {
        "pn": "dm",
        "lid": "dm",
        "group": "group",
        "broadcast": "broadcast",
        "newsletter": "newsletter",
    }.get(kind, "unknown")


def bare_address(jid: str | None) -> str | None:
    """`1234@s.whatsapp.net` -> `1234`; groups keep their full JID."""
    jid = normalize_jid(jid)
    if not jid:
        return None
    local, _, server = jid.partition("@")
    if server == WA_SERVER or server == LID_SERVER:
        return local
    return jid


def identity_from_info(info: dict) -> dict:
    """Return {'pn': .., 'lid': ..} for the SENDER of a message.

    `Sender` and `SenderAlt` may carry the same human in two encodings, in either
    order. `Participant` is the group member on group messages when `Sender` is
    the group itself.
    """
    pn = lid = None
    raw = info.get("Sender") or info.get("Participant") or info.get("SenderAlt")
    alt = info.get("SenderAlt") or info.get("RecipientAlt")

    for candidate in (raw, alt):
        jid = normalize_jid(candidate if isinstance(candidate, str) else None)
        if not jid:
            continue
        kind = jid_kind(jid)
        if kind == "pn" and pn is None:
            pn = bare_address(jid)
        elif kind == "lid" and lid is None:
            lid = bare_address(jid)

    return {"pn": pn, "lid": lid}


def parse_timestamp(value) -> datetime | None:
    """whatsmeow serialises MessageInfo.Timestamp as RFC3339 (string).

    Accepts a numeric epoch too, because some wrappers hand back milliseconds.
    Returns an aware UTC datetime, or None — never raises.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        seconds = value / 1000.0 if value > 10_000_000_000 else float(value)
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        # 🔴 A NUMERIC STRING IS AN EPOCH, NOT AN ISO DATE — and this case is not
        # hypothetical: protojson renders int64 fields as STRINGS, so a history
        # row can arrive as `"messageTimestamp": "1794247443"`. Without this branch
        # `fromisoformat` raises, the caller falls back to `now()`, and 3,755
        # backfilled messages all get today's timestamp while looking fine.
        if text.isdigit():
            return parse_timestamp(int(text))
        # Python < 3.11 chokes on a trailing Z for fromisoformat.
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def unwrap(message: dict) -> tuple[dict, str]:
    """Peel container wrappers. Returns (innermost_message, wrapper_kind)."""
    current = message or {}
    wrapper = ""
    for _ in range(8):  # bounded: a hostile payload must not spin here
        hit = next((k for k in _UNWRAP_KEYS if isinstance(current.get(k), dict)), None)
        if not hit:
            break
        wrapper = wrapper or hit
        inner = current[hit].get("message")
        if not isinstance(inner, dict):
            break
        current = inner
    return current, wrapper


def classify(message: dict) -> dict:
    """Map a waE2E.Message dict to (kind, body, caption).

    Returns the FIRST recognised key. Unknown shapes come back as `unknown` with
    any recoverable text — callers store `raw`, so nothing is lost.
    """
    inner, wrapper = unwrap(message)
    if not inner:
        return {"kind": "unknown", "body": None, "caption": None, "wrapper": wrapper}

    # `conversation` is a bare STRING, not a message object.
    plain = inner.get("conversation")
    if isinstance(plain, str):
        return {"kind": "text", "body": plain, "caption": None, "wrapper": wrapper}

    for key, text_key in _TEXT_KEYS:
        payload = inner.get(key)
        if not isinstance(payload, dict):
            continue
        body = caption = None
        if text_key:
            text = payload.get(text_key)
            if isinstance(text, str) and text:
                body = text
                caption = text
        return {
            "kind": _KIND_ALIAS.get(key, "unknown"),
            "body": body,
            "caption": caption,
            "wrapper": wrapper,
        }

    for key in ("reactionMessage", "protocolMessage", "senderKeyDistributionMessage"):
        if isinstance(inner.get(key), dict):
            return {
                "kind": _KIND_ALIAS[key],
                "body": None,
                "caption": None,
                "wrapper": wrapper,
            }

    return {"kind": "unknown", "body": None, "caption": None, "wrapper": wrapper}


def reaction_info(message: dict) -> dict | None:
    """`reactionMessage` -> {target_id, emoji, removed}."""
    inner, _ = unwrap(message)
    reaction = inner.get("reactionMessage") if isinstance(inner, dict) else None
    if not isinstance(reaction, dict):
        return None
    key = reaction.get("key") if isinstance(reaction.get("key"), dict) else {}
    return {
        "target_id": key.get("ID") or key.get("id"),
        "emoji": reaction.get("text") if isinstance(reaction.get("text"), str) else "",
        "removed": not bool(reaction.get("text")),
    }


def protocol_info(message: dict) -> dict | None:
    """`protocolMessage` -> {type, target_id, is_revoke, is_edit}.

    whatsmeow's waE2E.ProtocolMessage.Type is an enum: 0 = REVOKE, 14 = MESSAGE_EDIT.
    Both values are named rather than guessed at the call site.
    """
    inner, _ = unwrap(message)
    proto = inner.get("protocolMessage") if isinstance(inner, dict) else None
    if not isinstance(proto, dict):
        return None
    key = proto.get("key") if isinstance(proto.get("key"), dict) else {}
    return {
        "type": proto.get("type"),
        "target_id": key.get("ID") or key.get("id"),
        "is_revoke": proto.get("type") == 0,
        "is_edit": proto.get("type") == 14,
    }


def media_filename(info: dict, kind: str, index: int = 0) -> str:
    """A stable, filesystem-safe basename when the payload does not supply one."""
    msg_id = info.get("ID") or info.get("id") or f"anon{index}"
    safe = "".join(c for c in str(msg_id) if c.isalnum() or c in "-_") or "anon"
    ext = {
        "image": "jpg",
        "video": "mp4",
        "audio": "ogg",
        "voice": "ogg",
        "document": "bin",
        "sticker": "webp",
    }.get(kind, "bin")
    return f"{safe}.{ext}"


def quoted_id(message: dict) -> str | None:
    """The id of the message this one replies to, if any.

    WhatsApp puts `contextInfo.stanzaID` inside whichever content node carries the
    reply, so this walks the unwrapped message's children rather than assuming a
    single key.
    """
    inner, _ = unwrap(message)
    if not isinstance(inner, dict):
        return None
    for payload in inner.values():
        if not isinstance(payload, dict):
            continue
        ctx = payload.get("contextInfo")
        if isinstance(ctx, dict):
            stanza = ctx.get("stanzaID") or ctx.get("stanzaId")
            if isinstance(stanza, str) and stanza:
                return stanza
    return None


def history_pn_lid_pairs(payload: dict) -> list[tuple[str, str]]:
    """`(pn, lid)` pairs from `Data.phoneNumberToLidMappings`.

    🔴 THE KEYS ARE `pnJID` / `lidJID` — NOT `pn` / `lid`. Assuming the short names
    returned ZERO pairs from payloads that each carry 100 of them, which is exactly
    why a person stayed two identities: the bridge existed in data we had already
    stored and nothing read it.

    This mapping is WhatsApp's own statement that the two addresses are one human,
    which makes it authoritative evidence for merging — not a heuristic guess.
    """
    if not isinstance(payload, dict):
        return []
    event = payload.get("event") if isinstance(payload.get("event"), dict) else {}
    data = event.get("Data") if isinstance(event.get("Data"), dict) else {}
    out: list[tuple[str, str]] = []
    for entry in data.get("phoneNumberToLidMappings") or []:
        if not isinstance(entry, dict):
            continue
        pn = normalize_jid(entry.get("pnJID") or entry.get("pn"))
        lid = normalize_jid(entry.get("lidJID") or entry.get("lid"))
        if pn and lid:
            out.append((pn, lid))
    return out


def chat_label(conv: dict) -> tuple[str | None, str | None]:
    """`(display_name, username)` for a history conversation.

    🔴 THE FIELD IS `displayName` (with `username` beside it), and the first parser
    looked for `name`/`subject` — so all 119 captured chats ended up unnamed even
    though the payloads carry names for most of them. `username` is kept separately
    because it is a stable handle that survives a display-name change.
    """
    if not isinstance(conv, dict):
        return None, None
    name = conv.get("displayName") or conv.get("name") or conv.get("subject")
    username = conv.get("username")
    return (
        name if isinstance(name, str) and name else None,
        username if isinstance(username, str) and username else None,
    )


def history_conversations(payload: dict) -> list[dict]:
    """Conversations from a `type: HistorySync` payload.

    🔴 THE SHAPE IS `event.Data.conversations` — NOT `event.conversations`.
    The first parser looked only at the top level and therefore counted ZERO
    conversations in every payload, including the ones carrying 3,755 messages.
    Measured on real data: `event` holds exactly `{Data, Notification}`, and the
    conversations live one level down inside `Data`, alongside `syncType`.

    `Data.syncType` decides whether a payload carries messages at all (whatsmeow's
    enum): 0 INITIAL_BOOTSTRAP and 3 RECENT do; 4 PUSH_NAME and 1
    INITIAL_STATUS_V3 do not; 2 FULL arrives as a header with only `progress`.
    A payload with conversations and no messages is therefore normal, not a fault.
    """
    if not isinstance(payload, dict):
        return []
    event = payload.get("event") if isinstance(payload.get("event"), dict) else {}
    for container in (event.get("Data"), event.get("data"), event):
        if not isinstance(container, dict):
            continue
        found = container.get("conversations") or container.get("Conversations")
        if isinstance(found, list):
            return [c for c in found if isinstance(c, dict)]
    return []


def pushnames(payload: dict) -> dict:
    """`{jid: pushname}` from a PUSH_NAME (syncType 4) payload.

    Worth harvesting: history messages carry `pushName: null`, so without this a
    backfilled contact has no name until that person next messages.
    """
    if not isinstance(payload, dict):
        return {}
    event = payload.get("event") if isinstance(payload.get("event"), dict) else {}
    data = event.get("Data") if isinstance(event.get("Data"), dict) else {}
    out: dict = {}
    for entry in data.get("pushnames") or []:
        if isinstance(entry, dict) and entry.get("ID") and entry.get("pushname"):
            jid = normalize_jid(entry["ID"])
            if jid:
                out[jid] = entry["pushname"]
    return out


def from_web_message(wmi: dict, *, fallback_chat: str | None = None,
                     push_name: str | None = None,
                     account_jid: str | None = None) -> dict:
    """Wrap a history `WebMessageInfo` in the same envelope a live event uses.

    This is what lets history and live delivery share ONE ingest path: the stored
    rows are indistinguishable whichever side they came from.

    🔴 `fromMe` HISTORY ROWS GET NO SENDER UNLESS `account_jid` IS SUPPLIED.
    For an inbound message the peer is the chat, so `Chat` is the sender. For an
    OUTBOUND one the sender is the account itself, and history does not carry it —
    so attributing the chat's JID would name the RECIPIENT as the author. NULL is
    the honest answer; `from_me` still records the direction correctly.
    """
    key = wmi.get("key") if isinstance(wmi.get("key"), dict) else {}
    chat = normalize_jid(
        key.get("remoteJID") or key.get("remoteJid") or fallback_chat
    )
    from_me = bool(key.get("fromMe"))

    info: dict = {
        "ID": key.get("ID") or key.get("id"),
        "Chat": chat,
        "IsFromMe": from_me,
        "Timestamp": wmi.get("messageTimestamp"),
        "PushName": wmi.get("pushName") or push_name,
    }
    participant = key.get("participant") or wmi.get("participant")
    if participant:
        info["Sender"] = participant
    elif from_me:
        if account_jid:
            info["Sender"] = account_jid
    elif chat and not chat.endswith(f"@{GROUP_SERVER}"):
        info["Sender"] = chat

    return {"event": {"Info": info, "Message": wmi.get("message") or {}}}


def strip_blobs(payload: dict) -> dict:
    """Copy of the payload with base64 media removed.

    `raw` is stored in Postgres; a 16 MB video becomes ~21 MB of base64 and would
    bloat the table on every redelivery. The bytes live in MinIO, the row keeps
    the metadata.
    """
    if not isinstance(payload, dict):
        return {}
    out = {k: v for k, v in payload.items() if k != "base64"}
    event = out.get("event")
    if isinstance(event, dict):
        out["event"] = {k: v for k, v in event.items() if k != "base64"}
    return out