# Pairing a device, and the one-shot history

Linking this account as a WhatsApp companion device hands over the account's
**message history exactly once** — and getting that moment right is most of the
work.

## 🔴 The history is delivered ONCE

When a fresh pairing completes, WhatsApp delivers the recent history and then
**acks and deletes it server-side**. There is no re-fetch:

> A parse failure, or a rejected webhook, **at that moment**, costs the history
> **permanently**.

So the order of steps is **load-bearing**, not ceremonial:

1. The **consumer must be up and healthy** before the QR is scanned.
2. The **webhook HMAC key must be configured** before pairing (see the HMAC trap
   below) — a key that does not match means *nothing* is stored.
3. Create the wuzapi user with the history window (`days_to_sync_history`) set
   **before** starting the pairing flow — the window only applies on a **new**
   pairing.
4. Subscribe to **`Message,HistorySync` only** (see "Narrow the subscription").
5. `POST /session/connect` and scan the QR.
6. HistorySync events land in `whatsapp.history_syncs` **verbatim, before any
   parsing** — so a parser bug costs a re-parse, not the history.

### Store the raw payload BEFORE parsing

`consumer._handle_history()` **stores first, parses second**. The verbatim payload
is committed to `whatsapp.history_syncs` (with base64 blobs stripped, so the table
does not bloat) *before* anything is interpreted. A parser failure is recorded in
that row's `error` column and the raw data survives for a later re-parse.

This is not theoretical. The **first parser read the wrong level of the payload**
— `event.conversations` instead of `event.Data.conversations` — and reported
**zero** conversations in payloads that together carried **3,755 messages**.
Nothing was lost, because the raw JSON had been stored verbatim. Had it been
parsed in-flight, all 3,755 messages would have been gone.

## The history payload shape

Measured on a real pairing — the conversations nest under **`event.Data`**:

```json
{"type": "HistorySync",
 "event": {"Data": {"syncType": 3,
   "conversations": [
     {"ID": "120363123456789012@g.us", "displayName": "…",
      "messages": [
        {"msgOrderID": 1,
         "message": {                                  ← a WebMessageInfo
           "key": {"ID": "…", "fromMe": false, "remoteJID": "120363123456789012@g.us"},
           "message": {"conversation": "…"},
           "messageTimestamp": 1794247443
         }}]}]}}}
```

Each conversation's messages are wrapped as `{msgOrderID, message: WMI}`, where
`message` is a **`WebMessageInfo`** — the same structure the live-event path
carries. `_wa_parse.from_web_message()` wraps it back into the live envelope, so
history rows and live rows are **indistinguishable** in the store; they take one
ingest path.

The conversation's name fields are **`displayName`** and **`username`** — *not*
`name`/`subject`. The first parser looked for `name`/`subject`, so all **119**
captured chats ended up unnamed even though the payloads carried names for most of
them.

### `Data.syncType` decides whether a payload carries messages

whatsmeow's `HistorySyncType` enum:

| value | name | carries messages? |
|---|---|---|
| 0 | `INITIAL_BOOTSTRAP` | **yes** |
| 1 | `INITIAL_STATUS_V3` | no |
| 2 | `FULL` | no — arrives as a header with only `progress` |
| 3 | `RECENT` | **yes** |
| 4 | `PUSH_NAME` | no — carries `pushnames` instead |

⚠ **A payload with conversations and no messages is normal, not a fault.**
`_handle_history` reports it as `messages: 0, parsed: 0, error: null`.

### `PUSH_NAME` payloads are worth harvesting

A `syncType: 4` payload carries `Data.pushnames` — a list of
`{ID, pushname}` — and **history messages arrive with `pushName: null`**. Without
the push-name list, a contact backfilled from history has **no name until that
person messages again**.

The consumer applies them with `annotate_contact()`, which
**labels an existing contact and never creates one**: the push-name list is the
account's whole address book (~100 entries in a single payload), and minting a
contact per entry would fill `contacts` with people the account has never
exchanged a message with. Labelling only what already has messages keeps the
table meaning "people we have messages from".

`Data.phoneNumberToLidMappings` (also in the payload) is applied **before the
messages**: it is WhatsApp stating that a PN and a LID are one human, so applying
it up front means every sender in the payload resolves to the merged row
immediately, instead of the messages landing on two rows and being repointed
afterwards. See `docs/identity.md`.

## Recovery: `reparse-history`, do NOT re-pair

If history ever parses as empty again, the fix is to **replay what was stored**,
never to re-pair:

```bash
python3 consumer.py reparse-history --account 15550000001@s.whatsapp.net
```

It walks every stored `history_syncs` payload and re-runs the ingest path.

- **Idempotent** — message rows collide on `(chat_id, whatsapp_id)` and update in
  place, so a second run reports `updated` rather than duplicating.
- **Writes no new `history_syncs` rows** — that table is the record of what
  *arrived*, and a re-parse is not an arrival.
- **`--account`** supplies the account's own JID, used to attribute `fromMe`
  history rows. Without it those rows keep a NULL sender rather than naming the
  recipient as the author (see `_wa_parse.from_web_message`).

Measured: **3,755 / 3,755 recovered**, giving an archive spanning back to
2024-01-09.

⚠ **History contributes TEXT, not media.** The payload has no bytes, and
attachments for historical messages are not fetched — so `attachments` stays at 0
until **new** messages with media arrive. Do not read that as a fault.

## 🔴 The HMAC trap: two keys must agree

wuzapi signs each webhook with `HMAC-SHA256(key, raw_body)` in the
**`x-hmac-signature`** header, and the consumer verifies it **on the raw bytes
before parsing** (re-serialising a parsed dict changes key order and whitespace
and would invalidate every signature). The consumer accepts the signature as
**either hex or base64**, since upstream does not document the encoding.

**Two independent settings must hold the same value:**

- the wuzapi **global** key (an env setting on the api process), and
- a **per-user** key, which is separate — set with
  `POST /session/hmac/config` and the user's token.

The consumer's own key is `WHATSAPP_HMAC_KEY` (it refuses a key shorter than 32
bytes, and refuses to serve at all if it is unset).

> 🔴 If the keys diverge, `unauthenticated` climbs and **nothing is stored** — a
> failure that reads exactly like *"the integration does nothing"*. It does not
> announce itself as a signing problem. `GET /session/status` reports
> `hmac_configured`, but **`false` does not necessarily mean "unsigned"** — it
> means the signing path is a guess. **Verify with a signed probe, not by reading
> the config.**

When a delivery is refused, the consumer logs the **event type only** — never the
body, because a webhook carries private message text and logs are not the place
for it:

```
webhook REJECTED (bad HMAC): event_type=Message bytes=812 signature=present sha256=…
```

That line is what identifies *what* is being refused. Compare it against the set
you subscribed to.

## Narrow the subscription

Subscribe to **`Message` and `HistorySync` only** — the two event types the
consumer actually stores.

Other event types are **refused** by the HMAC check and then **retried** by wuzapi
(three times) before being dropped to its error queue. Measured: wuzapi sent
`ReadReceipt` and `ChatPresence` events in a form the check refuses, so the
`unauthenticated` counter climbed **purely as a function of other people typing**.
Narrowing the subscription to those two stopped the traffic.

⚠ The consequence to know: **read receipts and typing indicators are not
captured**, and would need the signing question resolved before they could be. The
consumer *does* model `ReadReceipt`/`ChatPresence` (it records them as delivered
and counts them under `ignored`) for the case where an operator widens the
subscription after fixing the signing — it does not, and should not, *require*
them to be signed to keep working.

## Getting a fresh QR

The QR rotates; a stale one is the usual reason a scan appears to do nothing.
Request it from wuzapi's session status and write the image out, then scan it
within about a minute:

```bash
# status carries the QR as a data: URL while unlinked; loggedIn flips to true after a scan
curl -s -H "Token: $WUZAPI_USER_TOKEN" \
  "http://127.0.0.1:8080/session/status" | python3 -c "
import sys, json, base64, pathlib
d = json.load(sys.stdin)['data']
print('loggedIn:', d['loggedIn'], '| jid:', d['jid'], '| hmac_configured:', d['hmac_configured'])
qr = d.get('qrcode') or ''
if qr.startswith('data:image/png;base64,'):
    pathlib.Path('whatsapp-qr.png').write_bytes(base64.b64decode(qr.split(',',1)[1]))
    print('QR written — scan it now, it expires in ~1 min')
"
```

## Gotchas that bite at pairing time

- 🔴 **Never run a second client on one device identity.** WhatsApp reads a second
  connection for one identity as a **compromise** — the failure is a logout or a
  device removal, not a merge. Hence `replicas: 1` + a `Recreate` strategy: a
  rollout cannot run two pods at once.
- **The phone must be used at least every ~14 days** or WhatsApp logs out every
  companion device, including a desktop client.
- 🔴 **Conversations in "Restricted chat" mode are structurally invisible to
  linked devices.** No setting reveals them; they simply never arrive.
- **Media arrives base64 inside the webhook**, not through wuzapi's S3 path — see
  `docs/architecture.md`.
- **`WUZAPI_AUTO_PRESENCE=unavailable`** is deliberate: with the upstream default
  the linked companion announces presence, and the operator's **own phone can stop
  receiving push notifications**.
