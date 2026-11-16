# Architecture

This repo ingests a personal WhatsApp account into a durable, queryable store and
sends replies from it — with receiving and sending kept in **two separate
processes that share a database and nothing else**.

The whole system in one picture:

```
  the phone's WhatsApp
        │  links as a companion device (multi-device)
        ▼
  asternic/wuzapi  (whatsmeow)            session credentials in its OWN database
        │  HMAC-signed webhook POST  /hook
        ▼
  consumer  ──► Postgres   schema `whatsapp`   (the archive)
            └─► S3 / MinIO bucket `whatsapp-attachments`   (media bytes)
                │
                └─ holds NO send token and NO send code

  sender  ──(the only process with a wuzapi send token)──►  wuzapi  ──►  the message
     draft ─► approve (operator) ─► send ─► reconcile
                │
                └─ writes what it sent back into `whatsapp.messages`
```

`wuzapi` is the daemon that speaks the WhatsApp multi-device protocol. It emits
each event to the consumer as a **webhook**, signed with
`HMAC-SHA256(key, raw_body)` in the `x-hmac-signature` header. The consumer
verifies that signature on the raw bytes *before* parsing, then turns each event
into rows. Nothing here talks to WhatsApp directly.

## The two halves, and why they are two

Receiving and sending are separate **images**, separate **Deployments**, and
separate **credential sets**.

| | consumer | sender |
|---|---|---|
| job | parse arbitrary inbound payloads → rows + media | transmit an approved draft |
| wuzapi token | **none** | the only holder |
| MinIO credential | yes (archives media) | **none** |
| send code | **none** | the only holder |
| serves | `POST /hook`, `GET /healthz` | `GET /healthz` only |

The reason is a blast-radius argument, not tidiness. The consumer's input is
**attacker-influenced**: it parses whatever WhatsApp delivers, including pasted
text, filenames, display names and (potentially) poisoned payloads from anyone who
can get a message to the account. It was built with **no send credential and no
function that posts to a send endpoint**, so a parser bug or a crafted payload
cannot reach a send endpoint — an unapproved send has no code route at all.

That property is kept rather than spent:

- The consumer image `COPY`s ingest modules only (`consumer.py`, `_wa_db.py`,
  `_wa_parse.py`, `_wa_webhook.py`, `_minio.py`).
- The sender image `COPY`s send modules only (`sender.py`, `_wa_outbound.py`,
  `_wa_db.py`).
- ⚠ **The disjointness is enforced, not merely intended.**
  `tests/test_image_split.py` reads *both* Dockerfiles and fails if either file
  list grows the other's modules — in **either** direction — and also fails if a
  module stops appearing in either list, so deleting a `COPY` line to silence a
  failure does not work. It is a guard on the *relationship*, not on one file.

Both images build from the same `requirements.txt` and share `_wa_db.py` (the DB
layer) and nothing else. Neither image uses `COPY . .`; every file is named, so a
stray scratch file in a working tree cannot be baked into a layer.

## The database-role split

One Postgres instance, **two databases with separate roles**:

- the `whatsapp` **schema** (the archive) lives in the app database, owned by a
  role — call it the *app role* — that can `USAGE`/own the `whatsapp` schema and
  nothing beyond it;
- `wuzapi`'s **session store** (the linked-device credentials, which are
  account-takeover material) lives in its **own database**, which the app role
  cannot reach.

This is the other half of the pod split, and it is easy to skip. If the app role
could read wuzapi's session store, then *"the ingester has no send credential"*
would be true at the pod level and false at the database level. The code states
the requirement (`_wa_outbound.py`) and the schema layer is built for it:
`ensure_schema()` **asks the catalog** whether the schema exists and only then
tries `CREATE SCHEMA`.

> 🔴 Why it asks the catalog instead of using `CREATE SCHEMA IF NOT EXISTS`:
> Postgres checks `CREATE` privilege **on the database** *before* taking the
> `IF NOT EXISTS` shortcut. For a role that owns no database but does own the
> schema — exactly the app role — `CREATE SCHEMA IF NOT EXISTS whatsapp` fails
> even though the schema is sitting right there. Measured against a real cluster:
> with the app role, statement 0 of 24 raised
> `InsufficientPrivilege: permission denied for database`, and the other 23 passed.
> So the app applies the DDL it *owns* on every start, while a genuinely missing
> schema still raises instead of being silently skipped.
> `tests/test_schema_privilege.py` pins this.

Rotating the two credentials is therefore two independent operations, and nothing
detects drift between a role's password and the DSN that names it until a process
restarts. `WHATSAPP_PG_DSN` is the app role's connection string; the archive never
needs the `wuzapi` credential.

## Where things are configured

The code is deliberately free of cluster facts. It reaches Postgres through a DSN
and object storage through an endpoint, both supplied by the environment:

| setting | required | meaning |
|---|---|---|
| `WHATSAPP_PG_DSN` | yes | any libpq DSN — URI or `key=value` |
| `WHATSAPP_PG_HOST` | no | rewrite the DSN's host (tunnel / port-forward) |
| `WHATSAPP_PG_PORT` | no | rewrite the DSN's port |
| `MINIO_ARCHIVE_ENDPOINT` | yes | `host[:port]` or a full URL |
| `MINIO_ARCHIVE_ACCESS_KEY` | yes | S3 access key |
| `MINIO_ARCHIVE_SECRET_KEY` | yes | S3 secret key |

`WHATSAPP_PG_HOST` / `WHATSAPP_PG_PORT` rewrite only the address, so the same DSN
can be reached through a different network without editing it. There is no
`kubectl` in, or behind, either image.

## Media path — base64 through the webhook, private bucket

Media arrives **base64-encoded inside the webhook**, not via wuzapi's own S3
upload, and the consumer writes the bytes itself with `_minio.py`. This is
deliberate: wuzapi's S3 path writes objects with **PUBLIC-READ** permission (its
own docs warn against sensitive data). Personal chat media in a world-readable
bucket is not a trade worth making, and routing the bytes through the consumer
also keeps the group-media allowlist in **one place** instead of splitting the
policy between wuzapi's per-user config and ours.

Layout in the bucket (`whatsapp-attachments`):

```
<chat>/<YYYY-MM-DD>/<whatsapp_id>_<filename>          the bytes
<chat>/<YYYY-MM-DD>/<whatsapp_id>_<filename>.json     a sidecar of metadata
```

The date comes from the message's **own** timestamp, not wall-clock now, and the
WhatsApp message id is part of the key. `put_attachment()` is **idempotent** — a
re-upload of an existing key is a no-op, so a redelivery replays without
duplicating objects. The message id is required for exactly this reason: keying on
`{chat}/{date}/{filename}` alone would collide on the commonest filename there is
(two `Screenshot.png` in one chat on one day), and the second attachment would
silently point at the first one's bytes.

## The schema, table by table

Everything lives under `whatsapp.*` in one database. `init-schema` applies the DDL,
which lives **only** in `_wa_db.py` — the image that runs the schema is the image
that ships it, so there is no second copy to drift.

| table | holds |
|---|---|
| `chats` | one row per conversation: `jid` (unique), `chat_type` (`dm`/`group`/`broadcast`/`newsletter`/`unknown`), `display_name`, `username`, first/last message times |
| `contacts` | one row per **person**, keyed on either address: `pn` and `lid` (both nullable, both uniquely indexed when present — see `docs/identity.md`), `push_name`, `business_name`, `username`; a `CHECK` requires at least one of `pn`/`lid` |
| `contact_merges` | the **audit** of identity merges: `kept_id`, `merged_id`, `evidence`, `merged_at` |
| `groups` | group metadata: `jid` (unique), `subject` |
| `group_participants` | membership: `(group_id, contact_id)` composite primary key, `role`, joined/left times |
| `group_allowlist` | groups whose **media** is archived: `group_jid` (PK), `note`, `added_by` |
| `messages` | the archive proper: `chat_id`, `whatsapp_id`, `sender_contact_id`, `from_me`, `kind`, `body`, `caption`, `quoted_whatsapp_id`, `message_ts`, `received_at`, `edited_at`, `revoked_at`, `is_view_once`, `expires_at`, `raw` (JSONB), and a generated `search` tsvector over `coalesce(body,'')`; `UNIQUE (chat_id, whatsapp_id)` is the redelivery/idempotency key |
| `attachments` | one row per media item: `message_id`, `media_type`, `mimetype`, `filename`, `file_size`, `sha256`, `minio_key` (NULL when not archived), `download_error`, `downloaded_at`; `UNIQUE (message_id, media_type)` |
| `reactions` | `chat_id`, `target_whatsapp_id`, resolved `message_id` (**nullable** — a reaction can arrive before its target), sender, `emoji`, `removed` |
| `webhook_deliveries` | the at-least-once dedupe ledger: `event_type`, `dedupe_key` (unique), `payload`, `processed_at`, `error` |
| `history_syncs` | **raw** HistorySync payloads plus parse counts, kept verbatim (see `docs/pairing.md`) |
| `consumer_health` | one row per instance: `last_event_at`, `last_success_at`, `counters` (JSONB) |
| `outbound_drafts` | the outbound gate: `to_jid`, `body`, `state` (`CHECK` over the six states), `approval_ref`, `approval_digest`, `whatsapp_message_id`, `server_ts` |
| `outbound_attachments` | a draft's attachment bytes stored **in Postgres** (`BYTEA`), keyed `(draft_id, filename)` |

Two of these carry the load-bearing design:

- **`webhook_deliveries`** is written *first* for every delivery. A seen
  `dedupe_key` is a no-op that still answers `200`, so a retry stops. The key is
  `type:chat:message_id` for a message, and a content hash (minus any embedded
  media blob) for everything else.
- **`history_syncs`** exists because the pairing history is a one-shot delivery.
  See `docs/pairing.md`.

## What the consumer will *not* do

- **No send path.** Structurally: no token, no function that POSTs to a send
  endpoint. Sending lives in `sender.py` (see `docs/send-gate.md`).
- **No sweeping of timed messages.** The `messages` table carries an `expires_at`
  column and nothing enforces it — the archive deliberately does not honour
  WhatsApp's disappearing-message timers.
- ⚠ **Media retention (a 90-day sweep) is not implemented.** Once an attachment is
  in the bucket it stays there. `attachments.download_error` is the honest marker
  for a row whose bytes were *not* archived.
- ⚠ **There is no `NetworkPolicy` and no `readOnlyRootFilesystem` in this repo's
  deployment assets.** The code assumes neither; supply them in your own manifests
  if your threat model wants them.

The read/write commands the consumer and sender expose are in
`docs/operating.md`; identity is `docs/identity.md`; pairing is `docs/pairing.md`.
