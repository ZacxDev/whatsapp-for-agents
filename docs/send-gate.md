# The outbound gate

Sending is a **separate image, a separate Deployment, and a separate credential**
from receiving. The consumer parses arbitrary inbound payloads and holds no send
token and no send code; the sender holds the token and is a tiny, auditable
surface — `draft`, `approve`, `send`, `reconcile`. A parser bug cannot reach a
send endpoint because an unapproved send has **no code route at all**. See
`docs/architecture.md` for the two-image split.

## Compose and transmit are separate calls

The two halves are **different invocations**, minutes or hours apart, with a
**durable row between them** (`whatsapp.outbound_drafts`). `draft` stores a row
and transmits **nothing**; `send` transmits a row that was **approved**.

```
draft ──► pending ──approve──► approved ──send──► sending ──► sent
                                            │                  └► (crash) reconcile ──► sent / failed
                                            └──► failed   (a failure BEFORE the request left)
```

## The states

Mirrored by a `CHECK` constraint on `whatsapp.outbound_drafts`:

| state | meaning |
|---|---|
| `pending` | composed and stored; transmits nothing |
| `approved` | an operator recorded a digest of the exact payload shown |
| `sending` | **INERT** — claimed, POST attempted, outcome unknown |
| `sent` | transmitted; `whatsapp_message_id` + `server_ts` recorded |
| `failed` | definitively **NOT** sent (a failure *before* the request left) |
| `reconciled` | an operator settled a `sending` row |

`approved → sending` is a **conditional** `UPDATE … WHERE state = 'approved'`, so
two senders cannot both claim one draft; the loser raises rather than
double-sending.

## 🔴 The digest is the gate

`approve` records a **digest** over the body **and each attachment's `sha256`**,
and `send` refuses a row that no longer hashes to it. So the
approve → edit → send window is closed: a draft edited after approval **cannot be
transmitted under the old approval**.

Details that are load-bearing:

- The digest is computed from **bytes**; the approve side reads SQL rows while the
  send side reads the same rows *with bytes as mappings*, and **both shapes must
  fingerprint identically** — that comparison *is* the gate. A unit test pins it,
  because the first live send hit exactly this: a normaliser that sliced tuples
  raised `KeyError: slice(None, 4, None)` on a bytes-carrying mapping *after* a
  successful mint, so every attachment send failed while the suite stayed green.
- The **filename** is part of the digest — it is what the recipient sees, so
  renaming it after approval cannot slip through.
- A **missing digest fails closed** — it is indistinguishable from one cleared by
  the writer being guarded against.

The capability (`SendAuthorization`) **cannot be constructed directly**: it is
minted only for an `approved` draft whose payload still matches its digest, and
`spend_authorization()` makes it **single-use** — an approved draft transmits
**exactly once**.

## The operator-only token

`approve`, `unapprove` and `reconcile` require **`WHATSAPP_APPROVAL_TOKEN`** in
the environment, and it is **deliberately absent from the sender's own
environment**. That is what stops anything that can exec into the pod from
approving its own draft. The operator supplies it on the command line; the durable
copy belongs in a secret.

⚠ **It is a speed bump against an agent that has the database but not the secret —
not proof of humanity.** Treat it as a decision record, not as authentication.
Without it in the environment, `approve` refuses:

```
SendError: WHATSAPP_APPROVAL_TOKEN is not set — `approve` is an operator command …
Refusing to approve draft <id> from an environment that cannot prove it.
```

## 🔴 The length policy is enforced at DRAFT time

A refusal at **send** time would arrive **after** the operator had already
approved the text. So the correction happens while it is still a draft:

- **no attachment** → the body must fit **`MAX_BODY_CHARS = 300`**;
- **with an attachment** → the body is a **cover note** of at most
  **`MAX_COVER_NOTE_CHARS = 150`**, and the detail belongs in the file.

Calibrated, not guessed — measured over the operator's own **815** sent messages
with prose: **median 17 chars, p90 72, p95 114, p99 257**, and only **4/815
(0.5%)** over 300. A body above the ceiling is not a shorter version of how a
person writes; it is a different medium.

There is also a per-draft attachment cap (**`MAX_DRAFT_ATTACHMENT_BYTES = 8 MiB`**),
which bounds one Postgres row and one non-streamed request body.

The error names the constant it hit, so a refusal is not "worked around" by
raising a number without reading why.

## What we send is written into the archive

**wuzapi does not echo messages sent through its own API back as webhooks** —
measured on the first live send: no `webhook_deliveries` row, and wuzapi logged no
webhook call. Untreated, the archive would silently omit **every reply an agent
sends** — exactly where a reply-only agent makes the store *least* complete.

So the sender writes the `whatsapp.messages` row itself, **keyed on the returned
`whatsapp_message_id`** and **upserted on `(chat_id, whatsapp_id)`**. If wuzapi
ever *does* start echoing, the echo **updates that row** instead of duplicating
it. A sent reply is then searchable and shows up in history like any other
message.

Three details:

- **The body/caption shape mirrors the ingest parser.** A media caption lands in
  **both** `body` and `caption`, because `messages.search` is a generated tsvector
  over `coalesce(body, '')` — a caption kept out of `body` would make our own sent
  documents unsearchable.
- **The write happens AFTER the draft is `sent`, and never raises.** The message
  is already out; recording `failed` would invite a retry that sends it twice. A
  failure is logged with the draft id, which is what a backfill keys on.
- **`sender_contact_id` stays NULL.** The author of a `from_me` row is the linked
  account itself; inventing a contact for the operator would put them into the
  PN/LID merge machinery that exists to stop **other people** becoming two
  contacts.

⚠ **Outbound attachment BYTES are not archived.** The sender holds no MinIO
credential — deliberately, since the pod that can post is not the pod that can
write the object store. The metadata row is still written, with `minio_key IS NULL`
plus a reason, so the archive states what it does not have rather than falling
silent.

## 🔴 The ambiguous send — `reconcile` exists for this

**"Failed" and "the POST was attempted and we don't know" are DIFFERENT FACTS.**
Recording the second as `failed` invites a retry, and retrying an attempted POST
is a **double send**. So:

- everything **before** the request leaves the process → `failed` (a fact);
- anything **during or after** it → the row is left **`sending`** (INERT) and the
  verb raises, telling you to `reconcile` once you know what happened.

```bash
python3 sender.py ls --state sending                        # anything stranded here needs a decision
python3 sender.py reconcile <id> --outcome sent   --message-id 3EB0…
python3 sender.py reconcile <id> --outcome failed
```

## Traps found by the first live send

- 🔴 **wuzapi authenticates with the `token` HEADER, not `Authorization`.** Its
  middleware reads `r.Header.Get("token")` (with a `token` query-param fallback);
  an `Authorization` header is silently ignored and every send answers
  `401 {"error":"unauthorized"}` — which reads like a bad credential rather than a
  wrong header name. Confirm the credential with a **read-only** session-status
  call before spending a draft.
- **The attachment bytes live in Postgres, not on the pod's disk** (`BYTEA` in
  `outbound_attachments`) — which is why a pod roll between draft and send cannot
  lose them.
- **One document per draft**, sent with the body as its caption
  (`/chat/send/document` takes a `data:` URI; `/chat/send/text` takes the body).
  A bare LID is accepted as the recipient.

## Negative controls — confirm the gate is intact

Reproduce these against your deployment:

```
# 1. an unapproved draft must refuse to transmit
send <a-non-approved-id>
#   SendGateError: draft N has state='failed'; only 'approved' drafts may be
#   transmitted (D3 approval gate)

# 2. approve must refuse without the operator token in the environment
approve <id> --ref test
#   SendError: WHATSAPP_APPROVAL_TOKEN is not set — `approve` is an operator command …
```

The sender pod serves `GET /healthz` on its port and **nothing else** — there is
deliberately **no HTTP route that sends**, so a stray port-forward cannot mint a
message. `replicas: 1` + a `Recreate` strategy is a hard constraint for the same
reason as the consumer: a second client on one device identity is what WhatsApp
reads as a compromise.

## 🔴 Reply-only is a POLICY, not a mechanism

The pod can address any JID it is handed. What stops it initiating is that **a
draft cannot exist without a `draft` call, and cannot transmit without an
`approve`** — that is the mechanism. Refusing to start a conversation
unprompted is a **policy that lives in how you operate it**, not an enforcement in
the code. Keep it that way by keeping `draft` and `approve` on the human side of
the line.
