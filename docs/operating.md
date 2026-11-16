# Operating it day to day

The public interface is the CLI: **`python3 consumer.py <subcommand>`** and
**`python3 sender.py <subcommand>`**. Run them *in the pod*, or anywhere that has
`WHATSAPP_PG_DSN` (and, for media, the MinIO variables) pointed at the same
database and bucket. Read commands run where the DSN and credential already are;
send commands run where the send token is.

## Consumer subcommands

```bash
python3 consumer.py serve                       # run the webhook server (the Deployment)
python3 consumer.py init-schema                 # apply the schema (idempotent)

python3 consumer.py health                       # heartbeat FILE — exit 0 healthy / 1 stale
python3 consumer.py health --from-db --json      # the ROW: counters + last event (not for a probe)
python3 consumer.py conversations --limit 20     # chats, newest first (a DM is named after the counterpart)
python3 consumer.py search "permit"              # full-text search over message bodies
python3 consumer.py search "invoice" --allowlisted-only
python3 consumer.py show '120363123456789012@g.us' --limit 30
python3 consumer.py stats --json                 # row counts + newest message
python3 consumer.py reparse-history --account 15550000001@s.whatsapp.net

python3 consumer.py allowlist list
python3 consumer.py allowlist add '120363123456789012@g.us' --note "family"
python3 consumer.py allowlist remove '120363123456789012@g.us'
```

`serve` requires `WHATSAPP_HMAC_KEY` (it refuses to accept unauthenticated
webhooks) and applies `ensure_schema()` on start. `init-schema` is idempotent and
ships the DDL in the same image that runs it.

## Sender subcommands

```bash
python3 sender.py draft '<jid>' --body 'text' [--attach FILE]…   # STORES a draft; transmits NOTHING
python3 sender.py ls [--state pending|approved|sending|sent|failed|reconciled]
python3 sender.py approve <id> --ref '<external ref>'            # operator-only
python3 sender.py unapprove <id>                                 # approved -> pending
python3 sender.py send <id>                                      # transmit an APPROVED draft, at most once
python3 sender.py reconcile <id> --outcome sent|failed|reconciled [--message-id ID]
python3 sender.py serve [--port 8081]                            # the pod: /healthz only
```

`approve`, `unapprove` and `reconcile` refuse unless `WHATSAPP_APPROVAL_TOKEN` is
in the environment. See `docs/send-gate.md` for the gate, the digest and the
states.

## 🔴 Liveness: the FILE and the ROW answer different questions

Liveness is **two sinks**, exactly as you would want:

- **The heartbeat FILE** says *"this process is still ticking"* and **depends on
  nothing external**. `serve` writes it every interval (default **30 s**) with the
  instance name, `updated_at`, pid and a counters snapshot. `health` reads it and
  compares the age against `max_age` (default **4× the interval = 120 s**):
  `exit 0` healthy, `1` stale. **This is the only safe liveness input** — a probe
  can read it without needing Postgres to be up.
- **The health ROW** (`health --from-db`, table `whatsapp.consumer_health`) adds
  *"…and Postgres is reachable"*, plus the counters and `last_event_at`. It is
  **ALLOWED to fail** — a row that cannot be read is not a process that has
  stopped.

> 🔴 **`last_event_at` is diagnosis, NEVER liveness.** An idle account legitimately
> produces nothing for hours. "No recent messages" can never be a liveness test —
> only "this thread stopped ticking" can.

⚠ **Honest caveat about the row in this build.** `health --from-db` reads the row
and `WaDB.update_health()` writes it, but the **serve loop does not call
`update_health`** — it writes only the heartbeat *file*. So with the code as
shipped, the `consumer_health` row stays empty unless something else calls the
writer. Treat `--from-db` as the place the row is *reported when populated*; do
not build a liveness probe on it, which the design already tells you not to do.

### Direct SQL — the store is authoritative

```sql
select count(*), max(message_ts) from whatsapp.messages;

select c.jid, m.message_ts, left(coalesce(m.body,'['||m.kind||']'),70)
  from whatsapp.messages m
  join whatsapp.chats c on c.id = m.chat_id
 order by m.message_ts desc
 limit 20;

select count(*) from whatsapp.attachments where minio_key is null;   -- media not archived
select kept_id, merged_id, evidence, merged_at from whatsapp.contact_merges;
```

## Counters

`serve` keeps these in memory and writes a snapshot into the heartbeat file (so
`health --json` shows them). They are per-process since start; a restart zeroes
them.

| counter | meaning |
|---|---|
| `stored` | new message rows |
| `updated` | rows updated in place (an edit, or a redelivery that changed the row) |
| `ignored` | a `ReadReceipt`/`ChatPresence` delivery — recorded, not stored |
| `duplicate` | a replayed delivery — a no-op by design, not a problem |
| `unauthenticated` | a delivery whose HMAC did not verify |
| `malformed` | not JSON, or missing `Chat`/`ID` — counted, never stored |
| `unknown_event` | a handled envelope shape we do not model — retained, never dropped |
| `reactions` | reactions stored |
| `revokes` | a revoke tombstoned its target |
| `edits` | a message-edit processed |
| `media_uploaded` | media archived |
| `media_skipped_policy` | media dropped because its group is not allowlisted |
| `media_failures` | an upload failed (the message row still stands) |
| `history_syncs` | pairing-history payloads received |
| `db_failures` | the error path's own DB write failed |
| `handled_errors` | an event handler raised |

### Reading `unauthenticated` correctly

> 🔴 **`unauthenticated` is **not** a clean "should be 0" indicator, and reading it
> that way costs real time.** wuzapi sends some event types in a form the HMAC
> check refuses, and it retries each one and then drops it to its own error queue
> — so this counter can climb **purely as a function of other people typing**.
> The fix is to narrow the subscription to `Message,HistorySync` (the two the
> consumer stores); see `docs/pairing.md`.

If it still moves after narrowing, **something subscribed is being refused**, and
the consumer log names the event type (never the body):

```
webhook REJECTED (bad HMAC): event_type=Message bytes=812 signature=present sha256=…
```

## 🔴 The group allowlist gates MEDIA, not messages

**Every group's message ROWS are stored; only allowlisted groups' MEDIA is
downloaded.** Rows are cheap and reversible, bytes are not.

- Adding a group to the allowlist gives you its **full text history immediately**,
  but media **from that point forward** only. **Nothing is backfilled.**
- A skipped attachment appears as a row with `minio_key IS NULL` and
  `download_error = 'skipped: group not in allowlist'` — that is the **intended
  state**, not a fault. Check the reason before treating it as a failure.
- ⚠ **A policy skip is also what a real failure looks like in the same two
  columns**, so **read `download_error`** — do not branch on `minio_key IS NULL`
  alone.

Seed it **empty** (the default) and populate it from the captured group list after
pairing. Use **group JIDs, never display names** — names change silently.

```sql
select group_jid, note, added_at from whatsapp.group_allowlist order by added_at;
```

## Environment variables

| variable | used by | required | notes |
|---|---|---|---|
| `WHATSAPP_PG_DSN` | consumer, sender | **yes** | any libpq DSN (URI or `key=value`) |
| `WHATSAPP_PG_HOST` | consumer, sender | no | rewrite the DSN's host |
| `WHATSAPP_PG_PORT` | consumer, sender | no | rewrite the DSN's port |
| `MINIO_ARCHIVE_ENDPOINT` | consumer | for media | `host[:port]` or full URL |
| `MINIO_ARCHIVE_ACCESS_KEY` | consumer | for media | S3 access key |
| `MINIO_ARCHIVE_SECRET_KEY` | consumer | for media | S3 secret key |
| `WHATSAPP_HMAC_KEY` | consumer (`serve`) | **yes** | ≥ 32 bytes; must match wuzapi's key(s) |
| `WHATSAPP_PORT` | consumer (`serve`) | no | default 8080 |
| `WHATSAPP_MEDIA_CONCURRENCY` | consumer (`serve`) | no | upload worker count, default 2 |
| `WHATSAPP_HEARTBEAT_PATH` | consumer | no | default `/tmp/whatsapp-consumer-heartbeat.json` |
| `WHATSAPP_HEARTBEAT_INTERVAL` | consumer | no | seconds, default 30 |
| `WHATSAPP_HEARTBEAT_MAX_AGE` | consumer | no | seconds, default `4 × interval` |
| `WHATSAPP_INSTANCE` | consumer | no | instance name on the heartbeat |
| `WUZAPI_URL` | sender | no | default `http://whatsapp-api:8080` |
| `WUZAPI_USER_TOKEN` | sender | **yes** (to send) | the wuzapi user token |
| `WHATSAPP_APPROVAL_TOKEN` | sender | **yes** (for `approve`/`unapprove`/`reconcile`) | **not** in the pod's own env |
| `WHATSAPP_SENDER_HEARTBEAT` | sender | no | health file path |
| `WHATSAPP_TEST_DSN` | the live-DB tests | to run them | see below |

## The live-Postgres test suites

Two suites are **gated** on a real database, because what they test is SQL that an
in-memory fake cannot reproduce:

```bash
WHATSAPP_TEST_DSN='postgresql://…' python3 -m unittest test_db_identity test_read_path_db -v
```

- `test_db_identity` exercises the **identity merge** — the partial unique indexes
  and the composite primary key on `group_participants` are the thing under test.
- `test_read_path_db` exercises the **read-path SQL** — that each query selects
  the columns the naming resolver needs (the defect it catches is a *missing
  column*, which nothing hermetic can see).

Both are self-cleaning: fixtures use a reserved `1999…` prefix and teardown deletes
exactly those rows, dependents first.
