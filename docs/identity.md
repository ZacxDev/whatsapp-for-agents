# Identity: one person, two addresses

WhatsApp does not address a person by a phone number alone any more, and the
identity model is the highest-risk correctness area in this repo.

## PN and LID

A person is addressed by **either**:

- a phone number — `<digits>@s.whatsapp.net`, a **"PN"**; or
- an opaque **LID** — `<digits>@lid`.

Since WhatsApp's 2025 LID migration the same human arrives as **both**, in
**either order**, and **LID → PN is not always resolvable**. `whatsmeow` exposes
the second encoding as `SenderAlt` when it knows both addresses, so one event can
carry `Info.Sender` and `Info.SenderAlt` naming the same person two ways.

> 🔴 **Consequence of ignoring this:** keying contacts on a single JID string
> produces **two rows for one person**. Then one person becomes two contacts,
> echoed messages stop deduping (the two rows do not match), and conversation
> history splits across two identities. Every "who said what" query then has a
> quiet, load-bearing hole in it.

So `_wa_parse.identity_from_info()` returns **both** addresses for a sender
(`{'pn':…, 'lid':…}`), and the DB layer merges them onto one contact row when it
sees a dual event.

## `contacts`: two nullable-unique keys

`whatsapp.contacts` carries `pn` and `lid` as **separate columns**, each uniquely
indexed **only when present**:

```
CREATE UNIQUE INDEX contacts_pn_key  ON whatsapp.contacts (pn)  WHERE pn  IS NOT NULL;
CREATE UNIQUE INDEX contacts_lid_key ON whatsapp.contacts (lid) WHERE lid IS NOT NULL;
```

They are stored **bare** (`15550000001`, not `15550000001@s.whatsapp.net`) — see
`_wa_parse.bare_address` — because that is what makes the two partial unique
indexes do the work of "one human, one row" per address. A `CHECK` still requires
at least one of the two to be non-null: a contact with neither address is not a
thing.

`find_or_create_contact(pn, lid, push_name)` resolves **both** addresses to one
row and **merges** when they arrive together:

- both addresses resolve to different rows → the two rows are merged on one;
- one or both resolve → that row is updated (gap-filling `COALESCE` for each
  field) and its `last_seen_at` bumped;
- neither resolves → a new row is created.

No guessing: two addresses stay two people **until an event ties them together**.
The tie is authoritative when it comes from WhatsApp itself —
`Data.phoneNumberToLidMappings` in a HistorySync payload is WhatsApp *stating*
that a PN and a LID are one human. See `docs/pairing.md`.

## The merge, and its audit row

`WaDB._merge_contacts()` collapses two rows into one and repoints every
dependent. **The order of its steps is the whole function**, and every step was
learned from a live Postgres failure that a hermetic fake accepts happily:

1. Read the loser's identity values **first** (the row is gone by step 3).
2. Repoint dependents — `messages`, `reactions`, and `group_participants`. The
   last one has a **composite primary key**, so the loser's duplicate rows for a
   shared group must be **deleted** before the repoint, or the `UPDATE` aborts.
3. **DELETE the loser** (this frees both partial unique indexes).
4. Only now copy its `pn`/`lid`/`push_name` (fill-only `COALESCE`) onto the
   survivor.

> 🔴 Both naive orderings fail, and only against a **real** Postgres:
> - **copy-then-delete** raises `duplicate key value violates unique constraint
>   "contacts_lid_key"` — the survivor is assigned `lid` while the loser still
>   holds it (the partial index is checked per statement, not at `COMMIT`);
> - **nulling the loser's columns** instead of deleting it raises the
>   `contacts_identity_present` `CHECK`.
>
> Measured: 4 of the 6 tests in `tests/test_db_identity.py` failed before this
> ordering, and **none of them could fail hermetically**. That file is therefore
> gated on `WHATSAPP_TEST_DSN` and run as a deploy step against a real Postgres —
> the constraints are the thing under test and an in-memory fake reproduces none
> of them. It is self-cleaning: fixtures use a reserved `1999…` prefix.

Every merge writes a row to **`whatsapp.contact_merges`** (`kept_id`, `merged_id`,
`evidence`, `merged_at`). A merge with no audit row is unrecoverable after the
fact, which is why the audit is not optional:

```sql
select kept_id, merged_id, evidence, merged_at from whatsapp.contact_merges;
```

## Naming a chat on the read path

`_wa_db._resolve_chat_name()` decides what name a chat row **shows**. The order is
the behaviour, so it lives in Python (pinned by the hermetic
`tests/test_read_path.py`) and the SQL queries only *fetch* the candidates.

For a **DM**, the sources are tried in this order:

1. **`wire_push_name`** — the `PushName` on the newest **incoming** message
   (`NOT from_me`). This is the one source that **cannot** be your own name:
   only a message *they* sent names *them*.
2. **`contact_push_name`** — the counterpart's matched `contacts` row.
3. **`chats_username`** — a stable handle from a history conversation
   (`Data.conversations[].username`); survives a display-name change.
4. **`chats_display_name`** — the chat's own stored name; **ranked last of the
   names on purpose** (see below).
5. **the counterpart's PN** — *only* on a **LID-keyed** chat. A LID is
   unreadable; the PN it maps to is not. On a PN-keyed chat the jid already *is*
   that number, so it is not printed twice. A chat with no name and no PN stays
   `None` — exactly "no name", never an invented one.

For a **group** (and every other chat type) the row is returned **unchanged** as
`chats.display_name`: a group's name is the group's, and an incoming `PushName`
inside a group is a **member's**, not the group's — the DM rule must not leak
there.

Two structural traps the code carries deliberately:

- 🔴 `row["chat_type"]` is **indexed, not `.get(…, default)`**. "The caller forgot
  to select it" and "this is not a DM" are different facts and only one is benign.
  Every query feeding the resolver selects `chat_type`; a fourth one that does not
  raises `KeyError` here instead of silently labelling its DMs with the wrong
  column. (Measured: `show()` alone was missing it, and the first version quietly
  returned `None`.)
- The counterpart join uses a lateral named `counterpart`, matched on the chat's
  address **including the server part**, so a number that is one person's PN and
  another person's LID cannot cross-match. (Measured: zero such collisions; the
  server comparison is what keeps it true.)

## The defect this repo fixes: `chats.display_name` can be *your own* name

> 🔴 **Measured on a real archive of 103 DMs: only 13 were named by the old rule,
> and 2 of those names were wrong.** The busiest chat in that archive — 1,207
> messages — was labelled with the **owner's own name**, and a 42-in/42-out chat
> whose counterpart's own messages name them something else was labelled with the
> owner's name too.

The mechanism: `consumer._handle_message()` stamps `chats.display_name` from the
**sender's** `PushName`. On a DM, it writes that name onto the **chat**
unconditionally — but a stored event for **your own outgoing message** carries
**your** push name while `Info.Chat` is the counterpart. So any DM you have
written to can end up carrying *your* name.

`_resolve_chat_name` fixes this by ranking `chats_display_name` **last** and by
filtering the winning source on `NOT from_me` — the only incoming message's push
name belongs to the counterpart.

⚠ **A naive contacts join would re-ship the same wrong name.** `contacts.push_name`
is written from the **same** event-level `PushName` field as
`chats.display_name`, so it is not, by itself, a safe source. Only the
**direction-filtered** incoming push name is. If you build your own read path, do
not simply `JOIN contacts ON …` and take `push_name`; carry the `from_me` filter
that makes the source trustworthy.

### Verifying it live

The naming order is pinned hermetically; the **SQL wiring** (that each query
selects the candidates) needs a real Postgres and lives in
`tests/test_read_path_db.py`, gated on `WHATSAPP_TEST_DSN`. It is the test that
catches a *missing column* — the exact class of defect that no hermetic test can
see. Run both against the live schema as a deploy step.
