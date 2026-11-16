#!/usr/bin/env python3
"""Postgres layer for the WhatsApp pipeline (`whatsapp` schema).

One Postgres database, one schema. It holds the whole archive — chats, contacts,
messages, attachments, reactions, the media allowlist and the outbound gate — so
there is no second store to keep in sync.

What lives where:

    whatsapp.chats / contacts / groups / group_participants
    whatsapp.messages / attachments / reactions
    whatsapp.group_allowlist          read-side filter, write-side media policy
    whatsapp.webhook_deliveries       at-least-once dedupe (wuzapi retries)
    whatsapp.history_syncs            RAW history payloads, parsed later
    whatsapp.consumer_health          counters + last-event
    whatsapp.outbound_drafts          the outbound gate (draft/approve/send); see sender.py

`history_syncs` deserves its own line: the pairing history payload is a ONE-SHOT
delivery that WhatsApp acks and deletes server-side as soon as it is handed over.
Storing it verbatim before parsing means a parser bug costs a re-parse, not the
history.

Connection modes:

  * `WHATSAPP_PG_DSN` — required. Any libpq DSN, URI or key=value form:
        postgresql://whatsapp_app:change-me@localhost:5432/whatsapp
  * `WHATSAPP_PG_HOST` / `WHATSAPP_PG_PORT` — optional. Rewrite the DSN's address
    so the same database can be reached through a tunnel, a port-forward or a
    different network without editing the DSN:
        WHATSAPP_PG_HOST=127.0.0.1 WHATSAPP_PG_PORT=15432
"""
from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager

try:
    import psycopg2
    import psycopg2.extras
except ImportError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "psycopg2 is required — pip install -r requirements.txt"
    ) from exc

DSN_ENV = "WHATSAPP_PG_DSN"
HOST_ENV = "WHATSAPP_PG_HOST"
PORT_ENV = "WHATSAPP_PG_PORT"
DEFAULT_PORT = "5432"

SCHEMA = "whatsapp"

# --------------------------------------------------------------------------- #
# THE DDL LIVES HERE AND ONLY HERE.
#
# It is applied by `consumer.py init-schema`, which runs from THIS image — so the
# image that runs the schema is the image that ships it. Deliberately no ConfigMap
# and no .sql file beside it: a second copy is a copy that drifts.
#
# Every statement is IF NOT EXISTS and the consumer also calls ensure_schema() at
# start, so re-running is safe. A Job's pod template is immutable, which is why
# the manifest still carries a -vN suffix to force a re-run on change.
# --------------------------------------------------------------------------- #
#: The schema ITSELF, kept separate from its objects — see `ensure_schema`.
#:
#: 🔴 NOT `if not exists` INSIDE THE OBJECT LIST. Postgres checks CREATE privilege
#: ON THE DATABASE before the existence shortcut, so
#: `CREATE SCHEMA IF NOT EXISTS whatsapp` fails for a role that may only USE an
#: existing schema — which is exactly the app role (`whatsapp_app`), granted
#: USAGE on `whatsapp` and nothing on the database. Keeping it out of the object
#: list lets the app apply the DDL it OWNS on every start, while a genuinely
#: missing schema still raises instead of being silently skipped.
CREATE_SCHEMA_STATEMENT = f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"

OBJECT_STATEMENTS = (
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.chats (
      id              BIGSERIAL PRIMARY KEY,
      jid             TEXT NOT NULL UNIQUE,
      chat_type       TEXT NOT NULL CHECK (chat_type IN
                        ('dm','group','broadcast','newsletter','unknown')),
      display_name    TEXT,
      first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_message_at TIMESTAMPTZ
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.contacts (
      id            BIGSERIAL PRIMARY KEY,
      pn            TEXT,
      lid           TEXT,
      push_name     TEXT,
      business_name TEXT,
      first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      CONSTRAINT contacts_identity_present CHECK (pn IS NOT NULL OR lid IS NOT NULL)
    )
    """,
    # Additive and idempotent: `username` is a stable WhatsApp handle that
    # survives a display-name change, and it is already present in every stored
    # history payload (`Data.conversations[].username`).
    f"ALTER TABLE {SCHEMA}.chats ADD COLUMN IF NOT EXISTS username TEXT",
    f"ALTER TABLE {SCHEMA}.contacts ADD COLUMN IF NOT EXISTS username TEXT",
    f"CREATE UNIQUE INDEX IF NOT EXISTS contacts_pn_key ON {SCHEMA}.contacts (pn) WHERE pn IS NOT NULL",
    f"CREATE UNIQUE INDEX IF NOT EXISTS contacts_lid_key ON {SCHEMA}.contacts (lid) WHERE lid IS NOT NULL",
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.contact_merges (
      id         BIGSERIAL PRIMARY KEY,
      kept_id    BIGINT NOT NULL,
      merged_id  BIGINT NOT NULL,
      evidence   TEXT NOT NULL,
      merged_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.groups (
      id         BIGSERIAL PRIMARY KEY,
      jid        TEXT NOT NULL UNIQUE,
      subject    TEXT,
      created_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.group_participants (
      group_id   BIGINT NOT NULL REFERENCES {SCHEMA}.groups(id) ON DELETE CASCADE,
      contact_id BIGINT NOT NULL REFERENCES {SCHEMA}.contacts(id),
      role       TEXT,
      joined_at  TIMESTAMPTZ,
      left_at    TIMESTAMPTZ,
      PRIMARY KEY (group_id, contact_id)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.group_allowlist (
      group_jid TEXT PRIMARY KEY,
      note      TEXT,
      added_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      added_by  TEXT
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.messages (
      id                 BIGSERIAL PRIMARY KEY,
      chat_id            BIGINT NOT NULL REFERENCES {SCHEMA}.chats(id),
      whatsapp_id        TEXT NOT NULL,
      sender_contact_id  BIGINT REFERENCES {SCHEMA}.contacts(id),
      from_me            BOOLEAN NOT NULL DEFAULT false,
      kind               TEXT NOT NULL DEFAULT 'unknown',
      body               TEXT,
      caption            TEXT,
      quoted_whatsapp_id TEXT,
      message_ts         TIMESTAMPTZ NOT NULL,
      received_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
      edited_at          TIMESTAMPTZ,
      revoked_at         TIMESTAMPTZ,
      is_view_once       BOOLEAN NOT NULL DEFAULT false,
      expires_at         TIMESTAMPTZ,
      raw                JSONB NOT NULL,
      UNIQUE (chat_id, whatsapp_id)
    )
    """,
    f"CREATE INDEX IF NOT EXISTS messages_ts_idx ON {SCHEMA}.messages (message_ts DESC)",
    f"CREATE INDEX IF NOT EXISTS messages_chat_idx ON {SCHEMA}.messages (chat_id, message_ts DESC)",
    f"""
    ALTER TABLE {SCHEMA}.messages
      ADD COLUMN IF NOT EXISTS search tsvector
      GENERATED ALWAYS AS (to_tsvector('english', coalesce(body, ''))) STORED
    """,
    f"CREATE INDEX IF NOT EXISTS messages_search_idx ON {SCHEMA}.messages USING gin (search)",
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.attachments (
      id             BIGSERIAL PRIMARY KEY,
      message_id     BIGINT NOT NULL REFERENCES {SCHEMA}.messages(id) ON DELETE CASCADE,
      media_type     TEXT NOT NULL,
      mimetype       TEXT,
      filename       TEXT,
      file_size      BIGINT,
      sha256         TEXT,
      minio_key      TEXT,
      download_error TEXT,
      downloaded_at  TIMESTAMPTZ,
      UNIQUE (message_id, media_type)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.reactions (
      id                 BIGSERIAL PRIMARY KEY,
      chat_id            BIGINT NOT NULL REFERENCES {SCHEMA}.chats(id),
      target_whatsapp_id TEXT NOT NULL,
      message_id         BIGINT REFERENCES {SCHEMA}.messages(id),
      sender_contact_id  BIGINT REFERENCES {SCHEMA}.contacts(id),
      emoji              TEXT,
      reaction_ts        TIMESTAMPTZ NOT NULL,
      removed            BOOLEAN NOT NULL DEFAULT false,
      UNIQUE (chat_id, target_whatsapp_id, sender_contact_id)
    )
    """,
    f"""
    CREATE INDEX IF NOT EXISTS reactions_unresolved_idx
      ON {SCHEMA}.reactions (target_whatsapp_id) WHERE message_id IS NULL
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.webhook_deliveries (
      id           BIGSERIAL PRIMARY KEY,
      event_type   TEXT NOT NULL,
      dedupe_key   TEXT NOT NULL UNIQUE,
      payload      JSONB,
      received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
      processed_at TIMESTAMPTZ,
      error        TEXT
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.history_syncs (
      id               BIGSERIAL PRIMARY KEY,
      received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
      conversation_cnt INT,
      message_cnt      INT,
      parsed_messages  INT,
      error            TEXT,
      payload          JSONB NOT NULL
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.consumer_health (
      instance        TEXT PRIMARY KEY,
      last_event_at   TIMESTAMPTZ,
      last_success_at TIMESTAMPTZ,
      counters        JSONB NOT NULL DEFAULT '{{}}'::jsonb,
      updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.outbound_drafts (
      id                  BIGSERIAL PRIMARY KEY,
      to_jid              TEXT NOT NULL,
      body                TEXT NOT NULL,
      state               TEXT NOT NULL DEFAULT 'pending'
                          CHECK (state IN
                            ('pending','approved','sending','sent','failed','reconciled')),
      approval_ref        TEXT,
      approval_digest     TEXT,
      approved_at         TIMESTAMPTZ,
      whatsapp_message_id TEXT,
      server_ts           TIMESTAMPTZ,
      error               TEXT,
      created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.outbound_attachments (
      draft_id     BIGINT NOT NULL REFERENCES {SCHEMA}.outbound_drafts(id) ON DELETE CASCADE,
      filename     TEXT NOT NULL,
      content_type TEXT NOT NULL,
      sha256       TEXT NOT NULL,
      bytes        BYTEA NOT NULL,
      PRIMARY KEY (draft_id, filename)
    )
    """,
)

#: Every statement, schema first — what `consumer.py init-schema` and the
#: schema-init Job apply. `ensure_schema` picks the half it is allowed to run.
SCHEMA_STATEMENTS = (CREATE_SCHEMA_STATEMENT,) + OBJECT_STATEMENTS


def resolve_dsn() -> str:
    """The DSN to connect with: `WHATSAPP_PG_DSN`, plus optional address overrides.

    `WHATSAPP_PG_HOST` / `WHATSAPP_PG_PORT` rewrite the DSN's address, which is
    what you want when the same database is reachable at a different address —
    a tunnel, a port-forward, a different network — without editing the DSN.
    """
    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        raise RuntimeError(
            f"{DSN_ENV} is not set — point it at your Postgres, e.g.\n"
            "  export WHATSAPP_PG_DSN="
            "'postgresql://whatsapp_app:change-me@localhost:5432/whatsapp'"
        )
    host = os.environ.get(HOST_ENV)
    port = os.environ.get(PORT_ENV)
    if host or port:
        return _replace_host(dsn, host, port)
    return dsn


def _replace_host(dsn: str, host: str | None, port: str | None) -> str:
    """Rewrite the host/port in a libpq DSN (URI or key=value form).

    A `None` leaves that half alone; the value it would have replaced is read back
    out of the DSN, so overriding only one of the two keeps the other.
    """
    if dsn.startswith("postgresql://") or dsn.startswith("postgres://"):
        head, _, tail = dsn.partition("://")
        creds, _, rest = tail.partition("@")
        addr, _, dbpart = rest.partition("/")
        cur_host, _, cur_port = addr.partition(":")
        host = host or cur_host or "localhost"
        port = port or cur_port or DEFAULT_PORT
        return f"{head}://{creds}@{host}:{port}/{dbpart}"
    parts = [p for p in dsn.split() if not p.startswith(("host=", "port="))]
    if host:
        parts.append(f"host={host}")
    if port:
        parts.append(f"port={port}")
    return " ".join(parts)


@contextmanager
def connect(dsn: str | None = None, *, autocommit: bool = False):
    conn = psycopg2.connect(dsn or resolve_dsn())
    conn.autocommit = autocommit
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def cursor(dsn: str | None = None):
    with connect(dsn) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur


def _schema_exists(cur) -> bool:
    cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", (SCHEMA,))
    return cur.fetchone() is not None


def ensure_schema(dsn: str | None = None) -> None:
    """Apply the DDL. Idempotent, and tolerant of a role that owns no database.

    🔴 THE SCHEMA STATEMENT IS CONDITIONAL ON THE CATALOG, NOT ON `if not exists`.
    The app role (`whatsapp_app`) owns `whatsapp` and can USE it, but holds nothing
    beyond it — by design, so a compromised ingester cannot reach wuzapi's session
    tokens, which live in a separate database on the same instance.
    `CREATE SCHEMA IF NOT EXISTS` still fails
    for such a role, because Postgres checks CREATE privilege before it takes the
    existence shortcut. So: ask the catalog, and only then try to create. A
    missing schema with no privilege still raises — this is not a silent skip.
    """
    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            statements = (OBJECT_STATEMENTS if _schema_exists(cur)
                          else SCHEMA_STATEMENTS)
            for statement in statements:
                cur.execute(statement)


# Transient faults that are worth retrying rather than dropping an event.
_TRANSIENT = (
    psycopg2.OperationalError,
    psycopg2.InterfaceError,
)


def with_retry(fn, *, attempts: int = 3, base_delay: float = 0.25):
    """Retry a DB operation; re-raise on the final attempt.

    🔴 A failed statement poisons the transaction (autocommit=False), so the
    caller's connection is thrown away and a NEW one is opened by `fn`. That is
    why `fn` is expected to own its own `cursor()` block.
    """
    last = None
    for attempt in range(attempts):
        try:
            return fn()
        except _TRANSIENT as exc:
            last = exc
            time.sleep(base_delay * (2 ** attempt))
    raise last  # type: ignore[misc]


#: Candidate sources for a DM's name, best first — the ORDER is the behaviour.
#: (Module-level rather than a class attribute so `tests/test_read_path.py` can
#: import it without a database, and so the class does not need an instance.)
DM_NAME_SOURCES = ("wire_push_name", "contact_push_name",
                   "chats_username", "chats_display_name")


def _resolve_chat_name(row: dict) -> str | None:
    """The name to SHOW for one chat row. For a DM, the counterpart's.

    WHY THIS IS PYTHON AND NOT A SQL `CASE`: the order below is the whole
    behaviour, and a SQL expression can only be exercised against a live
    Postgres — which the gate never runs (see `test_db_identity.py`'s docstring).
    As a pure function it is pinned by the hermetic suite, and the queries only
    have to fetch the candidates.

    A GROUP (and every other chat type) is returned unchanged as
    `chats.display_name`: a group's name is the group's, and an incoming
    `PushName` inside a group is a MEMBER's — the DM rule must not leak there.

    🔴 `row["chat_type"]` IS INDEXED, NOT `.get(..., 'dm' | None)` — because
    "the caller forgot to select it" and "this is not a DM" are different facts
    and only one of them is benign. Every query that feeds this (`conversations`,
    `search`, `show`) selects it, and a fourth one that does not raises a
    `KeyError` here instead of silently labelling its DMs with the contaminated
    column. (Measured: `show()` alone was missing it, and the first version of
    this function quietly returned `None` for every chat it named.)

    🔴 `chats_display_name` RANKS LAST OF THE NAMES ON PURPOSE. It is the one
    source measured to be able to hold OUR OWN name (see the block above
    `WaDB._CONTACT_OF_CHAT`). It stays in the list because for a handful of chats
    it is the only name there is (a bare `@handle`, in the live archive), but it
    # must never outrank a
    source that cannot be ours.

    The final fallback is the counterpart's PHONE NUMBER, and only where it adds
    information: on a PN-keyed chat the jid already IS that number, so printing
    it twice is noise. A LID-keyed chat with a known PN is the case this exists
    for — a LID-keyed chat reads as the counterpart's PN. A chat with no name and no
    PN keeps `None`, i.e. exactly today's behaviour, rather than inventing one.
    """
    if row["chat_type"] != "dm":
        return row.get("chats_display_name") or None
    for key in DM_NAME_SOURCES:
        value = (row.get(key) or "").strip()
        if value:
            return value
    if not (row.get("jid") or "").endswith("@s.whatsapp.net"):
        pn = (row.get("contact_pn") or "").strip()
        if pn:
            return pn
    return None


class WaDB:
    """Small, explicit data-access surface. No ORM, no hidden state."""

    def __init__(self, dsn: str | None = None):
        self.dsn = dsn

    # -- deliveries ---------------------------------------------------------
    def record_delivery(self, kind: str, key: str, payload: dict | None = None) -> int | None:
        """Insert the dedupe row. None means this is a REPLAY -> caller returns 200."""
        def op():
            with cursor(self.dsn) as cur:
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.webhook_deliveries (event_type, dedupe_key, payload)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (dedupe_key) DO NOTHING
                        RETURNING id""",
                    (kind, key, json.dumps(payload) if payload is not None else None),
                )
                row = cur.fetchone()
                return row["id"] if row else None
        return with_retry(op)

    def finish_delivery(self, delivery_id: int, *, error: str | None = None) -> None:
        def op():
            with cursor(self.dsn) as cur:
                cur.execute(
                    f"UPDATE {SCHEMA}.webhook_deliveries SET processed_at = now(), error = %s WHERE id = %s",
                    (error, delivery_id),
                )
        with_retry(op)

    # -- chats / contacts ---------------------------------------------------
    def upsert_chat(self, jid: str, chat_type: str, display_name: str | None = None,
                    username: str | None = None) -> int:
        def op():
            with cursor(self.dsn) as cur:
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.chats (jid, chat_type, display_name, username)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (jid) DO UPDATE
                          SET display_name = COALESCE(EXCLUDED.display_name, {SCHEMA}.chats.display_name),
                              username = COALESCE(EXCLUDED.username, {SCHEMA}.chats.username)
                        RETURNING id""",
                    (jid, chat_type, display_name, username),
                )
                return cur.fetchone()["id"]
        return with_retry(op)

    def touch_chat(self, chat_id: int, ts) -> None:
        def op():
            with cursor(self.dsn) as cur:
                cur.execute(
                    f"""UPDATE {SCHEMA}.chats
                        SET last_message_at = GREATEST(COALESCE(last_message_at, %s), %s)
                        WHERE id = %s""",
                    (ts, ts, chat_id),
                )
        with_retry(op)

    def find_or_create_contact(self, pn: str | None, lid: str | None,
                               push_name: str | None = None,
                               username: str | None = None) -> int | None:
        """Resolve BOTH addresses to ONE contact row, merging when they arrive together.

        This is the correctness heart of the module. See `_wa_parse.identity_from_info`
        for why one human can be two addresses.
        """
        if not pn and not lid:
            return None

        def op():
            with cursor(self.dsn) as cur:
                by_pn = by_lid = None
                if pn:
                    cur.execute(f"SELECT id FROM {SCHEMA}.contacts WHERE pn = %s", (pn,))
                    row = cur.fetchone()
                    by_pn = row["id"] if row else None
                if lid:
                    cur.execute(f"SELECT id FROM {SCHEMA}.contacts WHERE lid = %s", (lid,))
                    row = cur.fetchone()
                    by_lid = row["id"] if row else None

                if by_pn and by_lid and by_pn != by_lid:
                    self._merge_contacts(cur, keep=by_pn, merge=by_lid,
                                         evidence=f"dual_address pn={pn} lid={lid}")
                    contact_id = by_pn
                else:
                    contact_id = by_pn or by_lid

                if contact_id:
                    cur.execute(
                        f"""UPDATE {SCHEMA}.contacts
                            SET pn = COALESCE(pn, %s),
                                lid = COALESCE(lid, %s),
                                push_name = COALESCE(%s, push_name),
                                username = COALESCE(%s, username),
                                last_seen_at = now()
                            WHERE id = %s""",
                        (pn, lid, push_name, username, contact_id),
                    )
                else:
                    cur.execute(
                        f"""INSERT INTO {SCHEMA}.contacts (pn, lid, push_name, username)
                            VALUES (%s, %s, %s, %s) RETURNING id""",
                        (pn, lid, push_name, username),
                    )
                    contact_id = cur.fetchone()["id"]
                return contact_id
        return with_retry(op)

    def annotate_contact(self, address: str, *, push_name: str | None = None,
                         username: str | None = None) -> int:
        """Label an EXISTING contact, by either of its addresses. Returns rows hit.

        🔴 DELIBERATELY DOES NOT CREATE THE ROW. The push-name list is the account's
        entire address book — ~100 entries in a single payload — so creating a
        contact per entry would fill `contacts` with people the operator has never
        exchanged a message with. Labelling what already exists keeps the table
        meaning "people we have messages from".
        """
        if not address or (push_name is None and username is None):
            return 0

        def op():
            with cursor(self.dsn) as cur:
                cur.execute(
                    f"""UPDATE {SCHEMA}.contacts
                        SET push_name = COALESCE(%s, push_name),
                            username  = COALESCE(%s, username),
                            last_seen_at = now()
                        WHERE pn = %s OR lid = %s""",
                    (push_name, username, address, address),
                )
                return cur.rowcount
        return with_retry(op)

    @staticmethod
    def _merge_contacts(cur, *, keep: int, merge: int, evidence: str) -> None:
        """Collapse two contact rows into one, repointing every dependent.

        🔴 THE ORDER IS THE WHOLE FUNCTION, AND EVERY STEP WAS LEARNED FROM A LIVE
        POSTGRES FAILURE — a hermetic fake accepted all of these happily.

        1. Read the loser's identity values FIRST. They are needed for step 4 and
           the row is gone by then.
        2. Repoint the dependents. `group_participants` has a COMPOSITE primary key,
           so the loser's duplicate rows for a shared group must be deleted before
           the repoint or the UPDATE aborts on the constraint.
        3. DELETE the loser.
        4. Only now copy its pn/lid onto the survivor.

        Steps 3-then-4 are the part that matters, and both orderings naively
        suggest themselves wrong:
          * copy-then-delete raises
              duplicate key value violates unique constraint "contacts_lid_key"
            because the survivor is assigned `lid` while the loser still holds it —
            the partial unique index is checked per statement, not at COMMIT.
          * NULLing the loser's columns instead of deleting it raises the
            `contacts_identity_present` CHECK, which requires at least one of
            pn/lid to be non-null.
        Measured on the live schema: 4 of the 6 tests in
        `tests/test_db_identity.py` failed before this ordering, and none of them
        could fail hermetically.
        """
        if keep == merge:
            return

        cur.execute(
            f"SELECT pn, lid, push_name FROM {SCHEMA}.contacts WHERE id = %s",
            (merge,),
        )
        row = cur.fetchone()
        if row is None:  # already merged by an earlier event
            return
        loser_pn, loser_lid, loser_push = row["pn"], row["lid"], row["push_name"]

        # 2. dependents first — nothing may still reference the loser at step 3.
        cur.execute(f"UPDATE {SCHEMA}.messages SET sender_contact_id=%s WHERE sender_contact_id=%s", (keep, merge))
        cur.execute(f"UPDATE {SCHEMA}.reactions SET sender_contact_id=%s WHERE sender_contact_id=%s", (keep, merge))
        cur.execute(
            f"""DELETE FROM {SCHEMA}.group_participants gp
                USING {SCHEMA}.group_participants k
                WHERE gp.group_id = k.group_id AND gp.contact_id = %s AND k.contact_id = %s""",
            (merge, keep),
        )
        cur.execute(f"UPDATE {SCHEMA}.group_participants SET contact_id=%s WHERE contact_id=%s", (keep, merge))

        # 3. drop the loser (frees both partial unique indexes).
        cur.execute(f"DELETE FROM {SCHEMA}.contacts WHERE id = %s", (merge,))

        # 4. carry its identity onto the survivor, filling only the gaps.
        cur.execute(
            f"""UPDATE {SCHEMA}.contacts
                SET pn = COALESCE(pn, %s),
                    lid = COALESCE(lid, %s),
                    push_name = COALESCE(push_name, %s),
                    last_seen_at = now()
                WHERE id = %s""",
            (loser_pn, loser_lid, loser_push, keep),
        )

        cur.execute(
            f"INSERT INTO {SCHEMA}.contact_merges (kept_id, merged_id, evidence) VALUES (%s, %s, %s)",
            (keep, merge, evidence),
        )

    def upsert_group(self, jid: str, subject: str | None = None) -> int:
        def op():
            with cursor(self.dsn) as cur:
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.groups (jid, subject)
                        VALUES (%s, %s)
                        ON CONFLICT (jid) DO UPDATE
                          SET subject = COALESCE(EXCLUDED.subject, {SCHEMA}.groups.subject),
                              updated_at = now()
                        RETURNING id""",
                    (jid, subject),
                )
                return cur.fetchone()["id"]
        return with_retry(op)

    # -- messages -----------------------------------------------------------
    def insert_message(self, *, chat_id: int, whatsapp_id: str,
                       sender_contact_id: int | None, from_me: bool, kind: str,
                       body: str | None, caption: str | None,
                       quoted_whatsapp_id: str | None, message_ts,
                       is_view_once: bool, expires_at, raw: dict,
                       edited_at=None, revoked_at=None) -> tuple[int, bool]:
        """Returns (row_id, was_new). `xmax = 0` distinguishes INSERT from UPDATE."""
        def op():
            with cursor(self.dsn) as cur:
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.messages
                          (chat_id, whatsapp_id, sender_contact_id, from_me, kind, body,
                           caption, quoted_whatsapp_id, message_ts, is_view_once,
                           expires_at, raw, edited_at, revoked_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (chat_id, whatsapp_id) DO UPDATE
                          SET body = COALESCE(EXCLUDED.body, {SCHEMA}.messages.body),
                              caption = COALESCE(EXCLUDED.caption, {SCHEMA}.messages.caption),
                              kind = CASE WHEN {SCHEMA}.messages.kind = 'unknown'
                                          THEN EXCLUDED.kind ELSE {SCHEMA}.messages.kind END,
                              edited_at = COALESCE(EXCLUDED.edited_at, {SCHEMA}.messages.edited_at),
                              revoked_at = COALESCE(EXCLUDED.revoked_at, {SCHEMA}.messages.revoked_at),
                              raw = EXCLUDED.raw
                        RETURNING id, (xmax = 0) AS inserted""",
                    (chat_id, whatsapp_id, sender_contact_id, from_me, kind, body,
                     caption, quoted_whatsapp_id, message_ts, is_view_once,
                     expires_at, json.dumps(raw), edited_at, revoked_at),
                )
                row = cur.fetchone()
                return row["id"], bool(row["inserted"])
        return with_retry(op)

    def revoke_message(self, chat_id: int, target_id: str) -> int:
        """Tombstone, never delete: the row is evidence that something was sent."""
        def op():
            with cursor(self.dsn) as cur:
                cur.execute(
                    f"""UPDATE {SCHEMA}.messages
                        SET revoked_at = now(), body = NULL, caption = NULL
                        WHERE chat_id = %s AND whatsapp_id = %s""",
                    (chat_id, target_id),
                )
                return cur.rowcount
        return with_retry(op)

    def insert_attachment(self, *, message_id: int, media_type: str,
                          mimetype: str | None, filename: str | None,
                          file_size: int | None, sha256: str | None,
                          minio_key: str | None, download_error: str | None) -> None:
        def op():
            with cursor(self.dsn) as cur:
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.attachments
                          (message_id, media_type, mimetype, filename, file_size,
                           sha256, minio_key, download_error, downloaded_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,
                                CASE WHEN %s IS NULL THEN NULL ELSE now() END)
                        ON CONFLICT (message_id, media_type) DO UPDATE
                          SET minio_key = COALESCE(EXCLUDED.minio_key, {SCHEMA}.attachments.minio_key),
                              download_error = EXCLUDED.download_error,
                              downloaded_at = COALESCE(EXCLUDED.downloaded_at,
                                                       {SCHEMA}.attachments.downloaded_at)""",
                    (message_id, media_type, mimetype, filename, file_size,
                     sha256, minio_key, download_error, minio_key),
                )
        with_retry(op)

    def finish_attachment(self, *, message_id: int, media_type: str,
                          minio_key: str | None, error: str | None,
                          sha256: str | None, file_size: int | None) -> None:
        """Second half of the two-phase media write.

        The row is inserted BEFORE the upload so a crash leaves evidence; this
        fills in the key (or the error) afterwards. `minio_key IS NULL` plus an
        error is the honest marker for "message stored, bytes not archived".
        """
        def op():
            with cursor(self.dsn) as cur:
                cur.execute(
                    f"""UPDATE {SCHEMA}.attachments
                        SET minio_key = COALESCE(%s, minio_key),
                            download_error = %s,
                            sha256 = COALESCE(%s, sha256),
                            file_size = COALESCE(%s, file_size),
                            downloaded_at = CASE WHEN %s IS NULL THEN downloaded_at
                                                 ELSE now() END
                        WHERE message_id = %s AND media_type = %s""",
                    (minio_key, error, sha256, file_size, minio_key,
                     message_id, media_type),
                )
        with_retry(op)

    def upsert_reaction(self, *, chat_id: int, target_id: str,
                        sender_contact_id: int | None, emoji: str,
                        reaction_ts, removed: bool) -> None:
        def op():
            with cursor(self.dsn) as cur:
                # Resolve the target if we already have it; otherwise leave NULL and
                # let a later pass settle it (delivery is unordered, history is not backfilled).
                cur.execute(
                    f"SELECT id FROM {SCHEMA}.messages WHERE chat_id=%s AND whatsapp_id=%s",
                    (chat_id, target_id),
                )
                row = cur.fetchone()
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.reactions
                          (chat_id, target_whatsapp_id, message_id, sender_contact_id,
                           emoji, reaction_ts, removed)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (chat_id, target_whatsapp_id, sender_contact_id) DO UPDATE
                          SET emoji = EXCLUDED.emoji, removed = EXCLUDED.removed,
                              reaction_ts = EXCLUDED.reaction_ts,
                              message_id = COALESCE(EXCLUDED.message_id, {SCHEMA}.reactions.message_id)""",
                    (chat_id, target_id, row["id"] if row else None,
                     sender_contact_id, emoji, reaction_ts, removed),
                )
        with_retry(op)

    # -- history / health ---------------------------------------------------
    def store_history_sync(self, payload: dict, *, conversation_cnt: int | None,
                           message_cnt: int | None, parsed_messages: int,
                           error: str | None) -> int:
        def op():
            with cursor(self.dsn) as cur:
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.history_syncs
                          (conversation_cnt, message_cnt, parsed_messages, error, payload)
                        VALUES (%s,%s,%s,%s,%s) RETURNING id""",
                    (conversation_cnt, message_cnt, parsed_messages, error, json.dumps(payload)),
                )
                return cur.fetchone()["id"]
        return with_retry(op)

    def update_health(self, instance: str, counters: dict) -> None:
        def op():
            with cursor(self.dsn) as cur:
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.consumer_health
                          (instance, last_event_at, last_success_at, counters, updated_at)
                        VALUES (%s, now(), now(), %s, now())
                        ON CONFLICT (instance) DO UPDATE
                          SET last_event_at = now(), last_success_at = now(),
                              counters = EXCLUDED.counters, updated_at = now()""",
                    (instance, json.dumps(counters)),
                )
        with_retry(op)

    # -- allowlist ----------------------------------------------------------
    def allowlist(self) -> list[dict]:
        with cursor(self.dsn) as cur:
            cur.execute(f"SELECT group_jid, note, added_at FROM {SCHEMA}.group_allowlist ORDER BY added_at")
            return [dict(r) for r in cur.fetchall()]

    def allowlist_add(self, jid: str, note: str | None, added_by: str | None) -> None:
        def op():
            with cursor(self.dsn) as cur:
                cur.execute(
                    f"""INSERT INTO {SCHEMA}.group_allowlist (group_jid, note, added_by)
                        VALUES (%s,%s,%s)
                        ON CONFLICT (group_jid) DO UPDATE SET note = EXCLUDED.note""",
                    (jid, note, added_by),
                )
        with_retry(op)

    def allowlist_remove(self, jid: str) -> int:
        def op():
            with cursor(self.dsn) as cur:
                cur.execute(f"DELETE FROM {SCHEMA}.group_allowlist WHERE group_jid = %s", (jid,))
                return cur.rowcount
        return with_retry(op)

    def is_allowlisted(self, jid: str) -> bool:
        with cursor(self.dsn) as cur:
            cur.execute(f"SELECT 1 FROM {SCHEMA}.group_allowlist WHERE group_jid = %s", (jid,))
            return cur.fetchone() is not None

    # -- read surface -------------------------------------------------------
    # ----------------------------------------------------------------------- #
    # NAMING A CHAT ON THE READ PATH.
    #
    # 🔴 FOR A DM THE NAME IS THE COUNTERPART'S, AND `chats.display_name` IS NOT
    # IT. Measured on a real archive of 103 DMs: `chats.display_name` is written by
    # `consumer._handle_message` from the SENDER's push name, and on a `from_me`
    # message that push name is OUR OWN — the stored event carries our push name
    # while `Info.Chat` is the counterpart. So any DM we have written to can be
    # labelled with our own name: the busiest chat in that archive (1,207 messages)
    # read OUR name, and a 42-in/42-out chat whose counterpart's OWN messages name
    # them something else read OUR name too. (Counterparts' own names are chat
    # content and are not repeated here.) A wrong name is worse than no name, so
    # the sources below are ordered by how directly they belong to the COUNTERPART,
    # not by how friendly they look.
    # ----------------------------------------------------------------------- #

    #: The COUNTERPART's contact row for a DM chat, matched on the chat's address.
    #:
    #: `contacts.pn` / `contacts.lid` hold BARE addresses (`19990000003`, not
    #: `19990000003@s.whatsapp.net`) — see `_wa_parse.bare_address` — which is why
    #: this compares `split_part(c.jid, '@', 1)`. The SERVER part is compared as
    #: well, so a number that is one person's PN and another person's LID cannot
    #: cross-match (measured 2026-11-13: zero such collisions, and this is what
    #: keeps it true). `LIMIT 1` plus the ORDER BY makes the row deterministic and
    #: prefers one that actually carries a name.
    #:
    #: ⚠ `conversations()` GROUPs BY this lateral's columns — add a column here and
    #: that query needs it too. Postgres will say so; it does not infer the
    #: dependency from `c.id`.
    #:
    #: 🔴 THE ALIAS IS `counterpart`, NOT `ct`: `show()` already LEFT JOINs
    #: `contacts AS ct` for the per-MESSAGE sender, and the two are different
    #: people (on our own messages the sender is us, the counterpart is them).
    _CONTACT_OF_CHAT = f"""
        LEFT JOIN LATERAL (
            SELECT counterpart_contacts.push_name, counterpart_contacts.pn
              FROM {SCHEMA}.contacts counterpart_contacts
             WHERE (counterpart_contacts.pn = split_part(c.jid, '@', 1)
                    AND split_part(c.jid, '@', 2) = 's.whatsapp.net')
                OR (counterpart_contacts.lid = split_part(c.jid, '@', 1)
                    AND split_part(c.jid, '@', 2) = 'lid')
             ORDER BY (counterpart_contacts.push_name IS NULL), counterpart_contacts.id
             LIMIT 1
        ) counterpart ON true
    """

    #: The name the COUNTERPART declared on the most recent message THEY sent us.
    #:
    #: 🔴 `NOT m.from_me` IS THE POINT OF THIS SUBQUERY, not a refinement of it: on
    #: a `from_me` message `Info.PushName` is OUR push name, which is exactly how
    #: `chats.display_name` got contaminated. Only an INCOMING message names the
    #: counterpart, so this is the one source that cannot be our own name.
    _WIRE_PUSH_NAME = f"""
        (SELECT NULLIF(btrim(m.raw -> 'event' -> 'Info' ->> 'PushName'), '')
           FROM {SCHEMA}.messages m
          WHERE m.chat_id = c.id
            AND NOT m.from_me
            AND NULLIF(btrim(m.raw -> 'event' -> 'Info' ->> 'PushName'), '') IS NOT NULL
          ORDER BY m.message_ts DESC
          LIMIT 1) AS wire_push_name
    """

    @staticmethod
    def _named(row: dict) -> dict:
        """Set `display_name` to the name to SHOW (see `_resolve_chat_name`)."""
        row["display_name"] = _resolve_chat_name(row)
        return row

    def conversations(self, limit: int = 20) -> list[dict]:
        with cursor(self.dsn) as cur:
            cur.execute(
                f"""SELECT c.jid, c.chat_type, c.last_message_at,
                           c.display_name AS chats_display_name,
                           c.username     AS chats_username,
                           counterpart.push_name AS contact_push_name,
                           counterpart.pn        AS contact_pn,
                           {self._WIRE_PUSH_NAME},
                           count(m.id) AS messages,
                           (SELECT left(coalesce(m2.body, '[' || m2.kind || ']'), 80)
                              FROM {SCHEMA}.messages m2
                             WHERE m2.chat_id = c.id
                             ORDER BY m2.message_ts DESC LIMIT 1) AS last_message,
                           EXISTS (SELECT 1 FROM {SCHEMA}.group_allowlist a
                                    WHERE a.group_jid = c.jid) AS allowlisted
                      FROM {SCHEMA}.chats c
                      LEFT JOIN {SCHEMA}.messages m ON m.chat_id = c.id
                      {self._CONTACT_OF_CHAT}
                     GROUP BY c.id, counterpart.push_name, counterpart.pn
                     ORDER BY c.last_message_at DESC NULLS LAST
                     LIMIT %s""",
                (limit,),
            )
            return [self._named(dict(r)) for r in cur.fetchall()]

    def search(self, query: str, limit: int = 20, *, allowlisted_only: bool = False) -> list[dict]:
        predicate = ""
        params: list = [query]
        if allowlisted_only:
            predicate = f"""AND EXISTS (SELECT 1 FROM {SCHEMA}.group_allowlist a
                                        WHERE a.group_jid = c.jid)"""
        with cursor(self.dsn) as cur:
            cur.execute(
                f"""SELECT c.jid, c.chat_type, c.last_message_at,
                           c.display_name AS chats_display_name,
                           c.username     AS chats_username,
                           counterpart.push_name AS contact_push_name,
                           counterpart.pn        AS contact_pn,
                           {self._WIRE_PUSH_NAME},
                           m.message_ts, m.kind,
                           m.body, m.whatsapp_id,
                           ts_rank(m.search, websearch_to_tsquery('english', %s)) AS rank
                      FROM {SCHEMA}.messages m
                      JOIN {SCHEMA}.chats c ON c.id = m.chat_id
                      {self._CONTACT_OF_CHAT}
                     WHERE m.search @@ websearch_to_tsquery('english', %s)
                           {predicate}
                     ORDER BY m.message_ts DESC
                     LIMIT %s""",
                (query, query, limit),
            )
            return [self._named(dict(r)) for r in cur.fetchall()]

    def show(self, jid: str, limit: int = 30) -> list[dict]:
        with cursor(self.dsn) as cur:
            cur.execute(
                f"""SELECT m.message_ts, m.whatsapp_id, m.kind, m.body, m.caption,
                           m.from_me, m.revoked_at,
                           coalesce(ct.push_name, ct.pn, ct.lid, '(unknown)') AS sender,
                           a.minio_key, a.filename, a.download_error,
                           c.chat_type,
                           c.display_name AS chats_display_name,
                           c.username     AS chats_username,
                           counterpart.push_name AS contact_push_name,
                           counterpart.pn        AS contact_pn,
                           {self._WIRE_PUSH_NAME}
                      FROM {SCHEMA}.messages m
                      JOIN {SCHEMA}.chats c ON c.id = m.chat_id
                      LEFT JOIN {SCHEMA}.contacts ct ON ct.id = m.sender_contact_id
                      LEFT JOIN {SCHEMA}.attachments a ON a.message_id = m.id
                      {self._CONTACT_OF_CHAT}
                     WHERE c.jid = %s
                     ORDER BY m.message_ts DESC
                     LIMIT %s""",
                (jid, limit),
            )
            return [self._named(dict(r)) for r in cur.fetchall()]

    def history_syncs(self) -> list[dict]:
        """Every stored HistorySync payload, oldest first — the RE-PARSE input.

        These are kept verbatim precisely so a parser fix can replay them instead
        of depending on WhatsApp re-delivering history it has already deleted.
        """
        with cursor(self.dsn) as cur:
            cur.execute(f"SELECT id, payload FROM {SCHEMA}.history_syncs ORDER BY id")
            return [dict(r) for r in cur.fetchall()]

    def stats(self) -> dict:
        out: dict = {}
        with cursor(self.dsn) as cur:
            for label, sql in (
                ("chats", f"SELECT count(*) AS n FROM {SCHEMA}.chats"),
                ("messages", f"SELECT count(*) AS n FROM {SCHEMA}.messages"),
                ("attachments", f"SELECT count(*) AS n FROM {SCHEMA}.attachments"),
                ("media_missing", f"SELECT count(*) AS n FROM {SCHEMA}.attachments WHERE minio_key IS NULL"),
                ("contacts", f"SELECT count(*) AS n FROM {SCHEMA}.contacts"),
                ("contact_merges", f"SELECT count(*) AS n FROM {SCHEMA}.contact_merges"),
                ("unresolved_reactions", f"SELECT count(*) AS n FROM {SCHEMA}.reactions WHERE message_id IS NULL"),
                ("history_syncs", f"SELECT count(*) AS n FROM {SCHEMA}.history_syncs"),
                ("deliveries", f"SELECT count(*) AS n FROM {SCHEMA}.webhook_deliveries"),
            ):
                cur.execute(sql)
                out[label] = cur.fetchone()["n"]
            cur.execute(f"SELECT max(message_ts) AS newest FROM {SCHEMA}.messages")
            out["newest_message"] = cur.fetchone()["newest"]
        return out