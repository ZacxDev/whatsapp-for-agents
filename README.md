# whatsapp-for-agents

[![ci](https://github.com/ZacxDev/whatsapp-for-agents/actions/workflows/ci.yml/badge.svg)](https://github.com/ZacxDev/whatsapp-for-agents/actions/workflows/ci.yml)

**Your WhatsApp history, in Postgres, queryable — with sending that a human has to approve.**

WhatsApp has no API for this. There is no export, no search endpoint, no way to ask
"what did we agree about the deposit?" across a year of messages. This is the other
half: a durable, searchable archive that a program can read, plus an outbound path
built so that an agent can *draft* in your voice and never *send* without you.

> Status: works, runs, and is verified end to end against real Postgres. It is not
> a hosted product and it links your personal account — read [Before you run
> it](#before-you-run-it) first.

---

## Why it exists

A linked companion device gives you the message stream. Almost nobody ships the
**store**: schema, identity resolution, deduplication, media archiving, a read path
you can query, and a send path with a gate. That is what this is.

Two ideas carry most of the value:

**1. The two-image split.** The process that parses messages from the internet holds
no send credential and ships no code that can post a message. The process that can
send is a separate image, a separate deployment, and the only holder of a send
token. `tests/test_image_split.py` reads *both* Dockerfiles and fails if either
grows the other's modules — a guard on the *relationship*, so it fails whichever
direction the mistake is in. A parser bug cannot reach a send endpoint, structurally.

**2. The gate is bound to the payload.** Compose and transmit are separate calls
with a durable row between them, and the approval is a digest over the body **plus
the sha256 of every attachment**. Edit a draft after approving it and it refuses to
send. `approve` additionally needs an operator token that is deliberately absent
from the pod's own environment, so nothing that can exec there can approve its own
draft.

---

## Architecture

```
  your phone ──linked companion device──►  whatsapp-api  (asternic/wuzapi · whatsmeow)
                                            │  session keys in Postgres db `wuzapi`
                                            │
                                 HMAC-signed webhook POST /hook
                                            ▼
                                    whatsapp-consumer            ──►  Postgres  schema `whatsapp`
                                    (no send token, no send code)     (the archive)
                                            │
                                            └──────────────────►  S3  bucket `whatsapp-attachments`
                                                                  (private objects, own policy)

  whatsapp-sender  ── the ONLY pod holding a send token ──►  whatsapp-api ──►  the message
     draft ─► approve (operator) ─► send ─► reconcile
       └──────────── every step is an explicit operator command ───────────┘
```

Bundled: [`docker-compose.yaml`](./docker-compose.yaml) (Postgres + the three
processes) and [`deploy/k8s/`](./deploy/k8s/) (the same shape as manifests).
Design notes live in [`docs/`](./docs/).

---

## Quickstart

```bash
git clone https://github.com/ZacxDev/whatsapp-for-agents && cd whatsapp-for-agents
cp .env.example .env        # ⚠ fill it in — never reuse the CHANGE-ME values

docker compose up -d postgres
docker compose run --rm consumer python3 consumer.py init-schema
docker compose up -d

# link the device — this step has an order that matters, see docs/pairing.md
```

Then:

```bash
docker compose exec consumer python3 consumer.py health        # exit 0 = the process is ticking
docker compose exec consumer python3 consumer.py conversations --limit 20
docker compose exec consumer python3 consumer.py search "deposit"
docker compose exec consumer python3 consumer.py show '15550000001@s.whatsapp.net'
```

Media archiving is an opt-in profile, because an archive of *text* needs no object
store at all:

```bash
docker compose --profile media up -d
```

## Running it directly (no Docker)

Nothing here depends on a container: the code wants a Postgres DSN and an S3
endpoint, both by environment variable.

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -e .                        # or: pip install -r requirements.txt

export WHATSAPP_PG_DSN='postgresql://whatsapp_app:...@127.0.0.1:5432/whatsapp'
export WHATSAPP_HMAC_KEY=...            # must match wuzapi's — global AND per-user
export MINIO_ARCHIVE_ENDPOINT=http://127.0.0.1:9000
export MINIO_ARCHIVE_ACCESS_KEY=... MINIO_ARCHIVE_SECRET_KEY=...

whatsapp-for-agents init-schema         # applies the `whatsapp` schema
whatsapp-for-agents conversations --limit 20
whatsapp-for-agents search "deposit"
whatsapp-for-agents show '15550000001@s.whatsapp.net'
whatsapp-for-agents-sender ls
```

`init-schema` comes from the module that owns the DDL, so there is no separate
migration file to keep in sync. Drop the `MINIO_ARCHIVE_*` variables and the archive
still works — message rows are stored, media is recorded as failed rather than
fetched.

## Running it with Nix

The flake is a second supported build path, not an afterthought:

```bash
nix run github:ZacxDev/whatsapp-for-agents -- conversations --limit 20

nix build .#consumer-image              # the read half, as an OCI image
nix build .#sender-image                # the send half
docker load -i result

nix build .#checks.x86_64-linux.tests   # the hermetic suite, in the sandbox
nix develop                             # python3 + psycopg2 + minio + pytest
```

As a flake input:

```nix
{
  inputs.whatsapp-for-agents.url = "github:ZacxDev/whatsapp-for-agents";
  # provides, per system:
  #   packages.whatsapp-for-agents   the operator CLI
  #   packages.consumer-image        the read half, as an image
  #   packages.sender-image          the send half
  #   checks.tests                   the hermetic suite
  #   devShells.default
}
```

🔴 **The split survives the Nix path, and that took saying.** The consumer and
sender module sets are declared in `flake.nix`, and their disjointness is asserted
at **eval time** — so a `nix build` of anything in the flake fails before compiling
if either set drifts into the other. Three guards now cover this property from three
angles: `tests/test_image_split.py` reads the Dockerfiles' `COPY` lines, the flake
checks its own declaration, and CI reads the **built** images' `/app` listing — which
is the only one of the three that inspects what actually lands in an image rather
than a declaration of intent.

### Sending

```bash
S="docker compose exec sender python3 sender.py"
$S draft '15550000001@s.whatsapp.net' --body 'on my way'
$S ls
# the operator, and ONLY the operator, supplies the token — it is not in the pod:
docker compose exec -e WHATSAPP_APPROVAL_TOKEN="$TOKEN" sender python3 sender.py approve 1 --ref 'thread-42'
$S send 1
```

Full command reference: [`docs/operating.md`](./docs/operating.md).

---

## What it survives

The archive is built for the messy parts, each of which is documented in `docs/`:

| Problem | What happens |
|---|---|
| wuzapi retries a webhook | every delivery is recorded first; a seen key is a no-op that still answers 200 |
| Someone edits or revokes a message | the row is updated in place / tombstoned — never deleted, so it stays evidence |
| A reaction arrives before its target | kept, resolved later, unresolvable rows retained |
| One person exists as **two addresses** | WhatsApp now addresses humans by phone number *and* by an opaque LID; the same person arrives as both, in either order. Contacts are merged with an audit row |
| A group's media would flood the disk | every group's message **rows** are stored; only allowlisted groups' **media** is downloaded |
| The pairing history | delivered **once**, then acked and deleted server-side — so raw payloads are stored verbatim before parsing, making a parser bug cost a re-parse instead of the history |

### One bug worth reading, because it is the kind you ship

Chats in a DM were being named after the **wrong person** — and the wrong person was
*us*. The ingest stamps the *sender's* push name onto the *chat*, and on a message
you sent, the sender is you. Measured on a real archive of 103 DMs: only 13 were
named, and 2 were wrong — the busiest chat in the archive (1,207 messages) was
labelled with the owner's own name. A naive "join to the contacts table" would have
re-shipped exactly that wrong name.

`_wa_db._resolve_chat_name()` now orders its sources by how directly each belongs to
the **counterpart**: newest *incoming* message's PushName → the matched contact row →
`chats.username` → `chats.display_name` → the counterpart's phone number, and that
last one only on LID-keyed chats. Read [`docs/identity.md`](./docs/identity.md).

---

## Verified, with the controls

Everything below was run against the code in this repo, not reasoned about:

- **165 tests pass, 0 skips**, against a real Postgres — including the two suites
  that are skipped by default and the SQL-wiring suite that a hermetic test
  structurally cannot cover.
- **The role split holds**, with both controls: the archive role connects to its own
  database *and is refused* on wuzapi's (`permission denied for database "wuzapi"`).
- **The image split holds**: both images build, and the suite runs *inside* the
  consumer image against the modules that image actually installed.
- **RED/GREEN on the naming fix**: the regression suite errors against the pre-fix
  module and passes against this one.
- **A shipped bug found and fixed in the extraction.** The sender's `/healthz`
  handler and its `serve()` each spelled their own fallback path for
  `WHATSAPP_SENDER_HEARTBEAT`, with *different* values — so with that variable unset,
  the probe looked for a file nobody wrote and the pod reported unhealthy forever.
  Measured: **HTTP 503 before, HTTP 200 after.**
- **Both Nix images build**, and their `/app` listings are exactly the disjoint module
  sets — `consumer.py _wa_db.py _wa_parse.py _wa_webhook.py _minio.py` and
  `sender.py _wa_outbound.py _wa_db.py`. Asserted in CI against the *built* images.
- **The flake's split guard fires**: injecting `sender.py` into the consumer's module
  set makes `nix build` fail at eval time with the guard's message, before anything
  compiles.
- **A Nix-built consumer image works**: it applied the `whatsapp` schema to a real
  Postgres and answered `conversations` and `stats --json` from it — the same code
  path the Docker image takes.

---

## Before you run it

⚠ **This uses an unofficial WhatsApp client.** `whatsmeow` implements the WhatsApp
Web multi-device protocol; Meta does not sanction it. Linking your account this way
is against WhatsApp's Terms of Service, and the realistic failure mode is **your
account being banned**. This is a deliberate trade made by an adult about their own
account, not something to deploy for someone else.

⚠ **The archive is other people's messages.** Their words, their photos, their
phone numbers. If you run this, **you are the data controller** for that data —
consent, retention and subject-access obligations are yours, and they differ by
jurisdiction.

⚠ **Never run two consumers, two senders, or a second daemon against one linked
device.** WhatsApp treats a second connection for one device identity as a
compromise; the outcome is a logout or a device removal, not a merge. That is why
the k8s manifests use `strategy: Recreate` and `replicas: 1`.

⚠ **The phone must be used at least every ~14 days**, or WhatsApp logs out every
companion device, this one included.

**Not implemented, so you do not have to find out the hard way:** media retention (a
sweep — nothing deletes old attachments), NetworkPolicy, `readOnlyRootFilesystem`,
and any LLM action-extraction from chat. Expiring messages are stored and never
swept; the archive deliberately does not honour WhatsApp's timers.

---

## Docs

| | |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | the pipeline, every table, the image and role splits |
| [`docs/identity.md`](docs/identity.md) | PN vs LID, contact merging, the chat-naming rule |
| [`docs/pairing.md`](docs/pairing.md) | linking a device, the one-shot history, the HMAC trap |
| [`docs/send-gate.md`](docs/send-gate.md) | draft → approve → send, the digest, what it refuses |
| [`docs/operating.md`](docs/operating.md) | commands, liveness, the counters |
| [`deploy/k8s/README.md`](deploy/k8s/README.md) | running it on Kubernetes |

## Licence

MIT — see [LICENSE](./LICENSE). Not affiliated with, endorsed by, or connected to
WhatsApp or Meta. `wuzapi` and `whatsmeow` are separate projects with their own
licences.
