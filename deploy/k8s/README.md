# Deploying `whatsapp-for-agents` on Kubernetes

Three pods, and the split between them *is* the product:

| Pod | Image | Holds | Can send? |
|---|---|---|---|
| `whatsapp-api` | `asternic/wuzapi` (upstream) | the linked-device session, in Postgres | it *is* the WhatsApp client |
| `whatsapp-consumer` | `whatsapp-for-agents-consumer` | the archive (`whatsapp` schema) + media in object storage | **no — it holds no send token at all** |
| `whatsapp-sender` | `whatsapp-for-agents-sender` | the one send token, and nothing else | yes, but only for a draft a human approved |

`whatsapp-api` accepts the WhatsApp webhook and pushes HMAC-signed JSON to
`whatsapp-consumer`. `whatsapp-sender` is operator-driven; nothing else can reach it.

## 🔴 `replicas: 1` + `strategy: Recreate` is a hard constraint

All three Deployments carry it, and it is not capacity tuning. A WhatsApp linked
device holds credentials and ratchet state that assume a **single writer**, and
WhatsApp treats a second connection for the same device identity as a compromise —
the failure is a logout or a device removal, not a merge. `RollingUpdate` would run
two pods at once by definition, which is the exact thing being forbidden. Do not
scale these, and do not run a second client against the same session (including a
second WUZAPI user, which would be a second linked device).

## Prerequisites

* A Kubernetes cluster, and a place to run these images.
* A PostgreSQL server (this repo ships **no** database — the pods only consume one,
  and the two Jobs below create a database and a schema in it).
* An S3-compatible object store for archived media and for the nightly backup.
* Permission to reach WhatsApp from the cluster; the daemon needs outbound network
  access and a phone to scan the pairing QR code.

Everything below lands in the `whatsapp` namespace.

## 1. Build and push the two images

From the **repo root**:

```bash
docker build    -t <registry>/whatsapp-for-agents-consumer:<ver> .
docker build -f Dockerfile.sender \
                -t <registry>/whatsapp-for-agents-sender:<ver> .
docker push <registry>/whatsapp-for-agents-consumer:<ver>
docker push <registry>/whatsapp-for-agents-sender:<ver>
```

Then edit the three image references:

| File | Image |
|---|---|
| `deployment-consumer.yaml` | the consumer image |
| `schema-init-job.yaml` | the **consumer** image (it runs `consumer.py init-schema`) |
| `deployment-sender.yaml` | the sender image |

`deployment-api.yaml` runs upstream `asternic/wuzapi`. 🔴 **Image refs here are
placeholders or mutable tags.** A real deployment should pin every one of them by
digest (`…@sha256:…`), including the third-party ones (`alpine`, `postgres`,
`minio/mc`) in `backup-cronjob.yaml`. With a digest, Kubernetes' default pull
policy is already correct.

## 2. Replace the placeholders

`secrets.yaml` contains **only** the literal string `CHANGE-ME`, and the
non-secret values in the other files are obvious `CHANGE-ME-…` placeholders too.
Nothing in this directory is a real credential; applying it unchanged yields a
cluster that can connect to nothing.

| File | Field | Set it to |
|---|---|---|
| `secrets.yaml` | `whatsapp-auth` → `WUZAPI_ADMIN_TOKEN` | a token guarding the admin/session API |
| `secrets.yaml` | `whatsapp-auth` → `WUZAPI_GLOBAL_ENCRYPTION_KEY` | set **before** the first pairing |
| `secrets.yaml` | `whatsapp-auth` → `WUZAPI_GLOBAL_HMAC_KEY` | the webhook signing key, **≥ 32 bytes** |
| `secrets.yaml` | `whatsapp-pg-admin` → `password` | the broader Postgres role's password |
| `secrets.yaml` | `whatsapp-app-db` → `pg-dsn` | `postgresql://whatsapp_app:<pw>@<host>:5432/whatsapp` |
| `secrets.yaml` | `whatsapp-consumer-minio` → both keys | object-store access for media |
| `secrets.yaml` | `whatsapp-sender-auth` → `WUZAPI_USER_TOKEN` | the paired session's send token |
| `secrets.yaml` | `whatsapp-sender-auth` → `WHATSAPP_APPROVAL_TOKEN` | the human-approval token |
| `secrets.yaml` | `whatsapp-backup-s3` → both keys | object-store access for the backup prefix |
| `secrets.yaml` | `whatsapp-backup-s3` → `AGE_RECIPIENT` | an `age1…` recipient you control |
| `deployment-api.yaml` | `DB_HOST`, `DB_USER` | your Postgres host + the broader role |
| `db-init-job.yaml` | `PGHOST`, `PGUSER` | the same pair |
| `backup-cronjob.yaml` | `PGHOST`, `PGUSER` | the same pair |
| `deployment-consumer.yaml` | `MINIO_ARCHIVE_ENDPOINT` | `http://<host>:9000` |
| `backup-cronjob.yaml` | `S3_ENDPOINT`, `BACKUP_BUCKET` | the backup store and bucket |

Generate real material with `openssl rand -base64 32` (tokens and keys),
`openssl rand -hex 16` (passwords) and `age-keygen` (the backup recipient).

If you would rather not keep credentials in a manifest, delete `secrets.yaml` from
`kustomization.yaml` and create the six Secrets out of band with
`kubectl -n whatsapp create secret generic …`. The names and keys above are the
contract the pods read; nothing else about them is fixed.

### Who reads each Secret

| Secret | Keys | Read by |
|---|---|---|
| `whatsapp-auth` | `WUZAPI_ADMIN_TOKEN`, `WUZAPI_GLOBAL_ENCRYPTION_KEY`, `WUZAPI_GLOBAL_HMAC_KEY` | `whatsapp-api` (all three) · `whatsapp-consumer` (`WUZAPI_GLOBAL_HMAC_KEY`, as `WHATSAPP_HMAC_KEY`) |
| `whatsapp-pg-admin` | `password` | `whatsapp-api`, `db-init` Job, backup CronJob |
| `whatsapp-app-db` | `pg-dsn` | `whatsapp-consumer`, `whatsapp-sender`, `schema-init` Job |
| `whatsapp-consumer-minio` | `minio-access-key`, `minio-secret-key` | `whatsapp-consumer` |
| `whatsapp-sender-auth` | `WUZAPI_USER_TOKEN` | `whatsapp-sender` — **`WHATSAPP_APPROVAL_TOKEN` is NOT mounted anywhere** (see §7) |
| `whatsapp-backup-s3` | `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `AGE_RECIPIENT` | backup CronJob |

Two notes that are easy to get wrong:

* **The webhook key is one Secret key with two references.** The daemon signs with
  `whatsapp-auth/WUZAPI_GLOBAL_HMAC_KEY` and the consumer verifies the same key under
  its own env name. One value, two readers — so they cannot drift.
* **A `secretKeyRef` cannot cross namespaces.** It is always resolved in the
  *referencing pod's* namespace, so the correctly-scoped copy has to exist in
  `whatsapp`. Keeping it in a neighbouring namespace compiles and fails at container
  start.

## 3. Postgres: the role split

🔴 **This is a security property, not a convenience.** The consumer parses arbitrary,
internet-influenced payloads; it holds no wuzapi token, and it must also not be able
to *read* one. wuzapi's send tokens live in the `wuzapi` database, so the app role
must not be able to connect to that database at all.

Two credentials, deliberately:

* `whatsapp_app` — used by **consumer, sender, schema-init Job**. Owns schema
  `whatsapp`, can connect to the archive database, and has **no** path to `wuzapi`.
* the **broader** role in `whatsapp-pg-admin` — used by **the daemon, the db-init Job
  and the backup job**. It needs `CREATEDB`, and it is the only thing allowed to
  connect to `wuzapi`.

One-time setup, as a superuser:

```sql
-- 1. The app role: LOGIN only. No CREATEDB, no superuser, no membership in the
--    broader role.
CREATE ROLE whatsapp_app LOGIN PASSWORD '<app-password>';

-- 2. The archive database, and the schema the app role OWNS.
--    AUTHORIZATION matters: objects the app role creates are the ones the
--    consumer's `ensure_schema()` must later be able to ALTER. A schema created
--    by the broader role turns the next added column into a crash loop.
CREATE DATABASE whatsapp;
\connect whatsapp
CREATE SCHEMA IF NOT EXISTS whatsapp AUTHORIZATION whatsapp_app;
GRANT CONNECT ON DATABASE whatsapp TO whatsapp_app;

-- 3. The session database. The db-init Job creates it on first apply; creating it
--    here as well is harmless. What is NOT optional is the REVOKE:
--    PostgreSQL grants CONNECT on a new database to PUBLIC by default, so without
--    it `whatsapp_app` could connect to the session database and read the send
--    tokens, and the whole split would be a comment rather than a control.
CREATE DATABASE wuzapi;
\connect wuzapi
REVOKE CONNECT ON DATABASE wuzapi FROM PUBLIC;
GRANT CONNECT ON DATABASE wuzapi TO <broader-role>;

-- Verify the split from the app role: this MUST fail.
--   psql "postgresql://whatsapp_app:<pw>@<host>/wuzapi"
```

`whatsapp_app` has no CREATE privilege *on the database*, so `init-schema` cannot
create the schema from nothing — the `AUTHORIZATION` step above is what does.
Once the schema exists, the Job (and the consumer's startup `ensure_schema()`)
only ever adds the objects inside it.

## 4. Apply

```bash
kubectl apply -k deploy/k8s
```

Two Jobs run and must succeed before anything is useful:

* **`whatsapp-db-init-v1`** — creates the `wuzapi` database. Idempotent, and it
  asserts the database exists rather than trusting the branch its script took.
* **`whatsapp-schema-init-v1`** — applies the `whatsapp` schema by calling
  `consumer.py init-schema`. The schema is defined **once** in `_wa_db.py`; the
  consumer also calls `ensure_schema()` at startup, so this Job is a convenience,
  not the only path. Read its comments if it fails on a fresh database.

```bash
kubectl -n whatsapp logs job/whatsapp-db-init-v1
kubectl -n whatsapp logs job/whatsapp-schema-init-v1
```

🔴 Both are Jobs, and **a Job's pod template is immutable**. Any change to one of
those templates needs the `-vN` suffix in its name bumped (or the old object
deleted) — editing the file alone does not update the running object, and the apply
fails. The same convention applies to any Job you add.

Then confirm the pods and their probes:

```bash
kubectl -n whatsapp get pods
kubectl -n whatsapp get deploy
```

`whatsapp-api` will come up but the session is not paired yet — it is normal for it
to be connected to nothing at this point.

## 5. Pair the device (one-time)

`whatsapp-api` is a ClusterIP Service on purpose: a QR endpoint that hands out a link
to your WhatsApp account should not be reachable from outside the cluster, so it is
driven through a port-forward.

```bash
kubectl -n whatsapp port-forward svc/whatsapp-api 8080:8080
```

Then, via the port-forward: create a wuzapi user (with its history window set — it
only applies on a NEW pairing), subscribe it to `Message,HistorySync` only, then
`POST /session/connect` and scan the QR with the phone (**Linked devices → Link a
device**). The QR rotates, so scan within about a minute. The admin token from
`whatsapp-auth` authorises user creation; the session/QR calls are authenticated with
that user's own token. `docs/pairing.md` in this repo has the exact commands and the
payload shapes.

🔴 **The order is load-bearing, not ceremonial.** A fresh pairing delivers the
account's history **once** and then acks and deletes it server-side, so a delivery
refused at that moment costs the history permanently. **The consumer must be up and
healthy, and its HMAC key must be correct, before you scan.** `docs/pairing.md` in
this repo walks the whole sequence and the payload shapes.

**The token issued for that user is the send token** — put it in
`whatsapp-sender-auth/WUZAPI_USER_TOKEN` and restart the sender pod. Do not create a
second user: that is a second linked device, which is the compromise case described
at the top of this file.

Set `WUZAPI_GLOBAL_ENCRYPTION_KEY` **before** pairing; without it wuzapi generates
one at start and loses it on restart, taking the session with it.

### The webhook HMAC key is the failure that looks like nothing happening

The consumer verifies every delivery against `WHATSAPP_HMAC_KEY` and refuses a key
shorter than **32 bytes** — so the `CHANGE-ME` placeholder cannot be deployed by
accident: the consumer fails at startup rather than silently accepting nothing.
(That is the one place a placeholder failing loudly is a feature.)

🔴 Making the keys agree is more than one setting. wuzapi signs with its **global**
key *and* a **per-user** key set separately via `POST /session/hmac/config`; the
consumer only ever sees one value, so any key in the signing path must be that same
value. When they diverge the symptom is not an error — it is `unauthenticated`
climbing while nothing is stored, which reads as "the integration does nothing".
Verify with a signed probe, not by reading the config. `docs/pairing.md` has the
diagnosis.

After pairing:

```bash
kubectl -n whatsapp logs deploy/whatsapp-consumer   # webhook deliveries land here
```

## 6. Backup CronJob

`backup-cronjob.yaml` runs nightly at 04:00 UTC and needs three things:

1. **Both databases reachable by the broader role.** It dumps the archive
   (`whatsapp` schema of `APP_DATABASE`) *and* the whole `wuzapi` session database.
2. **A dedicated bucket credential.** Give `whatsapp-backup-s3` an account scoped to
   `<bucket>/whatsapp/*` and nothing else — not a shared backup credential that can
   read every other service's dumps.
3. **An age recipient you control.** The artifact is encrypted client-side, in the
   pod, before it leaves: the session dump is account-takeover material. Keep the
   private key off the cluster and off the object store.

It refuses to upload unless the artifact is age-encrypted, is above a small byte
floor, and the server reports back the same size. Before that it asserts the dumps
actually contain the archive's `messages` table and the session's device table — a
dump of the wrong database is a perfectly valid file, and a job that only checked
the upload would report green over a backup of nothing.

Kick one off by hand to prove it works:

```bash
kubectl -n whatsapp create job --from=cronjob/whatsapp-backup whatsapp-backup-test
```

Restore (scale `whatsapp-api` and `whatsapp-consumer` to 0 first):

```bash
mc cp <alias>/<bucket>/whatsapp/<TS>.tar.gz.age .
age -d -i <your-age-identity> <TS>.tar.gz.age | tar -xz
psql -d <archive-database> -f archive.sql   # the whatsapp schema
psql -d wuzapi             -f session.sql   # the linked device (daemon must be down)
```

Retention is `RETENTION` (14 days by default) applied to this prefix only.

## 7. Sending is operator-driven — there is no HTTP route that sends

`whatsapp-sender` has **no Service**. It serves `/healthz` for its own probes and
nothing else, so a stray `port-forward` cannot mint a message. Every act that sends
is a `kubectl exec` subcommand:

```bash
# Stores a draft and transmits NOTHING.
kubectl -n whatsapp exec deploy/whatsapp-sender -- \
  python3 /app/sender.py draft '<jid>' --body '…' [--attach FILE]

# Record the operator's approval. `--ref` is an EXTERNAL REFERENCE the approval is
# recorded under (a ticket id, your initials — whatever identifies the human
# decision), not a hash you have to compute. 🔴 The approval itself binds to the
# digest of the exact payload, so editing the draft afterwards invalidates it.
# The token is supplied HERE, never in the pod's environment.
kubectl -n whatsapp exec deploy/whatsapp-sender -- \
  env WHATSAPP_APPROVAL_TOKEN=<token> \
  python3 /app/sender.py approve <draft_id> --ref '<external-ref>'

kubectl -n whatsapp exec deploy/whatsapp-sender -- \
  python3 /app/sender.py send <draft_id>
```

`ls` lists drafts by state; `unapprove` withdraws an approval; and `reconcile
<id> --outcome sent|failed|reconciled` settles a draft stranded in `sending` — which
is the honest state after a crash mid-send, because "POSTed and died" and "never
POSTed" are indistinguishable from the pod. `docs/operating.md` and
`docs/send-gate.md` in this repo cover the states and flags.

🔴 `WHATSAPP_APPROVAL_TOKEN` is deliberately **not** in the Deployment, and
`approve`/`unapprove` refuse without it. `approve` is the human step, and a token in
the pod's environment would let anything that can exec there (an agent included)
approve its own draft — the gate would be decorative. Supply it on the exec that
approves, as above.

The archive and the media are read with the **consumer** image's other subcommands
(`conversations`, `search`, `show`, `allowlist`, `stats`), also via `kubectl exec`.
Consult the project docs for their flags; this file covers only the deployment.

## Directory layout

```
deploy/k8s/
├── namespace.yaml            the `whatsapp` namespace
├── secrets.yaml              🔴 placeholders — every value is CHANGE-ME
├── db-init-job.yaml          creates the `wuzapi` database
├── schema-init-job.yaml      applies the `whatsapp` schema
├── deployment-api.yaml       wuzapi — the linked-device session
├── service-api.yaml          ClusterIP + port-forward for the admin/pairing API
├── deployment-consumer.yaml  ingest: archive + media, no send token
├── service-consumer.yaml     ClusterIP: wuzapi's webhook target
├── deployment-sender.yaml    the one pod with a send token, no Service
├── backup-cronjob.yaml       nightly encrypted dump of both databases
└── kustomization.yaml        all of the above
```

## Caveats worth knowing before you copy this

* **Single writer is architectural.** See the top of this file; it applies to the
  consumer and the sender too, not just the daemon.
* **The archive is one-way.** WhatsApp deletes a history sync once it is delivered,
  so re-pairing a device does not re-fetch history.
* **`WUZAPI_AUTO_PRESENCE` is `unavailable`** in `deployment-api.yaml`. With the
  upstream default (`available`) the linked companion announces presence and the
  primary phone can stop receiving push notifications.
* **The daemon's probes are TCP, not `/session/status`.** A legitimate session sits
  disconnected for hours, so probing connectivity would restart the pod in a loop.
