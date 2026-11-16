# The WhatsApp CONSUMER image — `consumer.py serve`, the long-lived webhook server.
#
# Build from the REPO ROOT, so the paths below read the same as in a git URL:
#     docker build -t whatsapp-for-agents-consumer .
#
# 🔴 COPY BY NAME, NEVER `COPY . .`. A working tree routinely holds an
# un-gitignored scratch file — a .env, a database dump, a local experiment — and
# `COPY . .` would bake whatever happened to be sitting there into a layer and
# push it to a registry. The explicit list is also what `tests/test_image_split.py`
# reads to prove the consumer carries no send code.

FROM python:3.12-slim

# --------------------------------------------------------------------------- #
# 🔴 ONE DEPENDENCY LIST. Do not add a hand-written `pip install a b c` here: the
# two images' lists drift, and this service's failure mode would not be a crash —
# an absent `minio` turns every media archive into a per-message `media_failures`
# counter with the message rows still green.
# --------------------------------------------------------------------------- #
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# The ingest half, named individually so a stray file cannot ride along.
COPY consumer.py    /app/
COPY _wa_db.py      /app/
COPY _wa_parse.py   /app/
COPY _wa_webhook.py /app/
COPY _minio.py      /app/

# --------------------------------------------------------------------------- #
# 🔴 THE HERMETIC TESTS SHIP IN THIS IMAGE ON PURPOSE. A build can then run the
# suite against the modules the image actually installed: an import that resolves
# from a checkout but not from the image, or a dependency the image lacks, is
# otherwise discovered in production. The files are a few KB.
#
# `test_read_path.py` is hermetic (it imports `_wa_db` and nothing that needs a
# live server). Its sibling `test_read_path_db.py` is deliberately NOT copied —
# it is DSN-gated, so in an image it would only add a second skip. Neither is
# `test_image_split.py`: it needs BOTH Dockerfiles, so it stays in the repo.
# --------------------------------------------------------------------------- #
COPY tests/test_parse.py       /app/tests/
COPY tests/test_webhook.py     /app/tests/
COPY tests/test_ingest.py      /app/tests/
COPY tests/test_db_identity.py /app/tests/
COPY tests/test_read_path.py   /app/tests/

# --------------------------------------------------------------------------- #
# 🔴 PYTHONUNBUFFERED=1: without it a crashed pod's last words sit in a 4 KiB
# stdio buffer that is never flushed, and `kubectl logs` shows nothing.
# PYTHONDONTWRITEBYTECODE=1: a .pyc written beside a module in a mounted tree is a
# staleness source, not a speedup.
# HOME=/home/nonroot: a numeric-UID container has no passwd entry, so any
# `Path.home()` on an import path would raise before anything could say so.
#
# There is deliberately no kubectl in, or behind, this image: it reaches Postgres
# through WHATSAPP_PG_DSN and S3 through MINIO_ARCHIVE_ENDPOINT — cluster facts
# supplied by the Deployment, never baked in.
# --------------------------------------------------------------------------- #
ENV HOME=/home/nonroot \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN mkdir -p /home/nonroot && chown -R 65532:65532 /home/nonroot
USER 65532:65532
WORKDIR /app

# `serve` is the only subcommand a Deployment takes. Every other subcommand
# (conversations/search/show/allowlist/stats) is operator-driven and runs via
# `kubectl exec`, so it needs no entrypoint of its own — and 🔴 this pod is given
# NO wuzapi user token, which is what makes the read path read-only STRUCTURALLY
# rather than by convention.
CMD ["python3", "/app/consumer.py", "serve"]
