#!/usr/bin/env python3
"""MinIO (S3) access for WhatsApp attachments.

S3-compatible object storage for the attachment bytes — MinIO in the reference
deployment, but anything path-style that speaks the S3 API will do.

🔴 THIS CLIENT IS WHY `media_delivery` IS `base64`, NOT `s3`.
wuzapi can upload to S3 itself, but its own docs state objects are written with
PUBLIC-READ permissions ("Do not use this for sensitive data"). Personal chat
media in a world-readable bucket is not a trade worth making, so the bytes come
through the webhook and THIS client writes them with the bucket's own private
policy. It also puts the group-media allowlist in one place instead of splitting
the policy between wuzapi's per-user config and ours.

All three are required, read from the environment:

    MINIO_ARCHIVE_ENDPOINT     host[:port], or a full URL (`https://…`)
    MINIO_ARCHIVE_ACCESS_KEY
    MINIO_ARCHIVE_SECRET_KEY

Layout (bucket `whatsapp-attachments`):

    <chat>/<YYYY-MM-DD>/<whatsapp_id>_<filename>          the attachment bytes
    <chat>/<YYYY-MM-DD>/<whatsapp_id>_<filename>.json     a sidecar with the metadata

`put_attachment()` is idempotent: re-uploading an existing key is a NO-OP, not a
second copy — redelivery replays attachments and must not duplicate objects.

Usage:
    with MinioWhatsApp() as mc:
        mc.put_attachment(chat="…", timestamp_ms=…, filename="a.jpg",
                          data=b"…", content_type="image/jpeg", sidecar={…})
"""
from __future__ import annotations

import io
import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

try:
    from minio import Minio
    from minio.error import S3Error
except ImportError as exc:  # pragma: no cover - import guard
    raise SystemExit(
        "minio is required — pip install -r requirements.txt"
    ) from exc

BUCKET = "whatsapp-attachments"
SIDECAR_SUFFIX = ".json"
ENDPOINT_ENV = "MINIO_ARCHIVE_ENDPOINT"
ACCESS_KEY_ENV = "MINIO_ARCHIVE_ACCESS_KEY"
SECRET_KEY_ENV = "MINIO_ARCHIVE_SECRET_KEY"

# Anything outside this set is replaced in a key segment: a filename or a
# display name can carry slashes, spaces and control characters, and a `/` would
# silently reshape the key layout.
_UNSAFE_SEGMENT = re.compile(r"[^A-Za-z0-9._+-]+")
# A run of dots is collapsed AFTER separators are neutralised, so no key segment
# can carry a `..` traversal fragment even in an object browser that re-splits it.
_DOT_RUN = re.compile(r"\.{2,}")


def safe_segment(value: str, *, fallback: str = "unknown") -> str:
    """One key path segment, with separators and whitespace neutralised."""
    cleaned = _DOT_RUN.sub("_", _UNSAFE_SEGMENT.sub("_", str(value or ""))).strip("_")
    return cleaned or fallback


def object_key(*, chat: str, timestamp_ms: int, filename: str,
               attachment_id: str) -> str:
    """`{chat}/{YYYY-MM-DD}/{attachment_id}_{filename}` — the layout.

    🔴 `attachment_id` IS PART OF THE KEY, and required. Keying on
    `{chat}/{date}/{filename}` alone collides for the commonest filename
    there is: two `Screenshot.png` in one chat on one day produce the
    same key, `put_attachment` then sees the object already exists and SKIPS the
    write, and the second attachment's DB row points at the first one's bytes
    while its sidecar misattributes provenance. Silent, and unrecoverable once
    the sender deletes the original.

    The date comes from the message's own timestamp (UTC), NOT from wall-clock
    now: a redelivered or backfilled attachment must land on the same key as the
    first delivery, or idempotency is decided by when the consumer happened to
    run. The WhatsApp message id is stable across redelivery for the same
    reason, so the key stays idempotent while becoming unique.
    """
    if not attachment_id:
        raise ValueError(
            "object_key needs the WhatsApp message id: without it two files of "
            "the same name in one day collide and the second is silently dropped")
    day = datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=timezone.utc)
    return (f"{safe_segment(chat)}/{day.strftime('%Y-%m-%d')}/"
            f"{safe_segment(attachment_id, fallback='attachment')}_"
            f"{safe_segment(filename, fallback='attachment')}")


class MinioWhatsApp:
    """Context manager: an S3 client for the attachment archive.

    Pass `client=` to inject one (the hermetic suite's seam). Otherwise the
    endpoint and credentials come from the environment and the client is built on
    `__enter__`.
    """

    bucket = BUCKET

    def __init__(
        self,
        access_key: str | None = None,
        secret_key: str | None = None,
        endpoint: str | None = None,
        client=None,
    ):
        self._access_key = access_key or os.environ.get(ACCESS_KEY_ENV)
        self._secret_key = secret_key or os.environ.get(SECRET_KEY_ENV)
        self._endpoint = endpoint or os.environ.get(ENDPOINT_ENV)
        # An injected client short-circuits every connection concern — this is
        # the seam the hermetic suite drives.
        self.client = client
        self._bucket_checked = False

    # -- lifecycle ---------------------------------------------------------
    def __enter__(self) -> "MinioWhatsApp":
        if self.client is not None:
            return self
        missing = [name for name, value in (
            (ENDPOINT_ENV, self._endpoint),
            (ACCESS_KEY_ENV, self._access_key),
            (SECRET_KEY_ENV, self._secret_key),
        ) if not value]
        if missing:
            raise RuntimeError(
                "the media archive is not configured — set " + ", ".join(missing)
            )
        host, secure = self._split_endpoint(self._endpoint)
        self.client = Minio(
            host, access_key=self._access_key, secret_key=self._secret_key,
            secure=secure,
        )
        return self

    def __exit__(self, *_exc) -> None:
        # No connection outlives the context (the S3 client is stateless per
        # request), so there is nothing to tear down.
        return None

    @staticmethod
    def _split_endpoint(endpoint: str) -> tuple[str, bool]:
        if "://" in endpoint:
            u = urlparse(endpoint)
            return u.netloc, u.scheme == "https"
        return endpoint, False

    @property
    def _c(self):
        if self.client is None:
            raise RuntimeError("MinioWhatsApp used outside its context manager (no client)")
        return self.client

    # -- operations --------------------------------------------------------
    def ensure_bucket(self, bucket: str | None = None) -> bool:
        """Create the bucket if absent. Returns True if it was created."""
        name = bucket or self.bucket
        if self._c.bucket_exists(name):
            return False
        self._c.make_bucket(name)
        return True

    def object_exists(self, key: str, bucket: str | None = None) -> bool:
        try:
            self._c.stat_object(bucket or self.bucket, key)
            return True
        except S3Error:
            return False

    def put_attachment(self, *, chat: str, timestamp_ms: int, filename: str,
                       data: bytes, content_type: str, attachment_id: str,
                       sidecar: dict | None = None) -> str:
        """Upload one attachment + its sidecar. Returns the object key.

        Idempotent: an existing key is left alone (both the object and its
        sidecar), so a redelivery re-derives the same key and writes nothing.
        """
        if not self._bucket_checked:
            self.ensure_bucket()
            self._bucket_checked = True
        key = object_key(chat=chat, timestamp_ms=timestamp_ms,
                         filename=filename, attachment_id=attachment_id)
        if not self.object_exists(key):
            self._c.put_object(
                self.bucket, key, io.BytesIO(data), length=len(data),
                content_type=content_type,
            )
        sidecar_key = key + SIDECAR_SUFFIX
        if sidecar is not None and not self.object_exists(sidecar_key):
            blob = json.dumps(sidecar, sort_keys=True, default=str).encode()
            self._c.put_object(
                self.bucket, sidecar_key, io.BytesIO(blob), length=len(blob),
                content_type="application/json",
            )
        return key
