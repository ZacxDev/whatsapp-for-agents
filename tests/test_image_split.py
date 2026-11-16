#!/usr/bin/env python3
"""The image SPLIT guard — the consumer image must never carry send code.

Why this is its own file, and why it is NOT copied into either image
-------------------------------------------------------------------
`Dockerfile` builds the CONSUMER (parses arbitrary, internet-influenced
payloads, deliberately has no wuzapi token). `Dockerfile.sender` builds the
SENDER (holds the token, posts messages). The architectural property is that a
parser bug can never reach a send endpoint — which holds only while the consumer
image contains no send module.

🔴 This is a guard on the RELATIONSHIP, not on either file: it fails when the
consumer's COPY list GROWS into the send modules *and* when the sender's GROWS
into the consumer's, because a one-sided check passes in the direction nobody
wrote it for. It also fails if a module stops appearing in EITHER list, so
deleting a COPY line to silence a failure does not work.

It needs both Dockerfiles, which is exactly why it is not shipped into the
images (both COPY their test files BY NAME, so they simply do not list this one).
"""
from __future__ import annotations

import re
import unittest
from pathlib import Path

_WHATSAPP = Path(__file__).resolve().parent.parent          # the repo root
_CONSUMER_DOCKERFILE = _WHATSAPP / "Dockerfile"
_SENDER_DOCKERFILE = _WHATSAPP / "Dockerfile.sender"

#: Modules that constitute the SEND capability. None may appear in the consumer.
_SEND_MODULES = {"sender.py", "_wa_outbound.py"}
#: Modules that constitute the INGEST capability. None may appear in the sender.
_INGEST_MODULES = {"consumer.py", "_wa_parse.py", "_wa_webhook.py", "_minio.py"}


def _copied_basenames(dockerfile: Path) -> set[str]:
    """Every basename a Dockerfile `COPY`s, from any source path."""
    names = set()
    for line in dockerfile.read_text(encoding="utf-8").splitlines():
        m = re.match(r"\s*COPY\s+(.*)$", line, re.IGNORECASE)
        if not m:
            continue
        # Last token is the destination; everything before it is source.
        parts = m.group(1).split()
        if len(parts) < 2:
            continue
        for src in parts[:-1]:
            names.add(Path(src).name)
    return names


class TestImageSplit(unittest.TestCase):
    def setUp(self):
        if not (_CONSUMER_DOCKERFILE.is_file() and _SENDER_DOCKERFILE.is_file()):
            self.fail(
                "the two Dockerfiles are required for this guard and are not "
                "both present — if this ran inside an image, the test was copied "
                "in by name and should not have been; in the repo it means a "
                "Dockerfile was renamed or deleted"
            )
        self.consumer = _copied_basenames(_CONSUMER_DOCKERFILE)
        self.sender = _copied_basenames(_SENDER_DOCKERFILE)

    def test_the_consumer_image_carries_NO_send_module(self):
        leaked = self.consumer & _SEND_MODULES
        self.assertEqual(
            leaked, set(),
            f"the consumer image COPYs send module(s) {sorted(leaked)} — that "
            f"removes the property that a parser bug cannot reach a send "
            f"endpoint. Send code belongs in Dockerfile.sender only.")

    def test_the_sender_image_carries_NO_ingest_module(self):
        leaked = self.sender & _INGEST_MODULES
        self.assertEqual(
            leaked, set(),
            f"the sender image COPYs ingest module(s) {sorted(leaked)} — the "
            f"sender is meant to be a tiny auditable surface with no parser.")

    def test_the_two_file_lists_do_not_overlap_beyond_the_shared_db_layer(self):
        # `_wa_db.py` and `requirements.txt` are legitimately shared: both halves
        # need the connection helper and the dependency list.
        shared = self.consumer & self.sender
        self.assertLessEqual(
            shared, {"_wa_db.py", "requirements.txt"},
            f"the images share unexpected modules {sorted(shared)}")

    def test_every_live_module_is_COPYed_somewhere(self):
        # Deleting a COPY line to silence the guards above must not work.
        for module in _SEND_MODULES | _INGEST_MODULES:
            self.assertTrue((_WHATSAPP / module).is_file(),
                            f"{module} is referenced by the split but does not exist")
        self.assertTrue(_SEND_MODULES <= self.sender,
                        f"send modules missing from the sender image: "
                        f"{sorted(_SEND_MODULES - self.sender)}")
        self.assertTrue(_INGEST_MODULES <= self.consumer,
                        f"ingest modules missing from the consumer image: "
                        f"{sorted(_INGEST_MODULES - self.consumer)}")


if __name__ == "__main__":
    unittest.main()
