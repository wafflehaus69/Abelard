"""Scout-specific opportunity identity.

Deliberately NOT hoisted into `abelard_common` alongside the dedupe hash.
`compute_opportunity_id` encodes a rule that is Scout's alone: an opportunity's
identity is the source plus that source's own immutable id. News Watch has no
equivalent concept, and a shared library should carry what is shared rather
than everything that happened to live in the same file.

The dedupe hash it sits beside DID hoist -- see `abelard_common.dedupe`.
"""

from __future__ import annotations

import hashlib


def compute_opportunity_id(source: str, native_id: str) -> str:
    """Stable identity: sha256("source|native_id"), full hex.

    Keys on the source's own immutable id, never on the title. SC-R1 sampled a
    Dework task and an Opire reward both titled "c1work"; a title-derived key
    would have collided two unrelated real opportunities. Mirrors CD-R1 R3
    (entity state keys on CIK, never on ticker).
    """
    return hashlib.sha256(f"{source}|{native_id}".encode("utf-8")).hexdigest()


__all__ = ["compute_opportunity_id"]
