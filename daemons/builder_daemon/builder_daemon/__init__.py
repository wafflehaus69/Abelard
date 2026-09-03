"""builder_daemon -- the Tribe's code-PR drafter.

Drafts against work Mando has admitted. Emits a provenance packet with a patch
attached. Never submits, never authenticates, never holds a credential against
a counterparty. See SOUL.md; the invariants there are asserted by
`tests/test_soul.py`, not merely documented.
"""

__version__ = "0.1.0"
