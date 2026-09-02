"""abelard_common.prices — the shared price substrate (ORDER PS-1).

One writer, many readers. Every daemon that needs prices (Smart Money,
Correlation, and whatever follows) reads this store; none fetches on its own.

The doctrine this package exists to enforce, in one line:

    **Unadjusted closes are FACTS. Adjusted closes are a VIEW.
    Never cache a view as a fact.**

The Smart Money price layer this replaces cached vendor-adjusted closes as if
they were immutable (`price_backfill.py:11`), and `_covered()` guaranteed a held
date was never refetched. Adjusted closes are not immutable — every split and
dividend rewrites the whole history behind them. The measured result
(`abelard_common/recon/PS-1-P0.md`, and CR-R0 §R8): 92% of S&P names stitched
from more than one adjustment epoch, MNST provably corrupt, and no way from
inside the store to tell an unadjusted split from a crash.

Layering, and why each layer exists:

  * ``prices_raw``          — reconstructed true traded prices. Insert-only.
                              A recorded close never changes. If the vendor
                              later disagrees, that is a fail-loud fact-change
                              event, never an update.
  * ``corporate_actions``   — vendor-DECLARED splits and dividends. Yahoo
                              publishes these in the same request that fetches
                              prices (``&events=div,split``), so a split landing
                              tonight is detected tonight at zero extra cost.
  * ``adjustment_factors``  — the cumulative factor series, VERSIONED. A new
                              declaration writes a new version; old versions are
                              never deleted, so any past statistic stays
                              reproducible as-of its own factor version.
  * ``adjusted_view``       — the materialised current view. Derived, rebuildable,
                              and the only thing analytics reads.
  * ``vendor_adjusted``     — the vendor's own adjusted close, kept in a separate
                              table for COMPARISON ONLY. Deliberately not a
                              column on ``prices_raw`` so it cannot be joined
                              into analytics by accident.

Phase 1 (this module) is schema only. The writer, the analytics functions and
the reader migration are Phases 2-4 and are not present yet.

Library discipline, following ``alert_queue``: every entry point takes an
explicit ``db_path``. **No environment resolution happens in here** — call sites
own that. ``ABELARD_PRICES_DB_PATH`` is read by the Phase 2 CLI, not by this
package. Stdlib only.
"""

from __future__ import annotations

from .schema import (
    SCHEMA_VERSION,
    PRICE_STATUSES,
    CA_KINDS,
    INFERRED_KINDS,
    INDEX_CODES,
    PriceStoreError,
    connect,
    migrate,
    schema_version,
)

__all__ = [
    "SCHEMA_VERSION",
    "PRICE_STATUSES",
    "CA_KINDS",
    "INFERRED_KINDS",
    "INDEX_CODES",
    "PriceStoreError",
    "connect",
    "migrate",
    "schema_version",
]
