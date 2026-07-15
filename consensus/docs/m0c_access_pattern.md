# M0-C Access-Pattern Assessment (addendum v1.4 §5 / v1.5 §5.1)

**Question:** is a wallet-centric pull cheaper than a market-centric L1 batch for a *powered* M0-C run? Reported as a short finding **before** any large pull, per v1.5 §5.1.

## Finding 1 — data-api cannot bootstrap the historical circle (wallet-centric-via-data-api is out)

The wallet-centric hypothesis (v1.4 §5) was: use data-api per-wallet endpoints (4k cap is per-filter-value) to bootstrap circle identification, then deep-walk L1 only for convergence markets. **This fails for the replay window.** Measured live (2026-07-15):

- data-api returns the **newest** 4k trades per wallet. For currently-active wallets the 4k window spans only **0–2 days** of history.
- The M0-C replay window is **2025-06 → Apr 2026**. A wallet still trading in 2026 has its 2025 trades permanently pushed out of the newest-4k window — they are **unreachable** via data-api regardless of the per-wallet cap.
- Therefore data-api is a live/recent tool only; it cannot serve **as-of historical** circle identification. Any M0-C replay circle bootstrap must come from **L1** (the archival subgraph, which covers 2022 → Apr-28-2026).

## Finding 2 — both patterns need L1; market-centric is the pragmatic choice

With data-api out, the choice is market-centric-L1 vs wallet-centric-L1:

- **Wallet-centric-L1** still requires a *seed* of candidate wallets, which can only come from seeing market participants — i.e. walking markets. It does not remove the fundamental L1 walk, only reorders it, and adds a token→market remap per wallet. No clear cost win.
- **Market-centric-L1** is already built (the M0-C pull), bounded, and reuses proven machinery.

## Finding 3 — the mid-band universe is bounded (mega-markets excluded)

The 200k–3M volume cap **automatically excludes** the unwalkable mega-markets ($400M Trump, $269M Iran) — the exact exclusion v1.4 §5.1 mandated. Mid-band markets in target categories (geopolitics/politics/economy) ended within the replay window.

## Finding 4 — CORRECTED: the cost was badly underestimated; the pull is NOT one-session-feasible

Two numbers in the first draft of this doc were wrong, both discovered when the run was launched:

1. **Market count: ~3,692, not 348.** The "348" came from a probe that capped enumeration at 6 pages/tag; the full mid-band universe is **~3,692 markets**.
2. **Fill volume per market: ~20× the M0-F estimate.** The M0-F pull used the *2-week* Feb-28 window; M0-C walks the *full 10-month* replay window, so each market carries ~20× more fills. The cost is dominated by fills-over-time, not market count.

**Outcome of the launched run:** the market-centric L1 pull ran **>2.5 hours**, cached **12.6 GB / ~14.6M fills across 935 completed markets**, then **died on a network outage** (goldsky DNS resolution failure) — the machine-availability fragility the architect flagged, realized. The pull is **not resumable as built** (live mode re-fetches; only replay reads cache), so a restart would redo everything.

**This vindicates v1.4 §5's "full-tape L1 walks are slow" concern more strongly than this assessment first credited.** A complete powered pull of the full mid-band is a multi-session, resumability-dependent operation, not a background batch.

## Decision (revised)

1. **Salvage the completed work:** run the sweep in **replay** over the 935 markets that completed (partially-walked markets fail cleanly and drop — no silent truncation), bounded to a tractable subset (300 markets, 12× the pilot) for sweep compute. This yields a **genuinely powered verdict** without any further network exposure.
2. **Engineering follow-up (needed for any full run):** add a *prefer-cache* fetch mode for the frozen L1 subgraph so the pull is **resumable** across network drops (cached == fresh for an immutable archival source). Without it, the machine's fragility makes a full 3,692-market pull impractical.

**Scope-limit statements for the eventual M0-C report (v1.4 §5, restated):**
1. **Mega-markets are excluded for tractability** — the verdict is scoped to the mid-volume band and may not generalize to the highest-liquidity markets (which could be more efficient → less edge, or more informed → more edge; this run cannot tell).
2. **Signal sparsity is inherent** (consensus is rare-by-design): the pilot yielded ~1 signal / 25 markets, so 348 markets projects to a low-double-digit signal count. That is enough to see whether *any* parameter region shows positive expectancy, but small-N — **directional, not decisive**. Confidence scales with market count; the report will state the realized signal count plainly.
