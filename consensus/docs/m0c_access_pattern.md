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

## Finding 3 — the mid-band universe is bounded and walkable

The mid-band (v1.4 §5 scope limit: excludes mega-markets for tractability):

- **348 markets** in target categories (geopolitics/politics/economy), volume 200k–3M, ended within the replay window. Median volume ~$500k (vs the Iran cluster's $89M anchor).
- The 200k–3M cap **automatically excludes** the unwalkable mega-markets ($400M Trump, $269M Iran) — the exact exclusion v1.4 §5.1 mandated.
- Cost estimate: the M0-F pull was 114 markets / 996k events in ~15 min, dominated by a few mega-markets. 348 mid-band markets (all smaller than the Iran anchor) is ~1–1.5M events — a comparable, bounded background batch, rate-instrumented, every response cached.

## Decision

**Run market-centric L1 over the 348 mid-band markets.** No new access pattern; the existing `m0c pull` machinery scales to it as a background batch.

**Scope-limit statements for the eventual M0-C report (v1.4 §5, restated):**
1. **Mega-markets are excluded for tractability** — the verdict is scoped to the mid-volume band and may not generalize to the highest-liquidity markets (which could be more efficient → less edge, or more informed → more edge; this run cannot tell).
2. **Signal sparsity is inherent** (consensus is rare-by-design): the pilot yielded ~1 signal / 25 markets, so 348 markets projects to a low-double-digit signal count. That is enough to see whether *any* parameter region shows positive expectancy, but small-N — **directional, not decisive**. Confidence scales with market count; the report will state the realized signal count plainly.
