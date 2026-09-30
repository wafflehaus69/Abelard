# Purchase-day prep — findings from the pricing screen and a sizing query (2026-09-30)

## What the Dune pricing screen shows (Mando's screenshot, 2026-09-30)
| Plan | Price shown | Credits | Notes |
|---|---|---|---|
| Free | $0 | — | view and copy only; **no query execution** |
| Analyst | **$65/mo billed yearly** (toggle on) | **4,000** | run and schedule queries, faster engine, higher API limits, 3 seats. **CSV export not listed** (listed under Plus) |
| Plus | $349/mo billed yearly | 25,000 | **account is on a Plus trial, 5 days left** |

**Not on the screen, not in any fetchable doc:** whether Analyst bills overage past 4,000 credits, and Analyst's materialized-view storage cap. Both must be read at checkout or asked of Dune before paying.

## Correction — there is no recurring free tier
Dune's billing doc: *"New accounts receive 2,500 total trial credits for up to 14 days with Plus plan features. The trial ends when the time or credits run out, whichever comes first; the account then becomes view-only until you upgrade."* The usage API agrees: period 2026-09-22 → **2026-10-06**, 2,500 credits, `bytes_allowed` 15 GB (Plus storage). Every "free-tier" run since 2026-09-22 has drawn on a **one-time trial**. **On 2026-10-06 the account becomes view-only**: no API execution without a paid plan. Remaining trial credits: ~1,199. The purchase has a deadline.

## Billing trap on the checkout screen
* **"Keep Plus"** converts the trial to Plus at $349/mo **billed yearly** (~$4,188).
* **Analyst "Billed yearly"** at $65/mo is a **$780 annual commitment**. The expense doctrine's kill criterion (day 10, no renewal) requires the **monthly** toggle.

## Sizing finding — the derived trade table cannot be a per-swap materialized view
`recon/sql/swap_rows_by_month.sql` (27.9 credits — above its 10-credit expectation, under the watchdog cap): **7,943,558,839 PumpSwap swap rows** over 2025-03-20 → 2026-09-27 (buy + sell events; P plus other PumpSwap pools).

| stored bytes per row (23 cols) | size | × the 15 GB Plus cap |
|---|---|---|
| 30 | 238 GB | 16× |
| 60 | 477 GB | 32× |
| 100 | 794 GB | 53× |

MR-4.5 as written — one materialized view holding every swap in the window — does not fit Plus at any plausible compression, and Analyst's cap is undocumented and at most that. **Builder's proposal, for Architect ruling before purchase day:**
1. **Materialize per-token aggregates**, not per-swap rows: one row per mint × era with entry marks at G, reserves at horizons, gate verdicts, aligned-cluster flows, cost legs. ~180k rows for P; small.
2. **Per-swap rows only for scoped slices**: the §6 cost trial's 60-day window and the calibration slice, each materialized, used, and dropped.
3. **H3** needs per-wallet trade histories across rolling 60-day windows, which is the per-swap table by another name. That strengthens the A6 split: **H3 → M0b**, on its own budget, unless the cost trial shows a slice design that fits.

## Re-projection against 4,000 credits/month
With H1–H4 verdicts computed in-warehouse and only the result grid plus cited rows exported (A1_DUNE_FITNESS §4): census 1,100–1,900 + per-token materialization 750–1,900 + analysis passes 500–1,500 + exports a few hundred ≈ **2,500–5,600 credits → one to two Analyst months**, H3 excluded. The materialization unit is still unmeasured; the scoping month's cost trial measures it on day one. Two monthly Analyst months stay inside the ~$150 cap only if the monthly price is ≤ $75 — **read the monthly price with the toggle off**.
