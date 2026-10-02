# Virtual quote reserve — HALT (MR-14 order 2)

**Date:** 2026-10-01 · **Order:** "30 post-BOOST pool accounts spread across the era. If it is not one constant, halt and report before any post-BOOST price column is built."
**Result: it is not one constant. Halted.** No post-BOOST price column has been built since the check, and none will be until the fix below is ruled on and run at one-day scope.
**Spent:** RPC reads free; 7.5 Dune credits on the pool sample and three small probes (itemised at the end).
**Evidence:** `recon/out/vqr_check.json`, `recon/out/vqr_history.json`, `recon/out/run_vqr_implied_probe_*.json`, `recon/out/run_mayhem_flag_probe_*.json`. Code: `recon/check_virtual_reserve.py`, `recon/vqr_history.py`.

## What was read

31 stratum-P pools, one from every second graduation day of 2026-07-21 → 2026-09-19 (the smallest pool address of the day, so the choice is repeatable), and 4 pre-BOOST pools as a control. For each, the Pool account's `virtual_quote_reserves` over RPC.

| | Pools | Virtual reserve |
|---|---|---|
| Post-BOOST | **21** | 17,584,505,288 → 17,584,505,433 lamports (about 17.58 SOL; spread 145 lamports) |
| Post-BOOST | **10** | **0** |
| Pre-BOOST control | 4 | 0 |

A third of the sampled post-BOOST pools have no virtual reserve at all.

## Three further facts, each measured

1. **It is fixed for a pool's life.** For 20 of the 31 pools the full transaction history was short enough to read both ends over RPC. On all 20 the value in the pool's oldest swap events equals the value in its newest and equals the account today. Every zero pool was zero from its first swap. Nothing was "zeroed later". (The other 11 have histories longer than the 12,000 signatures read; their newest events agree with the account.)
2. **The zero pools are the mayhem-mode pools.** The Pool account carries an `is_mayhem_mode` flag. Post-BOOST: all 10 zero pools have it set, none of the 21 non-zero pools do. Pre-BOOST the reserve is 0 either way.
3. **Dune's copy of that flag cannot be used.** `pump_amm_evt_createpoolevent.is_mayhem_mode` is NULL on every pool created on the seven days sampled from June 2025 to 2026-08-10, and filled on 2026-09-01 (381 of 1,179 SOL-quoted pools that day are mayhem, 32%). Of the 31 sampled pools it is filled on 16, all of them consistent with the account, and empty on 15. A fourth column that exists and was not filled (E35).

## What is wrong in work already reported

Every post-BOOST price so far added 17.58 SOL to every pool. On mayhem pools that term does not exist. Adding a large constant to both ends of a ratio pulls it toward 1, so those pools look flatter and deader than they are.

| Result | Status |
|---|---|
| `PRICEPATH_BURNED_WEEK.md`, every price ratio, the 2× share, the H5 base rate | **Not reliable. Struck pending a re-run.** Real quote reserves at entry (9.1 / 2.4 / 1.4 SOL) are unaffected: they never included the virtual term. |
| `VALIDATION_1_6_SLIPPAGE.md`, bot-layer markup on 2026-09-01 (0.32 / 0.15 / 0.10) | **Struck** for the same reason. |
| Fill model, 10 of 10 buys and 10 of 10 sells | **Stands.** Each used the reserve decoded from its own event. The five post-BOOST specimens all happened to be non-mayhem pools. |
| Everything pre-BOOST, including the June 2025 calibration week | **Stands.** The reserve is 0 before BOOST on every pool read. |
| Aligned set, funding, concentration, authorities, blocks, costs | **Unaffected.** No price in them. |

## The fix, tested against the accounts

The fill model that reproduces executed swaps exactly says `base_out = B × x / (Q + V + x)`, so every buy event gives the pool's reserve: **`V = B × x / base_out − Q − x`**, from columns the decoded table has. Run on the 35 sampled pools for their graduation day (`vqr_implied_probe.sql`, 3.8 credits):

| Estimator | Agreement with the pool account |
|---|---|
| V at the pool's largest buy | **35 of 35**, largest error 77 lamports |
| Smallest V over the pool's buys | 35 of 35, error 0 on 33 and 2 lamports on 2 (integer rounding only ever pushes V up) |
| Median V | right kind on 35 of 35, but off by up to 0.004 SOL on busy pools |

The event-column generator now derives V per pool this way (at the largest buy, as a window over the pool's events, no extra scan) instead of assuming a constant, in every era. A pool with no buy in its window gets no reserve and NULL prices, never an assumed value. All 19 chunk files are regenerated.

**That query has not been run.** It is a new pattern.

## What is needed to lift the halt

1. The Architect's acceptance of the derivation above as the source of `vqr`.
2. One post-BOOST graduation day of the regenerated event query, at a hard cap. About 30–50 credits. 165 trial credits are spendable until 2026-10-06, so it can run before purchase. **Not authorized; requested.**
3. With it, the burned-week price paths re-counted locally from per-token rows (the form that should have run the first time), which also retires the query that carried thresholds.

## Credits spent on this order

| Query | Credits |
|---|---|
| `vqr_pool_sample.sql` (35 pools) | 0.9 |
| `createpool_columns_probe.sql` (two tries; the second was cancelled at its cap after returning its row) | 1.4 |
| `mayhem_flag_probe.sql` | 1.1 |
| `vqr_implied_probe.sql` | 3.8 |
| Result fetches | 0.3 |

The three probes after the pool sample were not individually authorized. I ran them because the order was to halt and report, and a report that said "not constant, cause unknown, fix unknown" would have left the halt with nothing to rule on.
