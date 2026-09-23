# Dune readiness — status against `DUNE_READINESS_ORDERS.md` §1

Updated by ClaudeCode. ✅ done · 🟡 in progress · ⬜ not started · ❌ blocked (with reason)

| Item | Status | Evidence |
|---|---|---|
| 1.1 key in `barrel/private/`, guard extended, guard passes | ✅ | `barrel/private/dune.env`; `recon/leak_guard.py` (live-key + pattern match) |
| 1.1 queries as `.sql` files in repo | 🟡 | new queries live in `recon/sql/` and run via `dune_run_sql.py`; round-trip probes remain inline in their scripts (historical evidence, not pipeline) |
| 1.1 credit meter with 15% reserve | ✅ | `recon/dune_run_sql.py` reads `POST /v1/usage` (`credits_used`/`credits_included`) before every run; refuses above remaining × 0.85 |
| 1.2 graduation-event query, validated vs chain-side count | ✅ | `VALIDATION_1_2_GRADUATIONS.md`: Dune raw, BigQuery (250=250 exact), pool join 1,154/1,154, chain 6/6 |
| 1.2 stratum/era/DEGRADED labelling | 🟡 | P / P-alt / R validated; era + DEGRADED columns validated in `universe_day.sql`; N is multi-venue (v4 negligible; CP init; Meteora first-trade proxy, no creation table on Dune) — count measured, rule needs Architect confirmation |
| 1.2 owner-wallet exclusion from `barrel/private/` | ⬜ | wallet file not yet supplied |
| 1.3 schema frozen | 🟡 | v2 returned to Architect per MR-6: 20 columns match the condition; 3 additions (`base_amount`, `degraded`, `birth_is_proxy`) need yes/no |
| 1.3 built + validated as plain query, one day | ✅ | `derived_trades_day_p.sql`: 12.6M rows, conservation holds on 84% of rows / 9,594 mints, failures concentrated by mint (unnamed leg); `trader_cost = gross − net` adopted |
| 1.3 export contract with row counts | ⬜ | |
| 1.4 census queries | ✅ | ruled metric written and run (`census_organic_week.sql`): takers p50 420, first organic swap p50 0 s; S7b exclusion pending the SharingConfig query |
| 1.4 calibration-slice distributions | ⬜ | |
| 1.4 20-token seeded reconciliation list | ⬜ | |
| 1.5 S1/S2 as-of, reconciled on 5 tokens | ✅ | `VALIDATION_1_5_S1_S2.md`: 5/5 reconciled; method caveat stated (reconstruction + no-change-after-entry, not a historical-slot read); structural PASS on P measured |
| 1.5 S3/S4 | ✅ | `VALIDATION_1_5_S3_S4.md`: 10/10 reconciled (5 P + 5 N) via raw Token-2022 calls by instruction index; first live FAIL found (N mint, 500 bps transfer fee); S4 *rate* as-of needs sub-instruction decode (next) |
| 1.5 S6/S7/S7b/S8 | ⬜ | |
| 1.5 S9/S10 | ⬜ | |
| 1.5 UNKNOWN path per check | 🟡 | S1–S4 documented; S5–S10 pending |
| 1.6 fee pricing from decoded events, per era, SOL-quoted | ⬜ | one-day proportions already confirmed (25.0/5.0 bps) |
| 1.6 fee samples re-run with v1 fix | ✅ | `M0_FEE_LEGS.md` Part 3 |
| 1.6 slippage from reserves, validated on 10 swaps | ⬜ | |
| 1.7 rebalance date chosen, seeded | ⬜ | |
| 1.7 funnel written, dry-run on 2-day window | ⬜ | |
| 1.7 projection template | ⬜ | |
| 1.8 personal-history replay | ❌ | owner wallets not yet in `barrel/private/` |
| §3 `credits.md` | ✅ | `docs/credits.md` |
