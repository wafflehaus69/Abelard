# BARREL — Dune free-tier credit ledger

Every execution, credits as Dune reports them (`execution_cost_credits`), cumulative.
Allowance: **2,500 / period (free; period 2026-09-22 → 2026-10-06 per API)**. **Authoritative balance is `POST /v1/usage` (`recon/dune_usage.py`); this table undercounts probes run before it existed.** Reserve per readiness §1.1: 15%. Regenerate with `recon/credits_ledger.py`.

| # | source | execution_id | credits | cumulative |
|---|---|---|---|---|
| 1 | dune_roundtrip.json | `01M33A5110YM…` | 6.835 | 6.84 |
| 2 | dune_roundtrip.json | `01M33ADYM2JF…` | 2.092 | 8.93 |
| 3 | dune_roundtrip.json | `01M33AETW06A…` | 3.377 | 12.30 |
| 4 | dune_roundtrip.json | `01M33AF9K41R…` | 0.728 | 13.03 |
| 5 | dune_roundtrip_followup.json | `01M33ATQAQBG…` | 0.000 | 13.03 |
| 6 | dune_roundtrip_followup.json | `01M33ATRJ3KD…` | 3.772 | 16.80 |
| 7 | dune_meta.json:cols_buy | `01M33B9340V2…` | 0.027 | 16.83 |
| 8 | dune_meta.json:cols_migrate | `01M33B93GVJN…` | 0.026 | 16.86 |
| 9 | dune_meta.json:cols_create | `01M33B93XT9G…` | 0.027 | 16.88 |
| 10 | dune_meta.json:cols_transfers | `01M33B94AVZB…` | 0.031 | 16.91 |
| 11 | dune_meta.json:freshness | `01M33B94QQDM…` | 7.711 | 24.63 |
| 12 | dune_meta2.json:cols | `01M33BM5GBYG…` | 0.051 | 24.68 |
| 13 | dune_meta2.json:transfers_0921 | `01M33BM5WTJV…` | 0.963 | 25.64 |
| 14 | dune_decoded_check.json:specimens | `01M33BSM36RM…` | 0.000 | 25.64 |
| 15 | dune_decoded_check.json:one_day | `01M33BSMG146…` | 0.568 | 26.21 |
| 16 | dune_decoded_check2.json:specimens | `01M33BW753ME…` | 0.779 | 26.99 |
| 17 | dune_decoded_check2.json:sanity_0901 | `01M33BW7J0FA…` | 0.751 | 27.74 |
| 18 | dune_quote_mix.json | `01M33BYNKA1E…` | 0.523 | 28.26 |
| 19 | dune_probe_columns.json:evt_block_date | `(not saved)…` | 0.065 | 28.32 |
| 20 | barrel/recon/sql/graduations_day.sql (1.2 decoded) | `01M359YM4Y7Y…` | 0.669 | 28.99 |
| 21 | barrel/recon/sql/graduations_day_raw.sql (1.2 raw) | `01M359YVACEV…` | 0.918 | 29.91 |
| 22 | barrel/recon/sql/graduations_quote_mint_day.sql (1.2 quote_mint) | `01M35A1ETJGC…` | 0.171 | 30.08 |
| 23 | barrel/recon/sql/migration_instructions_day.sql (1.2 instr names) | `01M35A1P78YM…` | 0.834 | 30.91 |
| 24 | barrel/recon/sql/graduations_quote_confirm_day.sql (1.2 quote confirm + samples) | `01M35A5JE99Y…` | 0.000 | 47.40 |
| 25 | barrel/recon/sql/graduations_quote_confirm_day.sql (1.2 quote confirm + samples) | `01M35A62113J…` | 0.784 | 48.18 |
| 26 | barrel/recon/sql/discover_venue_tables.sql (R/N table discovery) | `01M35A9VPEGG…` | 1.351 | 49.53 |
| 27 | barrel/recon/sql/stratum_r_day.sql (R sample day) | `01M35AF58XRM…` | 1.246 | 56.08 |
| 28 | barrel/recon/sql/raydium_init2_coverage_day.sql (R init2 coverage) | `01M35AGX2NHH…` | 0.770 | 58.18 |
| 29 | barrel/recon/sql/stratum_r_diagnostic.sql (R diagnostic) | `01M35AJMKSH1…` | 3.330 | 61.50 |
| 30 | barrel/recon/sql/universe_day.sql (universe day P/P_alt/R) | `01M35AMPVSGH…` | 1.392 | 62.90 |
| 31 | barrel/recon/sql/stratum_n_day.sql (N candidate day) | `01M35AN1VBHF…` | 0.633 | 63.53 |
| 32 | barrel/recon/sql/discover_meteora_tables.sql (meteora discovery) | `01M35AQ6YYFX…` | 4.767 | 69.04 |
| 33 | barrel/recon/sql/stratum_n_multivenue_day.sql (N multi-venue day) | `01M35AY1FY00…` | 1.197 | 70.27 |
| 34 | barrel/recon/sql/clmm_poolcreated_probe.sql (CLMM probe) | `01M35AYQE8WW…` | 2.461 | 72.73 |
| 35 | barrel/recon/sql/derived_trades_day_p.sql (1.3 derived table one day P) | `01M35B0GTHD6…` | 0.885 | 73.62 |
| 36 | barrel/recon/sql/census_week_sample.sql (1.4 census one week) | `01M35B11EA26…` | 1.344 | 74.96 |
| 37 | barrel/recon/sql/derived_trades_conservation_by_mint.sql (1.3 conservation by mint) | `01M35B2WRJHC…` | 0.814 | 75.78 |
| 38 | barrel/recon/sql/census_week_sample_preboost.sql (1.4 census pre-BOOST week) | `01M35B3KJ38M…` | 1.337 | 77.11 |
| 39 | barrel/recon/sql/discover_spl_token_tables.sql (1.5 spl token discovery) | `01M35RYXD3TQ…` | 1.941 | 79.06 |
| 40 | barrel/recon/sql/s1s2_sample_tokens.sql (1.5 sample tokens) | `01M35RZ7BF29…` | 0.188 | 79.25 |
| 41 | barrel/recon/sql/s1s2_authority_history.sql (1.5 S1/S2 history 5 mints) | `01M35S2M921X…` | 0.366 | 82.55 |
| 42 | barrel/recon/sql/census_organic_week.sql (1.4 census organic (MR-6)) | `01M35SFD1DZF…` | 3.567 | 86.12 |
| 43 | barrel/recon/sql/s3s4_extensions.sql (1.5 S3/S4 extensions 10 mints) | `01M35SK1TYR8…` | 0.000 | 86.12 |
| 44 | barrel/recon/sql/s3s4_raw_calls.sql (1.5 S3/S4 raw calls) | `01M35SMXTYHG…` | 0.000 | 88.28 |
| 45 | barrel/recon/sql/s3s4_raw_calls.sql (1.5 S3/S4 raw calls) | `01M35SNXJ2G3…` | 3.694 | 91.97 |
| 46 | barrel/recon/sql/s3_decoded_10mints.sql (1.5 S3 decoded 10 mints) | `01M35SQY23KQ…` | 1.351 | 93.33 |
| 47 | barrel/recon/sql/s3s4_raw_calls_n.sql (1.5 S3/S4 raw calls N) | `01M35ST6W6WZ…` | 5.819 | 99.15 |
| 48 | barrel/recon/sql/s1s2_differential_candidates.sql (1.5 S1/S2 differential candidates) | `01M35VMWRJ33…` | 0.191 | 99.34 |
| 49 | barrel/recon/sql/s1s2_differential_history.sql (1.5 S1/S2 differential history) | `01M35VP2V489…` | 0.215 | 99.56 |
| 50 | barrel/recon/sql/census_organic_v2_week.sql (1.4 census organic v1 vs v2) | `01M35VQ89ZJ8…` | 43.626 | 143.19 |
| 51 | barrel/recon/sql/s1s2_differential_history_redutae.sql (1.5 differential re-pull REDUTAE1) | `01M35VVV5RDH…` | 0.814 | 144.01 |
| 52 | barrel/recon/sql/discover_fees_and_lockers.sql (S7b/S5 discovery) | `01M35WJ2NBQM…` | 5.448 | 149.45 |
| 53 | barrel/recon/sql/slippage_validate_10.sql (1.6 slippage 10 swaps) | `01M35WRSJW49…` | 0.289 | 159.38 |
| 54 | barrel/recon/sql/slippage_validate_preboost.sql (1.6 slippage pre vs post BOOST) | `01M35WTRSJ8D…` | 0.424 | 159.81 |
| 55 | barrel/recon/sql/slippage_validate_v2.sql (1.6 slippage v2 + implied virtual reserve) | `01M35WW8GA68…` | 0.410 | 160.22 |
| 56 | barrel/recon/sql/slippage_preboost_txids.sql (1.6 pre-BOOST tx ids) | `01M35WY2AFY6…` | 0.152 | 160.37 |
| 57 | barrel/recon/sql/s7b_pfee_probe.sql (S7b pfee raw probe) | `01M35WYHAA1W…` | 1.153 | 161.37 |
| 58 | barrel/recon/sql/s7b_pfee_by_discriminator.sql (S7b pfee by discriminator) | `01M35X12GZY7…` | 1.118 | 162.64 |
| 59 | barrel/recon/sql/slippage_postboost_txids.sql (1.6 post-BOOST tx ids) | `01M35X91RKBH…` | 0.236 | 162.88 |
| 60 | barrel/recon/sql/s6_holders_one_mint.sql (S6 holders one mint (retry)) | `01M38AED8KKJ…` | 7.570 | 1010.74 |
| 61 | barrel/recon/sql/s6_actions_one_mint.sql (S6 per-action diagnosis) | `01M3MKHTB4T1…` | 5.713 | 1016.45 |
| 62 | barrel/recon/sql/slippage_sell_txids_v2.sql (1.6 sell tx ids v2 (pruned)) | `01M3MKNKXM4S…` | 0.441 | 1016.89 |
| 63 | barrel/recon/sql/discover_sol_transfers.sql (S7/S8 SOL transfer discovery) | `01M3MKVJMFGS…` | 2.683 | 1019.58 |
| 64 | barrel/recon/sql/universe_p_by_month.sql (1.3 P universe by month) | `01M3MKVZ1416…` | 0.273 | 1019.86 |
| 65 | barrel/recon/sql/createpool_by_month.sql (coverage: createpool by month) | `01M3MKXA6PV5…` | 0.970 | 1020.82 |
| 66 | barrel/recon/sql/stratum_p_preevent_day.sql (P pre-event admission 2025-09-15) | `01M3MM1X4NZY…` | 4.518 | 1025.38 |
| 67 | barrel/recon/sql/s7_funding_sample.sql (S7 funding sample 5 creators) | `01M3MM2HNGYM…` | 0.000 | 1025.38 |
| 68 | barrel/recon/sql/s7_funding_sample.sql (S7 funding sample v2) | `01M3MM84ZD9X…` | 23.395 | 1048.78 |
| 69 | barrel/recon/sql/s7_linked_supply_one_mint.sql (S7 linked supply one mint) | `01M3MMAZRF6R…` | 3.539 | 1052.32 |
| 70 | barrel/recon/sql/s8_bundle_sample.sql (S8 bundle sample 5 mints) | `01M3MMC6TPRP…` | 79.203 | 1131.52 |
| 71 | barrel/recon/sql/s8_bundle_sample.sql (S8 bundle sample v2 (literal bounds)) | `01M3MMHZT2BT…` | 1.810 | 1133.33 |
| 72 | barrel/recon/sql/s9_s10_day.sql (S9/S10 one day) | `01M3MMMDF23J…` | 2.755 | 1136.08 |
| 73 | barrel/recon/sql/universe_p_preevent_by_month.sql (P pre-event universe by month) | `01M3MMQ7MZMR…` | 0.000 | 1136.08 |
| 74 | barrel/recon/sql/universe_p_preevent_by_month.sql (P pre-event universe by month v2) | `01M3MMR29JC8…` | 0.000 | 1136.08 |
| 75 | barrel/recon/sql/universe_p_preevent_by_month.sql (P pre-event universe by month v3) | `01M3MMRSBGXQ…` | 3.301 | 1139.39 |
| 76 | barrel/recon/sql/recon_20_tokens.sql (1.4 20-token seeded list) | `01M3MN7JJF3E…` | 12.657 | 1152.04 |
| 77 | barrel/recon/sql/a3_reserve_decay_day.sql (A3 reserve decay + depth ratio, one day) | `01M3MNF1DCGA…` | 0.000 | 1152.05 |
| 78 | barrel/recon/sql/a3_venue_share_day.sql (A3 venue share (routing X), one day) | `01M3MNFPND3M…` | 85.132 | 1237.18 |
| 79 | barrel/recon/sql/s7b_sharing_config_day.sql (S7b sharing-config extraction, one day, 20 rows) | `01M3MNH4F0EM…` | 11.552 | 1267.79 |
| 80 | barrel/recon/sql/universe_day.sql (universe day P/P_alt/R/N(cp)) | `01M3MNJ9KRKG…` | 0.784 | 1268.61 |
| 81 | barrel/recon/sql/a3_reserve_decay_day.sql (A3 reserve decay + depth ratio, one day (retry)) | `01M3MNMP01CD…` | 2.318 | 1270.93 |
| 82 | barrel/recon/sql/fees_per_era.sql (1.6 fee pricing per era (3 sample days)) | `01M3N8581TMA…` | 1.400 | 1272.33 |
| 83 | barrel/recon/sql/bot_layer_markup_day.sql (bot-layer markup dry run, one day) | `01M3N867Y4ZX…` | 1.202 | 1273.54 |
| 84 | barrel/recon/sql/swap_rows_by_month.sql (storage sizing: swap rows by month) | `01M3T8AHZKYV…` | 27.864 | 1301.40 |
| 85 | barrel/recon/sql/pt_features_a_day.sql (3a tier A, one graduation day (validation)) | `01M3TE07T6HP…` | 27.355 | 1328.76 |

**Total consumed: 1328.76 credits. Remaining: 1171. Usable after 15% reserve: 996.**

_Generated 2026-09-22T19:35:40+00:00_
_INCIDENT 2026-09-23: `slippage_sell_txids.sql` (expect 8) billed **840.28 credits** — a `pool IN (subquery)` joined with an OR on the partition column defeated pruning and scanned the sell-event history. Cancel arrived after completion. Runner now cancels in flight above max(3×expect, 5) and refuses --expect > 25 without --confirm. Free tier after: 1,010.7 / 2,500 used._

_API says used (pre-incident line, superseded above): 1010.738 of 2500 (authoritative; ledger undercounts pre-ledger probes)_

_2026-09-30: `swap_rows_by_month.sql` billed 27.86 against an expectation of 10 (cap 30) — near-miss, recorded. Correction: the 2,500 credits are a one-time 14-day Plus trial ending 2026-10-06, not a recurring free tier; the account becomes view-only after._
_API says used: 1328.756 of 2500 (authoritative; ledger undercounts pre-ledger probes)_

_2026-09-30, burn-down item 3: usage 1,301.4 → 1,414.5 (113.1 credits). Authoritative per-step figures are in `recon/out/burn_3a.json` and `burn_3b.json`; these runs did not go through `dune_run_sql.py`, so they have no rows in the table above._