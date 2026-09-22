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

**Total consumed: 72.73 credits. Remaining: 2427. Usable after 15% reserve: 2063.**

_Generated 2026-09-22T19:35:40+00:00_
_API says used: 70.273 of 2500 (authoritative; ledger undercounts pre-ledger probes)_