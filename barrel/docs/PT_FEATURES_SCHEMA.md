# `pt_features` — per-token schema, 91 columns: 85 frozen 2026-09-30, plus 2 by MR-11 and 4 collapsed-count twins by MR-12 ruling 3 (2026-10-01)

**Frozen by:** `RULINGS_2026-09-30.md` (Architect sign-off with three changes). **Source of truth:** `recon/pt_features_schema.json`, written with this page by `recon/gen_pt_features_schema.py`. The **public name** is the only name that exists in Dune; internal names and definitions live only in this repo.

Dune table: `dune.<user>.result_pt_features` (view names must start with `result_`). The prior draft, `PER_TOKEN_SCHEMA.md`, is superseded and kept for its §0 constraints.

## Rules

* Public names only in any saved query or Dune table; this mapping never goes to Dune.
* Verdicts, thresholds and hypothesis logic are never in a saved query; ad-hoc /sql/execute only.
* Owner-wallet addresses never in a saved query.
* Every c04 = -1 also appears in u_codes.

## Sign-off changes applied

1. **24 h peak and drawdown per G** added (`peak_24h_*`, `mdd_24h_*`). The draft's claim that the 24 h figure is recomputable from the 7 d one was wrong: a 7-day maximum cannot give the 24-hour maximum.
2. **Price at first distribution** added (`cluster_px_t1`), to mark the H2 exit.
3. **Neutral names.** Beyond the three ordered renames, the builder also neutralised names that would narrate the method, by the same logic: `owner_correction_applied` → `adj_applied` (would disclose that an operator's own wallets are excluded), `unknown_reasons` → `u_codes`, `syndicate_first_distribution_time` → `cluster_t1`, `price_at_first_distribution` → `cluster_px_t1`. Flagged as builder choices under the neutrality order.

| # | public name | type | internal name | definition |
|---|---|---|---|---|
| 1 | `mint` | varchar | `mint` | token mint |
| 2 | `era` | varchar | `era` | pre_boost / post_boost by graduation time vs 2026-07-21 |
| 3 | `stratum` | varchar | `stratum` | P / P_alt / R / N (MR-6) |
| 4 | `adm_src` | varchar | `admission_source` | complete_to_pool_1d / migration_event / raydium_v4_migrator / cp_init / meteora_first_trade (MR-8.1) |
| 5 | `birth_proxy` | boolean | `birth_is_proxy` | true on Meteora first-trade rows (MR-6) |
| 6 | `degraded` | boolean | `degraded` | graduated or traded inside 2025-08-05 -> 08-11 (MR-5.6) |
| 7 | `pool` | varchar | `pool` | graduation pool (PumpSwap for P) |
| 8 | `quote_mint` | varchar | `quote_mint` | from pool creation (authoritative) |
| 9 | `token_program` | varchar | `token_program` | legacy SPL / Token-2022 |
| 10 | `create_time` | timestamp | `create_time` | mint creation (pump CreateEvent; chain fallback for N) |
| 11 | `grad_time` | timestamp | `grad_time` | graduation (pool birth for N) |
| 12 | `grad_slot` | bigint | `grad_slot` | graduation slot |
| 13 | `creator` | varchar | `creator` | deployer wallet |
| 14 | `vqr` | double | `virtual_quote_reserves` | per pool, lamports, derived from the pool's own buy events (MR-14): 0 before BOOST and on mayhem-mode pools, about 17.58 SOL otherwise; NULL when the pool has no buy to derive it from |
| 15 | `px_grad` | double | `price_grad` | first post-graduation event, pre-swap reserves |
| 16 | `qres_grad` | double | `quote_res_grad` | quote reserve at graduation |
| 17 | `px_g15` | double | `entry_price_g15` | effective quote reserve / base reserve, last event before grad + 15 min |
| 18 | `px_g60` | double | `entry_price_g60` | effective quote reserve / base reserve, last event before grad + 60 min |
| 19 | `px_g240` | double | `entry_price_g240` | effective quote reserve / base reserve, last event before grad + 240 min |
| 20 | `qres_g15` | double | `entry_quote_res_g15` | quote reserve at entry (depth for the $20 fill and the depth floor) |
| 21 | `qres_g60` | double | `entry_quote_res_g60` | quote reserve at entry (depth for the $20 fill and the depth floor) |
| 22 | `qres_g240` | double | `entry_quote_res_g240` | quote reserve at entry (depth for the $20 fill and the depth floor) |
| 23 | `bres_g15` | double | `entry_base_res_g15` | base reserve at entry |
| 24 | `bres_g60` | double | `entry_base_res_g60` | base reserve at entry |
| 25 | `bres_g240` | double | `entry_base_res_g240` | base reserve at entry |
| 26 | `px_1h` | double | `price_1h` | price at grad + 1h |
| 27 | `px_4h` | double | `price_4h` | price at grad + 4h |
| 28 | `px_24h` | double | `price_24h` | price at grad + 24h |
| 29 | `px_7d` | double | `price_7d` | price at grad + 7d |
| 30 | `qres_1h` | double | `quote_res_1h` | quote reserve at grad + 1h (A3 reserve decay) |
| 31 | `qres_4h` | double | `quote_res_4h` | quote reserve at grad + 4h (A3 reserve decay) |
| 32 | `qres_24h` | double | `quote_res_24h` | quote reserve at grad + 24h (A3 reserve decay) |
| 33 | `qres_7d` | double | `quote_res_7d` | quote reserve at grad + 7d (A3 reserve decay) |
| 34 | `peak_7d_g15` | double | `peak_price_7d_after_g15` | max price over (grad + 15 min, grad + 7d] |
| 35 | `peak_7d_g60` | double | `peak_price_7d_after_g60` | max price over (grad + 60 min, grad + 7d] |
| 36 | `peak_7d_g240` | double | `peak_price_7d_after_g240` | max price over (grad + 240 min, grad + 7d] |
| 37 | `mdd_7d_g15` | double | `max_drawdown_7d_after_g15` | largest peak-to-trough fall within (grad + 15 min, grad + 7d], fraction |
| 38 | `mdd_7d_g60` | double | `max_drawdown_7d_after_g60` | largest peak-to-trough fall within (grad + 60 min, grad + 7d], fraction |
| 39 | `mdd_7d_g240` | double | `max_drawdown_7d_after_g240` | largest peak-to-trough fall within (grad + 240 min, grad + 7d], fraction |
| 40 | `peak_24h_g15` | double | `peak_price_24h_after_g15` | max price over (grad + 15 min, grad + 15 min + 24h]; the H2/H3 24h time stop |
| 41 | `peak_24h_g60` | double | `peak_price_24h_after_g60` | max price over (grad + 60 min, grad + 60 min + 24h]; the H2/H3 24h time stop |
| 42 | `peak_24h_g240` | double | `peak_price_24h_after_g240` | max price over (grad + 240 min, grad + 240 min + 24h]; the H2/H3 24h time stop |
| 43 | `mdd_24h_g15` | double | `max_drawdown_24h_after_g15` | largest peak-to-trough fall within the same 24h window, fraction |
| 44 | `mdd_24h_g60` | double | `max_drawdown_24h_after_g60` | largest peak-to-trough fall within the same 24h window, fraction |
| 45 | `mdd_24h_g240` | double | `max_drawdown_24h_after_g240` | largest peak-to-trough fall within the same 24h window, fraction |
| 46 | `c01` | varchar | `s1_mint_auth` | PASS/FAIL/UNKNOWN as-of G240, instruction-history reconstruction (1.5) |
| 47 | `c02` | varchar | `s2_freeze_auth` | PASS/FAIL/UNKNOWN as-of G240 |
| 48 | `c03` | varchar | `s3_ext` | PASS/FAIL/UNKNOWN: Token-2022 extensions (hook, delegate, non-transferable, confidential) |
| 49 | `c04` | integer | `s4_fee_bps` | transfer-fee bps at init; NULL = none; -1 = FLAG/UNKNOWN (post-entry fee instruction, MR-7). INVARIANT: every -1 also appears in u_codes |
| 50 | `c05` | varchar | `s5_lp` | PASS/FAIL/UNKNOWN; P is PASS by construction (LP burned at migrate) |
| 51 | `c06_g15` | double | `s6_top10_pct_g15` | top-10 owners ex-pool, ex-curve, ex-burn, % of chain supply at entry |
| 52 | `c06_g60` | double | `s6_top10_pct_g60` | top-10 owners ex-pool, ex-curve, ex-burn, % of chain supply at entry |
| 53 | `c06_g240` | double | `s6_top10_pct_g240` | top-10 owners ex-pool, ex-curve, ex-burn, % of chain supply at entry |
| 54 | `c07_g15` | double | `s7_linked_pct_g15` | creator + 1-hop +/-24h funded wallets, % of supply at entry |
| 55 | `c07_g60` | double | `s7_linked_pct_g60` | creator + 1-hop +/-24h funded wallets, % of supply at entry |
| 56 | `c07_g240` | double | `s7_linked_pct_g240` | creator + 1-hop +/-24h funded wallets, % of supply at entry |
| 57 | `c07b` | integer | `s7b_feeshare_wallets` | fee-share recipients as-of entry; NULL until S7b is extracted (paid month) |
| 58 | `c08` | varchar | `s8_bundle` | BUNDLE / no / UNKNOWN: >= 5 same-slot buyers sharing one funder |
| 59 | `c11` | varchar | `s11_copycat` | classification only; name/symbol comparison runs outside Dune (quarantine) |
| 60 | `u_codes` | array(varchar) | `unknown_reasons` | every UNKNOWN in the row with its reason code (validation docs) |
| 61 | `c09a` | double | `s9a_vol_per_taker` | 24h volume / distinct takers (feature, MR-8.2) |
| 62 | `c09b` | double | `s9b_roundtrip_share` | share of 24h volume from same-wallet round trips (feature, MR-8.2) |
| 63 | `c10` | boolean | `s10_sellable_30m` | any non-creator, non-migrator sell within 30 min (feature, MR-8.3) |
| 64 | `adj_applied` | boolean | `owner_correction_applied` | false in Dune always; set true locally after the unsaved owner-participation query is applied |
| 65 | `seta_n` | integer | `aligned_set_size` | RAW wallet count: creator + wallets it funded within 24h + c08 bundle wallets, restricted to wallets that ever held the token; bonding curve excluded (MR-12.6). c07b fee-share is out of M0 |
| 66 | `seta_hold_g240` | double | `aligned_holdings_entry` | set balance at G240, raw units |
| 67 | `seta_flow_d0_7` | array(double) | `aligned_net_flow_d0_7` | 8 elements: net token flow per day 0-7, raw units, sells negative |
| 68 | `seta_sf_7d` | double | `aligned_sell_fraction_7d` | cumulative sold / holdings at entry (RUG-A X input; thresholds set in v1.2) |
| 69 | `cluster_n` | integer | `syndicate_cluster_size` | wallets funded by one source within 7d that bought within 30 min post-graduation |
| 70 | `cluster_src` | varchar | `syndicate_common_funder` | the common source, or NULL |
| 71 | `cluster_t1` | timestamp | `syndicate_first_distribution_time` | first block the cluster's net flow turns negative |
| 72 | `cluster_px_t1` | double | `price_at_first_distribution` | price at cluster_t1; marks the H2 exit (sign-off addition 2) |
| 73 | `org1_n_7d` | integer | `organic_v1_takers_7d` | MR-6 definition |
| 74 | `org2_n_7d` | integer | `organic_v2_takers_7d` | MR-7 definition |
| 75 | `org1_t_s` | integer | `t_first_organic_v1_s` | seconds from graduation |
| 76 | `org2_t_s` | integer | `t_first_organic_v2_s` | seconds from graduation |
| 77 | `cb_p50` | double | `cost_bps_buy_p50` | median (gross - net)/gross over buys in (grad, grad + 7d] |
| 78 | `cs_p50` | double | `cost_bps_sell_p50` | same over sells |
| 79 | `cf_p50` | double | `creator_fee_bps_p50` | median creator fee bps |
| 80 | `rs_p90` | double | `residual_bps_p90` | the unnamed leg, never folded (MR-6) |
| 81 | `mk_g15` | double | `markup_g15` | px_g15 / px_grad (MR-8.4) |
| 82 | `mk_g60` | double | `markup_g60` | px_g60 / px_grad (MR-8.4) |
| 83 | `mk_g240` | double | `markup_g240` | px_g240 / px_grad (MR-8.4) |
| 84 | `n_7d` | bigint | `n_swaps_7d` | swaps in (grad, grad + 7d] |
| 85 | `qv_7d` | double | `quote_volume_7d` | quote volume in (grad, grad + 7d] |
| 86 | `seta_lat_s` | double | `fund_to_first_buy_s` | seconds from a set member's first SOL inflow from its classified funder to its first acquisition of the token; per token, the median across set members; NULL when the funder is unknown (MR-12.2) |
| 87 | `blk` | varchar | `block_id` | block for effective-n, an address (MR-15 R2): creators are joined to their funder when that funder links, over the whole evaluation set, and the block is the component, named by its smallest address; a creator with no linking funder is its own block. Assigned locally (actors.assign_blocks), NULL in Dune |
| 88 | `seta_actors` | integer | `aligned_set_actors` | seta_n after funding-mesh collapse; NULL when any member's funding is unknown |
| 89 | `cluster_actors` | integer | `syndicate_cluster_actors` | cluster_n after collapse; NULL when unresolved |
| 90 | `org1_actors_7d` | integer | `organic_v1_actors_7d` | org1_n_7d after collapse; NULL when unresolved |
| 91 | `org2_actors_7d` | integer | `organic_v2_actors_7d` | org2_n_7d after collapse; NULL when unresolved |
