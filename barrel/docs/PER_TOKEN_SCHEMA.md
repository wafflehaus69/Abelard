# BARREL M0 — per-token foundation table: schema for sign-off (burn-down item 2)

**Author:** ClaudeCode · **Date:** 2026-09-30 · **Status:** DRAFT for Architect sign-off. Item 3 materializes this and does not run until it is frozen.
**Grain:** one row per **mint × era**, where era ∈ {pre_boost, post_boost}. A mint belongs to one era by its graduation time, so in practice this is one row per mint (~180,500 for P over the window).
**Replaces:** MR-4.5's per-swap table as the M0 foundation (`PURCHASE_DAY_PREP.md`: 7.94B swap rows don't fit any plan's storage). The frozen 20 + 3 per-swap schema (`DERIVED_TABLE_SCHEMA.md`) survives as the row definition *inside* the queries that build this table. It is never stored beyond a one-day measurement.

---

## 0. Constraints found while drafting (all from Dune's docs, 2026-09-30, 0 credits)

These bind item 3 and the purchase decision, and three of them need a decision from Mando or the Architect.

1. **Analyst storage is 1 GB**, per `api-reference/tables/endpoint/uploads-csv`: *"Storage: 100MB (free), 1GB (analyst), 15GB (plus)"*. The materialized-view doc says *"Materialized views count toward your plan's storage quota"*, so views and uploads share that 1 GB. At an estimated ~100–150 MB this table fits. **A per-swap slice does not fit beyond about one day:** one post-BOOST day of P is 12.6M rows, roughly 0.4–0.8 GB. So on Analyst, per-swap work is never stored. It is computed inside bounded queries whose output is per-token rows. Item 3b is still measured, on the trial's 15 GB, but as a fact about Plus, not a design option for Analyst.
2. **Creating a materialized view needs a Read/Write API key** (`POST /v1/materialized-views`, *"Minimum required API key scope: Read/Write"*). So does creating the saved query it reads from (`POST /v1/query`). **The key in `barrel/private/dune.env` has Read scope. Mando must create a Read/Write key before item 3 can run.** Both endpoints also require *"an Analyst plan or higher"*; the trial's Plus features cover that until 2026-10-06.
3. **A materialized view requires a refresh schedule.** `cron_expression` is mandatory, between 15 minutes and weekly, and *"each refresh consumes credits"*. Every view item 3 creates is set to **weekly with `expires_at` inside the trial**, and is **deleted as soon as its four measurements are recorded**. A forgotten view is a recurring charge.
4. **On Analyst, saved queries are public.** The pricing screen lists *"Private queries and dashboards"* under Plus only. A materialized view is built from a saved query, and its table is readable at `dune.<user>.result_<name>`. Two consequences:
   * **No saved query may contain an owner-wallet address.** The `__NOT_OWNER(col)__` substitution runs only in ad-hoc `/sql/execute` calls, which are never saved. This table therefore carries no owner exclusion. The correction is computed separately: an unsaved ad-hoc query counts owner-wallet participation per token, and the result is applied locally. Column `owner_correction_applied` records whether it has been applied.
   * **The table itself would be public on Analyst.** It is intermediate research data with no wallets of Mando's in it, but it does disclose the strategy's working set. Flagged for Mando, not decided by the builder.
5. **Whether views survive the trial → Analyst change is undocumented.** The hypothesis is that they don't. Item 3 treats every view as disposable, and every measured result goes into the repo the same day, per the ground rules.

---

## 1. Columns

Types are Trino/Dune. `bps` values are integers in basis points; amounts are raw base units; prices are quote lamports per base unit. **G** = the three entry lags {15, 60, 240} minutes after graduation. **H** = the four horizons {1h, 4h, 24h, 7d}.

### 1a. Identity and labels (13)
| # | column | type | definition |
|---|---|---|---|
| 1 | `mint` | varchar | token mint |
| 2 | `era` | varchar | `pre_boost` / `post_boost` by graduation time vs 2026-07-21 |
| 3 | `stratum` | varchar | `P` / `P_alt` / `R` / `N` (MR-6) |
| 4 | `admission_source` | varchar | `complete_to_pool_1d` / `migration_event` / `raydium_v4_migrator` / `cp_init` / `meteora_first_trade` (MR-8.1) |
| 5 | `birth_is_proxy` | boolean | true on Meteora first-trade rows (MR-6) |
| 6 | `degraded` | boolean | graduated or traded inside 2025-08-05 → 08-11 (MR-5.6) |
| 7 | `pool` | varchar | the graduation pool (PumpSwap for P) |
| 8 | `quote_mint` | varchar | from pool creation (authoritative) |
| 9 | `token_program` | varchar | legacy SPL / Token-2022 |
| 10 | `create_time` | timestamp | mint creation (pump `CreateEvent`; chain fallback for N) |
| 11 | `grad_time` | timestamp | graduation (pool birth for N) |
| 12 | `grad_slot` | bigint | |
| 13 | `creator` | varchar | deployer wallet |

### 1b. Prices and reserves (23)
| # | column | definition |
|---|---|---|
| 14 | `virtual_quote_reserves` | per pool; 17.585 SOL post-BOOST, 0 before (1.6 validation) |
| 15–16 | `price_grad`, `quote_res_grad` | first post-graduation event, pre-swap reserves |
| 17–19 | `entry_price_g15/g60/g240` | effective quote reserve ÷ base reserve, last event before grad + G |
| 20–22 | `entry_quote_res_g15/g60/g240` | depth at entry, for the $20 fill and the depth floor |
| 23–25 | `entry_base_res_g15/g60/g240` | |
| 26–29 | `price_1h/4h/24h/7d` | same method, from graduation |
| 30–33 | `quote_res_1h/4h/24h/7d` | reserve decay (A3) |
| 34–36 | `peak_price_after_g15/g60/g240` | max over (grad + G, grad + 7d] |

### 1c. Drawdown (3)
| # | column | definition |
|---|---|---|
| 37–39 | `max_drawdown_after_g15/g60/g240` | largest peak-to-trough fall within (grad + G, grad + 7d], as a fraction |

### 1d. Gate, as-of entry (18)
S1–S5, S8 and S11 are independent of G (set at creation or graduation). S6 and S7 are stored per G because holdings move. S9 and S10 are features, not gates (MR-8.2, MR-8.3).

| # | column | definition |
|---|---|---|
| 40–42 | `s1_mint_auth`, `s2_freeze_auth`, `s3_ext` | `PASS`/`FAIL`/`UNKNOWN`, as-of G240 via instruction-history reconstruction (1.5) |
| 43 | `s4_fee_bps` | transfer-fee bps at init; NULL = none; `-1` = FLAG/UNKNOWN because a fee instruction exists after entry (MR-7) |
| 44 | `s5_lp` | `PASS`/`FAIL`/`UNKNOWN`; P is PASS by construction (LP burned at migrate) |
| 45–47 | `s6_top10_pct_g15/g60/g240` | top-10 owners ex-pool, ex-curve, ex-burn, as % of chain supply at entry |
| 48–50 | `s7_linked_pct_g15/g60/g240` | creator plus 1-hop ±24 h funded wallets, % of supply at entry |
| 51 | `s7b_feeshare_wallets` | count of fee-share recipients as-of entry; NULL until S7b is extracted (paid month) |
| 52 | `s8_bundle` | same-slot buyers ≥ 5 sharing one funder: `BUNDLE`/`no`/`UNKNOWN` |
| 53 | `s11_copycat` | classification only; **name/symbol comparison runs outside this table** (quarantine), so this column holds only the result |
| 54 | `unknown_reasons` | array(varchar): every UNKNOWN in this row, with its reason code from the validation docs |
| 55–57 | `s9a_vol_per_taker`, `s9b_roundtrip_share`, `s10_sellable_30m` | features (MR-8.2, 8.3) |

### 1e. Owner exclusion (1)
| # | column | definition |
|---|---|---|
| 58 | `owner_correction_applied` | false in the Dune table always; set true locally after the unsaved owner-participation query is applied (constraint 4) |

### 1f. RUG-A inputs (4)
| # | column | definition |
|---|---|---|
| 59 | `aligned_set_size` | creator ∪ 1-hop funded ∪ S8 bundle wallets ∪ S7b fee-share (when present) |
| 60 | `aligned_holdings_entry` | their balance at G240, raw units |
| 61 | `aligned_net_flow_d0_7` | array(double), 8 elements: net token flow of the aligned set per day 0–7, raw units, sells negative |
| 62 | `aligned_sell_fraction_7d` | cumulative sold ÷ holdings at entry; the X input for RUG-A (thresholds set in v1.2) |

### 1g. H2 inputs (3)
| # | column | definition |
|---|---|---|
| 63 | `syndicate_cluster_size` | wallets funded by one source within 7 d that bought within 30 min post-graduation |
| 64 | `syndicate_common_funder` | the source, or NULL |
| 65 | `syndicate_first_distribution_time` | first block the cluster's net flow turns negative |

### 1h. Organic activity (4)
| # | column | definition |
|---|---|---|
| 66–67 | `organic_v1_takers_7d`, `organic_v2_takers_7d` | MR-6 / MR-7 definitions |
| 68–69 | `t_first_organic_v1_s`, `t_first_organic_v2_s` | seconds from graduation |

### 1i. Costs (9)
| # | column | definition |
|---|---|---|
| 70–71 | `cost_bps_buy_p50`, `cost_bps_sell_p50` | median (gross − net)/gross over the token's swaps in (grad, grad + 7d] |
| 72 | `creator_fee_bps_p50` | |
| 73 | `residual_bps_p90` | the unnamed leg, never folded (MR-6) |
| 74–76 | `markup_g15/g60/g240` | entry price at G ÷ `price_grad` (MR-8.4) |
| 77–78 | `n_swaps_7d`, `quote_volume_7d` | size context for every other column |

**Count: 78 columns.**

---

## 2. Size, and what item 3a measures
Estimated at 180,500 rows × 78 columns, with three arrays the only wide fields: roughly **100–150 MB stored**, 10–15% of Analyst's 1 GB. Item 3a replaces that estimate with a measured one on one calibration-slice week of P (~2,000–2,500 tokens), with four numbers: credits to build, stored bytes, credits for one full read, credits for one export. The four extrapolate linearly in tokens, apart from the week-to-week swap-volume swing, which is reported alongside them.

## 3. Choices the builder made that the Architect may want to overrule
* **Grain is mint × era**, not mint alone. Identical in practice today; kept because the orders named it.
* **Peak and drawdown run to 7 days**, not to the 24 h time stop that H2/H3 exits use. 7 d is the longer window and the 24 h figure is recomputable from it; 24 h alone would lose the tail.
* **S6/S7 are stored per G; the other checks at G240 only.** S1–S5, S8, S11 do not change between 15 and 240 minutes except in the post-entry cases already routed to UNKNOWN.
* **S11 is computed outside Dune** (name/symbol are quarantined, §1 / A7). Only its classification lands here.
* **No per-swap column anywhere.** FIFO lots and the personal replay (1.8) run against bounded ad-hoc slices in the paid month.
