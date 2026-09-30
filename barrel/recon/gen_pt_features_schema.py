"""Generate the frozen pt_features schema (MR-9, 2026-09-30).

Single source of truth for the 85-column per-token table: writes
recon/pt_features_schema.json (the public<->internal name mapping, repo only)
and docs/PT_FEATURES_SCHEMA.md (generated from it). The public names are the
only names that ever reach Dune.

    python barrel/recon/gen_pt_features_schema.py
"""
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
G = ("g15", "g60", "g240")
cols = []


def c(pub, internal, typ, group, d):
    cols.append({"public": pub, "internal": internal, "type": typ, "group": group, "definition": d})


# identity (13)
c("mint", "mint", "varchar", "identity", "token mint")
c("era", "era", "varchar", "identity", "pre_boost / post_boost by graduation time vs 2026-07-21")
c("stratum", "stratum", "varchar", "identity", "P / P_alt / R / N (MR-6)")
c("adm_src", "admission_source", "varchar", "identity", "complete_to_pool_1d / migration_event / raydium_v4_migrator / cp_init / meteora_first_trade (MR-8.1)")
c("birth_proxy", "birth_is_proxy", "boolean", "identity", "true on Meteora first-trade rows (MR-6)")
c("degraded", "degraded", "boolean", "identity", "graduated or traded inside 2025-08-05 -> 08-11 (MR-5.6)")
c("pool", "pool", "varchar", "identity", "graduation pool (PumpSwap for P)")
c("quote_mint", "quote_mint", "varchar", "identity", "from pool creation (authoritative)")
c("token_program", "token_program", "varchar", "identity", "legacy SPL / Token-2022")
c("create_time", "create_time", "timestamp", "identity", "mint creation (pump CreateEvent; chain fallback for N)")
c("grad_time", "grad_time", "timestamp", "identity", "graduation (pool birth for N)")
c("grad_slot", "grad_slot", "bigint", "identity", "graduation slot")
c("creator", "creator", "varchar", "identity", "deployer wallet")
# prices and reserves (23)
c("vqr", "virtual_quote_reserves", "double", "prices", "per pool; 17.585 SOL post-BOOST, 0 before (1.6)")
c("px_grad", "price_grad", "double", "prices", "first post-graduation event, pre-swap reserves")
c("qres_grad", "quote_res_grad", "double", "prices", "quote reserve at graduation")
for g in G:
    c(f"px_{g}", f"entry_price_{g}", "double", "prices", f"effective quote reserve / base reserve, last event before grad + {g[1:]} min")
for g in G:
    c(f"qres_{g}", f"entry_quote_res_{g}", "double", "prices", "quote reserve at entry (depth for the $20 fill and the depth floor)")
for g in G:
    c(f"bres_{g}", f"entry_base_res_{g}", "double", "prices", "base reserve at entry")
for h in ("1h", "4h", "24h", "7d"):
    c(f"px_{h}", f"price_{h}", "double", "prices", f"price at grad + {h}")
for h in ("1h", "4h", "24h", "7d"):
    c(f"qres_{h}", f"quote_res_{h}", "double", "prices", f"quote reserve at grad + {h} (A3 reserve decay)")
for g in G:
    c(f"peak_7d_{g}", f"peak_price_7d_after_{g}", "double", "prices", f"max price over (grad + {g[1:]} min, grad + 7d]")
# path: 7d drawdown (3), 24h peak + drawdown (6, sign-off addition 1)
for g in G:
    c(f"mdd_7d_{g}", f"max_drawdown_7d_after_{g}", "double", "path", f"largest peak-to-trough fall within (grad + {g[1:]} min, grad + 7d], fraction")
for g in G:
    c(f"peak_24h_{g}", f"peak_price_24h_after_{g}", "double", "path", f"max price over (grad + {g[1:]} min, grad + {g[1:]} min + 24h]; the H2/H3 24h time stop")
for g in G:
    c(f"mdd_24h_{g}", f"max_drawdown_24h_after_{g}", "double", "path", "largest peak-to-trough fall within the same 24h window, fraction")
# gate (18)
c("c01", "s1_mint_auth", "varchar", "gate", "PASS/FAIL/UNKNOWN as-of G240, instruction-history reconstruction (1.5)")
c("c02", "s2_freeze_auth", "varchar", "gate", "PASS/FAIL/UNKNOWN as-of G240")
c("c03", "s3_ext", "varchar", "gate", "PASS/FAIL/UNKNOWN: Token-2022 extensions (hook, delegate, non-transferable, confidential)")
c("c04", "s4_fee_bps", "integer", "gate", "transfer-fee bps at init; NULL = none; -1 = FLAG/UNKNOWN (post-entry fee instruction, MR-7). INVARIANT: every -1 also appears in u_codes")
c("c05", "s5_lp", "varchar", "gate", "PASS/FAIL/UNKNOWN; P is PASS by construction (LP burned at migrate)")
for g in G:
    c(f"c06_{g}", f"s6_top10_pct_{g}", "double", "gate", "top-10 owners ex-pool, ex-curve, ex-burn, % of chain supply at entry")
for g in G:
    c(f"c07_{g}", f"s7_linked_pct_{g}", "double", "gate", "creator + 1-hop +/-24h funded wallets, % of supply at entry")
c("c07b", "s7b_feeshare_wallets", "integer", "gate", "fee-share recipients as-of entry; NULL until S7b is extracted (paid month)")
c("c08", "s8_bundle", "varchar", "gate", "BUNDLE / no / UNKNOWN: >= 5 same-slot buyers sharing one funder")
c("c11", "s11_copycat", "varchar", "gate", "classification only; name/symbol comparison runs outside Dune (quarantine)")
c("u_codes", "unknown_reasons", "array(varchar)", "gate", "every UNKNOWN in the row with its reason code (validation docs)")
c("c09a", "s9a_vol_per_taker", "double", "feature", "24h volume / distinct takers (feature, MR-8.2)")
c("c09b", "s9b_roundtrip_share", "double", "feature", "share of 24h volume from same-wallet round trips (feature, MR-8.2)")
c("c10", "s10_sellable_30m", "boolean", "feature", "any non-creator, non-migrator sell within 30 min (feature, MR-8.3)")
# control (1)
c("adj_applied", "owner_correction_applied", "boolean", "control", "false in Dune always; set true locally after the unsaved owner-participation query is applied")
# RUG-A inputs (4)
c("seta_n", "aligned_set_size", "integer", "rug_a", "creator + 1-hop funded + c08 bundle wallets + c07b fee-share (when present)")
c("seta_hold_g240", "aligned_holdings_entry", "double", "rug_a", "set balance at G240, raw units")
c("seta_flow_d0_7", "aligned_net_flow_d0_7", "array(double)", "rug_a", "8 elements: net token flow per day 0-7, raw units, sells negative")
c("seta_sf_7d", "aligned_sell_fraction_7d", "double", "rug_a", "cumulative sold / holdings at entry (RUG-A X input; thresholds set in v1.2)")
# H2 inputs (4, incl. sign-off addition 2)
c("cluster_n", "syndicate_cluster_size", "integer", "h2", "wallets funded by one source within 7d that bought within 30 min post-graduation")
c("cluster_src", "syndicate_common_funder", "varchar", "h2", "the common source, or NULL")
c("cluster_t1", "syndicate_first_distribution_time", "timestamp", "h2", "first block the cluster's net flow turns negative")
c("cluster_px_t1", "price_at_first_distribution", "double", "h2", "price at cluster_t1; marks the H2 exit (sign-off addition 2)")
# organic activity (4)
c("org1_n_7d", "organic_v1_takers_7d", "integer", "activity", "MR-6 definition")
c("org2_n_7d", "organic_v2_takers_7d", "integer", "activity", "MR-7 definition")
c("org1_t_s", "t_first_organic_v1_s", "integer", "activity", "seconds from graduation")
c("org2_t_s", "t_first_organic_v2_s", "integer", "activity", "seconds from graduation")
# costs (9)
c("cb_p50", "cost_bps_buy_p50", "double", "cost", "median (gross - net)/gross over buys in (grad, grad + 7d]")
c("cs_p50", "cost_bps_sell_p50", "double", "cost", "same over sells")
c("cf_p50", "creator_fee_bps_p50", "double", "cost", "median creator fee bps")
c("rs_p90", "residual_bps_p90", "double", "cost", "the unnamed leg, never folded (MR-6)")
for g in G:
    c(f"mk_{g}", f"markup_{g}", "double", "cost", f"px_{g} / px_grad (MR-8.4)")
c("n_7d", "n_swaps_7d", "bigint", "cost", "swaps in (grad, grad + 7d]")
c("qv_7d", "quote_volume_7d", "double", "cost", "quote volume in (grad, grad + 7d]")

assert len(cols) == 85, len(cols)
assert len({x["public"] for x in cols}) == 85, "duplicate public name"
for i, x in enumerate(cols, 1):
    x["n"] = i

RULES = [
    "Public names only in any saved query or Dune table; this mapping never goes to Dune.",
    "Verdicts, thresholds and hypothesis logic are never in a saved query; ad-hoc /sql/execute only.",
    "Owner-wallet addresses never in a saved query.",
    "Every c04 = -1 also appears in u_codes.",
]
spec = {"table": "result_pt_features", "frozen": "2026-09-30", "ruling": "docs/RULINGS_2026-09-30.md",
        "rules": RULES, "columns": cols}
(ROOT / "recon" / "pt_features_schema.json").write_text(json.dumps(spec, indent=1), encoding="utf-8")

md = [
    "# `pt_features` — frozen per-token schema, 85 columns (2026-09-30)", "",
    "**Frozen by:** `RULINGS_2026-09-30.md` (Architect sign-off with three changes). "
    "**Source of truth:** `recon/pt_features_schema.json`, written with this page by `recon/gen_pt_features_schema.py`. "
    "The **public name** is the only name that exists in Dune; internal names and definitions live only in this repo.", "",
    "Dune table: `dune.<user>.result_pt_features` (view names must start with `result_`). "
    "The prior draft, `PER_TOKEN_SCHEMA.md`, is superseded and kept for its §0 constraints.", "",
    "## Rules", "", *[f"* {r}" for r in RULES], "",
    "## Sign-off changes applied", "",
    "1. **24 h peak and drawdown per G** added (`peak_24h_*`, `mdd_24h_*`). The draft's claim that the 24 h figure is "
    "recomputable from the 7 d one was wrong: a 7-day maximum cannot give the 24-hour maximum.",
    "2. **Price at first distribution** added (`cluster_px_t1`), to mark the H2 exit.",
    "3. **Neutral names.** Beyond the three ordered renames, the builder also neutralised names that would narrate the "
    "method, by the same logic: `owner_correction_applied` → `adj_applied` (would disclose that an operator's own "
    "wallets are excluded), `unknown_reasons` → `u_codes`, `syndicate_first_distribution_time` → `cluster_t1`, "
    "`price_at_first_distribution` → `cluster_px_t1`. Flagged as builder choices under the neutrality order.", "",
    "| # | public name | type | internal name | definition |", "|---|---|---|---|---|",
]
md += [f"| {x['n']} | `{x['public']}` | {x['type']} | `{x['internal']}` | {x['definition']} |" for x in cols]
(ROOT / "docs" / "PT_FEATURES_SCHEMA.md").write_text("\n".join(md) + "\n", encoding="utf-8")
print(f"frozen: {len(cols)} columns; mapping and page written")
