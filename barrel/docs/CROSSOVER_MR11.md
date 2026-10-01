# CONSENSUS crossover — builder's response to MR-11 (2026-10-01)

**Orders:** `RULINGS_2026-10-01_CROSSOVER.md` §4, orders 1–4 (BARREL).
**Status:** orders 1–4 executed and measured on the calibration week, inside burn-down order 4's budget. **Three points need the Architect's signature before they are final** (§6). Orders 5–7 are CONSENSUS work and were not touched in this session (§7).
**Code:** `recon/actors.py`, `tests/test_actors.py` (12 tests passing, run under the CONSENSUS environment), `recon/gen_heavy_b1.py`, `recon/gen_heavy_b2_b3.py`. Costs: `HEAVY_TIER_UNITS.md`.

---

## 1. What was ported, and how

| Order | Done |
|---|---|
| **3. `resolution.py` as the chokepoint** | `actors.py` loads CONSENSUS's file from `consensus/consensus/resolution.py`. It is not copied; a test asserts the path. BARREL set records carry its key (`actor_count_post_collapse`) and are read through `actor_count`, `collapse_state` and `fmt`. A set with one unfunded member reads `UNRESOLVED`, never a number, and adds `SETA_FUNDING_UNKNOWN` to `u_codes`. |
| **1. `collapse_actors`** | Ported verbatim. It cannot be imported, because `m10` loads the CONSENSUS data layer on import. A parity test runs both on the same cases whenever `m10` is importable, and passes. |
| **1. m5 funder classification** | Re-derived, not copied, because the discriminant does not transfer (§2). Pure function, no default threshold. |
| **1. `fund_to_first_buy_s`** | Schema column 86, public name `seta_lat_s`. Measured per member in the query; the per-token value is the median over members where both ends were measured. |
| **2. `block_id`** | Schema column 87, public name `blk`. `F:<funder>` when the creator's funder is dedicated, otherwise `D:<launch day>`. |
| **4. Cost measured with the heavy tier** | Yes. The funder, fan-out and latency columns ride on query B1a (76.7 credits for the week). They add one SOL-transfer reference to it; they are not a separate scan of their own. |

Classification and collapse run locally on fetched member records. No threshold, kind or verdict is in any query.

## 2. The fan-out discriminant does not carry over from Polygon

m5 calls a funder an exchange when it has paid 400 or more distinct recipients. That was calibrated on Polygon USDC. On Solana SOL transfers, calibration week, 12 days, transfers of 0.001 SOL or more (`fanout_calibration_week.sql`, 11.5 credits), against Dune's 166 labelled exchange wallets:

| Fan-out | Senders | Of which labelled exchange |
|---|---|---|
| 1–15 | 22.5 million | **16** |
| 16–127 | 537,000 | 0 |
| 128–1,023 | 122,000 | 1 |
| 1,024–8,191 | 13,200 | 2 |
| 8,192 and above | 2,100 | **11** |

Two things follow. **Half the labelled exchange wallets that sent anything paid 15 or fewer recipients**, so a fan-out floor misses them; they have to be matched by label. **And about 15,000 unlabelled senders paid more than 1,000 recipients.** Those are not all exchanges. A wallet factory that funds 5,000 snipers looks identical by fan-out, and it is exactly the funder that *should* link its wallets.

So the Solana classifier has five kinds, and only one links:

| Kind | Rule | Links wallets |
|---|---|---|
| `nonpersonal` | known program or infrastructure address | no |
| `cex` | labelled exchange wallet | no |
| `high_fanout` | unlabelled, fan-out at or above the threshold: exchange or factory, not known which | **no** |
| `dedicated` | unlabelled, fan-out below the threshold | **yes** |
| `unknown` | fan-out not measured | no |

This keeps m10's rule that a doubtful funder never links. The cost is stated plainly: **a large wallet factory is not collapsed**, so actors are over-counted for exactly the biggest operations. That is the safe direction for a screen, and the wrong one for anyone reading the count as a census.

## 3. What collapse does on the calibration week (1,523 tokens)

Aligned set, members that ever held the token (B1a, 4,411 member rows):

| Threshold | Funder kinds (member rows) | Multi-wallet sets resolved | Collapsed | Wallets → actors |
|---|---|---|---|---|
| 32 | dedicated 1,344 · high_fanout 1,110 · cex 306 · none found 1,651 | 291 of 414 | 41 | 1,426 → 943 |
| 400 | dedicated 1,842 · high_fanout 612 · cex 306 · none found 1,651 | 291 of 414 | 55 | 1,426 → 737 |
| 8,192 | dedicated 2,113 · high_fanout 341 · cex 306 · none found 1,651 | 291 of 414 | 76 | 1,426 → 668 |

Set states at 400: 1,024 solo, 236 independent, 55 collapsed, **208 unresolved** (14%). The larger effect on the set was not collapse but membership (§1 of `HEAVY_TIER_UNITS.md`): the readiness definition counted a median of 28 wallets per token, the corrected one a median of 1.

**Early-buyer groups (B3, the H2 cluster columns), 1,573 tokens:** this is where funder kind matters most.

| | Median | 90th pct | Max |
|---|---|---|---|
| Buyers in the first 30 minutes | 966 | 2,260 | 10,891 |
| Largest group sharing a funder, **any funder** (the schema's current `cluster_n`) | **142** | 577 | 8,602 |
| Largest group, dedicated funders only, threshold 32 | 3 | 5 | 25 |
| … threshold 400 | 5 | 21 | 121 |
| … threshold 8,192 | 9 | 27 | 2,946 |

On **1,529 of 1,573 tokens the largest group's funder has a fan-out above 8,192**; on 64 it is a labelled exchange. `cluster_n` as specified is, on almost every token, the number of early buyers who withdrew from the same exchange or hub. It measures nothing about coordination. With funder kind applied it falls by one to two orders of magnitude and depends heavily on the threshold, which is therefore a decision that has to be made on the calibration slice before H2 is read at all.

**Funding latency.** Measured for 2,683 members: median 886 s; 199 within 10 s; 556 within 60 s; 1,344 within 15 minutes. Per token (median over members), measured on 1,362 of 1,523: median 2,298 s, 10th percentile 130 s.

## 4. Blocks (order 2)

| Threshold | Blocks | By funder | By launch day | Tokens in funder blocks | Largest funder block |
|---|---|---|---|---|---|
| 32 | 513 | 504 | 9 | 648 | 12 |
| 400 | 632 | 623 | 9 | 797 | 12 |
| 8,192 | 711 | 702 | 9 | 910 | 12 |

1,523 tokens are 513 to 711 blocks. **The fallback does most of the work**, and it is coarse: every token whose creator's funder is an exchange, a hub or not found falls into one of nine launch-day blocks, the largest holding 108 to 162 tokens. So "block n" is low mainly because launch-day blocks are few, not because funders repeat: the largest funder block is 12 tokens. Over the full window the same rule gives at most about 610 launch-day blocks plus the funder blocks. Whether a day is the right fallback unit, or too coarse, is a question for the Architect (§6).

`actors.block_id` implements the rule as written. Every H1–H4 cell will report raw n and block n; the UNDERPOWERED rule reads block n. That is recorded in `M0_TASKING.md` MR-11 as binding.

## 5. What was not ported

* **m5's one-directional rule** (an address the wallet also sent to is a counterparty, not a funder). It needs each member's outbound transfers, one more reference to the SOL-transfer table. Not measured. On Solana its job is partly done by restricting members to wallets that held the token and by excluding the bonding curve.
* **Funding beyond the scanned window.** A funder is looked for only in the 4 days before the first graduation day scanned. 1,651 of 4,411 members had none in that window; 1,391 of those are one token's set, a creator that funded 1,391 wallets, none of which had another funder in the window. Creators: 1,401 of 1,523 found.
* **Collapse of c06 (top-10 holders) and organic-v2 taker counts.** The memo lists both as destinations. Neither is collapsed here. Both need a funder for every holder or taker, a much larger join than the aligned set (median 142 holders, 966 early buyers per token). Costed as an open item, not done.

## 6. For the Architect's signature

1. **Column 86, the definition line.** Proposed: *median seconds from a set member's funding to its first acquisition of the token, over members where both were measured; NULL when measured for nobody.* "First acquisition" is the first inbound token transfer, which includes a transfer in, not only a buy. The memo says "on the seta/cluster sets"; this covers the aligned set only. A second column for the early-buyer groups would be 88.
2. **Count columns.** The memo says every wallet-count column is inflated until collapsed, and orders two new columns. It does not say whether `seta_n` and `cluster_n` become actor counts or keep the raw count beside a new one. `resolution.py` treats the two as different facts, so the builder's proposal is **raw and collapsed side by side** (`seta_n` + `seta_actors`, `cluster_n` + `cluster_actors`), which takes the schema to 89 or 90. Not applied; the schema stands at 87.
3. **The fan-out threshold and the launch-day fallback.** Tables in §3 and §4 show both at 32, 400 and 8,192. No value is chosen. The week is inside the calibration slice, so choosing here does not touch the evaluation data.

Also flagged: `blk` puts a funder address into a table that is public under the in-warehouse design. Creator addresses are already there. A hash would remove it at no cost if wanted.

## 7. Orders 5–7 (CONSENSUS)

Not started. They are a different workstream with its own worktree, and D3's recalibration needs the 59 nights of store data on Basilic. Q1, Q7 and Q8 are answers and need no code. D1, D2, D7 and the dashboard restatement (D4, D5) are fixes to `consensus/`; D3 is a report followed by a ruling. A separate session has been proposed for them.

## 8. After MR-12 (rulings of 2026-10-01, second set)

The three open points in §6 are ruled (`RULINGS_2026-10-01_B.md`). What changed in code and what the rulings produce on the calibration week:

* **Classifier is label-first** (`actors.classify_funder`): labelled exchange → `cex` whatever its fan-out; unlabelled at or above 400 → `hub`, never links; unlabelled below 400 → `dedicated`, links. 400 is provisional; 32 and 8,192 are the sensitivity values in §3 and below. A `factory` class is in the code and links, with an empty list until v1.2 (`FACTORY_CLASS.md`: the two populations do separate).
* **Blocks, creator-wallet fallback (ruling 5).** Launch-day blocks are gone; each creator whose funder does not link is its own block.

| | Tokens | Blocks at 400 | at 32 | at 8,192 | Largest block |
|---|---|---|---|---|---|
| Calibration week | 1,523 | **1,287** (0.845 of raw n) | 1,299 | 1,262 | 12 tokens (a funder) |
| Post-BOOST day 2026-09-01 | 1,087 | **871** (0.801) | 885 | 869 | 16 tokens (a funder) |

  At 400, 623 blocks are funders holding 797 tokens and 664 are single creators holding 726 (a creator with several tokens is one block; the largest holds 8). **Block n is about 80–85% of raw n**, and the threshold barely moves it.
* **Column 86 is redefined (ruling 2):** the clock starts at the member's *first* inflow from its funder, not the last one before the acquisition. `gen_heavy_b1.py` is changed accordingly. **The latency figures in §3 were measured under the earlier definition (last inflow) and are a lower bound on the ruled one; they have not been re-measured.**
* **Count columns carry both values (ruling 3).** Schema is 91 columns: `seta_actors`, `cluster_actors`, `org1_actors_7d`, `org2_actors_7d` beside their raw counts. `c07b` gets its twin when it is built. **Cost consequence, not yet measured:** collapsing the organic taker counts needs a funder for every taker over seven days (a median of about 1,400 wallets per token). That is a join of the size of the early-buyer query or larger, and it is priced in `PURCHASE_DECISION.md` as an estimate, not a measurement.
* **Aligned-set definition (ruling 6):** the corrected one stands. `VALIDATION_1_5_S7_S8.md`'s "118" and "1,128 funded wallets" are struck as set sizes.
