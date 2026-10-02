# Grouped aligned-set query at one-day scope (MR-14 order 1)

**Date:** 2026-10-01 · **Authorized:** 60 credits · **Spent:** 46.2 for the run, 0.7 for one follow-up check
**Day:** graduations of 2025-06-09, 236 stratum-P tokens in scope. **Query:** `recon/sql/heavy_b1a_grouped_day.sql`, the build form: one row per token, funder and candidate class.
**Compared against:** the member-form rows for the same day produced earlier (461 rows, one per member).
**Evidence:** `recon/out/b1a_forms_agreement_day.json`, `recon/out/run_heavy_b1a_grouped_day_*.json`. Comparison: `recon/compare_b1a_forms.py`. Rows that name wallets are in `barrel/private/out/`.

## Result

| Check, per token | Agree |
|---|---|
| Set size | **236 of 236** |
| Creator present | 236 of 236 |
| Holdings at 15, 60 and 240 minutes | 236 of 236, each |
| Net flow after entry | 236 of 236 |
| Supply at entry | 236 of 236 |
| Funders, and members behind each | **235 of 236** |
| Fan-out per funder | 235 of 236 |
| Actors after collapse | 236 of 236 (after a fix; 231 before it) |

563 group rows came back. 251 are creation-slot traders that are candidates only by their same-funder count and fall outside the set once the cut of five is applied locally (ruling 5). The remaining 312 groups carry the same 461 members as the member form.

## The one funder disagreement is a tie, and it showed a real defect

On one token, one member has two different funders in the two forms. Both sent it SOL in the **same second** (02:05:36 UTC), one slot apart (`…455` and `…456`). Both queries picked "the last sender before the first acquisition" by `block_time`, which is whole seconds, so the choice between them was arbitrary. Checked on Dune for that wallet and hour (0.7 credits; the query names a trader wallet and is not in the repo).

**Fix:** the last sender is now chosen by slot, then by address, so the answer is the same on every run. All chunk files are regenerated. **The fixed text has not been run.** The change is in the step that picks one row per member, not in any scan, so cost is unchanged; it gets its first run as step A1 of phase A.

## A defect in my own local code, found by the comparison

Five tokens disagreed on actors after collapse. The cause was in `actors.token_record_grouped`: the same funder can appear in two group rows (once behind a creator-funded wallet, once behind sixteen bundle-only wallets), and the function counted it as two actors. It now counts distinct linking funders. A test pins the case. After the fix: 236 of 236.

## What this run proves and what it does not

* **Proves:** the grouped form returns the same sets, holdings and funders as the member form on a pre-BOOST day, at 46.2 credits against 44.5 for the member form; the bundle cut applied locally reproduces the cut that used to be in the query; the output is 563 small rows instead of one row per member.
* **Does not prove:** the post-BOOST day, where a few sets have thousands of members and where grouping matters most. That is phase A step A1.
* **Not yet run:** the tie-break by slot.
