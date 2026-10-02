# Grouped aligned-set query at one-day scope (MR-14 order 1)

**Date:** 2026-10-01 · **Authorized:** 60 credits · **Spent:** 46.2 for the run, 0.7 for one follow-up check
**Day:** graduations of 2025-06-09, 236 stratum-P tokens in scope. **Query:** `recon/sql/heavy_b1a_grouped_day.sql`, the build form: one row per token, funder and candidate class.
**Compared against:** the member-form rows for the same day produced earlier (461 rows, one per member).
**Evidence:** `recon/out/b1a_forms_agreement_day.json`, `recon/out/run_heavy_b1a_grouped_day_*.json`. Comparison: `recon/compare_b1a_forms.py`. Rows that name wallets are in `barrel/private/out/`.

## Result

| Check, per token | Agree |
|---|---|
| Set size | **236 of 236** |
| Exactly one creator on each side | 236 of 236 |
| Creator-funded members | 236 of 236 |
| Holdings at 15, 60 and 240 minutes | 236 of 236, each |
| Net flow after entry | 236 of 236 |
| Supply at 15, 60 and 240 minutes | 236 of 236 |
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
* **Shared blind spot:** both forms run the same query up to the last step, so the comparison tests the grouping and the local cut, not the definitions the two forms share. The review below found defects there that this comparison could not.

## After the review: the query on disk is no longer the query that ran

An adversarial review the same day found definitions in the shared part of the query that depended on where a token falls in a chunk. On a one-day run every token falls in the same place, so the run above could not see them.

| Definition | As run | Now |
|---|---|---|
| How far back a funder is looked for | from the chunk's first graduation day minus 4 days: 4 days for a first-day graduate, up to 34 for a last-day one | 4 days before the token's own graduation day |
| Which creations are found | from the chunk's first day minus 3 days | within 3 days before the token's own graduation day |
| "Ever held the token" | anywhere in the ledger scan: 9 to 39 days after graduation | within 9 days after graduation |
| Funding counted | strictly before the second of the first action | up to and including the slot of the first action (funding and acting in one slot is the bundle pattern) |
| Funder ties | arbitrary within a second | by slot, then address |
| Latency for column 86 | one approximate median per group, from which the ruled per-token median could not be computed | every member's seconds returned; the median is taken locally across members |
| S8 UNKNOWN | not expressible: no count of creation-slot traders | count returned |

Still open, because the fix changes what the classifier means: **fan-out is counted over the whole scan**, so it is on a 6-day window in this run, 13 in the week, and up to 36 in a monthly chunk (`RUNBOOK_v1.md` B3).

On a one-day run the first three per-token definitions give the same result as the chunk-wide ones did. The slot cutoff and the tie-break can change which funder a member gets. **The text on disk needs its own one-day run** (`RUNBOOK_v1.md` B2).
