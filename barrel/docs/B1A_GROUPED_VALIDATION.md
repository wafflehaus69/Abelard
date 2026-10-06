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

## The regenerated query, run — MR-15 B2, 2026-10-02

`recon/sql/heavy_b1a_grouped_pbday.sql`, graduations of 2026-09-01, **40.56 credits** against a cap of 70 (the member form cost 59.7 on the same day, when it also carried the fan-out scan). 2,361 group rows and 414 member-funder rows, 2.8 MB, exported to `barrel/data/`. Compared against the 76,999 member rows produced on 2026-10-01 by the earlier query (`recon/compare_b1a_forms.py pbday`, result in `recon/out/b1a_forms_agreement_pbday.json`).

| Check, per token (1,087) | Agree |
|---|---|
| Set size | **1,087** |
| Exactly one creator on each side | 1,087 |
| Creator-funded members | 1,087 |
| Holdings at 15, 60 and 240 minutes | 1,087 each |
| Net flow after entry | 1,087 |
| Supply at 15, 60 and 240 minutes | 1,087 |
| Funders, and members behind each | 755 |
| Latency median | 420 |
| Actors after collapse | 819 |

**Everything the definitions did not change agrees on every token.** The three that differ are the three the ratified definitions were meant to change:

* **Funders.** Of 76,999 members, **342 (0.44%) have a different funder.** 303 had none and now have one; 39 changed from one funder to another; **none lost a funder.** Members with no funder found fell from 422 to 119.
  * The 39: checked against the chain for a sample (`recon/roundtrip_b1a_funders.py`). Of 6 sampled, 4 could be read and **4 of 4 agree with the regenerated query**; in each the difference is which of several senders in one second came last by slot. Two wallets had histories too long to read.
  * The 303: 301 gained **the token's creator**. A query on those pairs (3.2 credits; it names wallets and is not in the repo) shows what the creator sent: **on 295 it is exactly 0.00203928 SOL in the same second as the member's first acquisition.** That is the rent of a token account. The creator created the member's token account in the transaction that delivered the tokens; under the earlier rule a transfer in that second was not "before" the acquisition, and under the ratified rule it is "through the slot of first action". Two others received 0.007 SOL and one 0.012.
* **Latency.** The ruled definition starts the clock at the funder's first transfer; the earlier rows used the last. They differ wherever a funder paid more than once.
* **Actors.** They follow the funders, and R1 now merges a member with the wallets it funds.

### One thing to put in front of the Architect

The slot rule does what it was ratified to do, and its largest effect is one nobody named in advance: **it makes the creator the funder of wallets whose token accounts the creator paid for.** That is a true creator link, and those wallets were already in the set as creator-funded. It is counted as funding only because the 0.001 SOL floor is below token-account rent (0.00204 SOL). A floor above rent would drop it. Under the earlier rule, rent-sized transfers chose the funder for under 1% of members (4% of creators on the post-BOOST day). Left as ratified.

### The latency and actor differences, checked

* **Latency: the new value is never smaller.** On the 755 tokens whose funders are identical in both forms, the median is the same on 272, larger on 384, smaller on none, and unmeasured in both on 99. On 535 single-member tokens with the same funder: equal on 236, larger on 299, smaller on 0. That is what "the clock starts at the funder's first transfer" predicts against "its last". **The earlier member rows therefore cannot validate column 86**; it is validated when a member-form and a grouped-form run of the same text are compared.
* **Actors: 759 agree, 87 differ, 241 not comparable** (their regenerated rows name a funder the earlier rows carry no fan measure for). The 87 are among the tokens whose funders changed.

### After a second review: the text changed once more, and one question is open

A second adversarial review of this work found one more chunk-level date in the query, and it is in the part this run exercised:

* **The funder scan stopped two days after the chunk's last graduation day**, while a member's first acquisition is accepted for nine days after graduation. For a token graduating late in a chunk, SOL received between those dates was never read. In this one-day run the scan stopped on 2026-09-03 for tokens whose members could first acquire until 2026-09-10; in the 2026-09 chunk the same tokens would have been scanned to 2026-09-22 and could have come back with different funders. **Fixed:** the funder scan now reaches eleven days past the chunk's last day and the per-token predicates decide. The ledger scan gains a day for the same reason. Cost: about nine more scanned days on one of the query's two SOL-transfer references.
* **So the text on disk is again not the text that ran.** It needs one one-day run (`RUNBOOK_v1.md` §1).

### The question for the Architect: does token-account rent count as funding?

Dune attributes the rent of a token account to the account's owner, and rent (0.00204 SOL) is above the 0.001 SOL floor. So whoever creates a wallet's token account is, to this query, a sender of SOL to that wallet.

* **In the funder:** the 295 members above. Where the creator delivered tokens and paid the rent in one transaction, the creator is now the member's last sender by construction, ahead of any earlier real funder, and the latency is 0.
* **In membership, under both the earlier and the ratified text:** a wallet is "creator-funded" if the creator sent it 0.001 SOL within 24 hours, rent included. In the June calibration week **100 of 2,317 creator-funded members are in the set on a rent-sized total, and 68 of those 100 hold the token at entry.** On 2026-09-01 it is 301 of 75,874, with 15 holding.

Three ways to rule it, with what each does:

| Option | Membership | Funder |
|---|---|---|
| **(a) Leave as ratified** | rent counts: the creator's distribution wallets are in the set | the creator, when it paid the rent in or before the slot of first acquisition |
| **(b) Exclude a transfer made in the member's first-acquisition transaction from the funder choice only** | unchanged | the earlier real funder, or none; same-slot funding in a separate transaction still counts |
| **(c) Raise the floor above rent (for example 0.0025 SOL) everywhere** | the 100 June-week wallets leave the set, 68 holders among them | rent never chooses a funder |

The builder's recommendation is **(a) or (b), not (c)**: the wallets a creator hands tokens to are exactly what the aligned set is for, and (c) removes them. (b) needs the first-acquisition transaction id carried out of the ledger step and one more predicate; no extra scan. Either way the proving run comes after the ruling, so it is run once.

### What is proven now

The grouped form, the per-token creation, lookback and acquisition windows, the slot cutoff and the tie-break ran on one post-BOOST day and agree with the earlier member rows wherever the definitions are the same. Not proven: the text on disk (funder scan lengthened), column 86 against a member-form run of the same text, and any chunk longer than one day.

## MR-16: rent is not funding — implemented, not run (2026-10-05)

The Architect ruled option (b) with an addition (`RULINGS_2026-10-05.md`):

* **Funder.** A SOL transfer inside the member's own first-acquisition transaction never chooses its funder. The query carries that transaction out of the ledger step (earliest inbound transfer of the token by slot, then by position in the slot) and excludes it in the funder step. Same-slot funding in another transaction still counts.
* **Link type.** A wallet the creator paid within 24 hours of creation stays in the set, and the tie is typed: `token_delivery_by_creator` (the creator's payment is in the wallet's first-acquisition transaction), `sol_funding` (in another transaction), `both`. The type is on every member row and is part of the group key, so set size, holdings and flows come back split by it.

**The re-run was cancelled without a result.** Authorized at ≤ 70 credits. Submitted 2026-10-05 23:52 UTC, cancelled by the watchdog at 72.0 after 243 seconds, billed 73.7. My estimate was 55–59. The earlier text cost 40.6 on the same day; the difference is the funder scan, lengthened from 6 days to 15, and it is the scan joined to every member wallet (77,000 on that day), so its days cost far more than the query's average. The query was still running when it was cancelled; its full cost is not known.

What that means for the build:

* **The text on disk has not run.** By the ruling it is step one of phase A.
* **Its one-day cap is 150, not 70.** A one-day run pays 15 days of funder scan for one day of tokens; a monthly chunk pays 45 days for 30. So the per-chunk projection (about 180 pre-BOOST, 245 for 2026-08) is not obviously wrong, but it was measured on an earlier text and is now a lower bound until phase A measures it.
* **Not yet checked:** whether `tx_id` and `tx_index` are filled on both transfer tables in every era. With the null-safe predicate below, an empty `tx_id` on either table excludes nothing and every creator tie reads `sol_funding`; the link column's distribution in the first run shows it at once (about 300 delivery ties are expected on 2026-09-01). An empty `tx_index` would make the choice of first-acquisition transaction arbitrary among transactions of one slot. Day 0 counts both (`recon/sql/tx_id_nullcount_probe.sql`, three eras, cap 30).

### One question for the Architect, not blocking

**Is a wallet tied by `token_delivery_by_creator` the creator's actor when actors are counted?** The ruling keeps it in the set and says the tie is real and arguably stronger than funding; it does not say what collapse does with it. With rent no longer a funder, a delivered wallet that received no other SOL has no funder, and under the standing rule one unfunded member makes the whole set unresolved. The local code has a switch, `delivery_as_creator`, default off. Off: such a set is unresolved. On: the delivered wallet is counted as the creator. Either reading works from the same exported rows.

## Third review, of the text MR-16 changed (2026-10-06, zero credits)

Two reviewers read `heavy_b1a_grouped_pbday.sql` and its generator, one for "will it run", one for "is it what was ruled"; each finding was then checked by a second reader. No parse, column or type defect. Three findings, all minor. Two are fixed in the generator; all 25 aligned-set files were regenerated and differ from the reviewed text by those two predicates only.

| # | Finding | What was done |
|---|---|---|
| 1 | **The rent predicate dropped a transfer whose `tx_id` is NULL.** `s.tx_id <> m.first_in_tx` is NULL, not true, when `tx_id` is NULL, and a WHERE keeps only true. Had `tx_id` been empty on the SOL table, every member with a first acquisition would have lost its funder, the opposite of what this document said. | **Fixed:** `(m.first_in_tx IS NULL OR s.tx_id IS NULL OR s.tx_id <> m.first_in_tx)`. Identical result when `tx_id` is filled. Only a transfer known to be in the transaction is excluded. |
| 2 | **A creator's transfer to itself made the creator "creator-funded".** The creator-paid branch had no guard against sender = recipient, so the creator's own row came back with `is_funded` true and, since MR-16, a link type; it was counted in the funded count. | **Fixed:** the branch requires recipient <> sender. **Measured on the member rows already held: 13 creator rows of 2,846 tokens** (0 of 236, 7 of 1,523, 6 of 1,087), none at the rent amount. The funded count of those tokens falls by one. Predates MR-16; the link type exposed it. |
| 3 | **The exclusion is by transaction, not by sender or amount.** Any SOL transfer inside the member's first-acquisition transaction is excluded from the funder choice, whoever sent it and however large. The same-funder count of creation-slot traders (`b_only_n`) has no such exclusion. So wallets funded and made to buy in one transaction by a common sender are counted as sharing a funder, pass the bundle cut, and come back with funder unknown, which makes their set unresolved; that sender never reaches the fan-out list. | **Not changed: it is the rule as ruled** ("a transfer inside the member's own first-acquisition transaction"). Recorded as a limit, and measured at A1 for free: bundle-only groups of five or more whose funder is NULL, against the same groups in the rows of 2026-10-02, where the old rule named their funder. Put to the Architect in `RUNBOOK_v1.md` §1. |

Also noted by the reviewers and left as is: the 2026-10-05 run was cancelled before it finished and the generator was edited after it, so no text of this query since 2026-10-02 has completed on Dune. The shape of the final grouping has; the MR-16 additions have not. A1 is the first run.
