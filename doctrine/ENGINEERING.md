# ENGINEERING.md — Cross-Daemon Engineering Doctrine Ledger

Authored by Abelard, ratified by Mando, consumed by engineers. Each entry:
principle, originating incident, operative rule. Entries are appended, never
silently edited; supersessions reference the superseded entry. Daemon-specific
doctrine lives in per-daemon AGENTS.md; only lessons that transfer live here.

**Canonicity (ratified by Mando, 2026-08-08): this monorepo copy is canonical.**
Unlike MEMORY.md and THESES.md — where the Orban workspace copy leads and the
monorepo mirror is stale — ENGINEERING.md has no upstream counterpart and is
authored here. `deploy_doctrine.sh` must not overwrite or delete it from a
workspace that does not carry it.

## E1 — Fail loud, never fake data, never empty success
Founding principle. A function that cannot produce its answer raises or returns
an explicit failure object. No silent defaults, no fabricated rows, no ok:true
wrapping an error string.

## E2 — Scripts-first, LLM-last
Founding principle. Deterministic extraction and computation in scripts; LLM
calls only for judgment. Data pipelines target zero LLM calls.

## E3 — Recon-first; disk is canonical
Incident: News Watch memory-lag (specced toward already-built work, 07-07).
Rule: before any build, verify the premise on disk/live-surface: does it exist,
is the premise right, is the fix in the right place? Briefs and memory are
inference; disk and live curls are canonical.

## E4 — Calibration-first for any new source or tool
Incidents: graphify value-prop refuted by live calibration (06-20);
bloomberg_crypto rejected on content inspection. Rule: live-curl every feed and
content-inspect samples before wiring; vendor claims and HTTP 200s are not
evidence of fitness.

## E5 — G1: unit scale or fail loud (extended: currency)
Origin: Smart Money queries.py G1 gate. Rule: never assume unit scale; resolve
per filer with an explicit basis, return "undetermined" when unresolvable,
report magnitude-warning bounds rather than silently correcting. Extension
(CD-R1): currency is a G1 dimension — same-CIK multi-currency facts (NBIS
RUB+USD) must be discriminated, not merged. Filing-level inline XBRL carries a
scale attribute (observed scale="9") that aggregation APIs do not — the
fallback path inherits a hazard the primary path lacks.

## E6 — Aggregation-layer blindness
Incident: CD-R1 — SEC companyfacts silently drops all dimension-qualified
facts; proven 8.2× understatement of Meta VIE exposure (both facts tagged in
one 10-K, API returned only the undimensioned one; no null, no error).
Rule: a convenience API is not the filing. For any load-bearing figure, know
which layer you are reading and what that layer excludes. Dimensioned,
custom-namespace, and not-yet-ingested facts require filing-level parsing.

## E7 — Plausible-stale-resolution
Incident: CD-R1 roster audit — fixed-preference tag resolution returned AMZN
capex from a tag abandoned in 2017: $7.42B vs true $173.03B, 23× low,
plausible-looking, no error. Rule: concept/tag resolution must be
recency-based per issuer and per era; record the resolved tag alongside every
series row; bound the result against an independent anchor.

## E8 — Measure-before-mandate
Incident: CD-1 spec — Abelard mandated a reconciliation tolerance nobody had
measured; measurement showed the identity does not exist at quarterly
frequency (residuals to −694%) and holds only at TTM (0.73–1.01×).
Rule: no spec constant (tolerance, threshold, cutoff) ships without an
observed distribution behind it. Distribution-before-thresholds applies to
rulings, not just classifiers.

## E9 — Dry-run-and-diff for in-place surgery
Incident: SM 401(k) ticker repair — three near-misses (absence-of-evidence
deletion, over-broad digit-paren rule, heredoc escape byte) caught by
dry-run diff, not by tests. Rule: any in-place data mutation runs as dry-run
with full diff review first; take a backup; ambiguous residue is
reported-not-decided.

## E10 — Identity by durable key; display names resolved at read
Incidents: FISV→FI, MSTR, FB ticker discontinuities (SM); KEEL←Bitfarms and
APLD rename boundary with stale name field (CD-R1). Rule: entity state keys on
the durable identifier (CIK for EDGAR); tickers and names are display
attributes resolved at read time and re-resolved every scan; renames get
identity-discontinuity markers, never hand-edits.

**Citation correction (ClaudeCode, 2026-08-13, per E15 re-check).** The APLD
half of the cited evidence was misread and does not describe a rename. APLD's
`formerNames` carries an entry whose name is *identical* to its current `name`
("Applied Digital Corp.") with an end date of 2026-08-06; no 8-K item 5.03 was
filed and no new name exists. It is an EDGAR identity-record artifact, not a
rename in flight. The rule is unaffected and KEEL←Bitfarms supports it fully.
The APLD case survives as a *different* hazard worth keeping: a `formerNames`
entry equal to the current name will trip naive rename-detection that diffs
`name` against `formerNames`, so change-detection must compare normalized name
*values across scans*, not name against the former-names list.

## E11 — Silent-fallback boundaries are not containment
Incident: ML-1 — local-model inference silently failed over to cloud Opus with
ok:true and relabeled provider; auth isolation did not stop it. Rule: any
property that is load-bearing (model identity, cost, egress) must be enforced
by construction (single-model no-fallback agent shape), never by an override
on a fallback-configured path. Provider-error-in-text with exit 0 is a
failure, not a success.

## E12 — Watermark and dedup discipline
Incident: NW watermark footgun (advance-to-now lost quiet-source items; fixed
72ecd7a). Rule: high-watermarks advance only to the newest ingested item's
timestamp, only on success-with-items; zero-item and failure runs preserve the
watermark. Dedup keys are content-derived, never time-derived.

## E13 — Window alignment
Origin: Full Brief orchestrator doctrine. Rule: the orchestrator aligns all
time-windowed operations to a single canonical timestamp; downstream modules
never compute now() independently. Cross-daemon joins share the canonical
timestamp.

## E14 — Composition disclosure for aggregates
Incidents: SM House/Senate band masking (92.9% aggregate hid an 88.0%
chamber); CD builder bucket spanning two orders of magnitude. Rule: any
published aggregate carries its composition — per-bucket decomposition and
concentration disclosure — always co-presented with the headline number.
Sums and composition; no weighted indices without a separate ruled decision.

## E15 — Negative verdicts expire
Origin: SM source verdicts. Rule: findings of absence or breakage (API lag,
missing coverage, dead source) are dated and carry an expiry/re-check
obligation; they describe a moment, not a permanent state. Positive
structural findings persist until superseded.

## E16 — Admission bugs masquerade as match failures
Incident: SM "69% join" — the matcher was fine; the admission filter accepted
records it shouldn't have. Rule: before tuning a matcher, verify the
population entering it. Decompose every coverage number into
admission vs matching vs genuine absence.

## E17 — Wrong-cadence classification
Incident: CD-R1 — Tesla annual series read as deceleration while the
half-year cut read +113%. Rule: phase/direction classification must run at
the finest reliable cadence; an aggregation window can invert the sign of
the conclusion it feeds.

## E18 — One writer per working tree
Ruled by Mando 2026-08-08; drafted by ClaudeCode from the originating incident
rather than by Abelard — reword to the ledger's voice if wanted.
Incident: CD-R1 — two sessions committed against the same checkout of `main`.
The resulting non-linear history read as three lost commits under a truncated
`git log`, and work crossed a standing disk-review-before-push gate. Nothing
was actually lost; the alarm itself was the cost.
Rule: a working tree has one writer at a time — either serialize sessions on a
shared checkout, or give each concurrent workstream its own `git worktree`.
Corollary: never report branch state from a truncated `git log`. Non-linear
history truncates misleadingly; verify with `merge-base --is-ancestor` (and the
reflog) before declaring commits lost.

**Amendment (Mando, 2026-08-13, ruling R-B6-5).** Serialization is withdrawn:
**a worktree per workstream is the standing mechanism.** Two corrections to the
entry above, both material.

*The deciding incident was one failure, not two.* All seven CD-R1 commits staged
by explicit pathspec; exactly one (`97fa231`) swept foreign content. What
happened twice was E18 *biting* — the truncated-`git log` false alarm, then the
E19 sweep. Staging-by-path failed once. Recorded because a doctrine entry
resting on an inflated count is the failure E8 exists to prevent.

*Pathspec staging is necessary and insufficient.* The sweep happened **through**
an explicit pathspec, because the foreign edit sat inside a file this session
was legitimately editing. No staging discipline can separate two writers inside
one file; only separate trees can.

**Ledger write protocol (resolves the E18/E20 conflict class).** ENGINEERING.md
is cross-workstream by nature, so a worktree-local ledger defeats its purpose
and is the one thing that does not move into the worktree:

  * Ledger entries commit **in the shared checkout only**, as **single-file
    commits**, **atomic**, **one entry per commit**.
  * On numbering collision the *later* writer renumbers their own entry.
  * Entry IDs are **stable once pushed** and are never renumbered afterwards.

This harmonizes with [E20] rather than contradicting it: E20's "commit in the
turn you edit" is exactly what a single-file atomic ledger commit does. The
conflict was only ever that E18 offered serialization as an equal option while
E20 assumed a shared checkout would keep being written to; the protocol removes
the choice for work and makes the ledger a named exception.

## E19 — An inter-judge agreement rate is not a calibration metric
Ruled by Mando 2026-08-13. Rule reworded to the ledger's voice by Mando the
same day; the incident paragraph is as ClaudeCode drafted it.
Incident: SC-1 Phase 3C — scout pre-registered "LLM veto rate over
mechanical-GREEN > 10% = halt" as its rubric-calibration gate. Four consecutive
measurements halted: 45.8% → 39.8% → 26.2% → 23.1%. Every halt traced to corpus
data quality, not rubric miscalibration. Per source the rate was 0–12% on five
of six sources and 45.3% on Questbook alone — a permissionless venue where
anyone can post, supplying 40.8% of the denominator and 80% of the vetoes, with
literal test rows (`Test123`, `wdsa`, `nothing fuck`) that are structurally
complete and semantically empty. The best mechanical proxy for the LLM's veto
(no published settlement evidence) was 51% precise, 21 of 41, and still left
10.1% while yellowing 20 real programs.
Rule: When two judges are built with opposite defaults — one passing on absent
triggers, one suspecting on thin data — their disagreement rate measures the
corpus, not the calibration. Do not gate on it. A veto that is permanent and
downward-only has no error rate to bound. Monitor its firing per-source and
alert on movement: a moving rate means a source degraded or a prompt drifted; a
level means only that a venue is thin.
Corollary: three times running, a gate that will not close because the DATA is
thin is evidence about the corpus; treat the fourth failure as a finding, not as
another patch target.

## E20 — Worktree guardrail: commit in the turn you edit, or take your own tree
Ruled by Mando 2026-08-13; drafted by ClaudeCode from the originating incident
rather than by Abelard — reword to the ledger's voice if wanted. Operationalizes
[E18], which stated the principle; this entry states what to actually do.
Incident: E19 above was drafted into this file and left modified across turns on
a checkout a second session was writing to. At 18:31 that session's commit
captured it — `97fa231` records the APLD citation correction *and* all 26 lines
of E19 under a message naming only the former. Nothing was lost and nothing
leaked (scout_daemon was untracked, so its `.env` was never a staging
candidate), but the ruling became unfindable in the log by its own subject. The
obvious repair, amending the message, was already unavailable: by the time it
was noticed the commit had been pushed, and three further commits sat on top.
Rule: on a shared checkout, a tracked file you have edited is committed in the
same turn you edit it, or you work in your own `git worktree`. Never carry a
modified tracked file across turns while another writer may be live.
Corollary — staging: stage by explicit pathspec. `git add -A` / `git add .`
captures whatever is in the tree, including another session's in-flight edits,
and the resulting commit message will describe only your half of it.
Corollary — the repair window closes at PUSH, not at the next commit. "Safe
because unpushed" is a claim to verify, not assume: check
`git rev-list --count origin/main..HEAD` and `git merge-base --is-ancestor`
BEFORE promising a message can still be fixed. Published history is repaired by
a new commit that references the old one, never by a force-push to a branch a
production host pulls.

## E21 — Temporal migration and concurrent stacking are different problems
Ruled by Mando 2026-08-13; drafted by ClaudeCode from the originating incident
rather than by Abelard — reword to the ledger's voice if wanted. Extends [E7],
which fixed resolution-by-recency and did not anticipate that recency itself
assumes a single line.
Incident: CD-1 B3. Recency-based resolution, adopted to stop
plausible-stale-resolution, elected WULF's `ProceedsFromShortTermDebt` at
$92,750,000 while `ProceedsFromIssuanceOfSecuredDebt` stood at $3,132,938,000
and `ProceedsFromConvertibleDebt` at $975,329,000 in the same year — a 34x
undercount produced by the fix for a 23x undercount. Panel-wide, 6 of 14 CORE
issuers carry a live debt stack.
Rule: before resolving a concept, decide which problem you have. A **temporal
migration** is one line retagged over time — the newest tag is the answer and
the old one owns only its own periods. A **concurrent stack** is several
instruments reported in the same filing — every member is live, selection
undercounts by construction, and recency picks arbitrarily among equals. Capex
migrates; debt stacks. A resolver must distinguish them and must never present
a selection from a stack as a total.
Corollary — collapse before summing, and refuse containment. Two concepts
carrying byte-identical values across all co-reported periods are one instrument
double-tagged: collapse, keep one, record it. Differing values are distinct
instruments: sum. A child persistently at or below a parent in the same periods
may be a subset rather than a sibling, and summing would double-count: refuse,
emit the pair with values, and have it ruled.
Corollary — scope the ambiguity test to the frontier. Disagreement at a
decade-old migration boundary is expected and already resolved by the era map;
only disagreement inside the live window can move today's number. An
unscoped check refuses healthy series (it killed a correct AMZN resolution over
a 2016 handover reading $7.804B against $6.737B).

## E22 — Debounce the judge, and gate the monitor by power
Ruled by Mando 2026-08-15; drafted by ClaudeCode from the originating
measurements — reword to the ledger's voice if wanted. **Ordered as "E21"; that
number was taken by a concurrent session before this landed, so it is E22.**
Extends [E19], which retired the veto rate as a gate but left it alerting
per-source with no noise floor beneath it.

Incident: SC-1. Re-running classification over a 21-minute gap — during which
listings cannot meaningfully change — flipped 4 of 157 mechanical-GREEN rows,
every one with byte-identical stored title, payout and contention. The judge is
stochastic. Reported per-source rates moved accordingly on unchanged data:
opire's veto rate halved, 14.8% → 7.4%, in those 21 minutes, and a 6.7% → 25.0%
jump on superteam that had been raised as a source-degradation signal turned out
to be two rows flipping.

Rule, three parts.

**1. Persona vetoes are unchanged** — permanent, downward-only,
promotion-ineligible. Small, deliberate, semantic. They are exempt from the
debounce below: debouncing a permanent gate would let two lucky scans unlock
something ruled unlockable, so the exemption is correctness, not convenience.

**2. Ordinary vetoes debounce asymmetrically.** A veto takes effect on ONE
observation; recovery to effective-GREEN requires the TWO most recent scans both
clean. Measured at the observed floor P = 2.55%, over 20,000 rows × 400 scans:

    design                standing false-VETO   standing false-GREEN
    no debounce  (1,1)          2.551%                2.5499%
    RULED        (1,2)          5.017%                0.0658%
    symmetric    (2,2)          0.128%                0.1316%
    inverted     (2,1)          0.065%                5.0118%

**Both columns must be quoted together or the rule reads as a mistake.** Against
no debounce, (1,2) makes false vetoes 1.97× MORE common and false GREENs 39×
LESS common. That is the trade, and it follows scout's cost asymmetry:
over-classification costs Mando a review, under-classification costs the tribe
its record. The expensive error is the false GREEN, so the design spends false
vetoes to buy them down. The ~0.06% figure that motivated this rule is real and
belongs to the false-GREEN column; attached to false-VETO — where the true value
is 5.017%, worse than no debounce — it inverts the argument. Implemented as
derived state over an append-only per-(row, scan) verdict table; raw history is
never overwritten, because a record that can be edited is not one.

**3. Monitor alerts are gated by power, tested on FLIP COUNTS.** A per-source
movement alert fires only when the observed flip count beats the floor by a
binomial test at that source's n (α = 0.05, provisional, review at first alert).
Test flips, not rate deltas: a rate delta conflates composition change with
flips, which is exactly how two flipped rows presented as an 18-point jump.
At the measured floor this means n=20 needs ≥3 flips (15 pp), n=27 needs ≥3
(11.1 pp), n=53 needs ≥4 (7.5 pp), n≈150 needs ≥8 (≈5 pp). Under-powered sources
are LISTED as under-powered — never silently suppressed — and roll into the
aggregate monitor, which is the only place n currently supports inference.

**The floor is a trailing estimate, not a constant.** P = 2.55% rests on a
single 21-minute control: 95% Wilson [1.00%, 6.37%], known to within a factor of
six. Every near-in-time scan pair updates it, and the power table above moves
with it. Any rule quoting P quotes the interval too.

Provenance limit: verdict history begins at scan `d697dd6f` (2026-08-11), the
oldest epoch snapshot available at migration. 1,534 observations across three
scans were backfilled from snapshots; 420 rows carry full depth-3 history, 109
depth-2, 56 depth-1. Nothing before 2026-08-11 exists — the four earlier
`.bak-*` files predate the current ledger generation and were deliberately not
backfilled, so any derived state for a row is only as old as its first recorded
verdict, and a depth-1 row has had no opportunity to debounce at all.

**Amendment (DRAFTED by ClaudeCode 2026-08-21, NOT YET RATIFIED — SC-E22R).**
The floor premise above is withdrawn. Part 2's rule survives; the arithmetic
that justified it does not, and the replacement was chosen by replay rather
than derived from a parameter.

*There is no scalar floor.* P = 2.55% rested on one 21-minute control. Nine
further intervals were measured: the flip rate ranges 1.88%–16.98% and is
**overdispersed** — chi-square 36.8 on 8 df, z = 7.20. No single value fits, so
retuning the debounce to 10.58% would repeat the original error at a new number.
Every rate quoted in the entry above should be read as conditional on the
occasion that produced it.

*The mechanism is per-scan, not per-row.* Elapsed time does not predict the flip
rate (Spearman −0.03). The **difference in two scans' aggregate veto-on-GREEN
rate** does: Pearson **+0.887**, Spearman +0.683 over 9 pairs. Two harsh scans
agree (46.2% vs 46.6% → 1.88% flips); two calm scans agree (38.4% vs 35.8% →
2.65%); mismatched scans disagree (20.0% vs 34.0% → 16.98%). Per-scan aggregate
rates ran 20.0%–46.6%, with 4 of 10 scans outside sampling bounds. The judge's
threshold moves between occasions and boundary rows follow it.

*The rule survives the replay; four candidates were tested over 10 judged scans
and 5,173 observations.* Anchors: the four known-noise control flips must not
move the effective state, and RCS — the one confirmed genuine offender — must
stay vetoed.

    rule                    veto rate  churn/row  noise  RCS        lag v/r
    (1,2) THIS ONE             12.45%      0.129    4/4  VETOED     1.00 / 2.00
    majority of trailing 3     10.86%      0.073    3/4  FREED      1.80 / 1.33
    majority of trailing 5     10.32%      0.055    3/4  FREED      1.16 / 1.27
    Wilson LB GREEN>=0.5   23.8%-36.4%  0.85-0.94    4/4  unstable   1.00 / 5.00

Both majority rules free RCS. The Wilson bound frees it in 3 of 8
parameterizations (window 3/5/7/10 x conf 0.90/0.95) — failing the anchor
*inconsistently*, which is worse than failing it cleanly — and never drops below
23.8% veto rate or 0.85 churn. **(1,2) is the only candidate holding RCS while
absorbing all four known-noise flips, and it preserves the ratified asymmetry
exactly.** Stated two-sided per this entry's own rule: it also carries the
highest veto rate and ~2x the churn of the disqualified candidates. It is
re-earned on evidence, not vindicated by its original derivation.

*Recommended, and NOT self-ratified:* keep the per-row rule at (1,2); add a
**scan-calibration layer** above it that measures each scan's aggregate rate
against a trailing baseline and down-weights or flags deviant scans. Excluding
deviant scans dropped overdispersion from z = 7.20 to z = 2.90 — a reduction,
not a cure, on 5 remaining pairs. That is a ruling for Mando and this amendment
does not make it.

*Defect found and fixed while replaying (commit `bcd4d44`).* A scan whose
classify call failed still wrote one verdict row per item carrying
`classes_disagreed = 0`. That zero means "no veto came back", not "the judge
cleared it". Three such scans had contributed **1,609 of 6,782 observations
(23.7%)**, and because recovery needs two consecutive clean scans they pushed
rows toward GREEN — the expensive direction. **16 of 669 rows carried a wrong
effective verdict.** `verdicts.judgeless_scans()` now excludes them, detected
from cost telemetry (`llm_calls = 0 AND items_classified > 0`) because telemetry
is persisted before any opportunity row and stays trustworthy when the rest of
the scan did not. The append-only table was not edited; nothing was deleted.
Any measurement in the entry above taken before 2026-08-21 includes those
spurious clean votes.

*Provenance, per [E27]:* Mando stated 2026-08-21 that E22 had been adjusted. No
adjustment was present on disk — the ledger diff since the entry landed was 189
insertions and zero deletions. This amendment records the SC-E22R findings so
they exist in writing; it does not invent the adjustment, and the rule above is
unchanged until Mando rules.

## E23 — Concept identity is not semantic identity
Ruled by Mando 2026-08-14; drafted by ClaudeCode from the originating incident
rather than by Abelard — reword to the ledger's voice if wanted. Extends [E7]
and [E21], which fixed *which* tag to read and *how many*; this one is about
what the tag turns out to mean. Numbered E23 rather than E22 per the [E18]
ledger protocol: E22 was pushed by another workstream first, and a pushed ID is
never renumbered.
Incident: CD-1 B8. `us-gaap:PaymentsToAcquireMachineryAndEquipment` resolves as
the current capex concept for two universe members and means opposite things in
each. RIOT's 2026Q1 cash flow presents it as "Deposits on equipment"
($16,184,000) — cash advanced for equipment not yet delivered or capitalized —
alongside a separate "Purchases of property and equipment, including
construction in progress" line at $115,465,000 under a different concept. HUT
tags the identical concept to its "Purchases of property and equipment" line at
$616,182,000 and carries no `PaymentsToAcquirePropertyPlantAndEquipment` at all.
Summing RIOT's deposits into capex double-counts when that equipment
capitalizes; reading HUT's as deposits understates its capex ~30x.
Rule: recency resolves which tag is CURRENT; only presentation linkage resolves
what the tag MEANS. Every load-bearing concept is verified once against the
filing's own line label per issuer-era, the verified line-mapping recorded in
the tag map with provenance, and re-verified only on era change. A concept name
is a pointer, not a definition — the definition lives in the statement the
issuer actually rendered.
Corollary: this is invisible to every purely numerical check. Both RIOT figures
are plausible, internally consistent, correctly scaled, and reconcile against
their own YTD totals. Cross-issuer agreement on a concept name is not evidence
that two issuers mean the same thing by it.

**Second citation (ClaudeCode, 2026-08-14) — the financing-statement face.**
Same lesson, cash-flow statement rather than investing section. CD-2 needed to
know whether a debt concept in an issuance sum was a GROSS inflow. Concept names
do not say: `ProceedsFromShortTermDebt` sounds gross and WULF presents it as
"Proceeds from issuance of short-term debt, **net of issuance costs** paid of
$7,250" — net of FEES, still gross of repayments, therefore still eligible.
Whereas `ProceedsFromRepaymentsOf*` concepts net issuance against repayment by
construction and are never eligible, at any issuer. The distinction is invisible
in the concept name and legible only in the rendered line, so the gate that
decides eligibility (`issuance.NET_PRESENTATION_CONCEPTS`) is keyed on verified
line-mappings and records the filing language that put each entry there.
Corollary — presentation verification answers "what does this measure", and
NOT "is this value trustworthy". WULF's actual defect was a different thing
entirely: a year-to-date cumulative that DECREASED ($975,329,000 at 9M, $0 at
FY) because the issuer re-tagged mid-year, which differencing turned into a
phantom -$975M quarter. Presentation was clean throughout. Two independent
gates were needed, and assuming one lesson covered both would have shipped the
bug (see [E21] corollaries).

## E24 — Never sum a parent and its consolidated subsidiary
Ruled by Mando 2026-08-14; drafted by ClaudeCode from the originating incident
rather than by Abelard — reword to the ledger's voice if wanted. Numbered E24
rather than the E23 the order named: E23 was already pushed for a different
lesson, and a pushed ID is never renumbered ([E18] ledger protocol).
Incident: CD-R2 roster recon. Bit Digital (BTBT) was surfaced as a strong
addition on $483M TTM capex — the largest in the miner-pivot cohort. It holds
majority ownership of WhiteFiber (WYFI) and CONSOLIDATES it, and WYFI was
already a ratified panel member. The $483M therefore CONTAINS WYFI's spend.
Admitting both would have counted the same dollars twice in the headline
aggregate, and nothing in the numbers themselves says so: both figures are real,
correctly tagged, internally consistent, and reconcile against their own filings.
Rule: consolidation is containment. An aggregate admits **exactly one entity per
consolidation tree**. Before adding any entity to a population that gets summed,
establish whether it consolidates, or is consolidated by, an entity already in
that population — the ownership question is part of admission, not a later
tidy-up.
Corollary — admit the name, exclude it from the sum. A parent that is genuinely
interesting is not thrown away: it is tracked in a bucket the aggregate does not
read (CD used `sidecar`), so the entity stays visible and the total stays honest.
The same shape covers an entity whose figure is real but not separable from a
larger consolidated line — CD's `host` bucket for IRM, AMT and CCOI, whose
datacenter capex cannot be split out of records-management, tower and telecom
spend respectively.
Corollary — this is invisible to every numerical check. Reconciliation,
magnitude bounds and anchor gates all pass on both entities independently. Only
the ownership relation exposes it, and that relation lives in the filing text,
not in the facts.

## E25 — A commit on a shared branch has no hold; holding is a worktree
Ruled by Mando 2026-08-20; drafted by ClaudeCode from the originating incidents
rather than by Abelard — reword to the ledger's voice if wanted. Closes the gap
[E18] and [E20] left open: E18 protects the working TREE, E20 protects the FILE
and the COMMIT, and neither protects a commit that has been made but
deliberately not pushed.

Incident, twice in one session. (1) `db81871` was committed and held for Mando's
disk review. Minutes later a concurrent session ran `git push` on the shared
branch, which publishes every ancestor, and the commit went to `origin/main`
without its author's involvement. Git records no pusher identity in a plain
clone, so it was not even attributable afterwards — only inferable from
timestamps. (2) Later the same session ran `git switch` on the shared checkout
while another workstream was mid-build on its own branch there. The commits
survived on the branch and nothing was lost, but the working tree silently
reverted and the next command failed on an import of a file that had been
present a minute earlier.

Rule: **"held for review" is not a state a shared branch can express.** Work
that must not publish yet lives on its own branch in its own `git worktree` —
never as unpushed commits on `main`, and never on a branch checked out in a tree
another writer can switch. A commit reachable from a shared branch is
publishable by anyone who pushes that branch, whether or not they know it is
there.

This needs no new mechanism: E18 already ruled worktree-per-workstream as
standing. The gap existed only because the ruling was not practised — measured
2026-08-16, `git worktree list` returned ONE entry across five active
workstreams. Adoption closes the push gate as a side effect, because a push of
`main` cannot carry commits that are not ancestors of `main`.

Corollary — do not describe a commit as held without checking. Publication state
is verified with `git merge-base --is-ancestor <sha> origin/main`, never assumed
and never read from a truncated `git log` (per [E18]). A gate that depends on
every other session choosing not to push is not a gate, and saying "held" when
the mechanism is absent is worse than documenting that there is none.

Operational detail in `HANDOFF_SHARED_CHECKOUT.md` §4-5.

## E26 — Filers file it wrong; the newest filing supersedes
Ruled by Mando 2026-08-21; drafted by ClaudeCode from the originating incident —
reword to the ledger's voice if wanted.

Incident. Building the CD-3 supplier leg, AMD's Data Center revenue series read
$922M for the quarter ending 2024-03-30. AMD reports $2,337M. The parser was
correct and the raw XBRL was checked by hand: in that 10-Q the segment members
are **rotated by one**. $2,337M carries `ClientMember`, $1,368M carries
`GamingMember`, and `DataCenterMember` carries $922M, which is Gaming's figure.
Three of four members are mislabelled in AMD's own instance document. The 10-Q
filed a year later restates the same quarter correctly at $2,337M.

The daemon published the wrong number because its de-duplication preferred the
statement with **fewer dimensions** and had no recency rule at all — a 2.5x
undercount, on the second-largest name in the bucket, presented as a measurement.

Rule, two parts.

**(a) A restatement supersedes.** Where the same fact is filed more than once,
the value from the newest filing wins, unconditionally. Every other preference —
fewer dimensions, cleaner units, closer to the period — is a tiebreak WITHIN one
filing and never across filings. This mirrors the Form-4/F5 precedent already
standing elsewhere in the house: the later document is the correction, and a
correction that loses to a heuristic is not a correction. Supersessions are
counted and published, never applied silently, so a number that changed under a
reader is a number they can see changed.

**(b) Member matching normalises spelling and case.** The same issuer spells the
same member differently by form type: AMD's 10-Q says `DataCenterMember` and its
10-K says `DatacenterMember`. Exact matching silently dropped every Q4 — Q4 is
only derivable from the 10-K — which left AMD with a hole a year wide and no TTM
at all. Match members on a normalised form, and expect the axis to vary too:
NVDA files the same disclosure on `ProductOrServiceAxis`, AMD on
`StatementBusinessSegmentsAxis`.

The generalisation is the point. [E7] says the ISSUER migrates its tags over
time; this says the FILER also simply gets it wrong, and fixes it later. Both
are answered by the same discipline — resolve by recency, per issuer, and never
by a fixed preference order.

Corollary. A figure that matches nothing you know is a signal to check the raw
document, not to adjust the parser. Here the parser was right twice over and the
first two hypotheses — a parser bug, then a wrong memory of AMD's reported
figures — were both wrong. The filing was wrong. That possibility belongs in the
hypothesis set from the start.

## E27 — A ruling not written down did not happen
Ruled by Mando 2026-08-21; drafted by ClaudeCode. Governs the ledger itself and
every artifact that carries a ruling.

Incident. `CD-PH1-VERIFY.md` §9 and §10 listed "Flask vs stdlib" and "retire the
matplotlib pipeline" as **open, awaiting ruling**. Both had already been ruled in
relay. The verify doc — the governing artifact, the thing a reader consults to
learn what is settled — carried a false statement about the state of the project,
and carried it across a merge to `main` and a push to `origin`. A later session
reading it would have re-raised two closed questions, or worse, waited on them.

This is a leak with a direction: rulings are made in conversation and applied in
code, and the conversation is not durable. The artifact is. Nothing in the
workflow forced the two to agree.

Rule: **a ruling is recorded in its governing artifact in the same session it is
applied** — the spec's rulings section, or the verify doc's open-items list,
whichever governs. Applying a ruling to code without recording it is an
incomplete application, not a completed one with paperwork outstanding. Where a
ruling closes an item listed as open, the item is struck in the same edit that
implements it, and the entry says what was ruled rather than being deleted, so
the history of the decision survives.

Corollary — an "Open" section is a claim, and claims get checked. Before
publishing or merging an artifact that carries one, re-read it against what has
actually been ruled. An item that is stale in that list is a defect in the
artifact of exactly the same kind as a wrong number in a table.
