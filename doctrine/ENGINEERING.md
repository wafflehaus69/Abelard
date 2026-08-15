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
