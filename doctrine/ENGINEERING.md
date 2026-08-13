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
