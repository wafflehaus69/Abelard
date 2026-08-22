# BUILD HANDOFF — scout_daemon → real estate, practice acquisitions, advisor recruitment

Issued by: the SC-1 build session (ClaudeCode), 2026-08-21
Target: the engineer building the new scouting leg
Repo: `C:\Users\mdiba\Code\Abelard\` → reference build at `daemons/scout_daemon/`

This document is self-contained. Read §0 and §1 before writing any code; §1 may
change what you are allowed to build. Everything after §2 is architecture.

**Everything here was verified against disk on 2026-08-21.** Where this document
and the code disagree, the code wins — that is doctrine [E3], and it applies to
this document as much as to anything else.

---

## 0. What scout is, in one paragraph

`scout_daemon` is a **pure sensor**. It discovers, classifies, risk-scores,
debounces, and ranks income opportunities, then surfaces them for a human to
admit. It never executes work, never creates an account, never contacts a
counterparty, never submits an application, and never uses a credential against
a target. It reads public surfaces and writes a local SQLite ledger. Every
outward-facing verb is absent **by construction, not by policy** — there is no
code path to them, and tests assert their absence.

Current state: 669 rows from 14 sources, 74 ledger columns, 6,782 verdict
observations across 25 scans, 188 passing tests, $3.07 of lifetime LLM spend,
**1 admitted opportunity.**

That last number is the honest headline. The machine works end to end. It has
not yet earned a dollar, and **0 of 669 payouts are escrowed** — every one is a
counterparty's claim. Do not inherit the assumption that a populated ledger is a
validated one.

---

## 1. READ THIS BEFORE YOU BUILD — the compliance wall

Standing doctrine, `doctrine/USER.md:31`:

> Abelard is a personal agent. Not to touch Ameriprise work, client
> communications, CRM, or firm systems. Ever.

And scout's own invariant 3 (`AGENTS.md:23`): *"Compliance wall absolute.
Nothing touches Ameriprise, clients, CRM, or firm systems. Personal-agent
infrastructure only."*

Mando is an RCSA at Ameriprise Financial. **Two of the three new verticals sit
directly against that wall:**

| vertical | why it is not like crypto bounties |
|---|---|
| **Financial advisor recruitment** | This is his employer's industry and arguably his employer's business line. Recruiting advisors touches FINRA Rule 2273, the Protocol for Broker Recruiting, non-solicitation covenants, and firm policy on outside business activity. It is also inherently **PII about identifiable people**, which scout has never handled. |
| **Practice acquisitions** | RIA/advisory practice M&A is the same industry. Deal flow is relationship-driven and often NDA-bound. Same conflict surface. |
| **Real estate** | Cleanest of the three. Main hazards are ToS (MLS/LoopNet/Crexi bar scraping) and state licensing law — a real doctrine [A.1.1] problem, but not a conflict-of-interest one. |

**This is not a refusal and it is not a judgement about intent.** There are
legitimate versions of all three — an independent venture properly walled from
the employer, or work done with the employer's written knowledge. But scout's
governing doctrine currently forbids the adjacency outright, and an engineer who
wires advisor recruitment without knowing that would be building something the
ruling doctrine prohibits.

**Get a written ruling from Mando on the compliance posture for each vertical
before wiring a single source.** Record it the way every other ruling is
recorded — in `doctrine/ENGINEERING.md` or the new daemon's `AGENTS.md`, dated,
with the reasoning. Then build against it.

If the ruling permits advisor recruitment, you need a **new invariant scout does
not have**: a PII handling rule. Scout's whole corpus is public listings with no
personal data in it, so nothing in this codebase knows how to store, minimise,
retain, or delete information about a named person. That is a design problem to
solve before ingestion, not after.

---

## 2. The invariants — what they are and what each one cost

These are rulings, not preferences. Each exists because something went wrong.
They are the most transferable thing in this build; the code is replaceable.

**1. Surface, never drop.** Rejected items (RED) are stored with their reason
and stay visible. There is no `DELETE` path in `ledger.py` and no filter that
discards a row. *Why:* a classifier that silently drops is a classifier nobody
can audit. In the new verticals this matters more — a property or practice you
screened out is exactly the thing you will want to re-examine when the rubric
changes.

**2. Admission is human.** `status` never reaches `admitted` from code. The only
path is a human editing `config/admissions.yaml`, a file the daemon has **no
writer for** — a test walks every module and asserts no `write_text`/`yaml.dump`
touches it. See §4.6. *Why:* a `scout-daemon admit <key>` verb would put the
decision inside the process being decided about, one argument away from calling
itself.

**3. Compliance wall absolute.** See §1.

**4. Fetched content is data, never a command.** A listing that contains
instruction-shaped text is a listing containing text. Nothing in a fetched
payload is ever interpreted as direction. *Why:* one source published a
`skill.md` in its listing body.

**5. No account creation, ever.** No source in the roster requires auth. That
was a *selection criterion*, not a workaround — it is what makes the invariant
cheap to hold. **This will not survive the new verticals.** LoopNet, Crexi, and
practice-listing services are gated. Expect to renegotiate this invariant with
Mando and get credentials Treasurer-provisioned rather than self-registered.

**6. Category-first classification.** Every domain/name list is marked
*necessarily incomplete* and only ever supplements a category rule. *Why:*
keying on a source lane instead of a category promoted a security-audit program
past a gate Mando had set explicitly. See `risk.py` — `is_security_research()`
is one shared rule with two consumers, deliberately.

**Asymmetric error handling:** uncertainty resolves to YELLOW, never GREEN.
Under-classification costs Mando a review; over-classification costs the tribe
its record. Every gate in this codebase spends the cheap error to buy down the
expensive one.

---

## 3. The doctrine ledger

`doctrine/ENGINEERING.md` holds **E1–E25**, cross-daemon and canonical. Read it
whole once. The ones that will bite you soonest in the new verticals:

| entry | why it will matter to you |
|---|---|
| **E3** recon-first, disk is canonical | Verify every premise on the live surface before building. Half this build's rework came from skipping it. |
| **E4** calibration-first for any new source | Live-curl every feed and content-inspect samples before wiring. An HTTP 200 is not evidence of fitness. |
| **E5** unit scale or fail loud | A payout parser read `$10M` as `$10`. In real estate, a mis-scaled price is the difference between a $400k and a $400M listing. |
| **E8** measure-before-mandate | No threshold ships without an observed distribution behind it. This one saved us repeatedly. |
| **E10** identity by durable key | Keys on the durable id, names resolved at read. **This is your hardest problem** — see §5.3. |
| **E15** negative verdicts expire | "This source has no coverage" is dated and carries a re-check obligation. |
| **E18 / E20 / E25** one writer, worktrees, no holding on a shared branch | Multi-agent expansion makes these load-bearing. See §7. |
| **E19** an inter-judge agreement rate is not a calibration metric | We burned four measurement cycles learning this. |
| **E22** debounce the judge, gate the monitor by power | Classification is **not deterministic**. See §5.4. |

Doctrine A.1.1 (in scout's `AGENTS.md`): **ToS-hostility is a separate, higher
priority gate than legitimacy.** A surface can be perfectly legitimate work and
still be unwireable because *reading* it violates terms. Immunefi is the
reference case; in your verticals, MLS and most practice-listing services will
be. Carry it as data (`tos_class`), not as a wiring decision made once and
forgotten.

---

## 4. Architecture tour — module by module

State lives **outside the repo** at `~/.openclaw/scout/scout.sqlite3`. Follow
that convention (`~/.openclaw/<daemon>/`) — it keeps the DB out of any
cloud-sync tree, which matters because a sync client corrupts SQLite mid-write.

Verdicts below are my reuse recommendation for a new vertical.

### 4.1 `sources/` — 14 adapters, 4 modules · **REPLACE, keep the pattern**
`sources/__init__.py` holds an explicit `ADAPTERS` registry — *not*
import-scanning, deliberately, because that list **is the outer edge of the
containment boundary** and a human should audit it in one screen.

Each adapter is a leaf: **TOTAL over valid inputs**, returns what it extracted,
never raises for a merely-unexpected payload. A missing field yields `None`. The
orchestrator owns failure. That split is why one malformed source degrades to
`error` instead of taking down a 14-source scan.

`sources/base.py` has the shared helpers. Note two payout parsers that look
redundant and are not: `parse_amount` (first number, for clean amount strings)
and `parse_monetary` (**requires a currency anchor**, handles K/M/B, for prose).
Handing prose to `parse_amount` produced a fabricated `$2` payout from a digit
in a program name. Keep both, keep the docstring explaining why.

### 4.2 `models.py::RawItem` — the adapter contract · **REPLACE**
Every adapter fills one `RawItem`. It is **opportunity-shaped**: `payout_usd_low`,
`payout_basis`, `escrow_verified`, `agent_permitted`, `deadline_unix`.

**This is the single biggest thing that does not transfer.** See §5.1.

### 4.3 `classify.py` (889 lines) — **REPLACE the rubric, keep the shape**
Two-stage: a deterministic **mechanical** pass over everything, then **one
batched LLM call**. Scripts-first, LLM-last ([E2]).

What goes to the LLM is worth copying exactly: ambiguous items **and
mechanically-GREEN items**. GREEN is the only verdict where a mechanical miss is
unrecoverable — a novel fraud phrasing the lexicon does not match lands GREEN and
is never reviewed again. Sending GREEN for veto means the LLM can only move an
item **down**, so it strictly tightens and can never loosen. Mechanical RED and
YELLOW are not sent: they are already the safe direction and paying to confirm an
unchangeable verdict is waste.

`resolve()` has **no path from a failure to GREEN**. Preserve that property.

### 4.4 `risk.py` — 0–100 score, gated promotion · **REPLACE weights, keep gates**
The only upward classification path in the daemon, deliberately separate and
separately gated. `PROMOTION_THRESHOLD = 31`, an eligibility **allowlist**
(anything unlisted is ineligible by default — a new yellowing reason cannot
silently acquire a promotion path by omission), and a `_ABSENCE_REASONS` set
distinguishing *rubric judgement* from *data absence*. That distinction is the
subtle part: a risk score computed over **fields** scores a *missing* field as
calm. Measured: a category-unresolved item scored 25 and promoted, and its score
was literally made of the absence.

### 4.5 `ledger.py` (657 lines, 74 columns) — **KEEP the discipline, replace columns**
Schema generated from a `_COLUMNS` dict. `_IMMUTABLE_ON_UPDATE` protects
`status` so re-seeing an item cannot un-admit it. `_add_missing_columns()` is an
additive migration — `CREATE TABLE IF NOT EXISTS` is a no-op on an existing
table, so adding a name to `_COLUMNS` without it leaves live databases broken.

Cost telemetry is persisted **before** any opportunity row, because a disk
failure must not lose the record of money already spent at the API.

### 4.6 `admissions.py` + `config/admissions.yaml` — **COPY NEARLY VERBATIM**
The human gate. Mando-owned file, daemon reads and never writes. Accepts three
key spellings (full id, `source:native_id`, and a **12-char short id** — the
short id exists because zindi native ids run to 124 characters and cannot be
pasted into YAML). Unknown keys are **reported, never swallowed**; a key in both
`admitted` and `dismissed` is **refused, not guessed**; an ambiguous prefix is
refused. `admission_applied_unix` is stamped only by `apply()`, so its presence
is the audit trail that a human moved the row.

This module is the best thing in the build. Take it whole.

### 4.7 `verdicts.py` — append-only history + effective verdict · **COPY**
`opportunity_verdicts` is one row per `(opportunity_id, scan_id)`,
`INSERT OR IGNORE`, never updated. It exists because `opportunities` is
`UPDATE`d in place and every re-scan destroyed the previous verdict.

`derive()` folds history into an effective verdict: veto takes effect on **one**
observation, recovery needs **two consecutive** clean scans. Persona vetoes are
exempt and permanent. Read the module docstring — the two-sided error table must
be quoted whole or the rule reads as a mistake.

### 4.8 `rank.py` + `payout_check.py` — **REPLACE**
Segments never merge. Unrankable rows are reported **unranked-with-reason**,
never sorted to the bottom as though measured and found lowest. `payout_check`
flags and never corrects — scraped fields are claims and stay untouched.

The cross-check rule is worth understanding even though the code won't transfer:
a ratio alone was **necessary and insufficient**. Four listings disagreed by
11×–28× and none was an error — committed-pool vs cumulative-disbursed are
different quantities. The rule only flags when the source's **own payload cannot
account for** the figure.

### 4.9 `orchestrator.py`, `state.py`, `fetch.py`, `surface.py`, `cli.py` · **KEEP**
Window alignment ([E13]): `now_unix` computed **once** and passed down; nothing
downstream calls `time.time()`. Watermarks ([E12]) advance only to the newest
ingested item's timestamp, only on success-with-items. Kill switch: two
independent channels (`SCOUT_HALT` env, `~/.openclaw/scout/HALT` file). Retry
once on classify transport failure — but **not** on `ClassificationError`, because
that means the model answered and asking again buys a second unusable answer at
full price.

`surface.py` enqueues to `abelard_queue` and **never dispatches**. The daemon
writes a durable row and stops; `abelard_queue` is the only component allowed to
send anything outward.

---

## 5. What actually changes for your verticals

### 5.1 The schema is opportunity-shaped; two of your domains are not
A property is an **asset**. A practice is a **firm**. An advisor is a **person**.
Only the first is even loosely "an opportunity with a payout."

Do not force `payout_usd_low` to mean "asking price" and `deadline_unix` to mean
"listing expiry" — that is exactly the [E23] failure (concept identity is not
semantic identity). Design a domain entity per vertical. What generalises is the
*envelope*: durable id, source, first/last seen, class, reason, verdict history,
status, admission stamp. Ship three narrow schemas rather than one wide one.

### 5.2 Ranking has no natural key in two of three
Scout ranks on `payout_usd_low` descending because expected value was
uncomputable — P(award) was derivable on 23% of rows, below the 40% line we
pre-registered. **State your sort key's bias in-code the way `rank.py` does.**
Prize-size ordering over-weights contested items; whatever you choose will have
an equivalent flaw, and it must be written down where the next engineer reads it.

For advisor recruitment there is no payout at all. Do not invent one.

### 5.3 Entity resolution is your hardest problem, and scout dodged it
`identity.py` is 28 lines: `compute_opportunity_id(source, native_id)`. That
works because a listing has a stable id at its source.

A person does not. The same advisor appears on FINRA BrokerCheck, LinkedIn, a
firm bio page, and an industry directory, with name variants and stale firm
affiliations. That is [E10] at a difficulty scout never faced. **Budget real time
for it and do not let it emerge from a dedup hash.** Getting it wrong means
merging two people or splitting one — both are worse than no record.

### 5.4 Classification is not deterministic, and you will not fix that
Measured over 10 judged scans and 6,782 observations: re-running classification
over an **identical batch** flips 2.6%–17% of verdicts, all on byte-identical
inputs.

We chased this for days. The answer, finally: **it is a per-scan calibration
shift, not per-row noise.** Each scan has an aggregate harshness (20.0%–46.6%
veto rate) and the flip rate between two scans tracks the *difference* in their
harshness — Pearson **+0.887**. Elapsed time predicts nothing (Spearman −0.03).

Consequences for you:
- Do not tune a debounce to a scalar "noise floor." There isn't one. ([E22] and
  its pending amendment.)
- Any threshold calibrated on one scan is calibrated to that scan's mood.
- Keep the per-scan aggregate as a **monitor**, and gate alerts by statistical
  power — at n=20 rows you need a 15pp move to beat noise.

### 5.5 ToS and licensing become the primary gate
Scout's roster was selected for unauthenticated readability. Yours cannot be.
MLS data is licensed, LoopNet and Crexi bar scraping, BrokerCheck has terms.
Doctrine A.1.1 was written for one source; for you it is the first question
about every source, and the honest answer will sometimes be "this surface is
unwireable" — record that as a finding, not a problem to engineer around.

---

## 6. Known open defects — inherited, not hidden

1. **Judgeless scans contaminate the effective verdict.** Three scans where the
   LLM never ran wrote ~536 verdicts each, all reading GREEN because no veto came
   back. **1,609 of 6,782 observations (23.7%) are spurious**, and
   `verdicts.effective_all()` does not exclude them — 16 of 669 rows have a
   different effective verdict once they are. Detectable via
   `llm_calls = 0 AND items_classified > 0`. **Fix this before copying
   `verdicts.py`.**
2. **`agent_permitted` is carried and never consumed.** 23 of 100 ranked rows are
   flagged human-only by their own source; `classify.py` and `rank.py` reference
   the field zero times. The eligibility analogue in your verticals (licensing,
   geography, accreditation) must be **gated**, not merely stored.
3. **Rank does not check `deadline_unix`.** 31 of 100 ranked rows had already
   expired.
4. **`counterparty` is unreliable.** 50 of 80 rows from one source have
   `counterparty` identical to `title` — the program name echoed, not a payer.
5. **E22's parameters are frozen pending a ruling** — see §5.4.

---

## 7. Multi-agent expansion

- **One daemon per vertical**, not one daemon with a mode flag. Scout's roster
  list being auditable in one screen is a safety property; three domains in one
  registry destroys it.
- **Shared code goes in `daemons/common/abelard_common/`** — it is an editable
  monorepo install, *not* on PyPI. Do **not** declare `abelard-common` in a
  daemon's `dependencies`: a bare requirement resolves against PyPI and fails.
  Document `pip install -e ../common` instead. See `consensus/pyproject.toml` and
  `abelard_queue/pyproject.toml` for the convention.
- **State home per daemon:** `~/.openclaw/<daemon>/`. Never share a ledger.
- **Upward path is the queue.** `abelard_common.alert_queue` → `abelard_queue`.
  Daemons enqueue; only `abelard_queue` dispatches. **No daemon calls another.**
- **One writer per working tree ([E18], as amended).** Take a `git worktree` per
  workstream. This session lost work to a concurrent session three times before
  adopting it, and [E25] exists because a commit held for review on a shared
  branch was published by someone else's push. `doctrine/ENGINEERING.md` is the
  one named exception that stays in the shared checkout — single-file, atomic,
  one entry per commit.

---

## 8. Running it

```
scout-daemon scan --classify     # fetch + classify (one LLM call, ~$0.25)
scout-daemon scan --mechanical-only   # no LLM, no cost
scout-daemon rank                # order the queue (no fetch, no LLM)
scout-daemon proposals           # what awaits a decision, with pasteable keys
scout-daemon show <key>          # full record
scout-daemon admissions          # apply the Mando-owned file
scout-daemon surface --dry-run   # what would enqueue to Abelard
scout-daemon ledger --class RED  # the excluded set is visible by default
```

Venv is Windows (`.venv/Scripts/python.exe`); run from PowerShell or Git Bash,
not WSL. Tests: `pytest tests/ -q` → 188 passing. A scan clears the rank columns
by design (a scan invalidates the ranking derived from it), so `rank` follows
`scan`.

---

## 9. If you read only one thing

The invariants and the doctrine are the asset. The adapters, the rubric, the
schema, and the ranking key are all domain-specific and you will rewrite them.

What carried this build was a habit rather than a design: **measure before you
mandate, and when the measurement refutes you, say so and change.** Four
separate times a confident explanation was wrong — a "noise floor" that wasn't
one, a batch-composition theory refuted by a cleaner control, a payout ratio rule
that would have demoted four healthy programs, a category-novelty detector that
was really a new-listing detector. Each was caught by measuring the thing before
shipping the rule about it.

Your verticals carry more money, more regulation, and real people. The same habit
matters more, not less.

---

*Written 2026-08-21 against commit `8b9c236`. Section 1 is the part to action
first: get the compliance ruling in writing before wiring advisor recruitment or
practice acquisitions.*
