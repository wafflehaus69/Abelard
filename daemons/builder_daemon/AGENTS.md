# builder_daemon — agent notes

**What it is.** The Tribe's code-PR drafter. Reads work Mando has admitted,
establishes whether the project will accept the contribution and whether the
issue is unclaimed, and emits a provenance packet with a patch attached.

**What it is not.** It never submits, never opens a PR, never comments, never
claims an issue, never authenticates, never holds a credential against a
counterparty. Execution stops at the artifact. Read `SOUL.md` — its seven
invariants are asserted by `tests/test_soul.py`, not merely documented.

**Phase 1 ships the gates and nothing past them.** There is no `draft()` and no
execution verb. A work item that clears both gates returns a packet saying so
and stops. That absence is the safest state to leave the daemon in between
phases: not a disabled path, no path.

## Layout

- `intake.py` — the input contract. Admitted rows, code-PR **by shape**.
- `policy.py` — gate one: CLA / DCO / AI-policy / licence.
- `liveness.py` — gate two: issue open, unassigned, unclaimed.
- `packet.py` — the provenance packet. The patch is its attachment.
- `outcomes.py` — the outcome vocabulary. Declines are first-class.
- `runner.py` — the execution contract: policy, then liveness, then stop.

## The thing most likely to be got wrong later

**There is no `code_pr` category in scout's ledger.** `category` is
source-supplied and, for the bounty sources, holds the repository's programming
language — `Rust`, `TypeScript`, `C++, C, GLSL`. Filtering on a category name
would be the YesWeHack failure verbatim. The type is established by SHAPE: a
single-issue URL on a known forge, per-task payout, agent-eligible segment.

The forge host list can only ever NARROW — an unknown host means "not mine",
never "mine". Adding one is a deliberate act with a test.

## What the gates actually did, measured 2026-09-02

Rehearsed against all 21 code rows on the live queue:

    declined(closed)      9    $5,508
    declined(contested)   8    $7,418
    declined(policy)      3      $865
    unresolved(policy)    1      $100
    cleared both gates    0        $0

Gate one, run against the 19 distinct repositories: 15 pass, 3 decline (all
CLA), 1 unresolved.

## Doctrine that bites here specifically

- **A.1.1 at repository granularity.** ToS-hostility outranks legitimacy. A
  legitimate repo with a real bounty can still be one we must not contribute to.
- **The third gate answer.** PASS / DECLINE / **UNRESOLVED**. A gate that cannot
  establish its fact escalates; it does not guess in either direction.
- **E8, measure before mandate.** Every marker pattern in `policy.py` was
  corrected by running it against live text. The invented-fixture tests passed
  while the gate declined two repositories that welcome the contribution and
  passed one that gates every PR on an unsigned CLA. See
  `tests/test_policy_regressions.py` — every string there is verbatim from a
  real project and every one of them broke the gate.
- **A false DECLINE is the expensive error for gate one.** It refuses real work
  and the refusal looks principled. Prefer UNRESOLVED to a decline you cannot
  quote.
- **Fetch the RAW file, never the rendered page.** godotengine/godot's AI rule
  ("Agents failing to self-disclose will be banned") lives inside an HTML
  comment and is invisible on GitHub's rendered CONTRIBUTING.md.

## Install

```
pip install -e ../common -e .[dev]
```

## State

`~/.openclaw/builder/` — `builder.sqlite3`, `audit.jsonl`, `packets/`.
Scout's ledger is opened `mode=ro` and never written.

Kill switch: `BUILDER_HALT=1`, or `touch ~/.openclaw/builder/HALT`.
