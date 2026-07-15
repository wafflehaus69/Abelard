# abelard_queue — Abelard's alert-queue consumer

GATE 2 (2026-07-14) architecture: **daemons never dispatch externally.**
They enqueue structured JSON alerts into the abelard_queue SQLite DB via
the shared primitive `abelard_common.alert_queue` (enqueue is the commit
point; idempotent on `dedupe_key`). This package is the OTHER side —
Abelard's, and only Abelard's:

- **Triage** (cheap-first materiality): explicit rules
  (cooldown-suppress > synthesis-brief-push > convergence-push), then
  Haiku (`claude-haiku-4-5`) for what rules can't decide. Undecidable
  items stay `pending` and exit non-zero — never silently pushed or
  dropped. Every verdict is journaled (`decision_journal`) in the same
  transaction, the calibration record.
- **Dispatch**: HTTPS Telegram Bot API `sendMessage`. Fail-loud on
  non-200/`ok:false` (item stays un-dispatched, error recorded, exit 2).
  Claim-before-send makes double-push impossible; crash-window
  ("unconfirmed") items are surfaced and only ever retried via the
  manual `reset-claim` after human verification.

Extracted out of `abelard_common` per Mando's Ruling 1 at the GATE 2
diff review — common stays a pure logic library (it keeps the queue
*schema*; daemons import only that).

## Install

`abelard-common` is a monorepo editable install (not on PyPI) — install
it first:

```bash
python3 -m venv .venv
.venv/bin/pip install -e ../daemons/common -e '.[dev]'
```

(`scripts/setup.sh` does not yet provision this package — it iterates
`daemons/` only. Flagged at the GATE 2 build; wire it there or in the
Phase 5 launchd bring-up.)

## Commands

```bash
abelard-queue status                 # counts + unconfirmed items (exit 2 if any)
abelard-queue triage [--no-haiku] [--cooldown-s N]
abelard-queue dispatch
abelard-queue run [--no-haiku]      # triage then dispatch
abelard-queue journal [--limit N]
abelard-queue reset-claim <id>      # MANUAL crash-window recovery
```

## Environment

| Var | Purpose |
|---|---|
| `ABELARD_QUEUE_DB_PATH` | queue DB (default `~/.openclaw/abelard_queue/queue.db`) |
| `TELEGRAM_BOT_TOKEN` | Bot API token — never logged; all error text token-redacted |
| `TELEGRAM_ALERT_CHAT_ID` | dispatch destination chat |
| `ANTHROPIC_API_KEY` | Haiku triage tier (optional; without it undecided items stay pending) |

## Tests

```bash
cd abelard_queue && <python-with-pytest+requests+abelard_common> -m pytest
```

`pythonpath = ["."]` in pyproject means the package needs no install to
test — the monorepo `daemons/common/.venv` python works.
